# Databricks notebook source
# MAGIC %run "../mount_points_paramerters/mountpoints"

# COMMAND ----------

url=dbutils.widgets.get('url')
scrap_url_path =dbutils.widgets.get('scrap_url_path')
patterns =dbutils.widgets.get('patterns')

# COMMAND ----------

patterns = patterns.split(',')

# COMMAND ----------

import concurrent.futures
thread = concurrent.futures.ThreadPoolExecutor(max_workers=10)

# COMMAND ----------

headers = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}


# COMMAND ----------

def parse_url(seed_url):
    temp=urlparse(seed_url)
    base_url=(temp.scheme+'://'+temp.netloc)
    return base_url

# COMMAND ----------

def scrap_sub_url(url,patterns):
    response = requests.get(url,headers=headers)
    if response.status_code == 200:
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        regex_patterns = [re.compile(pattern) for pattern in patterns]
        if len(regex_patterns)>0:
            url_links = [a['href'] for a in soup.find_all('a') if 'href' in a.attrs and any(pattern.search(a['href']) for pattern in regex_patterns)]
        else:
            url_links = [a['href'] for a in soup.find_all('a') if 'href' in a.attrs]
        url_links=[urllib.parse.quote(url, safe=':/') for url in url_links]
        return url_links
    else:
        print(f"Failed to retrieve the content. Status code: {response.status_code}")

# COMMAND ----------

def scrap_url(url,container_client=container_client,patterns=patterns):
        url_links=scrap_sub_url(url,'')
        
        regex_patterns = [re.compile(pattern) for pattern in patterns]
        concat_urls=[]
        for sub_url in url_links:
            if len(sub_url.split('/'))<=1 and any(pattern.search(sub_url) for pattern in regex_patterns):
                sub_url='/'.join(url.split('/')[:-1])+'/'+sub_url
                concat_urls.append(sub_url)
                
            elif sub_url.startswith('https://') and sub_url.endswith('/')  :
                sub_url=scrap_sub_url(sub_url,patterns)
                if sub_url:
                    concat_urls.extend(sub_url)
            else:
                if any(pattern.search(sub_url) for pattern in regex_patterns):
                    concat_urls.append(sub_url)
        if len(concat_urls)>0:
            url_list=concat_urls
        if len(url_list) > 0 :
            base_url = parse_url(url)
            pandas_df = pd.DataFrame(url_list,columns=["URL"])
            pandas_df['URL'] = pandas_df['URL'].apply(lambda x: base_url + x if not x.startswith('https://') else x)
            file_path = bronze_meta_info+base_url.replace('https://','') + '/' \
                + url.replace('https://','').replace('/','_').replace('.','_')+'.csv'
            blob_client = container_client.get_blob_client(file_path)
            pandas_df=pandas_df.drop_duplicates()
            csv_file = pandas_df.to_csv(index=False)
            blob_client.upload_blob(csv_file,overwrite=True)
            print(file_path,"uploaded Successfully")

# COMMAND ----------

error_list = []
try:
    # Allow SPARK to access from Blob remotely 
    if not url :

        input_txt_file_df = spark.read.csv(bronze_ingestion_meta+scrap_url_path,header=True)
        input_txt_file_df = input_txt_file_df.select(*[regexp_replace(col_name, ' ','%20').alias(col_name) for col_name in input_txt_file_df.columns])
        url_df = input_txt_file_df.select("URL")
        url_list = url_df.rdd.map(lambda row: row[0]).collect()
        if len(url_list) !=0:
            thread.map(scrap_url,url_list)
    else:
        scrap_url(url)
except Exception as e:
    error_list.append(str(e))
    print('error:',str(e))

# COMMAND ----------

error_df=pd.DataFrame(error_list)
if not error_df.empty :
    if url :
        scrap_url_path = parse_url(url).replace('https://','')
    error_records=bronze_meta_info.split('/')[0]
    bolb_path= scrap_url_path.replace('/','_').replace('.csv','')
    csv_filename = f'{error_records}'+'/bad_records/'+ f"meta_info/{bolb_path}_{run_datetime}.csv"
    output=error_df.to_csv(index=False)
    blob_client = container_client.get_blob_client(csv_filename)
    blob_client.upload_blob(output, overwrite=True)
    print('file_writed:',csv_filename)

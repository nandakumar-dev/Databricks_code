# Databricks notebook source
#importing mountpoints notebook
%run "../Templates/mountpoints"

# COMMAND ----------

#getting notebook parameters
input_Url_file_blob_path=dbutils.widgets.get("input_Url_file_blob_path")
source_group=dbutils.widgets.get("source_group")
common_blob_path=bronze_meta_info.split('/')[0]

# COMMAND ----------

headers = {'User-Agent':'(https://usafacts.org/; DataTeam@usafacts.org)'}

# COMMAND ----------

#importing Packages
import concurrent.futures
from pprint import pprint
thread = concurrent.futures.ThreadPoolExecutor(max_workers=20)
# Get the current date
run_datetime  = datetime.now()

# COMMAND ----------

#getting blob path
file_path=bronze_ingestion_meta+input_Url_file_blob_path
input_url_file_df = spark.read.csv(file_path,header=True)

# COMMAND ----------

url_df = input_url_file_df.select("URL")
url_list = url_df.collect()
url_list=[url.URL.strip() for url in url_list]

# COMMAND ----------

#function for writing in broze
def blob_write(Directory_List,part_links):

    # convert the DataFrame to a CSV file and save it to a local file 
    if Directory_List is not None:
            Directory_List=Directory_List[['Name','Last modified','Size','Url','Parent_Directory']]
            Directory_List = Directory_List.loc[Directory_List["Size"] != '-']
            Directory_List= Directory_List.dropna(thresh=2)
            single_csv_filename = f'{common_blob_path}'+'/'+'DF'+'/'+f'{part_links}.csv'
            output=Directory_List.to_csv(index=False)
            blob_client = container_client.get_blob_client(single_csv_filename)
            blob_client.upload_blob(output, overwrite=True)
            print('file_writen')
    else:
        print('Directory_List is None')


error_record = []

def parse_url(seed_url):
    temp=urlparse(seed_url)
    base_url=(temp.scheme+'://'+temp.netloc)
    #path_url=temp.path
    return base_url

#parse the text in the HTML content, returns dataframe of abbreviation, file size, and update date
def parse_html(url,error=error_record,headers=headers):
    try:
        url=url.replace(' ','%20')
        res = requests.get(url,headers=headers)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'html.parser')
        table_tags = soup.find_all('table')
        if len(table_tags) > 0:
            raw_df=pd.read_html(url)
            df = raw_df[0]
            # print(df)
            df['Url']=url+df['Name']
            df['Url'] = df['Url'].str.replace(' ','%20')
            df.drop(columns=['Description'], inplace=True)
            return df
        else:
            links = soup.select('pre a')
            directories = []
            for link in links:
                directory_name = link.text.strip()
                directory_url = link['href']
                date = link.previous_sibling.strip().replace(',','') if link.previous_sibling and link.previous_sibling.strip() else ''
                date_parts = date.split('   ')
                file_size = re.split(r'\s+',date.strip())[-1]
                date_value = date.replace(file_size,'').strip()        
                directories.append((date_value,file_size.strip(), directory_name, directory_url,url))
                
            df = pd.DataFrame(directories, columns=['Last modified','Size','Name','Url','Parent_Directory'])
            base_url = parse_url(url)    
            df['Url'] = f'{base_url}' + df['Url']
            df = df.iloc[1: , :]
            df['Size'] = df['Size'].str.replace('<dir>','-')
            return df

    except requests.exceptions.HTTPError as e :
        print("error1",str(e),url)
        error.append((url,str(e)))
        return None
    except BaseException as e :
        print("error2",str(e),url)
        error.append((url,str(e)))
        return None

# COMMAND ----------

# parse the HTML content to get a list of links to merge with the abbreviations from above.
# grabs all the linked values from the HTML content, returns dataframe of abbreviation and url 

# Orchestrate those functions to process a single directory
def process_a_directory(url):
    df = parse_html(url)
    if df is None or df.empty :
        return None
    else:
        df=df.loc[df['Name'] != 'Parent Directory']
        df['Parent_Directory'] = url
        return df

# COMMAND ----------

# Orchestrate those function in a loop to cover linked directories
def all_directories(url,error=error_record):
    df = process_a_directory(url)
    if df is None:
        return None
    reviewed = [url]
    while True:
        temp_df = pd.DataFrame()
        for url in df[(df['Size'] == '-')]['Url']:
            if url not in reviewed:
                sub_df=process_a_directory(url)
                if sub_df is not None:
                    temp_df = pd.concat([temp_df, sub_df]).drop_duplicates()
                reviewed.append(url)
        if temp_df.empty:
            break
        df = pd.concat([df, temp_df]).reset_index(drop=True)
    return df
    

# COMMAND ----------

#using thread for multi processing
Directory_List = thread.map(all_directories,url_list)

for index,df in enumerate(Directory_List):
    blob_write(df,index)

# COMMAND ----------

try:
    single_csv_filename = bronze+common_blob_path+'/'+'DF/'
    input_daily_file_df = spark.read.csv(single_csv_filename,header=True)
    single_df =input_daily_file_df.toPandas()
    file_name=input_Url_file_blob_path.split('/')[-1]
    file_location =  f'{common_blob_path}'+'/'+'meta_info'+'/'+source_group+'/'+file_name
    blob_client = container_client.get_blob_client(f"{file_location}")
    csv_file = single_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)
    print('Single file uploaded',file_location)
except BaseException as e :
    print("error3",str(e))
    error_record.append(('DF not found',str(e)))
finally:
    dbutils.fs.rm(single_csv_filename,recurse=True)

# COMMAND ----------

error_df=pd.DataFrame(error_record)

# COMMAND ----------

if not error_df.empty:
    bolb_path=input_Url_file_blob_path.replace('/','_').replace('.csv','')
    csv_filename = common_blob_path+'/bad_records/'+ f"ingestion-meta/{bolb_path}_{run_datetime}.csv"
    output=error_df.to_csv(index=False)
    blob_client = container_client.get_blob_client(csv_filename)
    blob_client.upload_blob(output, overwrite=True)
    print('file_writed',csv_filename)

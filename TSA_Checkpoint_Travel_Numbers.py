# Databricks notebook source
#importing the mountpoints
%run "../Templates/mountpoints"

# COMMAND ----------

#getting notebook parameters
file_path=dbutils.widgets.get('file_path')

# COMMAND ----------

#Importing necessary packages
from io import StringIO

# COMMAND ----------

#reading files from the blob
blob_client = container_client.get_blob_client(bronze_meta_info+file_path)
content = blob_client.download_blob().readall()

# COMMAND ----------

headers= {'User-Agent': '(https://usafacts.org/; DataTeam@usafacts.org)'}

# COMMAND ----------

error_list=[]
# Convert bytes to string using StringIO
blob_string = str(content, 'utf-8')
blob_csv = StringIO(blob_string)

# Create Pandas DataFrame
df = pd.read_csv(blob_csv)

# Create a list from the specified column
url_list = df.Url.tolist()

for url in url_list:
    try:
        header=[] 
        footer=[]
        # Sending HTTP GET request to the URL
        response = requests.get(url,headers)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Get the HTML content from the response
            html_content = response.text
            # Parse the HTML content with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            #getting header and footer part
            temp_header = soup.find('h1').get_text(strip=True)
            header.append(temp_header)
            if soup.find('div', {'class': "view-header"}):
                temp_header=soup.find('div', {'class': "view-header"}).get_text(strip=True)
                header.append(temp_header)
            temp_footer= soup.find('div', {'class': "view-footer"}).get_text(strip=True)
            footer.append(temp_footer)
            #getting table part
            table = soup.find('table')

            # Extract data from the table and create a DataFrame
            rows = table.find_all('tr')
            data = []
            #iterating rows from the table
            for row in rows:
                cols = row.find_all('th')
                cols = [col.text.strip() for col in cols]
                values = row.find_all('td')
                values = [col.text.strip() for col in values]
                data.append(cols+values)
            columns=data[0]
            pandas_df = pd.DataFrame(data[1:],columns=columns)
            spark_df=spark.createDataFrame(pandas_df)
            header='. '.join(header)
            footer='. '.join(footer)
            if header:
                spark_df=spark_df.withColumn('Header',lit(header))
            if footer:
                spark_df=spark_df.withColumn('Footer',lit(footer))
                
            spark_df=spark_df.coalesce(1)
            if not url.split('/')[-1].isdigit():
                ouput_file_path=silver+url.replace('https://','')+'/main'
            else:
                ouput_file_path=silver+url.replace('https://','')
            #uploding to the blob
            spark_df.write.format('delta').mode('overwrite').option("overwriteSchema",True).save(ouput_file_path)
            print('DELTA file uploaded successfully: ',ouput_file_path)
    except Exception as e:
        print(f"An error occurred while processing {url}: {str(e)}")
        error_list1.append((f'{url}', str(e)))
    

# COMMAND ----------

#uploading error logs
if len(error_list) !=0:
    pandas_df = pd.DataFrame(error_list,columns=["URL","Reason"])
    error_records=bronze_meta_info.split('/')[0]
    bolb_path=file_path.replace('/','_').replace('.csv','')
    filelocation = f'{error_records}'+'/bad_records/'+ f"meta_info/{bolb_path}_{run_datetime}.csv"
    blob_client = container_client.get_blob_client(f"{filelocation}")
    csv_file = pandas_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)
    print('error_file_uploaded',filelocation)

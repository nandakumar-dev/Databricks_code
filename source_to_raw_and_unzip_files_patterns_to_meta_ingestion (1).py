# Databricks notebook source
# MAGIC %md
# MAGIC This Notebook is for getting data from the file urls stored in the ingestion-meta containg under scraped_urls directory and ingest the raw data to the to_wild container 

# COMMAND ----------

# MAGIC %run "../Templates/mountpoints"

# COMMAND ----------

# Parameters

# input_files_blob_path = 'census/popest/popest_datasets.csv'
# patterns='\d+0(?:/[^/]+)*/[^/]+\.zip$'

# input_files_blob_path should be after the ingestion-meta container under scraped_urls directory
input_files_blob_path=dbutils.widgets.get('input_files_blob_path')
patterns=dbutils.widgets.get('patterns')


bolb_path=input_files_blob_path
input_files_blob_path=manual_ingestion_meta_scraped_urls+input_files_blob_path

# COMMAND ----------

#importing necessary packages
import concurrent.futures
import shutil

# COMMAND ----------

headers = {'User-Agent': '(https://usafacts.org/; DataTeam@usafacts.org)'}

# COMMAND ----------

def url_is_not_azure_blob(url):
    turl = url
    if turl.endswith('/'):
        turl = turl[:-1]
    safe_url = urllib.parse.quote(turl, safe=':/')
    blob_name = safe_url.split('//')[1]
    return blob_name

# COMMAND ----------

#function for getting data from the url and ingesting raw data to the manual to-wild container
error_list = []

def download_and_upload_file(file_url):
    try:
        # Extract the file name and folder name from the URL
      
        output_blob_name=file_url.replace('https://','')
        # Create a BlobServiceClient
        blob_client = manual_raw_container_client.get_blob_client(output_blob_name)

        # Create a temporary file to store the downloaded file
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            # Create a custom request with user agent header
            request = urllib.request.Request(file_url, headers=headers)
            
            # Download the file from the URL and write it to the temporary file
            with urllib.request.urlopen(request) as response:
                with open(tmp_file.name, 'wb') as file:
                    file.write(response.read())

        # Upload the file to Azure Blob Storage
        with open(tmp_file.name, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        # Delete the temporary file
        os.remove(tmp_file.name)

        print(f"File '{output_blob_name}' downloaded and uploaded successfully.")
    except Exception as e:
        print(f"Error occurred while processing file '{file_url}': {str(e)}")
        error_list.append((file_url, str(e)))
    finally:
        os.remove(tmp_file.name)

def download_and_upload_files(file_urls):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit tasks to the executor
        futures = [executor.submit(download_and_upload_file, file_url) for file_url in file_urls]
        
        # Wait for all tasks to complete
        concurrent.futures.wait(futures)

# COMMAND ----------

#reading urls from the ingeastion-metainfo containg under scraped_urls directory and passing url to the above function
try:
    input_txt_file_df = spark.read.csv(input_files_blob_path,header=True)
    input_txt_file_df = input_txt_file_df.select(*[regexp_replace(col_name, ' ','%20').alias(col_name) for col_name in input_txt_file_df.columns])
    new_column_names = [col_name.upper() for col_name in input_txt_file_df.columns]
    input_txt_file_df = input_txt_file_df.select([col(col_name).alias(new_col_name) for col_name, new_col_name in zip(input_txt_file_df.columns, new_column_names)])
    url_df = input_txt_file_df.select("URL")
    url_list = url_df.collect()
    url_list=[url.URL.strip() for url in url_list]

    if patterns != None and len(patterns) > 0:  
        patterns_split=patterns.split(',')  
        regex_patterns = [re.compile(pattern) for pattern in patterns_split]   

        filtered_urls = []
        for url in url_list:     
            for pattern in regex_patterns:         
                if pattern.search(url):             
                    filtered_urls.append(url)             
                    break
        url_list = filtered_urls
    if len(url_list) !=0:
        download_and_upload_files(url_list)

except Exception as e:
    error_list.append((input_files_blob_path, str(e)))
    print('path not found')


# COMMAND ----------

#function for unzipping the zip files in the provided file path in blob storage in to_wild container
def unzipping_files(folder_name):

    # List blobs in the folder
    blobs = manual_raw_container_client.list_blobs(name_starts_with=folder_name)

    for blob in blobs:
        unzip_blob_path='.'.join(blob.name.split('.')[:-1])
        
        if blob.name.endswith(".zip") or blob.name.endswith(".ZIP") :        
            try:
                # Download the blob to a local file
                blob_client = manual_raw_container_client.get_blob_client(blob)
                local_file_path = os.path.join(tempfile.gettempdir(), os.path.basename(blob.name))
                
                with open(local_file_path, "wb") as local_file:
                    blob_client.download_blob().readinto(local_file)

                # Extract the zip file
                extract_folder = os.path.join(tempfile.gettempdir(), os.path.splitext(os.path.basename(blob.name))[0])
                
                with zipfile.ZipFile(local_file_path, "r") as zip_ref:
                    zip_ref.extractall(extract_folder)
                # Re-upload extracted files to the same path
                for root, dirs, files in os.walk(extract_folder):
                    for extracted_file in files:
                        # Get the full path of the file
                        extracted_file_path = os.path.join(root, extracted_file)
                        dir_name=root.split('/')[-1]
                        path_name=unzip_blob_path.split('/')[-1]
                        if dir_name!=path_name:
                            new_blob_name = os.path.join(unzip_blob_path,dir_name,extracted_file)
                        else:
                            new_blob_name = os.path.join(unzip_blob_path,extracted_file)    
                        
                        with open(extracted_file_path, "rb") as extracted_file:
                            manual_raw_container_client.upload_blob(name=new_blob_name, data=extracted_file,overwrite=True)
                            print('file_uploaded',f'{new_blob_name}')

                # Clean up temporary files
                os.remove(local_file_path)
                shutil.rmtree(extract_folder)
            except Exception as e:
                # Handle the exception
                print(f"An error occurred while processing {blob.name}: {str(e)}")
                error_list.append(({blob.name}, str(e)))



# COMMAND ----------

#passing the zip files path to the above function
try:
    for url in url_list:
        if url.endswith('.zip') or url.endswith('.ZIP'):
            path=url.replace('https://','')
            unzipping_files(path)
except Exception as e:
    error_list.append((input_files_blob_path, str(e)))
    print('path not found')


# COMMAND ----------

#uploding error logs to the ingestion-metainfo container bad records
if len(error_list) !=0:
    pandas_df = pd.DataFrame(error_list,columns=["URL","Reason"])
    bolb_path=input_files_blob_path.replace('/','_').replace('.csv','')
    filelocation = 'bad_records/'+ f"{bolb_path}_{run_datetime}.csv"
    blob_client = manual_ingestion_meta_container_client.get_blob_client(f"{filelocation}")
    csv_file = pandas_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)
    print('error_file_uploaded',filelocation)

# Databricks notebook source
#commmon library
import os
from pyspark.sql.functions import *

#custom library
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from data_common_library.credentials import BlobAccount
from data_common_library.azure import helpers
from data_common_library.azure import client
from data_common_library.data_platform.bronze.RawToBronze import RawToBronze
from data_common_library.azure import utilities

# COMMAND ----------

blob_account = BlobAccount.MANUAL_ACCOUNT
blob_account_key = BlobAccount.AZURE_BLOB_ACCOUNT_KEYS[blob_account]
blob_container = BlobAccount.MANUAL_RAW_TO_BRONZE_CONTAINER
to_wild_container = BlobAccount.MANUAL_WILD_TO_RAW_CONTAINER

spark.conf.set(f"fs.azure.account.key.{blob_account}.blob.core.windows.net", blob_account_key)

# COMMAND ----------

#parameters
# pipeline_id = dbutils.widgets.get("pipeline_id")
# job_id = dbutils.widgets.get("job_id")
# blob_relative_path = dbutils.widgets.get("blob_relative_path") 

pipeline_id='54c7c819-7f45-4c39-a8af-8971c642e384'
job_id='a87eb85e-8df1-44e6-a72c-8f3c662e5a5b'
blob_relative_path='www.irs.gov/pub/irs-soi/1202insoca.xls/Sheet2_6/'

# COMMAND ----------

#get file path from to_bronze container
def get_file_paths(blob_relative_path):
    manual_account = BlobAccount.MANUAL_ACCOUNT                     
    to_bronze_container = BlobAccount.MANUAL_RAW_TO_BRONZE_CONTAINER   
    container_client = client.create_container_client(manual_account, to_bronze_container)
    blob_names = list(set(['/'.join(blob_name.split('/')[:-1]) + '/' for blob_name in container_client.list_blob_names(name_starts_with=blob_relative_path) if '.parquet' in blob_name]))
    return blob_names
    
file_path_list = get_file_paths(blob_relative_path)

print(len(file_path_list))
fail=[]
for file_path in file_path_list: 
    # try:
        print(file_path)
        name=file_path.split('/')[-2].replace('.','_')
        path = f"wasbs://{blob_container}@{blob_account}.blob.core.windows.net/{file_path}"
        df=spark.read.format('delta').load(path)
        new_column_names = [col_name for col_name in df.columns if len(col_name)>255]
        if new_column_names:
            # Assuming df is your DataFrame with long column names
            df = df.select([col(col_name).alias(col_name[:255]) for col_name in df.columns])

            df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(path)

        raw_to_bronze=RawToBronze(pipeline_id = pipeline_id, job_id=job_id ,created_by = 'Nandakumar A', description = 'soi_tax_stats',
            agency='soi',table_name=f"`soi_tax_stats_{name}`",environment= os.getenv('ENVIRONMENT'),spark=spark)   
        
        raw_to_bronze.raw_to_bronze_manual_delta(filename=file_path)
    # except Exception as e:
    #     fail.append(file_path)

# COMMAND ----------

fail_string = ", ".join(fail)
dbutils.notebook.exit("Number of failed urls: " + str(len(fail)) + ", " + fail_string)

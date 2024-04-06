# Databricks notebook source
# MAGIC %md
# MAGIC * The purpose of this notebook is to combine the individual delta files for BEA NIPA International Data in silver blob storage to one large delta file 

# COMMAND ----------

# MAGIC %run "/Repos/v-nandakumara@usafacts.org/usafacts-data-platform-notebooks/Templates/mountpoints"

# COMMAND ----------

# blob_path='apps.bea.gov/api/'
# patterns='IIP,ITA,IntlServSTA,IntlServTrade'
blob_path=dbutils.widgets.get('blob_path')
patterns=dbutils.widgets.get('patterns')
pattern_list=patterns.split(',')

# COMMAND ----------

# import packages
import json
import os
from io import BytesIO, StringIO
import numpy as np
import multiprocessing
import tempfile
import requests
import time
import datetime
import re
import warnings

# COMMAND ----------

#Reading the delta files from the silver layer and combining the all tables into large table
error_list=[]

for pattern in pattern_list:
    path=blob_path+pattern
    combined_path=blob_path+'BEA_NIPA_International_Data_combined_tables/'+pattern+'/'
    blobs = container_client.list_blobs(name_starts_with=path)
    for blob in blobs:
        if any(value in blob.name for value in pattern_list):
            try:
                df = spark.read.format("delta").load(silver + blob.name)
                df.write.format('delta').mode('append').option("mergeSchema", "true").save(silver+combined_path)
            except Exception as e:
                error_list.append((blob.name,str(e)))
                print(f'error:{blob.name}',str(e))
    print('files_uploaded',combined_path)


# COMMAND ----------

#Capturing the error logs and uploading in bronze
if len(error_list) !=0:
    pandas_df = pd.DataFrame(error_list,columns=["URL","Reason"])
    error_records=bronze_meta_info.split('/')[0]
    bolb_path=bolb_path.replace('/','_').replace('.csv','')
    filelocation = f'{error_records}'+'/bad_records/'+ f"meta_info/{bolb_path}_{run_datetime}.csv"
    blob_client = container_client.get_blob_client(f"{filelocation}")
    csv_file = pandas_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)
    print('error_file_uploaded',filelocation)

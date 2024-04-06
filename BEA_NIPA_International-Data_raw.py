# Databricks notebook source
# MAGIC %md
# MAGIC # Extract API Parameters
# MAGIC The goals of this notebook are to
# MAGIC
# MAGIC 1. Extract the relevant API parameters from the metadata table (ingested in first copy job of Pipeline)
# MAGIC 2. Create list of API call urls to iterate through
# MAGIC 4. Run through list of API calls, ingest data into bronze

# COMMAND ----------

# MAGIC %run "../Templates/mountpoints"

# COMMAND ----------

# MAGIC %md
# MAGIC ## import packages

# COMMAND ----------

import json
import os
from io import BytesIO, StringIO
import numpy as np
from multiprocessing import Pool
import tempfile
import requests
import time
import datetime
import re

# COMMAND ----------

# MAGIC %md
# MAGIC # Get API Parameters

# COMMAND ----------

#api keys
api_keys = ['F4993EB8-9C0D-4141-9E37-828C00D09143', '191DA714-6A67-402A-881B-E9F984EA5267', 'A1F7718B-67A6-4597-A30D-AB58AA712921', 'A501A017-5838-4D2C-95E7-B4624C3D2BCF', '930BA9FE-78E2-40FB-A62E-C89ED7DA772F','AE73C6F5-C9EE-440E-9316-EB5B5CDB654B','EB834443-9B97-468D-9EC2-8AAB6D78A922','5B41DF91-6B41-4692-8C16-3BE29CAC8E48','4DB3D6CD-EAD5-482E-BB14-FC7243B611E7','52F18528-56B4-4928-852D-BC222F28CDA5']

# COMMAND ----------

def get_parameters(url):
    response = requests.get(url)
    if response.status_code == 200:
        metadata_table = json.loads(response.text)
    else:
        print(f"Error retrieving data from {Indicator_url}. Status code: {response.status_code}")

    table_array = metadata_table["BEAAPI"]["Results"]["ParamValue"]
    table_df = pd.DataFrame.from_dict(table_array)
    table_df.drop("Desc", axis = 1,inplace=True)
    return table_df

# COMMAND ----------

# create API calls
# function to reformat blob_path url because it was messing things up in silver
def reformat_url(url):
    url = url.replace('https://', '')
    reserved_characters = ['?']
    new_url = ''
    for c in url:
        if c in reserved_characters:
            new_url = new_url + '-'
        else:
           new_url = new_url + c 
    return(new_url)

# COMMAND ----------

# MAGIC %md
# MAGIC International Transactions (ITA)

# COMMAND ----------

Indicator_url = 'https://apps.bea.gov/api/data/?&UserID=52F18528-56B4-4928-852D-BC222F28CDA5&method=GetParameterValues&DataSetName=ITA&ParameterName=Indicator&ResultFormat=json'

ITA_table=get_parameters(Indicator_url)
ITA_table = ITA_table.rename(columns = {"Key":"Indicator_Name"})
ITA_table['api_url']= '&method=GetData&DataSetName=ITA&Indicator=' \
    + ITA_table["Indicator_Name"] + '&AreaOrCountry=ALL&Frequency=A,QSA,QNSA&Year=ALL&ResultFormat=json'
ITA_table.drop("Indicator_Name", axis = 1,inplace=True)
ITA_table

# COMMAND ----------

# MAGIC %md
# MAGIC IIP

# COMMAND ----------

TypeOfInvestment_url = 'http://apps.bea.gov/api/data/?&UserID=52F18528-56B4-4928-852D-BC222F28CDA5&method=GetParameterValues&DataSetName=IIP&ParameterName=TypeOfInvestment&ResultFormat=json'

IIP_table=get_parameters(TypeOfInvestment_url)
IIP_table = IIP_table.rename(columns = {"Key":"TypeOfInvestment"})
IIP_table['api_url']= '&method=GetData&DataSetName=IIP&TypeOfInvestment=' \
    + IIP_table["TypeOfInvestment"] + '&Component=ChgPos,ChgPosNie,ChgPosOth,ChgPosPrice,ChgPosTrans,ChgPosXRate,Pos&Frequency=A,QNSA&Year=ALL&ResultFormat=json'
IIP_table.drop("TypeOfInvestment", axis = 1,inplace=True)
IIP_table

# COMMAND ----------

# MAGIC %md
# MAGIC International Services Trade (IntlServTrade)

# COMMAND ----------

TypeOfService_url = 'https://apps.bea.gov/api/data/?&UserID=52F18528-56B4-4928-852D-BC222F28CDA5&method=GetParameterValues&DataSetName=IntlServTrade&ParameterName=TypeOfService&ResultFormat=json'

IS_table=get_parameters(TypeOfService_url)
IS_table = IS_table.rename(columns = {"Key":"TypeOfService"})
IS_table['api_url']= '&method=GetData&DataSetName=IntlServTrade&TypeOfService=' \
    + IS_table["TypeOfService"] + '&TradeDirection=ALL&Affiliation=ALL&AreaOrCountry=ALL&Year=ALL&ResultFormat=json'
IS_table.drop("TypeOfService", axis = 1,inplace=True)
IS_table

# COMMAND ----------

# MAGIC %md
# MAGIC International Services Supplied Through Affiliates (IntlServSTA)

# COMMAND ----------

Industry_url = 'https://apps.bea.gov/api/data/?&UserID=52F18528-56B4-4928-852D-BC222F28CDA5&method=GetParameterValues&DataSetName=IntlServSTA&ParameterName=Industry&ResultFormat=json'

ISSTA_table=get_parameters(Industry_url)
ISSTA_table = ISSTA_table.rename(columns = {"Key":"Industry"})
ISSTA_table['api_url']= '&method=GetData&DataSetName=IntlServSTA&Channel=ALL&Destination=ALL&Industry=' \
    + ISSTA_table["Industry"] + '&AreaOrCountry=ALL&Year=ALL&ResultFormat=json'
ISSTA_table.drop("Industry", axis = 1,inplace=True)
ISSTA_table

# COMMAND ----------

#combining tables into a single table
combined_df=pd.concat([ITA_table,IIP_table,IS_table,ISSTA_table])

combined_df['base_url'] = 'https://apps.bea.gov/api/data/'
combined_df['api_key'] = pd.qcut(range(len(combined_df)), q = 10, labels = api_keys).to_list()
combined_df["relative_url"] = "?&UserID=" + combined_df['api_key'] \
    +combined_df['api_url']

combined_df['full_url'] = combined_df['base_url'] + combined_df['relative_url']
combined_df['blob_path'] = [reformat_url(u.replace('data',u.split('DataSetName=')[-1].split('&')[0])) for u in combined_df['full_url']]
combined_df.drop("api_url", axis = 1,inplace=True)

# COMMAND ----------

display(combined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Run API calls

# COMMAND ----------

# split df into chunks for Multi processing
num_processes = len(api_keys)
chunks = np.array_split(combined_df, num_processes)

# COMMAND ----------

# define function to read blob files
def ingest_to_bronze(chunk):
    chunk = chunk.reset_index()

    for i in range(len(chunk)):
        file_url = chunk['full_url'][i]
        blob_name = chunk['blob_path'][i]

        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                # Download the file from the URL and write it to the temporary file
                response = requests.get(file_url)
                tmp_file.write(response.content)
                tmp_file.flush()

                # Upload the file to Azure Blob Storage
                blob_client = blob_service_client.get_blob_client('bronze', blob_name)
                with open(tmp_file.name, "rb") as data:
                    blob_client.upload_blob(data, overwrite = True)
                
                # get size in mb
                file_size = os.path.getsize(tmp_file.name) / (1024 * 1024)

                # Delete the temporary file
                os.remove(tmp_file.name)
                print("uploaded blob: " + blob_name)
                # wait based on file size
                # side note: It's difficult to not throttle the API key while keeping this process fast. Some files are <1MB while others are 25 MB
                if file_size < 2:
                    time.sleep(2)
                else:
                    sleep = 60/(100/file_size) + 4
                    time.sleep(sleep)
                
        except requests.exceptions.RequestException as e:
            print(f"Error processing {file_url}: {str(e)}")
            fail.append(file_url)
        

# COMMAND ----------

# multi processing on API urls, each process uses different API key
fail = []

with Pool(num_processes) as p:
    p.map(ingest_to_bronze, chunks)

# COMMAND ----------

# add metadata to notebook activity output to check status of URL ingestion
fail_string = ", ".join(fail)
dbutils.notebook.exit("Number of failed urls: " + str(len(fail)) + ", " + fail_string)

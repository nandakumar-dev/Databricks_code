# Databricks notebook source
# MAGIC %md
# MAGIC Importing Mountpoints Notebook

# COMMAND ----------

# MAGIC %run "../Templates/mountpoints"

# COMMAND ----------

# MAGIC %md
# MAGIC Installing necessary packages

# COMMAND ----------

# MAGIC %pip install pdfplumber

# COMMAND ----------

# MAGIC %pip install tabula-py

# COMMAND ----------

# MAGIC %md
# MAGIC Importing necessary packages

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import BytesIO
import os
import tempfile
import pdfplumber
import re
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import tabula

# COMMAND ----------

#Getting File path
# record_layout_path='www2.census.gov/programs-surveys/gov-finances/tables/'
record_layout_path=dbutils.widgets.get('record_layout_path')

# COMMAND ----------

#Function for reading the pdf file and extracting text from it
def collect_data(content):  
    page_list=[]
    temp_list=[]
    pdf_file = content
    with tempfile.NamedTemporaryFile(suffix='.pdf') as tmp_file:
        tmp_file.write(pdf_file)
        tmp_file.seek(0)
        with pdfplumber.open(tmp_file.name) as pdf:
            pages = pdf.pages
            for page in pages :
                text=page.extract_text()
                values = text.split('\n')        
                page_list.extend(values)
    return page_list

# COMMAND ----------

#Function for writing files in blob storage
def write_bronze(filtered_df,pdf_path):
    ouput_file_path=pdf_path.replace("https://","").split('.')[:-1]
    ouput_file_path='.'.join(ouput_file_path)+'.csv'
    csv_data=filtered_df.to_csv(index=False)
    # Create a blob client for the new file
    blob_client = container_client.get_blob_client(ouput_file_path)
    # Upload the CSV data to the blob
    blob_client.upload_blob(csv_data, overwrite=True)
    print("CSV file uploaded successfully:",ouput_file_path)
    

# COMMAND ----------

#getting list of the pdf and processing the files

blobs = container_client.list_blobs(name_starts_with=record_layout_path)

for blob in blobs:
    try:
        if blob.name.endswith(".pdf") and ('Technical Documentation' in blob.name or 'Tech Doc' in blob.name) and not 'old' in blob.name:

            blob_client = container_client.get_blob_client(blob)
            content = blob_client.download_blob().readall()
            pdf_reader = content
            value_list=collect_data(pdf_reader)

            table_data=[]
            
            temp_data=[]
            table=False
            for value in value_list:  
                if (value.strip().replace(' ','').replace('–','').replace('(','').replace(')','').replace('.','').replace(',','').replace('/','').replace('3rd','').replace('"','').replace('-','').replace('“','').replace('”','').replace('=','').isalpha() and 
                    not (value.startswith("I ") or value.startswith('R ')) and 
                    not any(keyword in value for keyword in ['Analyst correction', 'Imputed', 'Unknown', 'Reported', 'Alternative source'])):

                    table=False
                    if temp_data:
                        table_data.append(temp_data)
                    
                        temp_data=[]

                if any(keyword in value for keyword in ['Level Code Description', 'State Name Abbreviation State Code', 'Type Code Description', 'Item Code Description', 'Indicates type of service provided; this field applies only to special districts (records with type=4).', 'Flags Description','Type-Codes Description']):
                    table=True
                if table:
                    temp_data.append(value)
            dfs=[]  
            dfs1=[]
            for table in table_data:
                if table[0]=="Level Code Description" :
                    sub_data=[tuple(tdata.split(' ',1)) for tdata in table[1:]]
                    df=pd.DataFrame(sub_data,columns=["Level_of_estimate_code","Description"])
                    dfs.append(df)
                
                if table[0]=="State Name Abbreviation State Code":

                    sub_data = [(words.split()[-1], ' '.join(words.split()[:-2]))  for words in table[1:]]
                    df=pd.DataFrame(sub_data,columns=["State_code","Description"])
                    dfs.append(df)

                if table[0] == "Type Code Description" or table[0] == 'Type-Codes Description':
                    sub_data=[tuple(tdata.split(' ',1)) for tdata in table[1:]]
                    df=pd.DataFrame(sub_data,columns=["Type_code","Description"])
                    dfs.append(df)

                if table[0]=="Item Code Description":
                    sub_data=[tuple(tdata.split(' ',1)) for tdata in table[1:]]
                    df=pd.DataFrame(sub_data,columns=["Item_code","Description"])
                    dfs.append(df)

                if table[0]=="Indicates type of service provided; this field applies only to special districts (records with type=4).":
                    sub_data=[tuple(tdata.split(' ',1)) for tdata in table[1:]]
                    df=pd.DataFrame(sub_data,columns=["Function_code_for_special_districts","Description"])
                    dfs.append(df)
                if table[0]=='Flags Description':
                    sub_data=[tuple(tdata.split(' ',1)) for tdata in table[1:]]
                    df=pd.DataFrame(sub_data,columns=["Imputation_type/item_data_flag","Description"])
                    dfs.append(df)

            schema = StructType([
                    StructField('Code', StringType(), True), 
                    StructField('Desription', StringType(), True),
                    StructField('Name', StringType(), True)])
            spark_df=spark.createDataFrame([],schema)

            if dfs:
                for df in dfs:
                   
                    df=spark.createDataFrame(df)
                    value_name=df.columns[0]
                    df=df.withColumn('Name',lit(value_name))
                    spark_df=spark_df.union(df)

            output_path='.'.join(blob.name.split('.')[:-1])+'_code_layout.csv'
            df=spark_df.toPandas()
            
            write_bronze(df,output_path)

    except Exception as e:
        error_list.append((blob.name,str(e)))
        print('error:',str(e))


# COMMAND ----------

# Extracing tables from the Pdf files
blobs = container_client.list_blobs(name_starts_with=record_layout_path)

for blob in blobs:
    try:
        if blob.name.endswith(".pdf") and ('Technical Documentation' in blob.name or 'Tech Doc' in blob.name) and not 'old' in blob.name:
            print(blob.name)
            blob_client = container_client.get_blob_client(blob)
            content = blob_client.download_blob().readall()
            tables = tabula.read_pdf(BytesIO(content),pages='all',guess=True,lattice=True) #multiple_tables=False,multiple_tables=False
            # Assuming 'tables' is your list of Pandas DataFrames
            filtered_tables = [table for table in tables if set(['Description', 'Positions', 'Length', 'Value']).issubset(table.columns)]
    
            for count,table in enumerate(filtered_tables):
                ouput_file_path=blob.name.replace("https://","").split('.')[:-1]
                ouput_file_path='.'.join(ouput_file_path)+'_file_layout_'+str(count)+'.csv'
                write_bronze(table,ouput_file_path)
    except Exception as e:
        error_list.append((blob.name,str(e)))
        print('error:',str(e))
 

# COMMAND ----------

if len(error_list) !=0:
    pandas_df = pd.DataFrame(error_list,columns=["URL","Reason"])
    error_records=bronze_meta_info.split('/')[0]
    bolb_path=blob_path.replace('/','_').replace('.csv','')
    filelocation = f'{error_records}'+'/bad_records/'+ f"meta_info/{bolb_path}_{run_datetime}.csv"
    blob_client = container_client.get_blob_client(f"{filelocation}")
    csv_file = pandas_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)
    print('error_file_uploaded',filelocation)

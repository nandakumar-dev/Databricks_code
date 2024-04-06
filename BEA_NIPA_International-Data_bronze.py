# Databricks notebook source
# MAGIC %md
# MAGIC # CONVERTING JSON TO DELTA
# MAGIC The goals of this notebook are to
# MAGIC
# MAGIC 1. Read the json files from the bronze layer
# MAGIC 2. Creating pyspark dataframe
# MAGIC 3. Ingesting Delta files into the silver layer
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../Templates/mountpoints"

# COMMAND ----------

# import packages
import json

# COMMAND ----------

#Blob path
# blob_path='apps.bea.gov/api/'
# patterns='IIP,ITA,IntlServSTA,IntlServTrade'
blob_path=dbutils.widgets.get('blob_path')
patterns=dbutils.widgets.get('patterns')
pattern_list=patterns.split(',')

# COMMAND ----------

#reading json files from the blob and writing as delta tables in silver
error_list=[]
blobs = container_client.list_blobs(name_starts_with=blob_path)
for blob in blobs:
    if any(value in blob.name for value in pattern_list):
        try:
            blob_client = container_client.get_blob_client(blob) 
            blob_data = blob_client.download_blob().readall()
            json_data = json.loads(blob_data)
            try:
                table_data=json_data["BEAAPI"]["Data"]
            except:
                table_data=json_data["BEAAPI"]["Results"]["Data"]
            df=pd.DataFrame.from_dict(table_data)
            spark_schema = StructType([StructField(col, StringType(), True) for col in df.columns])
            spark_df = spark.createDataFrame(df, schema=spark_schema)
            spark_df.write.format('delta').mode('overwrite').option("overwriteSchema",True).option("path",silver+blob.name).save()
            print('file_uploaded:',blob.name)
            
        except Exception as e:
            error_list.append((blob.name,str(e)))
            print(f'error:{blob.name}',str(e))



# COMMAND ----------

#capturing error logs and uploading in bronze
if len(error_list) !=0:
    pandas_df = pd.DataFrame(error_list,columns=["URL","Reason"])
    error_records=bronze_meta_info.split('/')[0]
    bolb_path=bolb_path.replace('/','_').replace('.csv','')
    filelocation = f'{error_records}'+'/bad_records/'+ f"meta_info/{bolb_path}_{run_datetime}.csv"
    blob_client = container_client.get_blob_client(f"{filelocation}")
    csv_file = pandas_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)
    print('error_file_uploaded',filelocation)

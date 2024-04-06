# Databricks notebook source
# MAGIC %md
# MAGIC ## csv_to_silver_delta
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#importing mountpoint notebook
%run "../mount_points_paramerters/mountpoints"

# COMMAND ----------

#getting parameters
blob_relative_path = dbutils.widgets.get('blob_relative_path')
file_extension=dbutils.widgets.get('file_extension')
header='True'
patterns=dbutils.widgets.get('patterns')


# COMMAND ----------

#assingning mountpoints
source_mount_point=bronze
output_mount_point=silver

# COMMAND ----------

# MAGIC %md
# MAGIC get_file_paths function will read all sub folder and returns only csv files

# COMMAND ----------

def get_file_paths(dir_path):
    file_paths= []
    files = dbutils.fs.ls(dir_path)
    for file in files:
        if file.isDir() :
            file_paths.extend(get_file_paths(file.path))
        else:
            if (file_extension== None or file_extension=='' ) or file.path.endswith(f'{file_extension}') :
                # path = file.path.split(bronze_wasbs_path)[-1]
                file_paths.append(file.path)

    return file_paths

# COMMAND ----------

# MAGIC %md
# MAGIC all_file_path contains all csv file paths

# COMMAND ----------

all_file_paths = get_file_paths(source_mount_point+blob_relative_path)
if patterns != None and len(patterns) > 0:  
    patterns=patterns.split(',')  
    regex_patterns = [re.compile(pattern) for pattern in patterns]   

    filtered_urls = []
    for url in all_file_paths:     
        for pattern in regex_patterns:         
            if pattern.search(url):             
                filtered_urls.append(url)             
                break
    all_file_paths = filtered_urls

# COMMAND ----------

# MAGIC %md
# MAGIC Reading all csv files from Bronze and writing into Silver

# COMMAND ----------

error_list=[]
for file_path in all_file_paths:
    try:
        spark_df = spark.read.option('multiLine','true').csv(file_path,header = f'{header}')
        spark_df = spark_df.select([col(col_name).alias(col_name.replace(' ','_').replace('.','_').replace('(','_').replace(')','_')) for col_name in spark_df.columns])
        file_location = output_mount_point+file_path.split(source_mount_point)[-1]
        display(spark_df)
        spark_df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path",file_location).save()
        print("file uploaded sucessfully",file_location)
    except Exception as e:
        # Handle the exception
        print(f"An error occurred while processing {file_path}: {str(e)}")
        error_list.append((f'{file_path}', str(e)))
    

# COMMAND ----------

#uploding error logs to the blob
if len(error_list) !=0:
    pandas_df = pd.DataFrame(error_list,columns=["URL","Reason"])
    error_records=bronze_meta_info.split('/')[0]
    bolb_path=blob_relative_path.replace('/','_').replace('.csv','')
    filelocation = f'{error_records}'+'/bad_records/'+ f"meta_info/{bolb_path}_{run_datetime}.csv"
    blob_client = container_client.get_blob_client(f"{filelocation}")
    csv_file = pandas_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)
    print('error_file_uploaded',filelocation)

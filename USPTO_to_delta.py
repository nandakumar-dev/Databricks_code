# Databricks notebook source
#importing the mountpoints
%run "../Templates/mountpoints"

# COMMAND ----------

# MAGIC %md
# MAGIC blob_path

# COMMAND ----------

blob_relative_path = dbutils.widgets.get('blob_relative_path')

# COMMAND ----------

# MAGIC %md
# MAGIC Installing packages for reading excel files

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

pip install xlrd

# COMMAND ----------

# MAGIC %md
# MAGIC Blob connection

# COMMAND ----------

# Azure storage access info 
blob_account_name = 'usafactsbronze' # replace with your blob name 
blob_container_name = 'bronze' # replace with your container name 
linked_service_name = 'bronze' # replace with your linked service name 

blob_sas_token = dbutils.secrets.get(scope = "usafacts-data-scope", key = "usafactsbronze-access-key")

# Allow SPARK to access from Blob remotely 
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path) 
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)

# Azure storage access info 
blob_account_name = 'usafactssilver' # replace with your blob name 
blob_container_name = 'silver' # replace with your container name 
linked_service_name = 'silver' # replace with your linked service name 


blob_sas_token = dbutils.secrets.get(scope = "usafacts-data-scope", key = "usafactsbronze-access-key")
# Allow SPARK to access from Blob remotely 
target_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path) 
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token) 

# COMMAND ----------

# MAGIC %md
# MAGIC Getting list of files from blob

# COMMAND ----------

def get_file_paths(dir_path):
    file_paths = []
    files = dbutils.fs.ls(dir_path)
    
    for file in files:
        allowed_extensions = ['.xls', '.xlsx']
        
        if file.isDir():
            file_paths.extend(get_file_paths(file.path))
        
        if any(file.path.endswith(ext) for ext in allowed_extensions):
            file_paths.append(file.path)

    return file_paths


# COMMAND ----------

all_file_paths = get_file_paths(wasbs_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Header concating function

# COMMAND ----------

def concat_xls_row(data,none_count):
                
    concat_list1 = []
    concat_list = []
    if none_count == 1:
        iter_range = 2
    else:
        iter_range = none_count
    for index in range(iter_range):
        if (none_count == 1 and index == 0) or index+1 < none_count :
            temp_value = None
            temp_list =[]

            for value in data[index]:
                if value is None:
                    value = temp_value
                else:
                    temp_value = value
                temp_list.append(value)
            cleaned_row = [value for value in data[index] if value is not None]
            if  index == 0 and len(cleaned_row) == 1:
                temp_list = [None] + (cleaned_row *(len(data[0])-1))
            if len(concat_list1) >= 1:
                concat_list1 = [(x if x is not None else ' ') +' '+ (y if y is not None else ' ') for x, y in zip(concat_list1,temp_list)]
            else:
                concat_list1 =  temp_list
        else:
            concat_list = [(x if x is not None else ' ') +' '+ (y if y is not None else ' ') for x, y in zip(concat_list1,data[index])]
            data[index] = concat_list
            
            if none_count == 1 :
                data = data[index:]
            else:
                data = data[none_count-1:]
    return data

# COMMAND ----------

def convert_list(lst):
    counts = {}
    converted_list = []
    for item in lst:
        if item not in counts:
            counts[item] = 1
            converted_list.append(item)
        else:
            counts[item] += 1
            converted_list.append(item +'_'+ str(counts[item]))

    return converted_list

# COMMAND ----------

# MAGIC %md
# MAGIC Creating the pyspark dataframe from cleaned data

# COMMAND ----------

def create_df (data,header,footer): 
    cleaned_columns = [re.sub(r'[^a-zA-Z0-9]', "_", value.strip().replace('.0','')) if value is not None else ('col_'+str(ind)) for ind,value in enumerate(data[0])]
    columns = cleaned_columns
    if len(cleaned_columns) !=  len(list(set(cleaned_columns))):
        columns = convert_list(cleaned_columns)
    schema = StructType([StructField(name, StringType(), True) for name in columns])
    
    df = spark.createDataFrame(data[1:], schema)
    empty_columns = [column for column in df.columns if df.filter(df[column].isNull()).count() == df.count() ]
    df = df.drop(*empty_columns)

    if len(df.columns) != len(list(set(df.columns))):
        columns = convert_list(cleaned_columns)
        df = spark.createDataFrame(data,columns)
    if header:
        header = '. '.join(header)
        df = df.withColumn("Header",lit(header))
    if footer:
        footer ='. '.join(footer)
        df = df.withColumn("Footer",lit(footer))
    return df

# COMMAND ----------

def cleaned_data(row):
    row = [None if value is None or value.strip() == '' or value.strip() =='nan' else value for value in row]
    return [value for value in row if value is not None]

# COMMAND ----------

# MAGIC %md
# MAGIC Cleaning the raw data from the blob

# COMMAND ----------


def filtered_data(clean_data):
    df_data = clean_data
    data = []
    temp_footer = ''
    header= []
    footer = []
    temp_data=''
    for ind , row in enumerate(df_data):
        row = [None if value is None or value.strip() == '' or value.strip() =='nan' else value for value in row]
        cleaned_row = [value for value in row if value is not None]
        if len(cleaned_row)==0:
            temp_data=''
            continue
        if len(cleaned_row ) > 1 or (len(cleaned_row) == 1 and row[0] is None):
            if temp_data !='' :
                if row[0]==None:
                    row[0]=''
                row[0]=temp_data+' '+row[0]
            else:
                if row[0]!=None :
                    temp_data=''
            data.append(row)
        elif len(data) < 1 :
            header += cleaned_row
        elif len(header)>1 and len(data) > 1 and len(cleaned_row)==1 and(not cleaned_row[0][0].isalpha() or cleaned_row[0].startswith('changed') or cleaned_row[0].startswith('Effective')):
            footer += cleaned_row
        elif len(footer)<1:
            if len(cleaned_data(df_data[ind-1]))==1:   
                temp_data = temp_data+ ' '+cleaned_row[0] 
            else:
                if cleaned_row:
                    temp_data=''
                    temp_data= cleaned_row[0] 

    none_count = 0 
    for value in data :
        
        if (value[0] is None) or (value[0] is None and value[1] is None) or (none_count < 1 and value.count(None) >= 1) or ( value[0] is not None and value[-1] is not None and value[0].isalpha() and value[-1].isalpha()) :
            none_count += 1
        else:
            break 
    if none_count > 0:
        data = concat_xls_row(data,none_count-1)

    if len(data) !=0:
        level_list = []
        level_dict = {}
        clean_data = [data[0]]
        for ind,row in enumerate(data[1:]):
            s = row[0]
            if row[0]!=None:
                current_key = len(s)-len(s.lstrip())
                if current_key == 0 :
                    level_dict = {}
                keys_to_delete = [key for key in level_dict.keys() if key > current_key]
                for key in keys_to_delete:
                    del level_dict[key]
                level_dict[current_key] = s.strip()
                row[0] = ' '.join(level_dict.values())
                cleaned_row = [value for value in row if value is not None]
                if len(cleaned_row) < 2:
                    pass
                else:
                    clean_data.append(row)

  
         
    df = create_df(clean_data,header,footer)

    return df

# COMMAND ----------

def filtered_data_1(clean_data):
    df_data = clean_data
    data = []
    temp_footer = ''
    header= []
    footer = []
    temp_data=''
    for ind , row in enumerate(df_data):
        row = [None if value is None or value.strip() == '' or value.strip() =='nan' else value for value in row]
        cleaned_row = [value.strip() for value in row if value is not None]
        if len(cleaned_row)==0:
            temp_data=''
            continue
        if len(cleaned_row ) > 1 or (len(cleaned_row) == 1 and row[0] is None):
            data.append(row)
        elif len(data) < 1 :
            header += cleaned_row
        elif len(header)>1 and len(data) > 1 and len(cleaned_row)==1 and(not cleaned_row[0][0].isalpha() or cleaned_row[0].startswith('changed') or cleaned_row[0].startswith('Effective')):
            footer += cleaned_row
        elif len(footer)<1:
            if len(cleaned_data(df_data[ind-1]))==1 and row[0][0] !=' ':   
                temp_data = [str(temp_data[0]) + ' ' + value.strip() if i == 0 else value for i, value in enumerate(row)]
                data.append(temp_data)
            else:
                if cleaned_row:
                    temp_data=''
                    temp_data= row 
                    if len(cleaned_data(df_data[ind+1]))!=1 or len(cleaned_data(df_data[ind+1]))==1:
                        data.append(temp_data)
    
    none_count = 0 
    for value in data :
        
        if (value[0] is None) or (value[0] is None and value[1] is None) or (none_count < 1 and value.count(None) >= 1) or ( value[0] is not None and value[-1] is not None and value[0].isalpha() and value[-1].isalpha()) :
            none_count += 1
        else:
            break 

    if none_count > 0:
        data = concat_xls_row(data,none_count-1)
    if len(data) !=0:
        level_list = []
        level_dict = {}
        clean_data = [data[0]]
        for ind,row in enumerate(data[1:]):
            s = row[0]
            cleaned_row = [value for value in row if value is not None]
            if len(cleaned_row)==1 and s=='Patent Matters':
                data[ind+2][0] = s+' '+data[ind+2][0]
                continue
            if row[0]!=None :

                current_key = len(s)-len(s.lstrip())
                if current_key == 0:
                    level_dict = {}
                keys_to_delete = [key for key in level_dict.keys() if key > current_key]
                for key in keys_to_delete:
                    del level_dict[key]
                level_dict[current_key] = s.strip()
                row[0] = ' '.join(level_dict.values())

                if len(cleaned_row) == 1 and (row[0].endswith(':') or s[0] !=' '):
                    pass
                else:
                    clean_data.append(row)
     
    df = create_df(clean_data,header,footer)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC Ignoring FutureWarning error

# COMMAND ----------

import warnings

# Ignore the specific FutureWarning related to iteritems
warnings.filterwarnings("ignore", category=FutureWarning, module="pyspark")

# COMMAND ----------

# MAGIC %md
# MAGIC Processing files and writing in silver layer as a delta tables

# COMMAND ----------

from urllib.parse import quote
bad_records = []
exclude = ['How_to_Query']

for text_path in all_file_paths:
    if not any( excl in text_path.split('/')[-1] for excl in exclude) :
        try:
            file_path = text_path.split('.net/')[-1]
            file_path = quote(file_path, safe="/:")
            file_location = target_path + text_path.split(wasbs_path)[-1]
            link = f'https://usafactsbronze.blob.core.windows.net/bronze/{file_path}'
            xls = pd.ExcelFile(link)
            sheet_dict = pd.read_excel(xls, sheet_name=None ,header = None)
            
            for sheet_name, sheet_data in sheet_dict.items():    
                
                if len(sheet_dict) > 1:
                    target_path = target_path.split('.net/')[-1]
                    file_location = target_path + text_path.split(wasbs_path)[-1] +'/'+sheet_name

                df = sheet_data
                df = df.applymap(lambda x: np.nan if isinstance(x, str) and x.strip()=='' else x)
                df = df.astype(str)
                df = spark.createDataFrame(df)
                first_row = [value for value in df.first()]

                raw_data = df.collect()

                if 'nan' in first_row :
                    if sheet_name=='Table 25 (Multiple BUs' or sheet_name=='Table 24 (Patents and TM)':
                        df= filtered_data_1(raw_data)
                    else:
                        df= filtered_data(raw_data)
                    df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path",f'/mnt/usafactssilver/silver/{file_location}').save()
                    
                else:
                    cleaned_columns = [re.sub(r'[^a-zA-Z0-9]', "_", value.strip().replace('.0','')) if value is not None else ('col_'+str(ind)) for ind,value in enumerate(raw_data[0])]
                    df = spark.createDataFrame(raw_data[1:],cleaned_columns)
                    df = df.select([when(col(c) != 'nan', col(c)).otherwise(None).alias(c) for c in df.columns])
                    df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path",f'/mnt/usafactssilver/silver/{file_location}').save()
                print(file_location,'uploaded sucessfully')
            
        except BaseException as e :
            bad_records.append((text_path,file_location.split('/')[-1],e))
            print('error:',str(e),file_location)

# COMMAND ----------

# MAGIC %md
# MAGIC Catching error files

# COMMAND ----------

if len(bad_records)>= 1:
    pandas_df = pd.DataFrame(bad_records,columns=["URL","File_name","Reason"])
    bad_path = blob_relative_path+'bad_records/bad_record.csv'
    blob_client = container_client.get_blob_client(bad_path)
    csv_file = pandas_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)
    print('error_file_writen',bad_path)

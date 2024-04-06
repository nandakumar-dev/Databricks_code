# Databricks notebook source
#commom library
import re
from pyspark.sql.functions import *
from pyspark.sql.types import *
from io import BytesIO
import pandas as pd
from datetime import datetime

#importing custom library
from data_common_library.credentials import BlobAccount
from data_common_library.azure.client import create_blob_client,create_container_client
 
#getting blob credentials
manual_account = BlobAccount.MANUAL_ACCOUNT
to_wild_container = BlobAccount.MANUAL_WILD_TO_RAW_CONTAINER
to_bronze_catainer=BlobAccount.MANUAL_RAW_TO_BRONZE_CONTAINER
blob_account_key = BlobAccount.AZURE_BLOB_ACCOUNT_KEYS[manual_account]
ingeastion_meta='ingestion-metainfo'
 
#creating container client

ingestion_meta_container_client=create_container_client(manual_account, 'ingestion-metainfo')
to_wild_container_client=create_container_client(manual_account, to_wild_container)
spark.conf.set(f"fs.azure.account.key.{manual_account}.blob.core.windows.net", blob_account_key)

# COMMAND ----------

# blob_relative_path = 'www.irs.gov/pub/irs-soi/'
# ingestion_meta_path = ''
blob_relative_path = dbutils.widgets.get("blob_relative_path")
ingestion_meta_path = dbutils.widgets.get("ingestion_meta_path")

# COMMAND ----------

# MAGIC %pip install xlrd

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

#function for getting files path from the blob
def get_file_paths(dir_path):
    file_paths= []
    files = dbutils.fs.ls(dir_path)
    for file in files:
        allowed_extensions = ['.xls', '.xlsx']
        if file.isDir() :
            file_paths.extend(get_file_paths(file.path))
        if any(file.path.endswith(ext) for ext in allowed_extensions):
            file_paths.append(file.path)

    return file_paths

# COMMAND ----------

if ingestion_meta_path :
    path = f"wasbs://{ingeastion_meta}@{manual_account}.blob.core.windows.net/{ingestion_meta_path}"
    input_txt_file_df = spark.read.csv(path,header=True)
    url_df = input_txt_file_df.select("URL")
    all_file_paths = [row.URL for row in url_df.collect()]
else:
    path = f"wasbs://{to_wild_container}@{manual_account}.blob.core.windows.net/{blob_relative_path}"
    all_file_paths = get_file_paths(path)

# COMMAND ----------

len(all_file_paths)

# COMMAND ----------

#function to create the schema if the data doesn't have header
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

#function to concat the rows for multipe hierarchy in headers
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
                concat_list1 = [(x.strip() if x is not None else ' ') +' '+ (y.strip() if y is not None else ' ') for x, y in zip(concat_list1,temp_list)]
            else:
                concat_list1 =  temp_list
        else:
            concat_list = [(x.strip() if x is not None else ' ') +' '+ (y.strip() if y is not None else ' ') for x, y in zip(concat_list1,data[index])]
            data[index] = concat_list
            
            if none_count == 1 :
                data = data[index:]
            else:
                data = data[none_count-1:]
    return data

# COMMAND ----------

#function for creating spark df from the lists of list 
def create_df (data,header,footer): 
    try:
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
    except BaseException as e :
        print(data)

# COMMAND ----------

#function to split the tables if multiple tables present in single sheet
def seperate_tables(raw_data):
    datas = []
    temp_data = []
    is_header = False
    for ind, row in enumerate(raw_data):
        
        row = [None if value =='nan'or value is None or value.strip() == '' else value for value in row ]
        cleaned_row = [value for value in row if value is not None]
        if ind+1 == len(raw_data):
            datas.append(temp_data)
        if len(cleaned_row) < 1  :
            if ind+2 < len(raw_data) and len(temp_data) > 10 :
            
                nxt_row = [None if value =='nan' or value is None or value.strip() == '' else value for value in raw_data[ind+1] ]
                nxt_cleaned_row = [value for value in nxt_row if value is not None]
                if len(nxt_cleaned_row) > 1 and (raw_data[ind+2].count(None) > 2 or  nxt_row.count(None) > 2)  :
                    if is_header :
                        datas.append(temp_data)
                        temp_data = []
                    is_header = True
                    
            continue
        
        if cleaned_row[0].strip().startswith('Table') :
            if len(temp_data) > 0:
                datas.append(temp_data)
                temp_data = []

            is_header = True
            
        if ( len(row) // 2 > len(cleaned_row) and (len(cleaned_row) != 1) or (len(cleaned_row) == 1 and row[0] is None)) :
            count_strings = 0
            
            for value in row :
                if value is not None and (value.strip()[0].isalpha()):
                    count_strings += 1
            if len(cleaned_row) == 1 :
                nxt_row = [None if value =='nan' or value is None or value.strip() == '' else value for value in raw_data[ind+1] ]
                nxt_cleaned_row = [value for value in nxt_row if value is not None]

            if (len(cleaned_row) != 1 and count_strings > 1) or (len(cleaned_row) == 1  and len(nxt_cleaned_row) > 1)  :
                
                if is_header and len(temp_data) > 10 :
                    datas.append(temp_data)
                    temp_data = []
                    is_header = False
        
        if  len(temp_data) < 1 and len(cleaned_row) > 1:
            is_header = True

        temp_data.append(row)
    return datas

# COMMAND ----------

#function to concat data hierarchy using the space before the first column rows
def arrange_row(data):
    level_list = []
    level_dict = {}
    clean_data = [data[0]]
    temp_value = ''
    data = data[1:]
    for ind,row in enumerate(data):
        s = row[0]
        cleaned_row = [value for value in row if value is not None]
        if len(cleaned_row)== 1 and ind+1 < len(data)- 1 :
            nxt_row = [value for value in data[ind+1] if value is not None]
            if len(nxt_row) == 1 :
                data[ind+1] = [cleaned_row[0]+' '+nxt_row[0]]+[None]*(len(row)-1)
            else:
                temp_value = cleaned_row[0]
            continue
        if s :
            current_key = len(s)-len(s.lstrip())
            if current_key == 0 :
                level_dict = {}
            keys_to_delete = [key for key in level_dict.keys() if key > current_key]
            for key in keys_to_delete:
                del level_dict[key]
            level_dict[current_key] = s.strip()
            row[0] = ' '.join(level_dict.values())
        if temp_value and s :
            row[0] = temp_value + ' ' + row[0]
        clean_data.append(row)
    return clean_data

# COMMAND ----------

#function to split the sheet data into headers, footers and data seperately
def filtered_data(clean_data):
    datas = seperate_tables(clean_data)
    dfs =[]
    temp_header = []
    for df_data in datas :
        data = []
        header = []
        footer = []
        
        if len(df_data) < 1:
            continue
        if df_data[0][-1] == '-1' :
            df_data[0][-1] = None
        if len(list(set(df_data[-1]))) == 1 :
            df_data[-1] = [df_data[-1][0]]+[None]*(len(df_data[-1])-1)
        for ind , row in enumerate(df_data):
            row = [None if  value is None or value.strip() == ''  else value for value in row ]
            if row[0] is not None  and row[0].strip().startswith('*') and len(list(set(row))) == 1 :
                row = [row[0]]+[None]*(len(row[0])-1)
                df_data[ind] = row
            if row[0]=='NUMBER':
                row[0] = None
            cleaned_row = [value for value in row if value is not None]
            if len(cleaned_row) < 1:
                continue
            if len(cleaned_row ) > 1 or (len(cleaned_row) == 1 and row[0] is None ):
                data.append(row)
            elif len(data) < 1 :
                header += cleaned_row 
            else :
                data.append(row)
                footer += cleaned_row  
            if footer :
                if len(cleaned_row) > 1: 
                    
                    if len(footer) > 1 :
                      data = data[:-len(footer)-1] 
                      temp = [' '.join(footer)]+ [None]*(len(row)-1)
                      
                      data.append(temp)
                      data.append(row) 
                    footer =[]
                   
                if ind+1 == len(df_data) :
                    data = data[:-len(footer)]
        
        none_count = 0 
        exclude_none = 0
        
        for value in data :
            if value.count(None) > 1 or (none_count <= 1 and value.count(None) == 1) or value[0] is None :
                none_count += 1
            else:
                if exclude_none > 2:
                    break
                else:
                    none_count += 1
                    exclude_none += 1
        
        if none_count > 0:
            if data[len(data)-1][0] is None :
                data.pop(len(data)-1)
            if none_count == len(data):
                none_count -=  1
           
            if len(data) < 5 or len(data) < none_count :
                continue
            
            while data[none_count][0] is not None  :
                none_count -= 1
                if none_count == 0:
                    break
            
            if none_count > 1:
                if data[none_count][1]:
                    cleaned_data = re.sub(r'[()\[\]-]', '', data[none_count][1])
                elif data[none_count][2]:
                    cleaned_data =re.sub(r'[()\[\]-]', '', data[none_count][2])
                else:
                    cleaned_data=''
                if data[none_count].count(None) > 3:
                    if not cleaned_data.isdigit():
                        data[none_count] = data[none_count][1:3]+data[none_count][2:]
                        none_count -= 1
                        if data[none_count][1]:
                            cleaned_data = re.sub(r'[()\[\]-]', '', data[none_count][1])
                        elif data[none_count][2]:
                            cleaned_data = re.sub(r'[()\[\]-]', '', data[none_count][2])
                        else:
                            cleaned_data=''
                if cleaned_data.isdigit():    
                    data.pop(none_count) 
                else:
                    none_count += 1
                
                if none_count > 1:
                    header_rows = data[:none_count]
                    data = concat_xls_row(data,none_count)
                    if data[0][0].strip()=='' :
                        temp_data = [row[1:2]+row[1:] for row in header_rows]
                        data = temp_data+data[1:]
                        data = concat_xls_row(data,none_count)
            if none_count == 1:
                data.pop(none_count)      
        
        if len(data) > 1:
            if header :
                temp_header = header
            else:
                header = temp_header
            data = arrange_row(data)
            
            # data = make_first_element_unique(data)
            df = create_df(data,header,footer)
            dfs.append(df)
         
    return dfs

# COMMAND ----------

import warnings

# Ignore the specific FutureWarning related to iteritems
warnings.filterwarnings("ignore", category=FutureWarning, module="pyspark")

# COMMAND ----------

def split_dfs(sheet_data) :
    try:
        df = sheet_data
        df = df.applymap(lambda x: np.nan if isinstance(x, str) and x.strip()=='' else x)
        df = df.dropna(axis=1, how='all')
        df = df.astype(str)
        df = spark.createDataFrame(df)    

        # Iterate over the columns and create separate DataFrames
        current_df = None
        start_index = 0
        dfs = []
        
        # Get the columns of the DataFrame
        columns = df.columns

        # Specify the first column
        first_column = columns[0]
        for ind, col_name in enumerate(columns):
            # Skip the first column
            if col_name == first_column:
                continue
            filtered_df = df.filter(col(col_name).startswith("Table"))
            # If the condition is True, create a new DataFrame
            if not filtered_df.isEmpty() :
                current_df = df.select(df.columns[start_index:ind-1])
                
                start_index = ind
                dfs.append(current_df)
        current_df = df.select(df.columns[start_index:])
        dfs.append(current_df)
        return dfs
    except BaseException as e :
        return []

# COMMAND ----------

from urllib.parse import quote
bad_records = []
for text_path in all_file_paths:
    try:
        text_path = text_path.replace('https://','')
        file_path = text_path.split('.net/')[-1]
        file_path = quote(file_path, safe="/:")
        blob_clien=to_wild_container_client.get_blob_client(file_path)
        blob=blob_clien.download_blob().readall()
        file_location='/mnt/usafactsdpmanual/to-bronze/'+file_path
        
        xls = pd.ExcelFile(BytesIO(blob))
        sheet_dict = pd.read_excel(xls, sheet_name=None ,header = None)
        for sheet_name, sheet_data in sheet_dict.items():
            if len(sheet_dict) > 1:
                file_location ='/mnt/usafactsdpmanual/to-bronze/' + file_path+'/'+sheet_name
            if sheet_data.empty:
                continue
            raw_dfs = split_dfs(sheet_data)
            footer_value = []
            footer_dfs =[]
            final_dfs = []
            for raw_df in raw_dfs :
                null_columns = [col_name for col_name in raw_df.columns if  raw_df.count() - raw_df.filter(col(col_name) == 'nan').count() < 2]
                
                # Filter out the identified columns
                raw_df = raw_df.select([col(col_name) for col_name in raw_df.columns if col_name not in null_columns])
                first_row = [value for value in raw_df.first()]
                raw_data = raw_df.collect()
                
                if 'nan' in first_row or None in first_row :
                    dfs = filtered_data(raw_data)   
                    for df in dfs:
                        if 'Footer' not in df.columns :
                            footer_dfs.append(df)
                        else:
                            temp_footer_value = df.select('Footer').collect()[0][0] 
                            if 'Footnotes at end of table' in temp_footer_value.strip() :
                                df = df.drop("footer")
                                footer_dfs.append(df)
                            else:
                                footer_value = df.select('Footer').collect()[0][0]
                                final_dfs.append(df)
                else:
                    cleaned_columns = [re.sub(r'[^a-zA-Z0-9]', "_", value.strip().replace('.0','')) if value is not None else ('col_'+str(ind)) for ind,value in enumerate(raw_data[0])]
                    df = spark.createDataFrame(raw_data[1:],cleaned_columns)
                    df = df.select([when(col(c) != 'nan', col(c)).otherwise(None).alias(c) for c in df.columns])
                    final_dfs.append(df)
            
            for temp_df in footer_dfs:
                if footer_value :
                    temp_df = temp_df.withColumn('Footer', lit(footer_value))
                final_dfs.append(temp_df)
            if len(final_dfs)>1 :
                for ind,df in enumerate(final_dfs): 
                    file_location_suffix = file_location + '_' + str(ind)
                    df = df.select([col(col_name).alias(col_name[:255]) for col_name in df.columns])
                    df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").option("path",file_location_suffix).save()
                    print(file_location+'_'+str(ind),'uploaded sucessfully')
            else:
                df = final_dfs[0]
                df = df.select([col(col_name).alias(col_name[:255]) for col_name in df.columns])
                
                df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").option("path",file_location).save()
                print(file_location,'uploaded sucessfully')
    except BaseException as e :
        bad_records.append((text_path,file_location.split('/')[-1],str(e)))
        print(e,file_location)

# COMMAND ----------

#upload error logs to ingestion-metainfo container
if len(bad_records) !=0:
    run_datetime  = datetime.now()
    pandas_df = pd.DataFrame(bad_records,columns=["URL","File_name","Reason"])
    bolb_path=blob_relative_path.replace('/','_').replace('.csv','')
    filelocation = 'bad_records/'+ f"{bolb_path}_{run_datetime}.csv"
    blob_client = ingestion_meta_container_client.get_blob_client(f"{filelocation}")
    csv_file = pandas_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)
    print('error_file_uploaded',filelocation)

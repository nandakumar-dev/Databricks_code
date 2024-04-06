# Databricks notebook source
# MAGIC %md
# MAGIC Importing Mountpoints Notebook

# COMMAND ----------

# MAGIC %run "../Templates/mountpoints"

# COMMAND ----------

#getting notebook parameters
# blob_relative_path='https_usafactsbronze.blob.core.windows.net/meta_info/www2.census.gov/Census_Government_Finance/Census_Government_Finance_txtfiles_recordlayout_code_layout.csv'
blob_relative_path = dbutils.widgets.get('blob_relative_path')

# COMMAND ----------

# MAGIC %md
# MAGIC Installing Necessary Packages

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %pip install xlrd

# COMMAND ----------

# MAGIC %md
# MAGIC Importing necessary Packages

# COMMAND ----------

from urllib.error import HTTPError

# COMMAND ----------

#function for getting all the files from the blob
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

# Reading file paths of the files
all_file_paths = get_file_paths(bronze+blob_relative_path)

# COMMAND ----------

#function for concating the rows and headers
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

# #function for creating the dataframe

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
        print("Error",e,data)

# COMMAND ----------

def cleaned_data(row):
    row = [None if value is None or value.strip() == '' or value.strip() =='nan' else value for value in row]
    return [value for value in row if value is not None]

# COMMAND ----------

import warnings
# Ignore the specific FutureWarning related to iteritems
warnings.filterwarnings("ignore", category=FutureWarning, module="pyspark")

# COMMAND ----------

#function for filtering the data
def filtered_data(clean_data):
    df_data = clean_data
    # print(df_data)
    data = []
    temp_footer = ''
    header= []
    footer = []
    for ind , row in enumerate(df_data):
        row = [None if value is None or value.strip() == '' or value.strip() =='nan'  else ' '+value for value in row ]
        cleaned_row = [value for value in row if value is not None]
        # Values for which spaces should be removed
        specific_values = ['Revenue1', 'Expenditure1','Debt outstanding','Cash and security holdings']

        # Removing spaces only from the specific values in the list
        row = [value.strip() if value is not None and value.strip() in specific_values else value for value in row]

        if (len(cleaned_row ) > 1 or (len(cleaned_row) == 1 and row[0] is None) or (cleaned_row[0].endswith(':') and len(cleaned_data(df_data[ind-1]))!=1)) and not any('.pdf' in col for col in cleaned_row) :
            data.append(row)
        elif len(data) < 1 :
            header += cleaned_row
        elif len(header)>1 and len(data) > 1 and len(cleaned_row)==1 :
            footer += cleaned_row


    none_count = 0 
    for value in data :
        if value[0] is None or value.count(None) >=1 or(value[0].isalpha() and value[1].isalpha()):
            none_count += 1
        else:
            break 
    if none_count > 0:
        data = concat_xls_row(data,none_count)
    
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

    df = create_df(data,header,footer)
    return df

# COMMAND ----------

# Find the column to drop based on the conditions
def contains_line_value(column):
    return df.filter(col(column) == 'Line').count() > 0

# COMMAND ----------

#iterating the file list and processing the files
bad_records = []

exclude = ['old']
   
filtered_urls = [url for url in all_file_paths if int(url.split('/')[8]) > 2009]
for text_path in filtered_urls:
    if not any( excl in text_path.split('/')[-1] for excl in exclude) :
  
        try:
            file_path = text_path.split(bronze)[-1]
            try:
                link = f'https://usafactsbronze.blob.core.windows.net/bronze/{file_path}'
                xls = pd.ExcelFile(link)
                sheet_dict = pd.read_excel(xls, sheet_name=None ,header = None)
            except HTTPError as e:
                blob_client = container_client.get_blob_client(file_path)
                local_file_path = "local_file.xlsx"
                # Download the blob content
                with open(local_file_path, "wb") as f:
                    data = blob_client.download_blob()
                    data.readinto(f)
                # Create ExcelFile object
                xls = pd.ExcelFile(local_file_path)
                sheet_dict = pd.read_excel(xls, sheet_name=None ,header = None)
                # Remove the local file
                if os.path.exists(local_file_path):
                    os.remove(local_file_path)

            file_path='.'.join(file_path.split('.')[:-1])
            for sheet_name, sheet_data in sheet_dict.items():
                file_location =  silver+file_path +'/'+sheet_name
                file_location=file_location.replace(' ','_')
                df = sheet_data
                df = df.applymap(lambda x: np.nan if isinstance(x, str) and x.strip()=='' else x)
                df = df.dropna(axis=0, how='all').dropna(axis=1, how='all')
                df = df.astype(str)
                df = spark.createDataFrame(df)
                
                # Get columns to drop
                columns_to_drop = [col_name for col_name in df.columns if contains_line_value(col_name) ]
                # Drop columns
                df = df.select([col_name for col_name in df.columns if col_name not in columns_to_drop])

                # Find the column to drop based on the conditions
                if all(col_name in df.columns for col_name in ['1', '2']):
                    df = df.filter(~((col('1') == 'Description') & (col('2') == 'C1'))).filter(~((col('1') == 'Description') & (col('2') == 'c1')))

                if df.first()[0] == 'nan' and df.first()[1] != 'nan':
                    # Drop the first column
                    df = df.select(*df.columns[1:])

                first_row = [value for value in df.first()]
                raw_data = df.collect() 
                if len(first_row)>=1 and not ('nan' in first_row) :
                    columns= [f'col_{ind}' for ind in range(len(raw_data[0]))]
                    df = spark.createDataFrame(raw_data,columns)
                    df = df.select([when(col(c) != 'nan', col(c)).otherwise(None).alias(c) for c in df.columns])
                    df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save(file_location)
                else:
                    df= filtered_data(raw_data)
                    if all(col_name in df.columns for col_name in ['Description']):
                        df = df.filter(~col("Description").endswith(':'))
                    df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save(file_location)
                print('files_writen',file_location)
        except BaseException as e :
            bad_records.append((text_path,file_path.split('/')[-1],e))
            print('error:',str(e),file_path)
            




# COMMAND ----------

#uploading the error logs to blob
if len(bad_records)>= 1:
    pandas_df = pd.DataFrame(bad_records,columns=["URL","File_name","Reason"])
    error_records=bronze_meta_info.split('/')[0]
    bolb_path=blob_relative_path.replace('/','_').replace('.csv','')
    filelocation = f'{error_records}'+'/bad_records/'+ f"meta_info/{bolb_path}_{run_datetime}.csv"  
    blob_client = container_client.get_blob_client(filelocation)
    csv_file = pandas_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)
    print('File_written',filelocation)

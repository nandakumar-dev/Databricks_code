# Databricks notebook source
#importing mountpoints notebook
%run "../Templates/mountpoints"

# COMMAND ----------

#getting notebook parameters
blob_relative_path = dbutils.widgets.get('file_path')

# COMMAND ----------

#installing packages
pip install openpyxl

# COMMAND ----------

pip install xlrd

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

#function for creating the dataframe
def create_df (data,header,footer): 
    # if data:
    column_names=cleaned_columns = [re.sub(r'[^a-zA-Z0-9=()-]', "", value.strip()) if value is not None else None  for value in data[0]]
    
    if (any(col.isalpha() if col!= None else False for col in column_names) and any('=' in col for col in column_names if col!=None )) or (all(not col.isalpha() if col!= None else False for col in column_names[1:] ) and all('-' not in col for col in column_names[1:]) and all('(' not in col for col in column_names[1:]) and column_names[0]!='' ) or any('Sheet' in col for col in column_names if col!=None)  :
        columns= [f'col_{ind}' for ind in range(len(data[0]))]
        df = spark.createDataFrame(data,columns)
                                   
    else:
        cleaned_columns = [re.sub(r'[^a-zA-Z0-9%#]', "_", value.strip().replace('.0','')) if value is not None and len(value.strip())>1 else ('col_'+str(ind)) for ind,value in enumerate(data[0])]
        columns = cleaned_columns
        if len(cleaned_columns) !=  len(list(set(cleaned_columns))):
            columns = convert_list(cleaned_columns)
        schema = StructType([StructField(name, StringType(), True) for name in columns])
        
        df = spark.createDataFrame(data[1:], schema)
        empty_columns = [column for column in df.columns if df.filter(df[column].isNull()).count() == df.count() ]
        df = df.drop(*empty_columns)

    if len(df.columns) != len(list(set(df.columns))):
        cleaned_columns = [re.sub(r'[^a-zA-Z0-9]', "_", value.strip().replace('.0','')) if value is not None else ('col_'+str(ind)) for ind,value in enumerate(data[0])]
        columns = convert_list(cleaned_columns)
        df = spark.createDataFrame(data[1:],columns)
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

import warnings

# Ignore the specific FutureWarning related to iteritems
warnings.filterwarnings("ignore", category=FutureWarning, module="pyspark")

# COMMAND ----------

#function for filtering the data
def filtered_data(clean_data):
    df_data = clean_data

    data = []
    temp_footer = ''
    header= []
    footer = []
    for ind , row in enumerate(df_data):
        row = [None if value is None or value.strip() == '' or value.strip() =='nan'  else value for value in row ]
        cleaned_row = [value for value in row if value is not None]

        if len(cleaned_row ) > 1 or (len(cleaned_row) == 1 and row[0] is None and ('=' not in cleaned_row[0]) and ('*' not in cleaned_row[0])) or (len(cleaned_row) == 1 and (cleaned_row[0].strip().isdigit() or cleaned_row[0].replace('.', '', 1).isdigit())) or (row[0]is not None and len(cleaned_row) == 1 and len(row[0])==2) or (len(cleaned_row) == 1 and row.count(None)==3 and cleaned_row[0].strip().replace(' ','').isalpha()) :
            data.append(row)
        elif len(data) < 1 :
            header += cleaned_row 
        else :
            footer += cleaned_row  
       
    none_count = 0 
    for value in data :
        
        if value[0] is None or (value.count(None) ==4 and value[2]==None and value[1]!=None) or (value.count(None) ==8 and value[12]!=None )or (value.count(None) > 2 and (value[1] is not None and value[2] is not None and value[-1] is None and value[-2] is not None) ) or (all(col.replace(' ','').replace('(','').replace(')','').replace('.','').isalpha() for col in value if col!=None) and value.count(None)<6 ):
            none_count += 1
        else:
            break 

    if none_count==1:
        data[0]= [f'col_{ind}' if value is None  else value for ind,value in enumerate(data[0])]
    if none_count > 1:
        data = concat_xls_row(data,none_count)

    if none_count==0 and data[0].count(None)>3:
        columns= [f'col_{ind}' for ind in range(len(data[0]))]
        df = spark.createDataFrame(data,columns)
        if header:
            header = '. '.join(header)
            df = df.withColumn("Header",lit(header))
        if footer:
            footer ='. '.join(footer)
            df = df.withColumn("Footer",lit(footer))
    else:   
        df = create_df(data,header,footer)

    return df

# COMMAND ----------

#iterating the file list and processing the files
bad_records = []

for text_path in all_file_paths:
    try:
        file_path = text_path.split(bronze)[-1]
        link = f'https://usafactsbronze.blob.core.windows.net/bronze/{file_path}'
        xls = pd.ExcelFile(link)
        sheet_dict = pd.read_excel(xls, sheet_name=None ,header = None)
        file_path='.'.join(file_path.split('.')[:-1])
        for sheet_name, sheet_data in sheet_dict.items():
            if len(sheet_dict) > 1:
                file_location =  silver+file_path +'/'+sheet_name
                file_location=file_location.replace(' ','_')
            df = sheet_data
            df = df.applymap(lambda x: np.nan if isinstance(x, str) and x.strip()=='' else x)
            df = df.dropna(axis=0, how='all').dropna(axis=1, how='all')
            # Find the column to drop based on the conditions
            column_to_drop = df.columns[((df.isna().sum() == (len(df) - 1)) | (df.isna().sum() == (len(df) - 2))) & (df.apply(lambda col: '*' in col.values))].tolist()
            # Drop the identified column
            df = df.drop(columns=column_to_drop)
            df = df.astype(str)
            df = spark.createDataFrame(df)
            first_row = [value for value in df.first()]
            raw_data = df.collect() 
            if len(first_row)==1 and not ('nan' in first_row) :
                columns= [f'col_{ind}' for ind in range(len(raw_data[0]))]
                df = spark.createDataFrame(raw_data,columns)
                df = df.select([when(col(c) != 'nan', col(c)).otherwise(None).alias(c) for c in df.columns])
                # df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save(file_location)
            else:
                df= filtered_data(raw_data)
                # df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save(file_location)
            print('files_writen',file_location)
    except BaseException as e :
        bad_records1.append((text_path,file_location.split('/')[-1],e))
        print('error:',str(e),file_location)

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

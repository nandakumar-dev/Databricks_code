# Databricks notebook source
def get_file_paths(dir_path,file_type):
    file_paths= []
    files = dbutils.fs.ls(dir_path)
    for file in files:
        if file.isDir() :
            file_paths.extend(get_file_paths(file.path,file_type))
        if file_type :
            if any(file.path.endswith(ext) for ext in file_type):
                file_paths.append(file.path)
        else:
            file_paths.append(file.path)
    return file_paths

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

def split_dfs(sheet_data) :
    try:
        df = sheet_data
        df = df.applymap(lambda x: np.nan if isinstance(x, str) and x.strip()=='' else x)
        df = df.dropna(axis=1, how='all')
        df = df.astype(str) 
        df = spark.createDataFrame(df)
        # Get the columns of the DataFrame
        columns = df.columns 

        # Specify the first column
        first_column = columns[0]

        # Iterate over the columns and create separate DataFrames
        current_df = None
        start_index = 0
        dfs = []
        for ind, col_name in enumerate(columns):
            # Skip the first column
            if col_name == first_column:
                continue
            filtered_df = df.filter(col(col_name).startswith("Table"))
            # If the condition is True, create a new DataFrame
            if not filtered_df.rdd.isEmpty() :
                c_df = df.select(df.columns[start_index:ind-1])
                columns_to_drop = [column for column in c_df.columns if (c_df.filter(col(column).isNotNull() & (col(column) != "nan")).count() <= 1)]
                # Drop identified columns

                current_df = c_df.drop(*columns_to_drop)
                start_index = ind
                dfs.append(current_df)
        c_df = df.select(df.columns[start_index:])
        columns_to_drop = [column for column in c_df.columns if (c_df.filter(col(column).isNotNull() & (col(column) != "nan")).count() <= 1)]
        # Drop identified columns
        current_df = c_df.drop(*columns_to_drop)
        dfs.append(current_df)
        return dfs
    except BaseException as e :
        print(e)
        return []

# COMMAND ----------

def write_bad_record(bad_records,bad_folder):
    run_datetime  = datetime.now()
    pandas_df = pd.DataFrame(bad_records,columns=["URL","File_name","Reason"])
    bad_path = bad_folder +f'/bad_records/{run_datetime}_bad_record.csv'
    blob_client = container_client.get_blob_client(bad_path)
    csv_file = pandas_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)
    print(bad_path,'uploaded file successfully')

# COMMAND ----------

bad_records = []
def process_file(text_path,silver=silver):
    global bad_records
    try :
        text_path = text_path.replace('https://','')
        file_path = text_path.split('.net/')[-1]
        file_path = quote(file_path, safe="/:")
        file_location = silver + file_path
        link = bronze_http + file_path
        xls = pd.ExcelFile(link)
        sheet_dict = pd.read_excel(xls, sheet_name=None ,header = None)
        for sheet_name, sheet_data in sheet_dict.items():
            if len(sheet_dict) > 1:
                file_location =silver + file_path+'/'+sheet_name
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
                    # display(df)
                    df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").option("path",file_location+'_'+str(ind)).save()
                    print(file_location+'_'+str(ind),'uploaded sucessfully')
            else:
                df = final_dfs[0]
                # display(df)
                df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").option("path",file_location).save()
                print(file_location,'uploaded sucessfully')
    except BaseException as e :
        bad_records.append((text_path,file_location.split('/')[-1],e))
        print(e,file_location)
    return bad_records

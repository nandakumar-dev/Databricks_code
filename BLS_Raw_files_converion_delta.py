# Databricks notebook source
#importing Mountpoints notebook
%run "../Templates/mountpoints"

# COMMAND ----------

#getting parameters
blob_relative_path = dbutils.widgets.get('blob_relative_path')

# COMMAND ----------

#assigning mountpoints
input_mount_point=bronze
target_mount_point=silver

# COMMAND ----------

#function for getting all the file paths
def get_file_paths(dir_path):
    file_paths = []
    
    # Use dbutils.fs.ls with display to get file information
    files = dbutils.fs.ls(dir_path)
    
    for file in files:
        if file.isDir():
            # If it's a directory, recursively call the function
            file_paths.extend(get_file_paths(file.path))
        else:
            # If it's a file, append the path to the list
            file_paths.append(file.path)

    return file_paths


# COMMAND ----------

all_file_paths = get_file_paths(input_mount_point+blob_relative_path)

# COMMAND ----------

#function for processing the error files
def try_again(text_path,path):

    lines_rdd = spark.sparkContext.textFile(text_path)
    lines_rdd = lines_rdd.filter(lambda line: line.strip() != '')
    values_rdd = lines_rdd.map( lambda line: line.split('\t')).map(lambda words: [word for word in words] )
    column_names = values_rdd.first()
    raw_data = values_rdd.collect()
    data_rdd = values_rdd.filter(lambda line: line != column_names)
    columns =[re.sub(r'[^A-Za-z0-9]+','_',col.strip()) for col in column_names if col != '']
    text_data =[]
    for data in raw_data:
        if '' in data and len(data) != len(columns):
            data = list(data)
            data.remove('')
            while len(data) > len(columns) and '' in data:
                data.remove('')
            text_data.append(data)
        else:
            text_data.append(data)
    data_rdd = sc.parallelize(text_data[1:])
    if text_path.endswith('.MapErrors'):
        text_data = raw_data[0]
        data=[]
        temp_tuple=[]
        for value in text_data:
            if value != '':
                temp_tuple.append(value)
            if len(temp_tuple)==5:
                data.append(tuple(temp_tuple))
                temp_tuple =[]   
        columns = data[0]
    for col in columns :
        if col.isnumeric():
            cols = []
            for ind in range(len(columns)):
                cols.append('col_'+str(ind+1))
            data_rdd =values_rdd
            columns = cols
    if text_path.endswith('.MapErrors'):
        df = spark.createDataFrame(data,columns) 
        df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path",path).save()
        return df
    df = data_rdd.toDF(columns)
    if df.columns[-1].startswith('_') or len(raw_data[1]) != len(columns):
        text_data = []
        if raw_data[2][-1]=='':
            for text in raw_data :
                if text[-1]=='':
                    text_data.append(text[:-1])
                else:
                    text_data.append(text)
            columns =[re.sub(r'[^A-Za-z0-9]+','_',col.strip()) for col in text_data[0]]
            data = text_data[1:]
            for col in columns :
                if col.isnumeric():
                    cols = []
                    for ind in range(len(columns)):
                        cols.append('col_'+str(ind+1))
                    columns = cols
                    data = text_data
                    break
        else:
            columns = []
            
            for ind in range(len(raw_data[1])):
                columns.append('col_'+str(ind+1))
            raw_data_col = raw_data[0]
            for index in range(len(columns)-len(raw_data[0])):
                raw_data_col.append(' ')
            data = []
            data.append(raw_data_col)
            data.extend(raw_data[1:])
        df = spark.createDataFrame(data,columns)
    df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path",path).save()


# COMMAND ----------

#function for processing the column names 
def proceed_to_df(text_data):
   
    columns =[re.sub(r'[^A-Za-z0-9]+','_',col.strip()) for col in text_data[0] if col != '' ]
    
    for col in columns :
        if col.isnumeric():
            cols = []
            for ind in range(len(columns)):
                cols.append('col_'+str(ind+1))
            columns = cols
            df = spark.createDataFrame(text_data,columns)
            return df
    df = spark.createDataFrame(text_data[1:],columns)
    return df

# COMMAND ----------

#reading the files from the list and processing files to delta foramt
dfs = []
bad_record = []
bad_records = []
exclude = ['.exe','.txt','.contacts','.zip','.tar','.xml','.gz','README','.doc','.pdf','.gif','.sf']

for text_path in all_file_paths :

    if not any( excl in text_path.split('/')[-1] for excl in exclude) :
        if text_path.endswith('.csv'):
            df = spark.read.csv(text_path,header = True,inferschema = True)
            df = spark.read.option("multiLine","true").csv(text_path,header = True,inferschema = True)
            dfs.append((df,text_path))
            continue
        try: 
            lines_rdd = spark.sparkContext.textFile(text_path)
            lines_rdd = lines_rdd.filter(lambda line: line.strip() != '')
            values_rdd = lines_rdd.map( lambda line: line.split('\t')).map(lambda words: [tuple(re.split(r'\s\s+',word.strip())) for word in words] ) 
            text_data = values_rdd.collect()
            formatted_data =[]
            for text in text_data:
                tuple_data = ()
                if len(text)>1 : 
                    for word in text:
                        tuple_data += word
                    formatted_data.append(tuple_data)
                else:
                    formatted_data.append(text[0])       
            column_names =[col for col in formatted_data[0]]  
            text_data = []
            text_data.append(column_names)
            text_data.extend(formatted_data[1:])
        except BaseException as e:
            try:
                lines_rdd = spark.sparkContext.textFile(text_path)
                lines_rdd = lines_rdd.filter(lambda line: line.strip() != '')
                values_rdd = lines_rdd.map( lambda line: line.split('\t')).map(lambda words: [word for word in words] )
                column_names = values_rdd.first()
                data_rdd = values_rdd.filter(lambda line: line != column_names)
                column_names =[re.sub(r'[^A-Za-z0-9]+','_',col.strip()) for col in column_names if col != '' ]
                df = data_rdd.toDF(column_names)
                dfs.append((df,text_path)) 
                continue
            except BaseException as e:
                bad_record.append(text_path)
                continue

        try:
            if len(text_data) == 1:
                schema = StructType([ StructField(re.sub(r'[^A-Za-z0-9]+','_',name.strip()),StringType(),True) for name in text_data[0] if name != ''])
                df= spark.createDataFrame([],schema)
                dfs.append((df,text_path))
                continue
            if len(column_names) <= 2 :
                if any(re.match(r'^[- ]*$',line[0]) for line in text_data[:10]) :
                    for ind,data in enumerate(text_data):
                        if re.match(r'^[- ]*$',data[0]) :
                            data =[]
                            columns = text_data[ind-1]
                            break
                
                    if len(columns) < len(text_data[ind+1]):
                        columns = [col.split() for col in columns][0]

                    if len(columns) == len(text_data[ind+1]):
                        data.append(columns)
                        data.extend(text_data[ind+1:]) 
                        df = proceed_to_df(data)
                        dfs.append((df,text_path))
                        continue

            if len(column_names) < len(text_data[1]):
                if text_data[1][-1] == '':
                    data =[]
                    data.append(column_names)
                    for text in text_data[1:] :
                        data.append(tuple(list(text)[:-1]))
                    text_data = data
                if len(column_names) == len(text_data[1]) :
                    df = proceed_to_df(text_data)
                    dfs.append((df,text_path))
                    continue

                if len(text_data[1]) == len(text_data[2]):
                    columns=[text.strip() for text in text_data[1] ]
                    df = spark.createDataFrame(text_data[2:],columns)
                    new_df = df.drop(col(''))
                    if len(column_names) == len(new_df.columns):
                        data=[]
                        columns=[text.strip() for text in text_data[1] if text !='']
                        data.append(column_names)
                        data.append(tuple(columns))
                        df_data =[tuple(row) for row in new_df.collect()]
                        data.extend(df_data)
                        df = proceed_to_df(data)
                        dfs.append((df,text_path))
                        continue    
                    else:
                        bad_record.append(text_path)
                        continue
            else:
                df = proceed_to_df(text_data)
                dfs.append((df,text_path)) 
                continue 
        except BaseException as e:
            bad_record.append(text_path)
            continue
    else :
        bad_records.append((text_path,text_path.split('/')[-1]))

# COMMAND ----------

#iterating the dataframes and converting to delta format
for df in dfs:
    file_location=target_mount_point+df[1].split(input_mount_point)[-1]
    try:
        df[0].write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path",file_location).save()
        print('files_written:' ,file_location)
    except BaseException as e:
        if df[1] not in bad_record:
            bad_record.append(df[1])

# COMMAND ----------

bad_record

# COMMAND ----------

#processing the error files 
for record in bad_record:
    file_location=target_mount_point+record.split(input_mount_point)[-1]
    try:
        try_again(record,file_location)
        print('files_written:',file_location)
    except BaseException as e :
        bad_records.append((record,record.split('/')[-1]))

# COMMAND ----------

bad_records

# COMMAND ----------

#uploading the error logs to the blob
if len(bad_records)> 1:
    pandas_df = pd.DataFrame(bad_records,columns=["File_path","File_name"])
    error_records=bronze_meta_info.split('/')[0]
    bolb_path=blob_relative_path.replace('/','_').replace('.csv','')
    filelocation = f'{error_records}'+'/bad_records/'+ f"meta_info/{bolb_path}_{run_datetime}.csv"

    blob_client = container_client.get_blob_client(filelocation)
    csv_file = pandas_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)
    print('error_file_written',filelocation)

# Databricks notebook source
from data_common_library.credentials import BlobAccount
from data_common_library.azure import helpers
from data_common_library.azure import client

# COMMAND ----------

#getting file path 
# blob_path='https_usafactsbronze.blob.core.windows.net/meta_info/www2.census.gov/Census_Government_Finance/Census_Government_Finance_txtfiles_recordlayout_code_layout.csv'
blob_path=dbutils.widgets.get('blob_path')

# COMMAND ----------

# # Azure storage access info 
blob_account = BlobAccount.MANUAL_ACCOUNT
blob_account_key = BlobAccount.AZURE_BLOB_ACCOUNT_KEYS[blob_account]
blob_container = BlobAccount.MANUAL_RAW_TO_BRONZE_CONTAINER
to_wild_container = BlobAccount.MANUAL_WILD_TO_RAW_CONTAINER

spark.conf.set(f"fs.azure.account.key.{blob_account}.blob.core.windows.net", blob_account_key)
path = f"wasbs://{to_wild_container}@{blob_account}.blob.core.windows.net/{blob_path}"

url_df = spark.read.csv(path,header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC creating a list of tuples that will be used to split the columns of a dataset based on their index positions

# COMMAND ----------

def tuples_list(code_book_df):
    df_no_duplicates =code_book_df.dropDuplicates(["START", "Length","Description"]) 
    selected_columns = ["START", "Length","Description"]
    list_of_tuples = df_no_duplicates.select(selected_columns).collect()
    split_columns=[]
    for Tape_start,length,Field_name in list_of_tuples :
        column_name = 'value'
        split_columns.append(expr(f"substring({column_name}, {Tape_start},{length})").alias(Field_name))
    return split_columns


def tuples_list_with_state_type_code(code_book_df):
    df_no_duplicates =code_book_df.dropDuplicates(["START", "Length","Description"]) 
    selected_columns = ["START", "Length","Description"]
    list_of_tuples = df_no_duplicates.select(selected_columns).collect()
    split_columns=[]
    for Tape_start,length,Field_name in list_of_tuples :
        column_name = 'value'
        split_columns.append(expr(f"substring({column_name}, {Tape_start},{length})").alias(Field_name))

    split_columns.append(expr(f"substring({column_name}, 1,2)").alias('State code'))
    split_columns.append(expr(f"substring({column_name}, 3,1)").alias('Type code'))   
    return split_columns

# COMMAND ----------

# MAGIC %md
# MAGIC Mapping the text files with the layout using the Index and length

# COMMAND ----------

error_list=[]
collected_list=url_df.collect()
for file_path,record_layout_path,code_layout_path in collected_list:
    try:
        txt_file_blob_path =f"wasbs://{to_wild_container}@{blob_account}.blob.core.windows.net/{file_path}"
        record_layout_path = bronze+record_layout_path.replace('.pdf','/')
        code_layout_path = bronze+code_layout_path

        file_name=file_path.split("/")[-1].split(".")[0]
        folder_path='/'.join(file_path.split("/")[:-1])

        output_blob_path1= f'{folder_path}/{file_name}_first_view'
        output_blob_path2= f'{folder_path}/{file_name}_second_view'

        #reading files from the blob
        input_txt_file_df = spark.read.text(txt_file_blob_path)
        code_book_df=spark.read.csv(record_layout_path,header=True,multiLine=True)
        code_layout_df=spark.read.csv(code_layout_path,header=True,multiLine=True)

        #cleaning the codebook data for mapping
        code_book_df = code_book_df.withColumn("Description", regexp_replace(col("Description"), " ", "_"))
        split_col = split(code_book_df["Positions"], "-")
        code_book_df = code_book_df.withColumn("START", split_col.getItem(0))  
        code_book_df=code_book_df.dropDuplicates()

        #mapping the values from the tuples_list function
        if  'Fin_PID' in txt_file_blob_path or 'modp_pu' in txt_file_blob_path:
            input_txt_file_df = input_txt_file_df.select("*",*tuples_list_with_state_type_code(code_book_df)).drop('value')
        else:
            input_txt_file_df = input_txt_file_df.select("*",*tuples_list(code_book_df)).drop('value')
        new_columns = [col(c).alias(c.replace(" ", "_").replace(",", "_").replace(";", "_")
                            .replace("{", "_").replace("}", "_")
                            .replace("(", "_").replace(")", "_")
                            .replace("\n", "_").replace("\t", "_").replace("=", "_"))
                for c in input_txt_file_df.columns]

        # Apply the new column names to the DataFrame
        input_txt_file_df = input_txt_file_df.select(*new_columns)
        input_txt_file_df.write.format('delta').mode('overwrite').option("overwriteSchema",True).option("path",silver+output_blob_path1).save()
        print('file_written: ', output_blob_path1)


        #mapping description with the codes of first view
        column_updates = {}
        for row in code_layout_df.collect():
            value = row['Code']
            valuelabel = row['Desription']
            variable_name = row['Name']

            if variable_name in input_txt_file_df.columns:
                condition = (col(variable_name) == value)
                update_value = lit(valuelabel)

                if variable_name in column_updates:
                    column_updates[variable_name] = when(condition, update_value).otherwise(column_updates[variable_name])
                else:
                    column_updates[variable_name] = when(condition, update_value).otherwise(col(variable_name))

        for column_name, update_expr in column_updates.items():
            input_txt_file_df = input_txt_file_df.withColumn(column_name, update_expr)

        input_txt_file_df.write.format('delta').mode('overwrite').option("overwriteSchema",True).option("path",silver+output_blob_path2).save()
        print('file_written: ', output_blob_path2)


    except Exception as e:
        error_list.append((file_path, str(e)))
        print('error:', file_path, str(e))


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

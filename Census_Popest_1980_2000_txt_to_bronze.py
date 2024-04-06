# Databricks notebook source
# MAGIC %md
# MAGIC This Notebook is for mapping the txt file with record layout provided in the ticket and creating the table using fixed width parameters and ingesting the table to the to_bronze container in csv format

# COMMAND ----------

# MAGIC %md
# MAGIC Importing mountpoints notebook

# COMMAND ----------

# MAGIC %run "../Templates/mountpoints-1"

# COMMAND ----------

# blob_relative_path='www2.census.gov/programs-surveys/popest/datasets/'
# patterns='.*/[Ee]\d{4}[A-Za-z]{3}\.(?i:txt)$'
# record_layout_path='www2.census.gov/programs-surveys/popest/datasets/POPEST text file layout 1980-2000.xlsx'


#getting parameters
# blob_relative_path should be after the to-wild container
#record_layout_path should be after the to-wild container

blob_relative_path = dbutils.widgets.get('blob_relative_path')
patterns=dbutils.widgets.get('patterns')
record_layout_path=dbutils.widgets.get('record_layout_path')

# COMMAND ----------

#function for getting all the file paths
def get_file_paths(dir_path):
    file_paths = []
    
    # Use dbutils.fs.ls with display to get file information
    files = dbutils.fs.ls(dir_path)
    allowed_extensions = ['.txt','.TXT']
    for file in files:

        if file.isDir():
            # If it's a directory, recursively call the function
            file_paths.extend(get_file_paths(file.path))
        if any(file.path.endswith(ext) for ext in allowed_extensions):
            # If it's a file, append the path to the list
            file_paths.append(file.path)

    return file_paths


# COMMAND ----------

all_file_paths = get_file_paths(manual_to_wild+blob_relative_path)

# COMMAND ----------

#filtering the files using regex patterns
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
print(len(all_file_paths))

# COMMAND ----------


# Get the blob client for the Excel file
blob_client = manual_raw_container_client.get_blob_client( blob=record_layout_path)

# Download the Excel file to a local directory
local_file_path = 'test.xlsx'
with open(local_file_path, 'wb') as file:
    file.write(blob_client.download_blob().readall())

xls = pd.ExcelFile(local_file_path)
file_list_df = pd.read_excel(xls, sheet_name='file_list', header=0).drop_duplicates(subset='File Name')
layout_df = pd.read_excel(xls, sheet_name='layout', header=0).drop_duplicates()

#remove local file
os.remove(local_file_path)

# COMMAND ----------

#creating list of tuples for mapping with txt file
def tuples_list(code_book_df):
    df_no_duplicates =code_book_df.dropDuplicates(["start", "length","Data"]) 
    selected_columns = ["start", "length","Data"]
    list_of_tuples = df_no_duplicates.select(selected_columns).collect()
    split_columns=[]
    for Tape_start,length,Field_name in list_of_tuples :
        column_name = 'value'
        split_columns.append(expr(f"substring({column_name}, {Tape_start},{length})").alias(Field_name))
    return split_columns

# COMMAND ----------


for file in all_file_paths:
    file_name=file.split('/')[-1].upper()

    # Now filtered_df contains only the rows where the 'file_name' column has the desired value
    filtered_df = file_list_df[file_list_df['File Name'] == file_name]
    if filtered_df.empty:
        continue
    merged_df = pd.merge(layout_df, filtered_df, left_on='decade',right_on='use_layout_decade', how='inner')
    # Split the column based on "-"
    split_values = merged_df['Location'].str.split('-', expand=True)
    # Check if any columns were created
    if split_values.empty:
        continue
    else:
        # Assign the first split value to a new column 'start' and the last split value to a new column 'end'
        merged_df['start'] = split_values.iloc[:, 0]
        merged_df['end'] = split_values.iloc[:, -1]
        # Convert 'start' and 'end' columns to numeric type
        merged_df['start'] = pd.to_numeric(merged_df['start'], errors='coerce')
        merged_df['end'] = pd.to_numeric(merged_df['end'], errors='coerce')

        # Add a new column 'length' by subtracting 'end' from 'start'
        merged_df['length'] = merged_df['end'] - merged_df['start'] +1
        merged_df = merged_df.dropna(subset=['length'])

    if merged_df.empty:
        continue 

    output_blob_path=file.split(manual_to_wild)[-1].replace('.txt','.csv').replace('.TXT','.csv')
    #reading txt file from blob
    input_txt_file_df = spark.read.text(file)
    input_txt_file_df = input_txt_file_df.filter((col("value") != "") )
    #converting merged_df from pandas to pyspark dataframe
    code_book_df=spark.createDataFrame(merged_df)
    #mapping the input_txt_file_df with the code_book_df
    input_txt_file_df = input_txt_file_df.select("*",*tuples_list(code_book_df)).drop('value')
    #converting csv files and uploading to blob
    pandas_df=input_txt_file_df.toPandas()
    blob_client = manual_bronze_container_client.get_blob_client(output_blob_path)
    csv_file = pandas_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)
    print('file_written: ', output_blob_path)
    

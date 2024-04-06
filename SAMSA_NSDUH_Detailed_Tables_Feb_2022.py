# Databricks notebook source
# MAGIC %run "/Repos/v-nandakumara@usafacts.org/usafacts-data-platform-notebooks/Templates/mountpoints" 

# COMMAND ----------

blob_relative_path = dbutils.widgets.get("blob_relative_path")
# blob_relative_path ='www.samhsa.gov/data/sites/default/files/reports/rpt42728/NSDUHDetailedTabs2022/NSDUHDetailedTabs2022/2022-nsduh-detailed-tables/2022-nsduh-detailed-tables.htm'


# COMMAND ----------

def get_file_paths(dir_path):
    file_paths= []
    files = dbutils.fs.ls(dir_path)
    for file in files:
        allowed_extensions = ['.htm', '.html']
        if file.isDir() :
            file_paths.extend(get_file_paths(file.path))
        if any(file.path.endswith(ext) for ext in allowed_extensions):
            file_paths.append(file.path.split(bronze)[-1])

    return file_paths

# COMMAND ----------

all_file_paths = get_file_paths(bronze+blob_relative_path)

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

def create_df (data,header,footer): 
    cleaned_columns = [re.sub(r'[^a-zA-Z0-9]', "_", value.strip().replace('.0','')).strip('_') if value is not None else ('col_'+str(ind)) for ind,value in enumerate(data[0])]
    columns = cleaned_columns
    if len(cleaned_columns) !=  len(list(set(cleaned_columns))):
        columns = convert_list(cleaned_columns)
    schema = StructType([StructField(name, StringType(), True) for name in columns])
    df = spark.createDataFrame(data[1:], schema)

    if header:
        header = '. '.join(header)
        df = df.withColumn("Header",lit(header))
    if footer:
        footer ='. '.join(footer)
        df = df.withColumn("Footer",lit(footer))
    return df

# COMMAND ----------

def filtered_data(clean_data):
    datas = [clean_data]
    for df_data in datas :
        data = []
        header= []
        footer = []
        temp_footer = ''
        for ind , row in enumerate(df_data):
            row = [None if value is None or value.strip() == '' or value.strip() =='nan'  else value for value in row ]
            
            cleaned_row = [value for value in row if value is not None]
            if len(cleaned_row) < 1:
                continue
            if len(cleaned_row ) > 1 or (len(cleaned_row) == 1 and row[0] is None):
                data.append(row)

            elif len(data) < 1 :
                header += cleaned_row 
            else:
                temp_footer = cleaned_row[0]
                footer = cleaned_row

        level_list = []
        level_dict = {}
        clean_data = [data[0]]
        
        for ind,row in enumerate(data[1:]):
            s = row[0]
            if s:
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

def process_data(input_data):
    process_data = [row[:] for row in input_data]

    for index, row in enumerate(input_data):
        for ind, value_tuple in enumerate(row):
            count = value_tuple[1]
            while count > 1:
                process_data[index + count - 1].insert(ind, ['', 1])
                count -= 1
    data = []
    for row in process_data :

        data.append([word[0] for word in row])

    return data


# COMMAND ----------

# Function to extract cell data considering rowspan and colspan
def extract_cell_data(cell):
    cell_text = cell.get_text(strip=True)
    rowspan = int(cell.get('rowspan', 1))
    colspan = int(cell.get('colspan', 1))
    return cell_text, rowspan, colspan

# COMMAND ----------

def scrap_tables(blob_name,container_client = container_client):
    blob_client = container_client.get_blob_client(blob_name)
    # Download the blob content
    blob_content = blob_client.download_blob()

    html_content = blob_content.readall()
    soup = BeautifulSoup(html_content, 'html.parser')
            
    # Initialize a list to store table 
    table_data_list = []
    
    tables = soup.find_all('table') 
    if not tables :
        return []
    table_list =[]
    for table in tables:          
        # Find all rows in the header
        header_rows = table.find('thead').find_all('tr')
        caption = table.find('caption')  
        if caption:          
            # Extract the text from the caption        
            caption = caption.get_text() 
        else:
            caption = ''
        if table.find('tfoot'):   
            footer = table.find('tfoot').get_text()
        else:
            footer = ''
        # Initialize a list to store the concatenated header data
        concatenated_rows = []
        header = []
        if len(header_rows) > 1 :
            for row in header_rows:
                row_data = []
                # Iterate through cells in the row
                cells = row.find_all(['th', 'td'])
                for cell in cells:
                    cell_data, rowspan, colspan = extract_cell_data(cell)
                    # Add cell data to the row, including empty strings
                    row_data.extend([[cell_data,rowspan]] * colspan)
                    
                # Add row data to the concatenated headers
                concatenated_rows.append(row_data)
            processed_header = process_data(concatenated_rows)
            header = [word for word in processed_header[0]]
            for list2 in processed_header[1:]:
                header = [word1 + ' ' + word2 for word1, word2 in zip(header, list2)]

        else:

            rows = table.find('thead').find_all('tr')
            header = [cell.get_text(strip=True) for row in rows for cell in row.find_all(['th', 'td']) for _ in range(int(cell.get('colspan', 1)))]
        
        # Extracting the tbody information
        tbody_data = []
        tbody_rows = table.tbody.find_all('tr')

        for tr in tbody_rows:
            row_data = []
            row_data = []
        
            for td in tr.find_all(['th', 'td']):
                cell_data, rowspan, colspan = extract_cell_data(td)
                # Add cell data to the row, including empty strings
                row_data.extend([[cell_data,rowspan]] * colspan)

            # Adding the class name for the tbody row
            class_name = ''
            if tr.th :
                class_name = tr.th.get('class', [''])[0]
            get_space = ''
            if 'subhead' in class_name :
                get_space =' ' * int(class_name.split('subhead')[-1])
            elif 'sub' in class_name :
                get_space =' ' * int(class_name.split('sub')[-1])  

            
            row_data[0][0] = get_space + row_data[0][0]
            if tbody_data and len(row_data) > len(header):
                for ind in range(len(header),len(row_data),len(header)) :
                    tbody_data.append(row_data[:ind])
            else:
                tbody_data.append(row_data)
        body_data = process_data(tbody_data)
        
        data = [[caption]]+[header]+body_data +[[footer]]
        df = filtered_data(data)
        caption = caption.strip().split('–')[0].strip()[:-1].replace(' ',"_").replace('.',"_")

        table_list.append((df,caption))

    return union_dfs(table_list)

# COMMAND ----------

def remove_duplicates(lst):
    return [item for index, item in enumerate(lst) if item not in lst[:index]]

# COMMAND ----------

def union_dfs(list_of_dfs):
    # Create a dictionary to store DataFrames based on their columns
    df_dict = {}

    # Iterate through the list of DataFrames
    for df,caption in list_of_dfs:
        key = caption
    
        # Check if the key already exists in the dictionary
        if key in df_dict:
            # If it exists, append the DataFrame to the existing list
            df_dict[key].append(df)
        else:
            df_dict[key] = [df] 

    combined_dfs = []
    for key,dfs in df_dict.items():
        result_df = dfs[0]
        if len(dfs) > 1 :
            for ind,df in enumerate(dfs[1:]):
                if len(result_df.columns) != len(df.columns):
                    combined_dfs.append((df,key+'_'+str(ind)))
                else:
                    result_df = result_df.unionAll(df)
        combined_dfs.append((result_df,key))

    return combined_dfs

# COMMAND ----------


bad_records = []
for folder in all_file_paths :
    NSDUHDetTabsSect_tables = []
    NSDUHDetTabs_tables = []
    files = dbutils.fs.ls(bronze+folder)
    files = [blob.path.split(bronze)[-1]  for blob in files if blob.path.endswith('.htm') or blob.path.endswith('.html') ]
    print(files)
    for blob in files:
        try :
            tables = scrap_tables(blob)
            file_name = blob.split('/')[-2]+'/'+blob.split('/')[-1]
            dfs = []
            for table,table_name in tables:
                table = table.withColumn("file_name",lit(file_name))
                dfs.append((table,table_name))
            if 'NSDUHDetTabsSect' in blob :
                NSDUHDetTabsSect_tables.extend(dfs)
            else:
                NSDUHDetTabs_tables.extend(dfs)
        except BaseException as e :
            print(blob,e)
            bad_records.append(blob)
    file_location = silver +'/'.join(remove_duplicates(folder.split('/')))
    NSDUHDetTabsSect_tables = union_dfs(NSDUHDetTabsSect_tables)
    NSDUHDetTabs_tables =union_dfs(NSDUHDetTabs_tables)
    index = 0
    for df,table_name in NSDUHDetTabsSect_tables:
        if table_name.strip() =='':
            index += 1
            table_name = 'index_'+str(index)
        print(file_location+f'NSDUHDetTabsSect/{table_name}')
        # display(df)
        df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path",file_location+f'NSDUHDetTabsSect/{table_name}').save() 
    index = 0
    for df,table_name in NSDUHDetTabs_tables:
        if  table_name.strip() =='':
            index += 1
            table_name = 'index_'+str(index)
        print(file_location+f'NSDUHDetTabsApp/{table_name}')
        # display(df)
        df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path",file_location+f'NSDUHDetTabsApp/{table_name}').save()   

# COMMAND ----------

from datetime import datetime

# Get the current date and time
current_datetime = datetime.now()

error_df=pd.DataFrame(bad_records,columns =["URL"])
if not error_df.empty :
    scrap_url_path = blob_relative_path.split('/')[0]
    csv_filename = f'{scrap_url_path}'+'/bad_records/'+ f"errors_on_{current_datetime}.csv"
    output=error_df.to_csv(index=False)
    blob_client = container_client.get_blob_client(csv_filename)
    blob_client.upload_blob(output, overwrite=True)
    print('file_writed:',csv_filename)

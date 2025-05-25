import csv
from config.postgres_connection import postgres_connection
import configparser

config1 = configparser.ConfigParser()
config2 = configparser.ConfigParser()
connection = postgres_connection()
cursor = connection.cursor()
config1.read(r'C:\Users\KMC\OneDrive\Desktop\pyspark_practice\Python_Projects\CSV_generation_loading\config\db_table_config.ini')
config2.read(r'C:\Users\KMC\OneDrive\Desktop\pyspark_practice\Python_Projects\CSV_generation_loading\config\file_config.ini')
def get_file_path(file_name):
    if file_name == 'sample':
        return config2['sample']['path'] 
    elif file_name == 'trade':
        return config2['trade']['path'] 
    elif file_name == 'book':
        return config2['book']['path'] 

def contains_no_digits(s):
    return not any(char.isdigit() for char in s)

for section in config1.sections():  #iteration for each table to be inserted
    print(f'CREATING TABLE: {section}')    # use section name  
    columns = ',\n'.join([f'{column} {data_type}' for column, data_type in config1.items(section)])  
    # print(columns)
    # print(f'CREATE TABLE IF NOT EXISTS {section} ({columns});') 
    cursor.execute(f'CREATE TABLE IF NOT EXISTS {section} ({columns});')
    connection.commit()
    
    print("table created successfully")
    column_names = ', '.join([f'{column}' for column in config1[section].keys()])   

    file_path =  get_file_path(section)

    #table data exists conditin
    cursor.execute(f'SELECT * FROM {section} LIMIT 1')
    res_sel = cursor.fetchone()
    if res_sel is None:
        with open(file_path,'r') as f:
            reader = csv.reader(f)
            
            for row in reader:
                if reader.line_num == 1:
                    pass
                else:
                    # print(type(row))
                    # row = [ '\''+i+'\''   if contains_no_digits(i)==True else i   for i in row]
                    row = ['\''+i+'\'' for i in row]
                    row1 = ', '.join(row)
                    cursor.execute(f"insert into {section} ({column_names}) values ({row1})")
                    connection.commit()
        print("insertion successful")
    else:
        print(f"data already exists in {section}")





import pandas as pd
from pyhive import hive

def create_hive_table(table_name, schema):
    conn = hive.Connection(host="localhost", port=10000)
    cursor = conn.cursor()

    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    existing_tables = cursor.fetchall()
    if existing_tables:
        print(f"------------Table '{table_name}' already exists in the database------------")
        return

    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for column, data_type in schema.items():
        create_table_query += f"`{column.replace(' ', '_').replace('é', 'e').replace('à', 'a').replace('ç', 'c')}` {data_type}, "
    create_table_query = create_table_query[:-2]  
    create_table_query += f")"

    cursor.execute(create_table_query)
    conn.commit()
    conn.close()
    print(f"------------Table '{table_name}' created successfully------------")

def insert_data_into_hive_table(table_name, data):
    conn = hive.Connection(host="localhost", port=10000)
    cursor = conn.cursor()

    insert_query = f"INSERT INTO TABLE {table_name} VALUES "
    for index, row in data.iterrows():
        values = ','.join([f"'{str(value)}'" if isinstance(value, str) else str(value) for value in row.values])
        insert_query += f"({values}),"
    insert_query = insert_query[:-1]
    cursor.execute(insert_query)
    conn.commit()
    conn.close()

def get_hive_table_schema(table_name):
    conn = hive.Connection(host="localhost", port=10000)
    cursor = conn.cursor()

    cursor.execute(f"DESCRIBE {table_name}")
    table_schema = {}
    for row in cursor.fetchall():
        column_name = row[0]
        data_type = row[1]
        table_schema[column_name] = data_type

    conn.close()

    return table_schema

def excel_to_hive(excel_file):
    xls = pd.ExcelFile(excel_file)
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
        df = df.apply(lambda x: x if x.dtype != 'object' else x.apply(lambda y: str(y).replace('\'', ' ').replace('l\'', 'l ')))

        column_types = df.dtypes.apply(lambda x: 'STRING' if x == 'object' else 'INT').to_dict()
        hive_table_name = sheet_name.replace(' ', '_').replace('é', 'e').replace('à', 'a').replace('ç', 'c')
        create_hive_table(hive_table_name, column_types)
        insert_data_into_hive_table(hive_table_name, df)
        hive_schema = get_hive_table_schema(hive_table_name)
        print(f"\n--->Hive Table Schema for {hive_table_name}:")
        for column, data_type in hive_schema.items():
            print(f"{column}: {data_type}")
        print("-----------------------------------")
    print("Tables are created successfully in the Hive database.")

excel_file = 'Data_Cata_1.xlsx'
excel_to_hive(excel_file)
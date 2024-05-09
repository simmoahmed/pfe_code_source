import os
import pandas as pd
from pyhive import hive
from datetime import datetime
from time import sleep
import glob
from unidecode import unidecode
import warnings
import numpy as np
warnings.filterwarnings("ignore")


class OracleOps:    
    def create_hive_table(table_name, schema):
        conn = hive.Connection(host="host.docker.internal", port=10000)
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
        hive_schema = OracleOps.get_hive_table_schema(table_name)
        print(f"\n--->Hive Table Schema for {table_name}:")
        for column, data_type in hive_schema.items():
            print(f"{column}: {data_type}")
        print("-----------------------------------")

    def clean_value(value):
        if pd.isnull(value):
            return 'null'
        elif isinstance(value, str):
            cleaned_value = unidecode(value)
            cleaned_value = cleaned_value.replace("'", "")
            return f"'{cleaned_value}'"
        elif isinstance(value, datetime):
            return f"'{value}'"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            return "'"+str(value)+"'"

    def insert_data_into_hive_table(table_name, data):
        conn = hive.Connection(host="host.docker.internal", port=10000)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        existing_data = cursor.fetchall()
        insert_query = f"INSERT INTO TABLE {table_name} VALUES "
        for index, row in data.iterrows():
            values = ', '.join([OracleOps.clean_value(value) for value in row.values])
            if "("+values+")" not in str(existing_data):
                insert_query += f"({values}),"
        if insert_query != f"INSERT INTO TABLE {table_name} VALUES ":
            insert_query = insert_query[:-1]
            cursor.execute(insert_query)
            conn.commit()
        conn.close()



    def get_hive_table_schema(table_name):
        conn = hive.Connection(host="host.docker.internal", port=10000)
        cursor = conn.cursor()

        cursor.execute(f"DESCRIBE {table_name}")
        table_schema = {}
        for row in cursor.fetchall():
            column_name = row[0]
            data_type = row[1]
            table_schema[column_name] = data_type

        conn.close()

        return table_schema

    def infer_column_types(df):
        for col in df.columns:
            if df[col].dtype in ['int64','float64']:
                continue    
            try:
                df[col] = pd.to_datetime(df[col], infer_datetime_format=True)
            except:
                continue
        column_types = {}
        for column in df.columns:
            for value in df[column]:
                if isinstance(value, int):
                    column_types[column] = 'BIGINT'
                    break
                elif isinstance(value, float):
                    column_types[column] = 'DOUBLE'
                    break
                elif isinstance(value, datetime):
                    column_types[column] = 'DATE'
                    df[column] = pd.to_datetime(df[column]).dt.date
                    break
                elif pd.api.types.is_string_dtype(df[column]):
                    column_types[column] = 'STRING'
                    break
        return column_types


def csv_to_hive(csv_file):
    df = pd.read_csv(csv_file)

    # Infer column types dynamically
    column_types = OracleOps.infer_column_types(df)

    csv_file_name = os.path.basename(csv_file)
    table_name = os.path.splitext(csv_file_name)[0]
    table_name = table_name.replace('.', '')
    
    OracleOps.create_hive_table(table_name, column_types)
    OracleOps.insert_data_into_hive_table(table_name, df)
    hive_schema = OracleOps.get_hive_table_schema(table_name)
    print(f"\n--->Hive Table Schema for {table_name}:")
    for column, data_type in hive_schema.items():
        print(f"{column}: {data_type}")
    print("-----------------------------------")
    print("Table is created successfully in the Hive database.")


def process_new_excel_files(**kwargs):
    path_to_watch = '/opt/airflow/data/'  # Directory to check for Excel files
    excel_files = glob.glob(os.path.join(path_to_watch, '*.csv'))
    
    for excel_file in excel_files:
        print(f"Processing file: {excel_file}")
        csv_to_hive(excel_file)

process_new_excel_files()
import pandas as pd
import requests
from pyhive import hive

def insert_data_into_hive_table(table_name, data):
    conn = hive.Connection(host="host.docker.internal", port=10000, database='test')
    cursor = conn.cursor()

    select_query = f"SELECT employeeid FROM {table_name}"
    cursor.execute(select_query)
    existing_ids = set([row[0] for row in cursor.fetchall()])
    print(existing_ids)
    insert_query = f"INSERT INTO TABLE {table_name} VALUES "
    for index, row in data.iterrows():
        employee_id = row['employeeid']
        print(employee_id)
        if employee_id not in existing_ids:
            values = ','.join([f"'{str(value)}'" if isinstance(value, str) else str(value) for value in row.values])
            insert_query += f"({values}),"

    if insert_query.endswith(","):
        insert_query = insert_query[:-1]
        cursor.execute(insert_query)
        conn.commit()
        print(f"Inserted {len(data)} records into Hive table '{table_name}'")
    else:
        print("No new records to insert")

    conn.close()


def get_data_from_api(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        return df
    else:
        print(f"Failed to fetch data from API. Status code: {response.status_code}")
        return None

def api_to_hive(api_url, table_name):
    df = get_data_from_api(api_url)
    if df is not None:
        hive_table_name = table_name.replace(' ', '_').replace('é', 'e').replace('à', 'a').replace('ç', 'c')
        print(df.head())
        insert_data_into_hive_table(hive_table_name, df)
        print(f"\n--->Data loaded into Hive table '{hive_table_name}' successfully")
        print("-----------------------------------")

api_url = 'http://host.docker.internal:5000/employees'
hive_table_name = 'employees'
api_to_hive(api_url, hive_table_name)

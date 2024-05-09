import random
from faker import Faker
from datetime import datetime
import uuid

# Initialize Faker to generate realistic data
fake = Faker()

def generate_rib():
    return ''.join([str(random.randint(0, 9)) for _ in range(24)])

# Function to generate random opening date
def generate_opening_date():
    return fake.date_between(start_date='-10y', end_date='today')

# Function to generate ACCOUNTID starting with "BP"
def generate_account_id():
    return "BP" + str(uuid.uuid4().hex)[:10]  # Assuming the UUID4 hex representation is sufficiently random

# Function to generate sample data for a single row
def generate_row():
    return (
        generate_account_id(),                          # ACCOUNTID
        fake.random_element(['Savings', 'Checking', 'Investment']),  # ACCOUNTTYPE
        generate_rib(),                         # RIB
        fake.swift(),                           # CODESWIFT
        round(random.uniform(0, 100000), 2),    # CURRENTBALANCE
        "TO_DATE('{}', 'YYYY-MM-DD')".format(generate_opening_date()),                # OPENINGDATE
        fake.random_element(['Active', 'Inactive']),  # ACCOUNTSTATUS
        round(random.uniform(0, 100), 2)        # MONTHLYFEES
    )

# Generate sample data for the table and generate INSERT INTO statements
def generate_data_and_insert_statements(num_rows):
    data = [generate_row() for _ in range(num_rows)]
    insert_statements = []
    for row in data:
        insert_statements.append("INSERT INTO account (ACCOUNTID, ACCOUNTTYPE, RIB, CODESWIFT, CURRENTBALANCE, OPENINGDATE, ACCOUNTSTATUS, MONTHLYFEES) VALUES ('{}', '{}', '{}', '{}', {}, {}, '{}', {});".format(*row))    
    return data, insert_statements

# Write sample data and INSERT INTO statements to a text file
def write_to_file(data, insert_statements, filename):
    with open(filename, 'w') as file:        
        file.write("-- INSERT INTO statements\n")
        for statement in insert_statements:
            file.write(statement + '\n')

# Example usage
if __name__ == "__main__":
    # Generate 10 sample rows and INSERT INTO statements
    sample_data, insert_statements = generate_data_and_insert_statements(100)
    
    # Write sample data and INSERT INTO statements to a text file
    write_to_file(sample_data, insert_statements, 'account_table.sql')

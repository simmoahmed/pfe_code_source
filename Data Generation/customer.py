import random
from faker import Faker
from datetime import datetime
import uuid
# Function to generate a random Customer ID
def generate_customer_id():
    return 'CL'+''.join(random.choices('12345', k=4))

# Function to generate ACCOUNTID starting with "BP"
def generate_account_id():
    return "BP" + str(uuid.uuid4().hex)[:10]

# Function to generate a random Customer Type
def generate_customer_type():
    customer_types = ['Individual', 'Business', 'Corporate', 'Small Business', 'Retail', 'Wholesale', 'Government', 'Non-profit', 'Startup', 'Enterprise','Freelancer']
    return random.choice(customer_types)

# Generating data for 100 customers
customers_data = []
for i in range(250):
    customer_id = generate_customer_id()
    person_id = generate_customer_id()  # Generating a random PersonID for demonstration purposes
    account_id = generate_account_id()  # Generating a random AccountID for demonstration purposes
    customer_type = generate_customer_type()

    customer_data = (customer_id, person_id, account_id, customer_type)
    customers_data.append(customer_data)

# Writing data to a file
with open('customer_data.sql', 'w') as file:
    for customer_data in customers_data:
        query = f"INSERT INTO Customer (CustomerID, PersonID, AccountID, CustomerType) VALUES {customer_data};\n"
        file.write(query)

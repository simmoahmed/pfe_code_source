import random
from datetime import datetime, timedelta

# Function to generate a random Transaction ID
def generate_transaction_id():
    return random.randint(1, 10000)

# Function to generate a random Loan Payment ID
def generate_loan_payment_id():
    return random.randint(1, 100)

# Function to generate a random Account ID
def generate_account_id():
    return 'ACC' + ''.join(random.choices('1234567890', k=4))

# Function to generate a random Employee ID
def generate_employee_id():
    return 'EMP' + ''.join(random.choices('1234567890', k=4))

# Function to generate a random Transaction Type
def generate_transaction_type():
    transaction_types = ['Deposit', 'Withdrawal', 'Transfer']
    return random.choice(transaction_types)

# Function to generate a random Amount
def generate_amount():
    return round(random.uniform(10, 1000), 2)

# Function to generate a random Transaction Date within the last year
def generate_transaction_date():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    random_date = start_date + timedelta(days=random.randint(0, 365))
    return random_date.strftime('%Y-%m-%d')

# Generating data for 100 transactions
transactions_data = []
for i in range(310):
    transaction_id = generate_transaction_id()
    loan_payment_id = generate_loan_payment_id()
    account_id = generate_account_id()
    employee_id = generate_employee_id()
    transaction_type = generate_transaction_type()
    amount = generate_amount()
    transaction_date = generate_transaction_date()

    transaction_data = (transaction_id, loan_payment_id, account_id, employee_id, transaction_type, amount, transaction_date)
    transactions_data.append(transaction_data)

# Writing data to a file
with open('transaction_data.sql', 'w') as file:
    for transaction_data in transactions_data:
        transaction_id, loan_payment_id, account_id, employee_id, transaction_type, amount, transaction_date = transaction_data
        query = f"INSERT INTO Transaction (TransactionID, LoanPaymentID, AccountID, EmployeeID, TransactionType, Amount, TransactionDate) VALUES ({transaction_id}, {loan_payment_id}, '{account_id}', '{employee_id}', '{transaction_type}', {amount}, TO_DATE('{transaction_date}', 'YYYY-MM-DD'));\n"
        file.write(query)

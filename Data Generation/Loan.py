import random
from datetime import datetime, timedelta

# Function to generate a random Loan ID
def generate_loan_id():
    return random.randint(1, 2000)

# Function to generate a random Loan Type
def generate_loan_type():
    loan_types = ['Personal Loan', 'Home Loan', 'Auto Loan', 'Business Loan', 'Education Loan']
    return random.choice(loan_types)

# Function to generate a random Loan Amount
def generate_loan_amount():
    return round(random.uniform(1000, 100000), 2)

# Function to generate a random Interest Rate
def generate_interest_rate():
    return round(random.uniform(1, 15), 2)

# Function to generate a random Opening Date within the last year
def generate_opening_date():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    random_date = start_date + timedelta(days=random.randint(0, 365))
    return random_date.strftime('%Y-%m-%d')

# Function to generate a random Loan Term Duration in months
def generate_loan_term_duration():
    return random.randint(12, 120)

# Function to generate a random Start Date based on Opening Date and Loan Term Duration
def generate_start_date(opening_date, loan_term_duration):
    start_date = datetime.strptime(opening_date, '%Y-%m-%d') + timedelta(days=random.randint(1, loan_term_duration*30))
    return start_date.strftime('%Y-%m-%d')

# Function to generate a random End Date based on Start Date and Loan Term Duration
def generate_end_date(start_date, loan_term_duration):
    end_date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=loan_term_duration*30)
    return end_date.strftime('%Y-%m-%d')

# Function to generate a random Loan Status
def generate_loan_status():
    loan_statuses = ['Active', 'Closed', 'Pending', 'Defaulted']
    return random.choice(loan_statuses)

# Function to generate a random Customer ID
def generate_customer_id():
    return 'CL' + ''.join(random.choices('12345', k=4))

# Generating data for 100 loans
loans_data = []
for i in range(100):
    loan_id = generate_loan_id()
    customer_id = generate_customer_id()  # Generating a random CustomerID for demonstration purposes
    loan_type = generate_loan_type()
    loan_amount = generate_loan_amount()
    interest_rate = generate_interest_rate()
    opening_date = generate_opening_date()
    loan_term_duration = generate_loan_term_duration()
    start_date = generate_start_date(opening_date, loan_term_duration)
    end_date = generate_end_date(start_date, loan_term_duration)
    status = generate_loan_status()

    loan_data = (loan_id, customer_id, loan_type, loan_amount, interest_rate, opening_date, loan_term_duration, start_date, end_date, status)
    loans_data.append(loan_data)

# Writing data to a file
with open('loan_data.sql', 'w') as file:
    for loan_data in loans_data:
        loan_id, customer_id, loan_type, loan_amount, interest_rate, opening_date, loan_term_duration, start_date, end_date, status = loan_data
        query = f"INSERT INTO LOAN (LoanID, CustomerID, LoanType, LoanAmount, InterestRate, OpeningDate, LoanTerm_Duration, StartDate, EndDate, Status) VALUES ({loan_id}, '{customer_id}', '{loan_type}', {loan_amount}, {interest_rate}, TO_DATE('{opening_date}', 'YYYY-MM-DD'), {loan_term_duration}, TO_DATE('{start_date}', 'YYYY-MM-DD'), TO_DATE('{end_date}', 'YYYY-MM-DD'), '{status}');\n"
        file.write(query)

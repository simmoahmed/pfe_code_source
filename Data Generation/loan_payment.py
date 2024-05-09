import random
from datetime import datetime, timedelta

# Function to generate a random Loan Payment ID
def generate_loan_payment_id():
    return random.randint(1, 1000)

# Function to generate a random Loan ID
def generate_loan_id():
    return random.randint(1, 100)

# Function to generate a random Scheduled Payment Date within the next year
def generate_scheduled_payment_date():
    start_date = datetime.now()
    end_date = start_date + timedelta(days=365)
    random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    return random_date.strftime('%Y-%m-%d')

# Function to generate a random Payment Amount
def generate_payment_amount():
    return round(random.uniform(10, 1000), 2)

# Function to generate a random Principal Amount
def generate_principal_amount():
    return round(random.uniform(5, 500), 2)

# Function to generate a random Interest Amount
def generate_interest_amount(payment_amount, principal_amount):
    return round(payment_amount - principal_amount, 2)

# Function to generate a random Paid Amount
def generate_paid_amount(payment_amount):
    return round(random.uniform(0, payment_amount), 2)

# Function to generate a random Pain Date
def generate_pain_date(payment_amount):
    return round(random.uniform(0, payment_amount), 2)

# Generating data for 100 loan payments
loan_payments_data = []
for i in range(190):
    loan_payment_id = generate_loan_payment_id()
    loan_id = generate_loan_id()
    scheduled_payment_date = generate_scheduled_payment_date()
    payment_amount = generate_payment_amount()
    principal_amount = generate_principal_amount()
    interest_amount = generate_interest_amount(payment_amount, principal_amount)
    paid_amount = generate_paid_amount(payment_amount)
    pain_date = generate_pain_date(payment_amount)

    loan_payment_data = (loan_payment_id, loan_id, scheduled_payment_date, payment_amount, principal_amount, interest_amount, paid_amount, pain_date)
    loan_payments_data.append(loan_payment_data)

# Writing data to a file
with open('loan_payment_data.sql', 'w') as file:
    for loan_payment_data in loan_payments_data:
        loan_payment_id, loan_id, scheduled_payment_date, payment_amount, principal_amount, interest_amount, paid_amount, pain_date = loan_payment_data
        query = f"INSERT INTO LoanPayment (LoanPaymentID, LoanID, ScheduledPaymentDate, PaymentAmount, PrincipalAmount, InterestAmount, PaidAmount, PainDate) VALUES ({loan_payment_id}, {loan_id}, TO_DATE('{scheduled_payment_date}', 'YYYY-MM-DD'), {payment_amount}, {principal_amount}, {interest_amount}, {paid_amount}, {pain_date});\n"
        file.write(query)

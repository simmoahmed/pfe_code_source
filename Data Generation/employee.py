import random

# Function to generate a random Employee ID
def generate_employee_id():
    return 'EM'+''.join(random.choices('12345', k=4))

def generate_peroson_id():
    return ''.join(random.choices('123456', k=4))

# Function to generate a random Branch ID
def generate_branch_id():
    return 'BR'+''.join(random.choices('12345', k=4))

# Function to generate a random Account ID
def generate_account_id():
    return ''.join(random.choices('123456', k=4))

# Function to generate a random Position
def generate_position():
    positions = ['Data Engineer','Data Scientist','Auditeur','Manager', 'Assistant Manager', 'Sales Associate', 'Customer Service Representative', 'Financial Analyst', 'Human Resources Specialist']
    return random.choice(positions)

# Generating data for 100 employees
employees_data = []
for i in range(300):
    employee_id = generate_employee_id()
    person_id = generate_peroson_id()  # Generating a random PersonID for demonstration purposes
    branch_id = generate_branch_id()
    account_id = generate_account_id()
    position = generate_position()

    employee_data = (employee_id, person_id, branch_id, account_id, position)
    employees_data.append(employee_data)

# Writing data to a file
with open('employee_data.sql', 'w') as file:
    for employee_data in employees_data:
        query = f"INSERT INTO Employee (EmployeeID, PersonID, BranchID, AccountID, Position) VALUES {employee_data};\n"
        file.write(query)

import random

# Function to generate a random branch ID
def generate_branch_id():
    return 'BR'+''.join(random.choices('0123456789', k=4))

# Function to generate a random branch name
def generate_branch_name():
    branch_names = ['Agdal Branch','Hassan II Branch','Maarif Branch','Sidi Othman Branch','Ifrane Branch','Derb Sultan Branch',
        'Gueliz Branch',
        'Sidi Yahya Branch',
        'Tanger Ville Branch',
        'Moulay Youssef Branch',
        'Oasis Branch',
        'El Jadida Branch',
        'Dar Bouazza Branch',
        'Roches Noires Branch',
        'Hay Mohammadi Branch',
        'Jnane Sbile Branch',
        'Habous Branch',
        'Mohammed V Branch',
        'Fes El Jadid Branch',
        'Medina Branch'
    ]    
    return random.choice(branch_names)

# Function to generate a random branch code
def generate_branch_code():
    return 'BP_BR'+''.join(random.choices('0123456789', k=3))

# Function to generate a random address
def generate_address():
    street_names = ['Avenue Mohammed V', 'Rue Hassan II', 'Avenue Allal Ben Abdellah', 'Rue Mohammed El Fassi', 'Avenue Abdelkrim El Khattabi', 'Rue Mohamed Zerktouni', 'Avenue Hassan II', 'Rue Ahmed El Mokri', 'Avenue des FAR', 'Rue Ibn Sina', 'Avenue Hassan I', 'Rue El Mansour Eddahbi', 'Avenue Abdelmoumen', 'Rue Ibn Battouta', 'Avenue Tarik Ibn Ziad', 'Rue Yacoub El Mansour', 'Avenue Mohammed VI', 'Rue Abou Bakr Essedik', 'Avenue Annakhil', 'Rue des Merinides']
    return f"{random.randint(1, 1000)} {random.choice(street_names)}"

# Function to generate a random phone number
def generate_phone_number():
    return '+212 6' + ''.join(random.choices('0123456789', k=8))

# Generating data for 100 branches
branches_data = []
for i in range(100):
    branch_id = generate_branch_id()
    branch_name = generate_branch_name()
    branch_code = generate_branch_code()
    address = generate_address()
    phone_number = generate_phone_number()

    branch_data = (branch_id, branch_name, branch_code, address, phone_number)
    branches_data.append(branch_data)

# Writing data to a file
with open('branch_data.sql', 'w') as file:
    for branch_data in branches_data:
        query = f"INSERT INTO BRANCH (BranchID, BrancheName, BranchCode, Address, PhoneNumber) VALUES {branch_data};\n"
        file.write(query)

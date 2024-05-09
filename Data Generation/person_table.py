import random
from datetime import datetime, timedelta

# List of Moroccan cities
moroccan_cities = ['Casablanca', 'Rabat', 'Marrakech', 'Fes', 'Tangier', 'Agadir', 'Oujda', 'Kenitra', 'Meknes', 'Taza','Temara','Safi','Khenifra']

# Function to generate a random date of birth between 1950 and 2005
def random_dob():
    start_date = datetime(1950, 1, 1)
    end_date = datetime(2005, 12, 31)
    random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    return random_date.strftime('%Y-%m-%d')

# Function to generate a random Moroccan phone number
def random_phone_number():
    return '+212 6' + ''.join([str(random.randint(0, 9)) for _ in range(8)])

# Function to generate a random Moroccan zip code
def random_zip_code():
    return ''.join([str(random.randint(0, 9)) for _ in range(5)])

# Function to generate a random social security number
def random_social_security_number():
    return 'MA-' + ''.join([str(random.randint(0, 9)) for _ in range(7)])

# Function to generate marital status
def random_marital_status():
    return random.choice(['Single', 'Married', 'Divorced', 'Widowed'])

# Function to generate random email
def random_email(first_name, last_name):
    domains = ['@gmail.com', '@hotmail.com', '@icloud.com']
    return f'{first_name.lower()}.{last_name.lower()}{random.randint(10,99)}{random.choice(domains)}'

with open('person_table.sql', 'w') as file:

    # Generating 100 records
    for i in range(150):
        person_id = str(10000000 + i)
        last_name = random.choice(['Alaoui', 'Alami', 'El Maadoudi', 'Chokrane', 'Atmani', 'Idrissi', 'Ait Hasson', 'Hmaimou', 'Rouicha', 'Houiry', 'Benaddi', 'Ouahbi', 'Fassi', 'Bouzidi', 'El Fakir', 'Zerouali', 'Laghzali', 'Belkadi', 'Ouazzani', 'Bennani','Lazrak', 'Bekkali', 'Hamdi', 'Tazi', 'El Abbassi', 'Hajji', 'El Amrani', 'El Khattabi', 'Najjar', 'Zouaoui', 'El Harrak', 'Berrada', 'El Idrissi', 'Zerhouni', 'El Kandoussi', 'El Hamdouni', 'El Hachimi', 'El Kassimi', 'El Moussaoui', 'El Kabbaj','Nechba'])
        first_name = random.choice(['Fatima Zahra', 'Youssef', 'Sanaa', 'Hicham', 'Nadia', 'Sami', 'Hajar', 'Abderrahmane', 'Nora', 'Omar', 'Saida', 'Khalid', 'Meryem', 'Hamza', 'Asma', 'Ali', 'Naima', 'Adil', 'Imane', 'Karim','Khadija', 'Mohammed', 'Amina', 'Abdelilah', 'Soukaina', 'Yassine', 'Mouna', 'Hassan', 'Nawal', 'Mustapha', 'Rim', 'Younes', 'Latifa', 'Mehdi', 'Salma', 'Brahim', 'Hafsa', 'Anas', 'Fatima', 'Youness'])
        date_of_birth = random_dob()
        country = 'Morocco'
        city = random.choice(moroccan_cities)
        zip_code = random_zip_code()
        social_security_number = random_social_security_number()
        marital_status = random_marital_status()
        email = random_email(first_name, last_name)
        phone_number = random_phone_number()
        address = f'{random.randint(1, 1000)} {random.choice(["Street", "Avenue", "Road"])}'

        # Write SQL query to file
        query = f"INSERT INTO Person (PersonID, LastName, FirstName, Email, PhoneNumber, Address, DateOfBirth, Country, City, ZipCode, SocialSecurityNumber, MaritalStatus) VALUES ('{person_id}', '{last_name}', '{first_name}', '{email}', '{phone_number}', '{address}', TO_DATE('{date_of_birth}', 'YYYY-MM-DD'), '{country}', '{city}', '{zip_code}', '{social_security_number}', '{marital_status}');\n"
        file.write(query)

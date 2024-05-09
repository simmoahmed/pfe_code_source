import random
from datetime import datetime, timedelta

# Function to generate a random Card ID
def generate_card_id():
    return ''.join(random.choices('0123456789', k=4))

# Function to generate a random Card Number
def generate_card_number():
    return ''.join(random.choices('0123456789', k=14))

# Function to generate a random Card Type
def generate_card_type():
    card_types = ['VISA', 'MasterCard', 'Etudiant', 'Premium']
    return random.choice(card_types)

# Function to generate a random PIN Code
def generate_pin_code():
    return random.randint(1000, 9999)

# Function to generate a random Expiration Date within the next 5 years
def generate_expiration_date():
    current_date = datetime.now()
    end_date = current_date + timedelta(days=365*5)
    random_date = current_date + timedelta(days=random.randint(0, (end_date - current_date).days))
    return random_date.strftime('%Y-%m-%d')

# Generating data for 100 bank cards
bank_cards_data = []
for i in range(220):
    card_id = generate_card_id()
    person_id = ''.join(random.choices('1234567890', k=4))
    card_number = generate_card_number()
    card_type = generate_card_type()
    pin_code = generate_pin_code()
    expiration_date = generate_expiration_date()

    bank_card_data = (card_id, person_id, card_number, card_type, pin_code, expiration_date)
    bank_cards_data.append(bank_card_data)

# Writing data to a file
with open('bank_card_data.sql', 'w') as file:
    for bank_card_data in bank_cards_data:
        card_id, person_id, card_number, card_type, pin_code, expiration_date = bank_card_data
        query = f"INSERT INTO BANK_CARD (CardID, PersonID, CardNumber, CardType, PINCode, ExpirationDate) VALUES ('{card_id}', '{person_id}', '{card_number}', '{card_type}', {pin_code}, TO_DATE('{expiration_date}', 'YYYY-MM-DD'));\n"
        file.write(query)

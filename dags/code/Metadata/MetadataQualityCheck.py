import json
import logging
import re
from datetime import datetime
# from kafka import KafkaConsumer
# from confluent_kafka import Consumer, KafkaError

logging.basicConfig(filename='metadata_quality_report.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load JSON data
with open('/opt/airflow/dags/data/output.json', 'r') as json_file:
    metadata = json.load(json_file)

qualified_names_list = []
pattern = re.compile(r'^[a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?@[a-zA-Z0-9_]+(?:\s+#\s+clusterName\s+to\s+use\s+in\s+qualifiedName\s+of\s+entities\.\s+Default:\s+[a-zA-Z0-9_]+)?$')
guid_list_tab = []

# Function to check metadata quality for a specific table
def check_table_metadata_quality(table_metadata):
    db_name = table_metadata['Name of DB']
    table_name = table_metadata['Name of Table']
    logging.info(f"Checking metadata quality for table: {table_name}")
    
    # Vérification des noms (DB, Table, et Colonnes)
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', db_name):
        logging.warning(f"Invalid database name: '{db_name}'.")
        has_warnings = True

    # Vérification des noms (DB, Table, et Colonnes)
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        logging.warning(f"Invalid table name '{table_name}' in DB '{table_metadata['Name of DB']}'.")
        has_warnings = True

    # Vérifier l'unicité du GUID de la table
    table_guid = table_metadata.get("GUID", "")
    if table_guid:
        if table_guid in guid_list_tab:
            logging.warning(f"GUID pour la table '{table_name}' n'est pas unique.")
            has_warnings = True
            # Ajouter un drapeau pour indiquer un GUID non unique
            table_metadata["NonUniqueGUID"] = True
        else:
            guid_list_tab.append(table_guid)
    else:
        logging.warning(f"La table '{table_name}' n'a pas de GUID.")
        has_warnings = True
        # Ajouter un drapeau pour indiquer un GUID manquant
        table_metadata["MissingGUID"] = True
    
    # Vérifier l'unicité et le format du Qualified Name de la table
    qualified_name_table = table_metadata.get("Qualified Name", "")
    if not pattern.match(qualified_name_table):
        logging.warning(f"Qualified Name '{qualified_name_table}' for table '{table_name}' does not follow the expected format.")
        has_warnings = True
    elif qualified_name_table in qualified_names_list:
        logging.warning(f"Qualified Name '{qualified_name_table}' for table '{table_name}' is not unique.")
        has_warnings = True
    else:
        qualified_names_list.append(qualified_name_table)

    # Vérifier que le nombre d'attributs correspond au nombre réel de colonnes définies
    expected_num_attributes = table_metadata.get("Number of Attributes", 0)
    actual_num_attributes = len(table_metadata.get("columns", []))
    
    if expected_num_attributes != actual_num_attributes:
        logging.warning(f"Number of Attributes '{expected_num_attributes}' for table '{table_name}' does not match the actual number of columns '{actual_num_attributes}'.")
        has_warnings = True

    # Check if table has columns
    if 'columns' not in table_metadata:
        logging.warning(f"Table {table_name} does not have columns information.")
        return False
    
    # Check if table has attributes
    if not table_metadata['columns']:
        logging.warning(f"Table {table_name} does not have any attributes.")
        return False
    
    # Dictionary to store data types of columns
    column_types = {}
    has_warnings = False  # Flag to track if there are any warnings
    
    # Check required fields
    required_fields = ['Name of Table', 'columns']  # Add more required fields as needed
    missing_fields = [field for field in required_fields if field not in table_metadata]
    if missing_fields:
        logging.warning(f"Table {table_name} is missing required fields: {', '.join(missing_fields)}")
        return False
    
    # Define the expected date formats
    create_time_format = "%Y-%m-%d %H:%M:%S"
    last_access_time_format = "%Y-%m-%d %H:%M:%S"  # Adjust if lastAccessTime has a different expected format

    # Extract the date fields
    create_time = table_metadata.get("Create Time", "")
    last_access_time = table_metadata.get("lastAccessTime", "")
    
    # Check "Create Time" format
    if create_time:
        try:
            datetime.strptime(create_time, create_time_format)
        except ValueError:
            logging.error(f"Invalid format for Create Time '{create_time}'. Expected format: '{create_time_format}'.")
            has_warnings = True
    
    ############################################################################
    # Check "lastAccessTime" format
    if last_access_time:
        try:
            datetime.strptime(last_access_time, last_access_time_format)
        except ValueError:
            logging.error(f"Invalid format for lastAccessTime '{last_access_time}'. Expected format: '{last_access_time_format}'.")
            has_warnings = True
    
    # Convertir les strings de date en objets datetime
    create_time_dt = datetime.strptime(create_time, create_time_format) if create_time else None
    last_access_time_dt = datetime.strptime(last_access_time, last_access_time_format) if last_access_time else None
    current_time = datetime.now()

    # Vérifier que la date de création n'est pas dans le futur
    if create_time_dt and create_time_dt > current_time:
        logging.error(f"Create Time '{create_time}' for table '{table_name}' is in the future.")
        has_warnings = True

    # Vérifier que lastAccessTime est égal ou postérieur à la date de création
    if create_time_dt and last_access_time_dt and last_access_time_dt < create_time_dt:
        logging.error(f"lastAccessTime '{last_access_time}' for table '{table_name}' is before its Create Time '{create_time}'.")
        has_warnings = True

    ############################################################################
    # Check "Status" is "ACTIVE"
    status = table_metadata.get("Status", "")
    if status == "ACTIVE":
        logging.info(f"Status of table '{table_name}' is ACTIVE.")
    else:
        logging.warning(f"Status of table '{table_name}' is not ACTIVE. Current status: {status}")
        has_warnings = True

    # Check and log table-specific attributes
    table_specific_attrs = {
        "Total Size": "N/A",
        "Number of Rows": "N/A",
        "Raw Data Size": "N/A",
        "Number of Files": "N/A",
    }

    # Vérification des valeurs numériques pour "Total Size", "Number of Rows", "Raw Data Size", et "Number of Files"
    numeric_attrs = ["Total Size", "Number of Rows", "Raw Data Size", "Number of Files"]
    invalid_attrs = []  # Liste pour suivre les attributs avec des valeurs invalides

    for attr in numeric_attrs:
        value = str(table_metadata.get(attr, "0"))
        if not value.isdigit() or int(value) < 0:
            logging.error(f"{attr} for table '{table_name}' has an invalid value: {value}.")
            has_warnings = True
            invalid_attrs.append(attr)  # Marquer l'attribut comme invalide

    for attr, default_value in table_specific_attrs.items():
        value = table_metadata.get(attr, default_value)

        if value != default_value and attr not in invalid_attrs:
            logging.info(f"{attr} for table '{table_name}': {value}")
        elif value == default_value:
            # Si la valeur est celle par défaut (indicative d'une donnée manquante), consigner un avertissement
            logging.warning(f"{attr} for table '{table_name}' is not available ({value}).")


    # Check if Classification Names exist
    classification_names = table_metadata.get('Classification Names', [])
    if classification_names:
        logging.info(f"Classification Names for table {table_name}: {', '.join(classification_names)}")
    else:
        logging.warning(f"No classification names found for table {table_name}")
    
    ############################################## Check attributes quality ##################################
    guid_set = set()
    for column in table_metadata['columns']:

        column_name = column['Name of Column']
        logging.info(f"Checking metadata quality for attribute: {column_name} in table: {table_name}")

        if column_name:
            classification_names = column.get('Classification Names', [])
            if classification_names:
                logging.info(f"Classification Names for attribute '{column_name}' in table '{table_name}': {', '.join(classification_names)}")
            else:
                logging.warning(f"No classification names found for attribute '{column_name}' in table '{table_name}'")


        # List of static data types
        valid_data_types = ['binary', 'boolean', 'byte', 'short', 'int', 'smallint', 'tinyint', 'bigint', 'long', 'float', 'double', 'bigdecimal', 'string', 'date', 'timestamp', 'array<string>', 'uniontype<int,float,string>', 'map<string,int>', 'struct<field1:int,field2:string>']

        # Regex pattern for types with dynamic lengths like varchar(..) and char(..)
        dynamic_types_pattern = re.compile(r'(varchar|char)\(\d+\)$|decimal\(\d+,\d+\)$')

        # Assuming 'column' is a dictionary representing the column's metadata
        column_type = column.get('Type')
        if not column_type:
            logging.warning(f"Data type not specified for attribute '{column_name}' in table '{table_name}'.")
            has_warnings = True
        else:
            # Check against static types list and dynamic types pattern
            if not (column_type in valid_data_types or dynamic_types_pattern.match(column_type)):
                logging.error(f"Invalid data type '{column_type}' for attribute '{column_name}' in table '{table_name}'.")
                has_warnings = True
            else:
                column_types[column_name] = column_type

        
        # Check completeness
        if column.get('Is Incomplete', False):
            logging.warning(f"Attribute '{column_name}' in table '{table_name}' is marked as incomplete.")
            has_warnings = True
        
        # Check for uniqueness of GUIDs
        guid = column.get('GUID')
        if guid:
            if guid in guid_set:
                logging.warning(f"GUID for attribute '{column_name}' in table '{table_name}' is not unique.")
                has_warnings = True
            else:
                guid_set.add(guid)
        
        # Check validity of attribute name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
            logging.warning(f"Invalid attribute name '{column_name}' in table '{table_name}'. Attribute names must start with a letter or underscore, and contain only letters, digits, and underscores.")
            has_warnings = True
    
    return not has_warnings
    
for table_name, table_info in metadata.items():
    if check_table_metadata_quality(table_info):
        logging.info(f"Table '{table_name}' metadata has good quality.")
    else:
        logging.warning(f"Table '{table_name}' metadata quality check failed.")
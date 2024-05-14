import json
import logging
import re
from datetime import datetime
# from kafka import KafkaConsumer
# from confluent_kafka import Consumer, KafkaError

logging.basicConfig(filename='metadata_quality_report_v2.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load JSON data
with open('output_f.json', 'r') as json_file:
    metadata = json.load(json_file)

qualified_names_list = []
pattern = re.compile(r'^[a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?@[a-zA-Z0-9_]+(?:\s+#\s+clusterName\s+to\s+use\s+in\s+qualifiedName\s+of\s+entities\.\s+Default:\s+[a-zA-Z0-9_]+)?$')
guid_list_tab = []


# Function to check metadata quality for a specific table
def check_table_metadata_quality(table_metadata):
    db_name = table_metadata['Name of DB']
    table_name = table_metadata['Name of Table']
    logging.info(f"Checking metadata quality for table: {table_name}")
    motifs = []
    anomaly = 0


    # Vérification des noms DB
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', db_name):
        logging.warning(f"Invalid database name: '{db_name}'.")
        has_warnings = True
        table_metadata['Valid_DbName'] = False
        motifs.append("Invalid Database Name")
        anomaly=+1
    else:
        table_metadata['Valid_DbName'] = True

    # Vérification des noms Table
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        logging.warning(f"Invalid table name '{table_name}' in DB '{table_metadata['Name of DB']}'.")
        has_warnings = True
        table_metadata['Valid_TableName'] = False
        motifs.append("Invalid Table Name") 
        anomaly=+1
    else:
        table_metadata['Valid_TableName'] = True


    # Vérification des GUID
    table_guid = table_metadata.get("GUID", "")
    
    if table_guid:
        if table_guid in guid_list_tab:
            logging.warning(f"GUID for the table '{table_name}' is not unique.")
            has_warnings = True
            table_metadata["Unique_GUID"] = False
            motifs.append("Non-unique GUID")
            anomaly=+1
        else:
            guid_list_tab.append(table_guid)
            table_metadata["Unique_GUID"] = True
    else:
        logging.warning(f"The table '{table_name}' does not have a GUID.")
        has_warnings = True
        table_metadata["Missing_GUID"] = True
        motifs.append("Missing GUID")
        anomaly=+1
    

    # Vérifier l'unicité et le format du Qualified Name de la table
    qualified_name_table = table_metadata.get("Qualified Name", "")
    if not pattern.match(qualified_name_table):
        logging.warning(f"Qualified Name '{qualified_name_table}' for table '{table_name}' does not follow the expected format.")
        has_warnings = True
        table_metadata["ValidQualifiedName"] = False
        motifs.append("Invalid Qualified Name")
        anomaly=+1

    elif qualified_name_table in qualified_names_list:
        logging.warning(f"Qualified Name '{qualified_name_table}' for table '{table_name}' is not unique.")
        has_warnings = True
        table_metadata["ValidQualifiedName"] = True
        table_metadata["Not_Unique_QualifiedName"] = True
        anomaly=+1

    else:
        qualified_names_list.append(qualified_name_table)


    # Vérifier que le nombre d'attributs correspond au nombre réel de colonnes définies
    expected_num_attributes = table_metadata.get("Number of Attributes", 0)
    actual_num_attributes = len(table_metadata.get("columns", []))
    
    if expected_num_attributes != actual_num_attributes:
        logging.warning(f"Number of Attributes '{expected_num_attributes}' for table '{table_name}' does not match the actual number of columns '{actual_num_attributes}'.")
        has_warnings = True
        table_metadata["Valid_Number_of_Attributes"] = False
        motifs.append("Invalid Number of Attributes")


    # Check if table has columns
    if 'columns' not in table_metadata:
        logging.warning(f"Table {table_name} does not have columns information.")
        motifs.append(f"Table does not have columns information.")
        return False
    

    # Check if table has attributes
    if not table_metadata['columns']:
        logging.warning(f"Table {table_name} does not have any attributes.")
        motifs.append(f"Table does not have any attributes.")
        table_metadata["No_Columns"] = True
        return False
    table_metadata["No_Columns"] = False


    # Dictionary to store data types of columns
    column_types = {}
    has_warnings = False  # Flag to track if there are any warnings
    
    # Check required fields
    required_fields = ['Name of Table', 'columns', 'GUID']  # Add more required fields as needed
    missing_fields = [field for field in required_fields if field not in table_metadata]
    if missing_fields:
        logging.warning(f"Table {table_name} is missing required fields: {', '.join(missing_fields)}")
        motifs.append(f"Table is missing required fields: {', '.join(missing_fields)}")
        return False
    
    # Define the expected date formats
    create_time_format = "%Y-%m-%d %H:%M:%S"
    last_access_time_format = "%Y-%m-%d %H:%M:%S"

    # Extract the date fields
    create_time = table_metadata.get("Create Time", "")
    last_access_time = table_metadata.get("lastAccessTime", "")
    
    # Check "Create Time" format
    if create_time:
        try:
            datetime.strptime(create_time, create_time_format)
        except ValueError:
            logging.error(f"Invalid format for Create Time '{create_time}'. Expected format: '{create_time_format}'.")
            motifs.append(f"Invalid format for Create Time '{create_time}'.")
            anomaly=+1
            has_warnings = True
    
    ############################################################################
    # Check "lastAccessTime" format
    if last_access_time:
        try:
            datetime.strptime(last_access_time, last_access_time_format)
        except ValueError:
            logging.error(f"Invalid format for lastAccessTime '{last_access_time}'. Expected format: '{last_access_time_format}'.")
            motifs.append(f"Invalid format for last Access Time '{last_access_time}'.")
            anomaly=+1
            has_warnings = True
    

    # Convertir les strings de date en objets datetime
    create_time_dt = datetime.strptime(create_time, create_time_format) if create_time else None
    last_access_time_dt = datetime.strptime(last_access_time, last_access_time_format) if last_access_time else None
    current_time = datetime.now()


    # Check if the creation date is in the future
    if create_time_dt and create_time_dt > current_time:
        logging.error(f"Create Time '{create_time}' for table '{table_name}' is in the future.")
        motifs.append(f"Create Time '{create_time}' is in the future.")
        has_warnings = True
        table_metadata["Future_CreateTime"] = True
        anomaly=+1
    table_metadata["Future_CreateTime"] = False


    # Check if the creation date is in the future
    if last_access_time_dt and last_access_time_dt > current_time:
        logging.error(f"last Access Time '{last_access_time_dt}' for table '{table_name}' is in the future.")
        motifs.append(f"last Access Time '{last_access_time_dt}' is in the future.")
        has_warnings = True
        table_metadata["Future_lastAccessTime"] = True
        anomaly=+1
    table_metadata["Future_lastAccessTime"] = False


    # Check if lastAccessTime is before create time
    if create_time_dt and last_access_time_dt and last_access_time_dt < create_time_dt:
        logging.error(f"lastAccessTime '{last_access_time}' for table '{table_name}' is before its Create Time '{create_time}'.")
        motifs.append(f"lastAccessTime '{last_access_time}' is before its Create Time '{create_time}'.")
        has_warnings = True
        table_metadata["LastAccessBeforeCreate"] = True
        anomaly=+1
    table_metadata["LastAccessBeforeCreate"] = False


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
    invalid_attrs = []
    numeric_attrs_corr = ["Total Size","Raw Data Size"]

    for attr in numeric_attrs:
        value = str(table_metadata.get(attr, "0"))
        if not value.isdigit() or int(value) < 0:
            logging.error(f"{attr} for table '{table_name}' has an invalid value: {value}.")
            motifs.append(f"{attr} has an invalid value: {value}.")
            has_warnings = True
            invalid_attrs.append(attr)
            anomaly=+1


    # If there are invalid attributes, add them to the metadata
    if invalid_attrs:
        table_metadata['Invalid_Attributes'] = invalid_attrs
        table_metadata['HasInvalidAttributes'] = True 
    else:
        table_metadata['HasInvalidAttributes'] = False

    for attr, default_value in table_specific_attrs.items():
        value = table_metadata.get(attr, default_value)

        if value != default_value and attr not in invalid_attrs:
            logging.info(f"{attr} for table '{table_name}': {value}")
        elif value == default_value:
            logging.warning(f"{attr} for table '{table_name}' is not available ({value}).")


    # Check if Classification Names exist
    classification_names = table_metadata.get('Classification Names', [])
    if classification_names:
        logging.info(f"Classification Names for table {table_name}: {', '.join(classification_names)}")
    else:
        logging.warning(f"No classification names found for table {table_name}")
        table_metadata["No_Classifications"] = True
        motifs.append("No Classifications")
        anomaly=+1
        has_warnings = True


    ############################################## Check attributes quality ##################################
    guid_set = set()
    for column in table_metadata['columns']:

        column_name = column['Name of Column']
        #logging.info(f"Checking metadata quality for attribute: {column_name} in table: {table_name}")
        if column_name:
            classification_names = column.get('Classification Names', [])
            if classification_names:
                logging.info(f"Classification Names for attribute '{column_name}' in table '{table_name}': {', '.join(classification_names)}")
            else:
                logging.warning(f"No classification names found for attribute '{column_name}' in table '{table_name}'")


        valid_data_types = ['binary', 'boolean', 'byte', 'short', 'int', 'smallint', 'tinyint', 'bigint', 'long', 'float', 'double', 'bigdecimal', 'string', 'date', 'timestamp', 'array<string>', 'uniontype<int,float,string>', 'map<string,int>', 'struct<field1:int,field2:string>']

        dynamic_types_pattern = re.compile(r'(varchar|char)\(\d+\)$|decimal\(\d+,\d+\)$')

        column_type = column.get('Type')
        if not column_type:
            logging.warning(f"Data type not specified for attribute '{column_name}' in table '{table_name}'.")
            has_warnings = True
            anomaly=+1
        else:
            if not (column_type in valid_data_types or dynamic_types_pattern.match(column_type)):
                logging.error(f"Invalid data type '{column_type}' for attribute '{column_name}' in table '{table_name}'.")
                has_warnings = True
                anomaly=+1
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
                anomaly=+1
            else:
                guid_set.add(guid)
        
        # Check validity of attribute name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
            logging.warning(f"Invalid attribute name '{column_name}' in table '{table_name}'. Attribute names must start with a letter or underscore, and contain only letters, digits, and underscores.")
            has_warnings = True

        if anomaly == 0:
            table_metadata['label'] = 1
        else:
            table_metadata['label'] = 0

        anomaly == 0

        if motifs:
            table_metadata['Motifs'] = motifs
            table_metadata['IsValid'] = False

        else:
            table_metadata['Motifs'] = ["All checks passed"]
            table_metadata['IsValid'] = True

    return not has_warnings


    
for table_name, table_info in metadata.items():
    if check_table_metadata_quality(table_info):
        logging.info(f"Table '{table_name}' metadata has good quality.")
    else:
        logging.warning(f"Table '{table_name}' metadata quality check failed.")

### Step 3: Write the Updated Data Back to the JSON File
with open('tables_updated.json', 'w') as file:
    json.dump(metadata, file, indent=4)
    
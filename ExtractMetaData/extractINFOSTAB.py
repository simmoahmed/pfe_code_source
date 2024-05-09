import json
import requests
from datetime import datetime as dt

url = 'http://localhost:21000/api/atlas/v2/search/basic'

data = {
    "typeName": "hive_table",
    "excludeDeletedEntities": True,
    "attributes": [
        "columns",
        "lastAccessTime",
        "parameters"
    ],
    "limit":10000
}

headers = {
    "Content-Type": "application/json"
}

auth = ("admin", "admin")

# Make the HTTP request
response = requests.post(url, data=json.dumps(data), headers=headers, auth=auth)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Extract the JSON response
    response_data = response.json()
    
    # Extract and format the desired information
    extracted_data = []
    for entity in response_data["entities"]:
        extracted_entity = {
            "Name of DB": entity["attributes"]["qualifiedName"].split(".")[0],
            "Name of Table": entity["attributes"]["name"],
            "Owner": entity["attributes"]["owner"],
            "Create Time": str(dt.fromtimestamp(entity["attributes"]["createTime"]/1000)),
            "lastAccessTime": str(dt.fromtimestamp(entity["attributes"]["lastAccessTime"]/1000)),
            "Total Size": entity["attributes"]["parameters"].get("totalSize", "N/A"),
            "Number of Rows": entity["attributes"]["parameters"].get("numRows", "N/A"),
            "Raw Data Size": entity["attributes"]["parameters"].get("rawDataSize", "N/A"),
            "Column Stats Accurate": entity["attributes"]["parameters"].get("COLUMN_STATS_ACCURATE", "N/A"),
            "Number of Files": entity["attributes"]["parameters"].get("numFiles", "N/A"),
            "Transient LastDdlTime": str(dt.fromtimestamp(int(entity["attributes"]["parameters"].get("transient_lastDdlTime", "0")))),
            "Qualified Name": entity["attributes"]["qualifiedName"],
            "GUID": entity["guid"],
            "Status": entity["status"],
            "Display Text": entity["displayText"],
            "Classification Names": entity["classificationNames"],
            "Meaning Names": entity["meaningNames"],
            "Meanings": entity["meanings"],
            "Is Incomplete": entity["isIncomplete"],
            "Labels": entity["labels"],
            "Number of Attributes": len(entity["attributes"].get("columns", [])),  # Also handle potentially missing 'columns'
        }

        extracted_data.append(extracted_entity)
    
    # Print the extracted data in JSON format
    # print(json.dumps(extracted_data, indent=4))
else:
    print("Error:", response.status_code)

def tabs():
    return extracted_data
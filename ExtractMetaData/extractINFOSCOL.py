import json
import requests

url = 'http://localhost:21000/api/atlas/v2/search/basic'

data = {
    "typeName": "hive_column",
    "excludeDeletedEntities": True,
    "attributes": [
        "type"
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
            "Name of Column": entity["attributes"]["name"],
            "Name of Table": entity["attributes"]["qualifiedName"].split(".")[1],
            "Type": entity["attributes"]["type"],
            "Type Name": entity["typeName"],
            "Owner": entity["attributes"]["owner"],
            "GUID": entity["guid"],
            "Qualified Name": entity["attributes"]["qualifiedName"],
            "Status": entity["status"],
            "Display Text": entity["displayText"],
            "Classification Names": entity["classificationNames"],
            "Meaning Names": entity["meaningNames"],
            "Meanings": entity["meanings"],
            "Is Incomplete": entity["isIncomplete"],
            "Labels": entity["labels"],
            # "Create Time": entity["attributes"]["createTime"],
            # "Qualified Name": entity["attributes"]["qualifiedName"],
        }
        extracted_data.append(extracted_entity)
    
    # Print the extracted data in JSON format
    # print(json.dumps(extracted_data, indent=4))
else:
    print("Error:", response.status_code)

def cols():
    return extracted_data
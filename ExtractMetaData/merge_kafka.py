from extractINFOSTAB import tabs
from extractINFOSCOL import cols
import json
from confluent_kafka import Producer

tabs = tabs()
cols = cols()
tab_dict = {}

for tab in tabs:
    tab['columns'] = []
    for col in cols:
        if tab['Name of Table'] == col['Name of Table']:
            tab['columns'].append(col)
    tab_dict[tab['Name of Table']] = tab

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:9093',  # Replace with your Kafka broker address
    'client.id': 'metadata_group',
}

# Create Kafka producer
producer = Producer(conf)

# Kafka topic
topic = 'metadata_topic'

# Callback function to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Convert dictionary to JSON
json_data = json.dumps(tab_dict)

# Produce JSON data to Kafka topic
producer.produce(topic, json_data.encode('utf-8'), callback=delivery_report)

# Wait for all messages to be delivered
producer.flush()

# Handle message delivery callbacks
while True:
    producer.poll(0)
    if producer.flush() == 0:
        break

print("Data sent to Kafka topic successfully.")

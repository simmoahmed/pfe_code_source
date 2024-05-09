from kafka import KafkaProducer
import json

# Kafka broker address
bootstrap_servers = 'localhost:9093'

# Input topic
input_topic = 'input_topic'

# Example metadata
metadata = {
    'name': 'Dataset1',
    'description': 'This is a sample dataset',
    'owner': 'John Doe',
    'created_at': '2023-01-01 12:00:00',  # Valid datetime format
    'file_format': 'CSV'
}

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Send the metadata to the input topic
producer.send(input_topic, value=metadata)
print("Metadata sent to input topic:", metadata)

# Close Kafka producer
producer.close()
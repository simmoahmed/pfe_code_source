from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json

# Kafka broker address
bootstrap_servers = 'localhost:9093'

# Input and output topics
input_topic = 'input_topic'
output_topic = 'output_topic'
issues_topic = 'issues_topic'  # New topic for issues

# Initialize Kafka consumer
consumer = KafkaConsumer(input_topic, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Initialize Kafka producer for output and issues
producer_output = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                value_serializer=lambda x: json.dumps(x).encode('utf-8'))

producer_issues = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Define a function to perform quality checks on metadata
def check_metadata_quality(metadata):
    issues = []  # List to store all issues with the metadata
    
    # Check for completeness
    required_fields = ['name', 'description', 'owner', 'created_at']
    missing_fields = [field for field in required_fields if field not in metadata]
    if missing_fields:
        issues.append(f"Missing required fields: {missing_fields}")
    
    # Check for validity
    if 'created_at' in metadata:
        try:
            datetime.strptime(metadata['created_at'], '%Y-%m-%d %H:%M:%S')
        except ValueError:
            issues.append("Invalid format for 'created_at'")
    
    # Add more validity checks as needed
    
    if issues:
        return issues  # Return all issues if any
    else:
        return "Metadata quality is good!"  # Return success message if no issues found

# Define a function to process metadata
def process_metadata(metadata):
    # Process metadata and perform quality checks
    quality_check_result = check_metadata_quality(metadata)
    
    if isinstance(quality_check_result, list):
        # Send each issue to the issues topic
        for issue in quality_check_result:
            producer_issues.send(issues_topic, value=issue)
            print("Sending Issues to issues_topic")
        
        # Return None to indicate failed quality check
        return None
    else:
        # Metadata quality is good, proceed with processing
        # For demonstration, let's just add a timestamp to the metadata
        metadata['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return metadata

# Consume data from the input topic, process it, and send it to the output topic
for message in consumer:
    metadata = message.value
    
    # Process metadata
    processed_metadata = process_metadata(metadata)
    
    if processed_metadata is not None:
        # Send the processed metadata to the output topic
        producer_output.send(output_topic, value=processed_metadata)
        print("Processed metadata sent to output topic:", processed_metadata)

# Close Kafka consumer and producers
consumer.close()
producer_output.close()
producer_issues.close()

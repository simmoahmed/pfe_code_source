from kafka import KafkaConsumer

# Kafka consumer configuration
consumer = KafkaConsumer(
    'output_topic',
    bootstrap_servers=['localhost:9093'],  # Change if your Kafka broker is hosted elsewhere
    auto_offset_reset='earliest',  # To read messages from the beginning of the topic
)

# Infinite loop to keep the consumer running and listening for new messages
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")


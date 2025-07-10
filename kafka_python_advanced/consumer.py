from kafka import KafkaConsumer

def decode_str(string):
    if string is None:
        return(None)
    else:
        return(string.decode("utf-8"))

kafka_consumer = KafkaConsumer(
    "test",
    bootstrap_servers="localhost:9092",
    group_id="consumer",
    auto_offset_reset="earliest",
    key_deserializer=decode_str,
    value_deserializer=decode_str,
    enable_auto_commit=False,
    consumer_timeout_ms=5000  # stops after 5s without new messages
)

for message in kafka_consumer:
    print(f"Received: partition={message.partition}, "
          f"offset={message.offset}, value={message.value}")

kafka_consumer.close()
print("Consumer closed")
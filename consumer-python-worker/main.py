from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import time

# Kafka configuration
KAFKA_BROKER = 'kafka:29092' # Service name in docker-compose.yml
KAFKA_TOPIC = 'orders_processing' # Topic name from Laravel's OrderController
# Group ID for consumer
GROUP_ID = 'python-order-processors'

def process_message(msg):
    """
    All logic to process the message goes here
    """
    try:
        # Decode Kafka messages.
        event_value = msg.value().decode('utf-8')
        event_data = json.loads(event_value)

        # Extract main information
        order_id = event_data['data']['order_id']
        event_type = event_data['event_type']

        # -- Intentional failure simulation
        # if order_id == 1003:
        #     print(f"🚨 Fatal error: Failed to process Order ID {order_id} (divided by zero).")
        #     raise ValueError("Irrecoverable error simulated.")
        # -- End of intentional failure simulation

        print(f"[{event_type}] Received event for Order ID: {order_id}")

        # Async logic simulation 
        print(f"  > Simulating inventory check to the Order ID: {order_id}...")
        time.sleep(2)
        print(f"  > ✅ Order ID {order_id} processed successfully")

    except Exception as e:
        print(f"❌ Error processing message: {e}. Message was not processed successfully.")
        print(f"Raw message: {msg.value()}")
        # IMPORTANT: Avoid consumer.commit(). The offset cannot be advanced.
        return False
    
    return True


def consume_events():
    """ 
    Configure and run Kafka consumer
    """
    # Consumer configuration
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest', # Start from the beginning of the topic
        'enable.auto.commit': False, # Do not commit automatically
    }

    # Create the consumer
    consumer = Consumer(conf)

    # Subscribe to the topic
    consumer.subscribe([KAFKA_TOPIC])

    try:
        print(f"🚀 Python consumer started. Listening topic {KAFKA_TOPIC}...")

        while True:
            # Poll: Wait for messages (timeout of 1.0 seconds)
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition reached (no more messages)
                    continue
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    # Topic or partition not found (Must be exist if producer was configured to create it)
                    print(f"Error: Unknown topic. Make sure the topic '{KAFKA_TOPIC}' exists")
                elif msg.error().code() == KafkaError._UNKNOWN_GROUP:
                    # Group not found (Must be exist if producer was configured to create it)
                    print(f"Error: Unknown group. Make sure the group '{GROUP_ID}' exists")
                else:
                    raise KafkaException(msg.error())
            else:
                # Message received successfully
                success = process_message(msg)

                if success:
                    # Commit: Confirm that offset has been processed
                    # This is important to avoid reprocessing messages
                    consumer.commit(message=msg, asynchronous=True)
                else:
                    # Do not commit. The offset cannot be advanced.
                    print(f"🛑 Irrecoverable error. Finished worker to simulate a failure.")
                    break
            
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        # Close the consumer
        consumer.close()
        print("Consumer stopped.")

if __name__ == '__main__':
    consume_events()
import json, time, os, threading, keyboard
from fastapi import APIRouter, HTTPException, UploadFile, File
from confluent_kafka import Consumer, KafkaException
from cryptographic_tool import extract_did_from_private_key, extract_did_from_private_key_bytes
import mysql.connector
from dotenv import load_dotenv

load_dotenv()  # This will load the .env file from the current directory

# Configuration for Kafka broker
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'consumer_group',
    'auto.offset.reset': 'earliest',  # Start reading from the earliest message (modify to latest)
    'enable.auto.commit': False,  # Don't commit offsets automatically
    }

# MySQL Database Configuration
db_config = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),  # Ensure this is set in environment
    "database": os.getenv("DB_NAME", "did_registry")
}

router = APIRouter()

# Track authorized consumers and active consumer threads
authorized_users = {}
consumers = {}
consumer_threads = {}

def delivery_report(err, msg):
    if err:
        print(f'Error: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def authorize(private_key_path: str, topic: str) -> bool:
    """Authorize the consumer before subscribing to a Kafka topic."""
    print(f"Loading private key from: {private_key_path}")
    consumer_did = extract_did_from_private_key(private_key_path)

    db = mysql.connector.connect(**db_config)
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT allowed_did FROM did_keys WHERE kafka_topic = %s", (topic,))
    result = cursor.fetchone()
    db.close()

    if result and result["allowed_did"]:
        allowed_dids = result["allowed_did"].split(",")
        if consumer_did in allowed_dids:
            print(f"Authentication successful. {consumer_did} is authorized for topic {topic}.")
            authorized_users[topic] = consumer_did
            return True
        else:
            print(f"Authentication failed. {consumer_did} is NOT authorized for topic {topic}.")
            return False
    else:
        print(f"Authentication failed. No allowed consumers for {topic}.")
        return False

# Use this one:
def authorize_keyfile(private_key_bytes: bytes, topic: str) -> bool:
    """Authorize the consumer before subscribing to a Kafka topic."""
    try:
        consumer_did = extract_did_from_private_key_bytes(private_key_bytes)
    except Exception as e:
        print(f"Failed to extract DID: {e}")
        return False

    db = mysql.connector.connect(**db_config)
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT allowed_did FROM did_keys WHERE kafka_topic = %s", (topic,))
    result = cursor.fetchone()
    db.close()

    if result and result["allowed_did"]:
        allowed_dids = result["allowed_did"].split(",")
        if consumer_did in allowed_dids:
            print(f"Authorization successful. {consumer_did} is authorized for topic {topic}.")
            authorized_users[topic] = consumer_did
            return True
        else:
            print(f"Authorization failed. {consumer_did} is NOT authorized for topic {topic}.")
    else:
        print(f"Authorization failed. No allowed consumers for {topic}.")

    return False

    
def receive_messages(topic:str):
    """Continuously poll messages from Kafka for the given topic."""
    consumer = consumers.get(topic)
    if not consumer:
        print(f"No consumer found for topic: {topic}")
        return
    
    print(f"Listening for messages on topic '{topic}'... Press 'Esc' to stop.")

    try:
        while topic in authorized_users:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            print(f"Received message on '{topic}': {msg.value().decode('utf-8')}")
            
            if keyboard.is_pressed("esc"):
                print("Stopping consumer.")
                break

    except Exception as e:
        print(f"Error in consumer: {str(e)}")
    finally:
        consumer.close()
        print(f"Consumer for topic {topic} stopped.")

#@router.post("/authorize")
#def api_authorize(private_key_path: str, topic: str):
#    """Authorize a consumer and start listening to messages."""
#    if authorize(private_key_path, topic):
#        consumer = Consumer(KAFKA_CONFIG)
#        consumer.subscribe([topic])
#        consumers[topic] = consumer
#        return {"message": f"Authorization successful for topic {topic}"}
#    else:
#        raise HTTPException(status_code=403, detail="Authorization failed.")

@router.post("/authorize")
def api_authorize(private_key: UploadFile = File(...), topic: str = None):
    key_bytes = private_key.file.read()
    if authorize_keyfile(key_bytes, topic):
        consumer = Consumer(KAFKA_CONFIG)
        consumer.subscribe([topic])
        consumers[topic] = consumer
        return {"message": f"Authorization successful for topic {topic}"}
    else:
        raise HTTPException(status_code=403, detail="Authorization failed.")
    
@router.post("/start") # For Continuous Streaming
def start_consumer(topic: str):
    """Start consuming messages from Kafka topic."""
    if topic not in authorized_users: # Check if user has been authorized
        raise HTTPException(status_code=401, detail="User not authorized for this topic.")

    #if topic in consumer_threads:  # Prevent starting multiple consumers
    #    return {"message": f"Consumer for topic {topic} is already running."}

    thread = threading.Thread(target = receive_messages, args=(topic,), daemon=True)
    consumer_threads[topic] = thread
    thread.start()

    return {"message": f"Consumer started for topic {topic}"}

@router.post("/stop")
def stop_consumer(topic: str):
    """Stop consuming messages from Kafka topic."""
    if topic in authorized_users:
        authorized_users.pop(topic, None)
        return {"message": f"Consumer for topic {topic} stopped."}
    else:
        return {"message": f"No active consumer found for topic {topic}."}
import json
import time
import threading
from fastapi import APIRouter, HTTPException
from confluent_kafka import Producer
import keyboard
from cryptographic_tool import extract_did_from_private_key
import mysql.connector

# Configuration for Kafka broker
KAFKA_CONFIG = {'bootstrap.servers': 'localhost:9092'}

# MySQL Database Configuration
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "MoonwalkerJPL85!",
    "database": "did_registry"
}

router = APIRouter()

authenticated_users = {}
producers = {}
producer_threads = {}

def delivery_report(err, msg):
    if err:
        print(f'Error: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def authenticate(private_key_path: str, topic: str) -> bool:
    print(f"Loading private key from: {private_key_path}")
    producer_did = extract_did_from_private_key(private_key_path)

    db = mysql.connector.connect(**db_config)
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT did FROM did_keys WHERE did = %s AND kafka_topic = %s", (producer_did, topic))
    result = cursor.fetchone()
    db.close()

    if result:
        print(f"Authentication successful: {producer_did} is authorized for topic {topic}.")
        authenticated_users[topic] = producer_did
        return True
    else:
        print(f"Authentication failed: {producer_did} is NOT authorized for topic {topic}.")
        return False

def send_data(topic: str):
    producer = producers.get(topic)
    if not producer:
        print(f"No producer found for topic: {topic}")
        return
    
    print(f"Producer started for topic: {topic}")

    while topic in authenticated_users:
        message = json.dumps({"data": "Simulation update"})
        producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
        producer.flush()
        time.sleep(0.02)  # 20 ms delay for continuous sending

        if keyboard.is_pressed('q'):
            print(f"Stopping producer for topic: {topic}")
            authenticated_users.pop(topic, None)  # Remove user authentication
            break

    print(f"Producer stopped for topic: {topic}")

@router.post("/authenticate")
def api_authenticate(private_key_path: str, topic: str):
    if authenticate(private_key_path, topic):
        producers[topic] = Producer(KAFKA_CONFIG)
        return {"message": f"Authentication successful for topic {topic}"}
    else:
        raise HTTPException(status_code=403, detail="Authentication failed.")

@router.post("/start")
def start_producer(topic: str):
    if topic not in authenticated_users: # Check if user has authenticated
        raise HTTPException(status_code=401, detail="User not authenticated for this topic.")

    if topic in producer_threads:  # Prevent starting multiple producers
        return {"message": f"Producer for topic {topic} is already running."}

    thread = threading.Thread(target=send_data, args=(topic,), daemon=True)
    producer_threads[topic] = thread
    thread.start()

    return {"message": f"Producer started for topic {topic}"}

@router.post("/stop")
def stop_producer(topic: str):
    if topic in authenticated_users:
        authenticated_users.pop(topic, None)
        return {"message": f"Producer for topic {topic} stopped."}
    else:
        return {"message": f"No active producer found for topic {topic}."}
    
@router.post("/send-message")
def send_message(topic: str, message: dict):
    """Sends a user-provided JSON message to Kafka after authentication."""
    if topic not in authenticated_users:
        raise HTTPException(status_code=401, detail="User not authenticated for this topic.")

    producer = Producer(KAFKA_CONFIG)
    producer.produce(topic, json.dumps(message).encode('utf-8'), callback=delivery_report)
    producer.flush()
    return {"message": "Message sent successfully"}


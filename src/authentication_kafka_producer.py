import json, time, os, threading
from fastapi import APIRouter, HTTPException, UploadFile, File
from confluent_kafka import Producer
from cryptographic_tool import extract_did_from_private_key, extract_did_from_private_key_bytes
import mysql.connector
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import Optional, List, Any

load_dotenv()  # This will load the .env file from the current directory

# Testing only
TEST_MODE_TOPICS = {"test-topic"}
# Configuration for Kafka broker
KAFKA_CONFIG = {'bootstrap.servers': '172.26.30.154:9092'}

# MySQL Database Configuration
db_config = {
    "host": os.getenv("DB_HOST", "172.26.30.154"),
    "port": os.getenv("DB_PORT", "3306"),
    "user": os.getenv("DB_USER", "did_app"),
    "password": os.getenv("DB_PASSWORD", "csun2014"),  # Ensure this is set in environment
    "database": os.getenv("DB_NAME", "did_registry")
}

# Models for Physics
class Inertia(BaseModel):
    # "diagonal" or "full_matrix" from Omniverse
    type: Optional[str] = None
    # Allow either a flat list [ix,iy,iz] or nested 3x3
    values: Optional[Any] = None

class MessageWithPhysics(BaseModel):
    # Match what Omniverse Cadre script currently writes:
    # position: [x, y, z]
    # rotation: [w, x, y, z]
    position: Optional[List[float]] = None
    rotation: Optional[List[float]] = None
    mass: Optional[float] = None
    inertia: Optional[Inertia] = None
    center_of_mass: Optional[List[float]] = None
    hosting: Optional[bool] = None
    modifiedDate: Optional[str] = None

    # IMPORTANT: let Unity’s existing HSML fields pass through unchanged
    class Config:
        extra = "allow"


router = APIRouter()

authenticated_users = {}
producers = {}
producer_threads = {}

def delivery_report(err, msg):
    if err:
        print(f'Error: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
        
def authenticate_keyfile(private_key_bytes: bytes, topic: str) -> bool:
    try:
        producer_did = extract_did_from_private_key_bytes(private_key_bytes)
    except Exception as e:
        print(f"Failed to extract DID: {e}")
        return False

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

def send_data(topic: str, json_message:dict):
    producer = producers.get(topic)
    if not producer:
        print(f"No producer found for topic: {topic}")
        return
    
    print(f"Producer started for topic: {topic}")

    while topic in authenticated_users:
        message = json.dumps(json_message)  # Send the actual JSON file contents
        producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
        producer.flush()
        time.sleep(0.05)  # 50 ms delay for continuous sending

    print(f"Producer stopped for topic: {topic}")
    

@router.post("/authenticate")
def api_authenticate(private_key: UploadFile = File(...), topic: str = None):
    key_bytes = private_key.file.read()  # Read uploaded key as bytes

    if authenticate_keyfile(key_bytes, topic):  # Use the simplified helper
        producers[topic] = Producer(KAFKA_CONFIG)
        return {"message": f"Authentication successful for topic {topic}"}
    else:
        raise HTTPException(status_code=403, detail="Authentication failed.")


@router.post("/start") # For Continuous Streaming
def start_producer(topic: str, json_message: dict):
    if topic not in authenticated_users: # Check if user has authenticated
        raise HTTPException(status_code=401, detail="User not authenticated for this topic.")

    if topic in producer_threads:  # Prevent starting multiple producers
        return {"message": f"Producer for topic {topic} is already running."}

    thread = threading.Thread(target=send_data, args=(topic, json_message), daemon=True)
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
    
@router.post("/send-message") # For Manual Messages - for more control of what is sent and when
def send_message(topic: str, message: dict):
    """
    Sends a user-provided JSON message to Kafka.
    For topics in TEST_MODE_TOPICS, authentication is bypassed (dev only).
    """
    if topic not in authenticated_users and topic not in TEST_MODE_TOPICS:
        raise HTTPException(status_code=401, detail="User not authenticated for this topic.")

    producer = Producer(KAFKA_CONFIG)
    producer.produce(topic, json.dumps(message).encode('utf-8'), callback=delivery_report)
    producer.flush()
    return {"message": "Message sent successfully"}

import json
import os
import subprocess
import random
import string
import mysql.connector
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
from cryptographic_tool import extract_did_from_private_key
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pydantic import BaseModel

# No Interactive Prompts:
# All input is now passed via HTTP request parameters and file uploads.

# File Uploads:
# The HSML JSON, user private key, and (if needed) credential domain private key are all received as file uploads.

# Form Fields:
# The output directory and optionally the registered_by DID are provided as form data.

# Endpoint Separation:
# The /entity endpoint now directly calls the business logic in register_entity rather than invoking interactive functions.


router = APIRouter()

# Kafka Configuration
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092"
}
admin_client = AdminClient(KAFKA_CONFIG)
producer = Producer(KAFKA_CONFIG)

# MySQL Database Configuration
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "MoonwalkerJPL85!",
    "database": "did_registry"
}

def connect_db():
    return mysql.connector.connect(**db_config)

def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1):
    """Creates a Kafka topic using Confluent Kafka AdminClient."""
    topic_list = [NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
    fs = admin_client.create_topics(topic_list)
    for topic, f in fs.items():
        try:
            f.result()  # Block until topic creation is done
            print(f"Kafka topic '{topic}' created successfully.")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")

def generate_random_string(length=6):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def send_kafka_message(topic, message):
    """Sends a message to a Kafka topic."""
    try:
        producer.produce(topic, json.dumps(message))
        producer.flush()
        print(f"Message sent to Kafka topic '{topic}': {message}")
    except Exception as e:
        print(f"Failed to send message to Kafka topic '{topic}': {e}")

def generate_did_key(output_directory):
    """
    Runs the CLI tool to generate a unique DID:key and extract the private key.
    The tool is expected to output a line like:
    "Generated DID:key: did:key:xyz..."
    """
    while True:
        # Path to store the private_key.pem
        private_key_path = os.path.join(output_directory, "private_key.pem")
        # Run CLI tool to generate a private key file
        result = subprocess.run(
            ["python", "CLItool.py", "--export-private", "--private-key-path", private_key_path],
            capture_output=True, text=True
        )
        print(f"Subprocess output:\n{result.stdout}")
        output = result.stdout.splitlines()
        did_key = None
        for line in output:
            if line.startswith("Generated DID:key:"):
                did_key = line.split("Generated DID:key:")[1].strip()
        if not did_key:
            raise ValueError("Failed to generate DID:key from output")
        
        # Read the generated private key file
        try:
            with open(private_key_path, "rb") as key_file:
                private_key = key_file.read()
        except FileNotFoundError:
            raise ValueError(f"Private key file '{private_key_path}' not found")
        
        # Ensure the new DID is unique in the database
        db = connect_db()
        cursor = db.cursor()
        cursor.execute("SELECT COUNT(*) FROM did_keys WHERE did = %s", (did_key,))
        if cursor.fetchone()[0] == 0:
            return did_key, private_key.decode("utf-8")

def register_entity(data: dict, output_directory: str, registered_by: str = None, 
                    credential_domain_private_key_content: str = None):
    """
    Validates, registers, and stores an HSML entity using the provided JSON data.
    Note: For Credential registrations, an additional private key file for the domain
    must be provided via credential_domain_private_key_content.
    """
    # Validate that the HSML JSON uses the expected context
    if "@context" not in data or "https://digital-twin-interoperability.github.io/hsml-schema-context/hsml.jsonld" not in data["@context"]:
        return {"status": "error", "message": "Not a valid HSML JSON"}
    
    print("HSML JSON accepted.")
    
    entity_type = data.get("@type")
    required_fields = {
        "Entity": ["name", "description"],
        "Person": ["name", "birthDate", "email"],
        "Agent": ["name", "creator", "dateCreated", "dateModified", "description"],
        "Credential": ["name", "description", "issuedBy", "accessAuthorization", "authorizedForDomain"],
        "Organization": ["name", "description", "url", "address", "foundingDate", "email"]
    }
    if entity_type not in required_fields:
        return {"status": "error", "message": "Unknown or missing entity type"}
    
    missing_fields = [field for field in required_fields[entity_type] if field not in data]
    if missing_fields:
        return {"status": "error", "message": f"Missing required fields: {missing_fields}"}
    
    if entity_type == "Person" and "affiliation" not in data:
        print("Warning: 'affiliation' field is missing.")
    
    if entity_type == "Credential" and ("validFrom" not in data or "validUntil" not in data):
        print("Warning: Credential has no expiration date.")
    
    if entity_type == "Entity" and "linkedTo" not in data:
        print("Warning: Object not linked to any other Entity. It will be registered under this user’s SWID.")
    
    # If a swid exists in the provided JSON, warn that it will be overwritten.
    swid = data.get("swid")
    db = connect_db()
    cursor = db.cursor()
    if swid:
        cursor.execute("SELECT COUNT(*) FROM did_keys WHERE did = %s", (swid,))
        if cursor.fetchone()[0] > 0:
            print(f"Warning: The provided 'swid' ({swid}) already exists and will be overwritten.")
    
    # Generate a new DID:key and associated private key using the CLI tool
    did_key, generated_private_key = generate_did_key(output_directory)
    data["swid"] = did_key
    print(f"Generated unique SWID: {did_key}")
    public_key_part = did_key.replace("did:key:", "")
    
    # If no registered_by is provided, assume this is a new registration.
    if registered_by is None:
        registered_by = did_key
    
    topic_name = None
    # If the entity is an Agent, create a Kafka topic for it.
    if entity_type == "Agent":
        flagUniqueName = False
        while not flagUniqueName:
            random_suffix = generate_random_string()
            topic_name = f"{data['name'].replace(' ', '_').lower()}_{random_suffix}"
            cursor.execute("SELECT COUNT(*) FROM did_keys WHERE kafka_topic = %s", (topic_name,))
            if cursor.fetchone()[0] == 0:
                flagUniqueName = True
                create_kafka_topic(topic_name)
                send_kafka_message(topic_name, {"message": f"New Agent registered: {data['name']}"})
    
    # If the entity is a Credential, perform additional checks.
    if entity_type == "Credential":
        issued_by_did = data.get("issuedBy", {}).get("swid")
        authorized_for_domain_did = data.get("authorizedForDomain", {}).get("swid")
        credential_domain_name = data.get("authorizedForDomain", {}).get("name")
        access_authorization_did = data.get("accessAuthorization", {}).get("swid")
        if not (issued_by_did and authorized_for_domain_did and access_authorization_did):
            return {"status": "error", "message": "Missing required 'swid' in Credential fields"}
        if issued_by_did != registered_by:
            return {"status": "error", "message": "issuedBy field must match the user registering the Credential"}
        
        if credential_domain_private_key_content is None:
            return {"status": "error", "message": f"Credential registration requires private key for '{credential_domain_name}'"}
        
        # Write the provided credential domain private key to a temporary file for DID extraction.
        temp_key_path = os.path.join(output_directory, f"{credential_domain_name.replace(' ', '_')}_private_key.pem")
        with open(temp_key_path, "w") as f:
            f.write(credential_domain_private_key_content)
        credential_domain_did = extract_did_from_private_key(temp_key_path)
        if credential_domain_did != authorized_for_domain_did:
            return {"status": "error", "message": f"Invalid private key for '{credential_domain_name}'"}
        
        cursor.execute("SELECT metadata FROM did_keys WHERE did = %s", (authorized_for_domain_did,))
        domain_did_result = cursor.fetchone()
        if not domain_did_result:
            return {"status": "error", "message": f"DID not found in database. Please register '{credential_domain_name}' first."}
        
        domain_data = json.loads(domain_did_result[0])
        new_access_auth = data.get("accessAuthorization", {})
        if "canAccess" not in domain_data:
            domain_data["canAccess"] = [new_access_auth]
        else:
            existing_can_access = domain_data.get("canAccess", [])
            if not isinstance(existing_can_access, list):
                existing_can_access = [existing_can_access]
            existing_swids = {entry["swid"] for entry in existing_can_access if "swid" in entry}
            if new_access_auth.get("swid") and new_access_auth["swid"] not in existing_swids:
                existing_can_access.append(new_access_auth)
            else:
                print(f"{new_access_auth['swid']} already has access to '{credential_domain_name}'")
            domain_data["canAccess"] = existing_can_access
        
        cursor.execute("UPDATE did_keys SET metadata = %s WHERE did = %s", (json.dumps(domain_data), authorized_for_domain_did))
        domain_json_output = os.path.join(output_directory, f"{domain_data['name'].replace(' ', '_')}.json")
        with open(domain_json_output, "w") as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Updated {credential_domain_name} JSON saved to: {domain_json_output}")
        
        cursor.execute("SELECT allowed_did FROM did_keys WHERE did = %s", (authorized_for_domain_did,))
        allowed_did_result = cursor.fetchone()
        if allowed_did_result and allowed_did_result[0]:
            allowed_did_list = allowed_did_result[0].split(",")
        else:
            allowed_did_list = []
        new_did = new_access_auth.get("swid")
        if new_did and new_did not in allowed_did_list:
            allowed_did_list.append(new_did)
        allowed_did_string = ",".join(allowed_did_list)
        cursor.execute("UPDATE did_keys SET allowed_did = %s WHERE did = %s", 
                       (allowed_did_string, authorized_for_domain_did))
    
    # Store the entity information in the database.
    cursor.execute(
        "REPLACE INTO did_keys (did, public_key, metadata, registered_by, kafka_topic) VALUES (%s, %s, %s, %s, %s)",
        (did_key, public_key_part, json.dumps(data), registered_by, topic_name)
    )
    db.commit()
    db.close()
    
    # Save the generated private key and the updated JSON to the output directory.
    private_key_output = os.path.join(output_directory, "private_key.pem")
    json_output = os.path.join(output_directory, f"{data['name'].replace(' ', '_')}.json")
    
    with open(private_key_output, "w") as private_key_file:
        private_key_file.write(generated_private_key)
    
    with open(json_output, "w") as json_file:
        json.dump(data, json_file, indent=4)
    
    print(f"Private key saved to: {private_key_output}")
    print(f"Updated JSON saved to: {json_output}")
    
    return {
        "status": "success",
        "message": "Entity registered successfully",
        "did_key": did_key,
        "private_key_path": private_key_output,
        "updated_json_path": json_output
    }

@router.post("/entity")
async def register_entity_api(
    hsml_file: UploadFile = File(...),
    private_key_file: UploadFile = File(...),
    output_directory: str = Form(...),
    registered_by: str = Form(None),
    credential_domain_private_key_file: UploadFile = File(None)
):
    """
    Endpoint to register an HSML entity.
    
    - **hsml_file**: The HSML JSON file.
    - **private_key_file**: The private key file for the user (for authentication).
    - **output_directory**: Directory path where updated JSON and generated keys are saved.
    - **registered_by**: Optional DID of the user registering the entity.
    - **credential_domain_private_key_file**: Optional file for Credential registrations.
    """
    try:
        hsml_content = await hsml_file.read()
        data = json.loads(hsml_content)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid HSML JSON format")
    
    # Read the user private key content (assumed to be used for authentication elsewhere)
    private_key_content = (await private_key_file.read()).decode("utf-8")
    
    credential_domain_private_key_content = None
    if credential_domain_private_key_file is not None:
        credential_domain_private_key_content = (await credential_domain_private_key_file.read()).decode("utf-8")
    
    try:
        result = register_entity(data, output_directory, registered_by, credential_domain_private_key_content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("your_module_name:router", host="0.0.0.0", port=8000, reload=True)

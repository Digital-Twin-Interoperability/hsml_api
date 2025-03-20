from fastapi import APIRouter, HTTPException, File, UploadFile
from fastapi.responses import JSONResponse
import mysql.connector as mysql
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import json, subprocess, os, random, string
from cryptographic_tool import extract_did_from_private_key  
from dotenv import load_dotenv

load_dotenv()  # This will load the .env file from the current directory
router = APIRouter()

# Securely load database configuration from environment or config (avoid hard-coding passwords)
db_config = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),  # Ensure this is set in environment
    "database": os.getenv("DB_NAME", "did_registry")
}

# Initialize Kafka client (adjust bootstrap server via env if needed)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
try:
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
except Exception as e:
    # If Kafka initialization fails, raise an error (could also choose to disable Kafka features)
    print(f"Kafka initialization error: {e}")
    admin_client = None
    producer = None

def connect_db():
    """Connect to the MySQL database using credentials in db_config."""
    try:
        return mysql.connect(**db_config)
    except mysql.Error as err:
        # If database connection fails, raise HTTPException for API calls to catch
        raise HTTPException(status_code=500, detail=f"Database connection failed: {err}")

def create_kafka_topic(topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
    """Create a Kafka topic for an Agent entity, if Kafka is configured."""
    if not admin_client:
        return  # Kafka not configured or unavailable
    topic_list = [NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
    # Attempt to create topic (if it already exists, this might raise an exception)
    try:
        fs = admin_client.create_topics(topic_list)
        # Wait for each topic future to complete to ensure topic creation
        for topic, future in fs.items():
            future.result()  # If topic exists, confluent_kafka will raise an exception here
            print(f"Kafka topic '{topic}' created successfully.")
    except Exception as e:
        # If topic creation fails (e.g., topic already exists), log or print an error
        print(f"Kafka topic creation failed or already exists: {e}")

def generate_random_string(length=6):
    """Generate random string for Kafka Topic name"""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def send_kafka_message(topic: str, message: dict):
    """Send a JSON message to a Kafka topic, if Kafka is configured."""
    if not producer:
        return
    try:
        producer.produce(topic, json.dumps(message))
        producer.flush()
        print(f"Message sent to Kafka topic '{topic}': {message}")
    except Exception as e:
        print(f"Failed to send message to Kafka topic '{topic}': {e}")

# Helper: generate_did_key uses cryptographic_tool to produce a DID and private key
def generate_did_key():
    """Generate a unique DID:key and return (did, private_key_pem)."""
    while True:
        result = subprocess.run(["python", "cryptographic_tool.py", "--export-private"], capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError("Cryptographic tool did not execute properly.")
        output_lines = result.stdout.splitlines()
        did_key = None
        for line in output_lines:
            if line.startswith("Generated DID:key:"):
                did_key = line.split("Generated DID:key:")[1].strip()
                break
        if not did_key:
            raise RuntimeError("Failed to parse DID from cryptographic tool output.")
        # Check if this DID already exists
        db = connect_db()
        cursor = db.cursor()
        cursor.execute("SELECT COUNT(*) FROM did_keys WHERE did = %s", (did_key,))
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        db.close()
        if not exists:
            try:
                with open("private_key.pem", "r") as key_file:
                    private_key_pem = key_file.read()
            except FileNotFoundError:
                raise RuntimeError("Private key file not found after DID generation.")
            try:
                os.remove("private_key.pem")
            except OSError:
                pass
            return did_key, private_key_pem


@router.post("/login")
async def login_user(pem_file: UploadFile = File(...)):
    """
    Authenticate a user by their private key (.pem file).
    The .pem file is used to extract the DID and verify it exists in the registry.
    """
    # Save uploaded .pem content to a temporary file for DID extraction
    pem_content = await pem_file.read()
    if not pem_content:
        raise HTTPException(status_code=400, detail="Empty .pem file received.")
    temp_path = f"/tmp/{pem_file.filename}"
    try:
        with open(temp_path, "wb") as temp_pem:
            temp_pem.write(pem_content)
        user_did = extract_did_from_private_key(temp_path)
    except Exception as e:
        # Raise an error if DID extraction fails (e.g., invalid file format)
        raise HTTPException(status_code=400, detail=f"Error reading .pem file: {e}")
    finally:
        # Remove the temporary file for security
        try:
            os.remove(temp_path)
        except OSError:
            pass

    # Verify DID exists in database and corresponds to a Person or Organization (registered user)
    db = connect_db()
    cursor = db.cursor()
    cursor.execute("SELECT metadata FROM did_keys WHERE did = %s", (user_did,))
    result = cursor.fetchone()
    cursor.close()
    db.close()
    if not result:
        raise HTTPException(status_code=401, detail="DID not found. Please register first.")
    # Load user metadata and ensure the type is allowed to log in
    try:
        user_data = json.loads(result[0])
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Corrupt user data in database.")
    if user_data.get("@type") not in ["Person", "Organization"]:
        raise HTTPException(status_code=403, detail="Only Person or Organization DIDs can be used for login.")
    # Login successful
    return {"status": "success", "did": user_did, "name": user_data.get("name")}

@router.post("/register-user")
async def register_user(data: dict):
    """
    Register a new user (Person or Organization) with the provided HSML JSON data.
    Expects `data` to include required fields for Person/Organization.
    """
    entity_type = data.get("@type")
    if entity_type not in ["Person", "Organization"]:
        raise HTTPException(status_code=400, detail="Can only register a Person or Organization as a new user.")
    # Ensure required fields for Person/Organization are present
    if entity_type == "Person":
        required_fields = ["name", "birthDate", "email"]
    else:  # Organization
        required_fields = ["name", "description"]
    missing = [field for field in required_fields if field not in data or data[field] == ""]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")
    # Add HSML context if not present (ensures it's a valid HSML structure)
    if "@context" not in data:
        data["@context"] = "https://digital-twin-interoperability.github.io/hsml-schema-context/hsml.jsonld"

    # Check if SWID exists in the JSON - added
    swid = data.get("swid")
    if swid:
        # Check if this swid is already registered
        db = connect_db()
        cursor = db.cursor()
        cursor.execute("SELECT COUNT(*) FROM did_keys WHERE did = %s", (swid,))
        existing = cursor.fetchone()[0]
        if existing:
            print(f"Warning: The provided 'swid' ({swid}) already exists in the database. You should not register an already existing object.")
            user_input = input("Do you want to continue and overwrite the existing 'swid' property? (yes/no): ").strip().lower()
            if user_input != "yes":
                print("Process aborted. No changes were made.")
                exit()
        print(f"Warning: SWID '{swid}' in JSON file will be overwritten.")
    
    # No SWID in JSON. Generate a unique DID and private key for the new user
    try:
        did_key, private_key_pem = generate_did_key()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating DID: {e}")
    data["swid"] = did_key  # attach the generated DID to the HSML JSON data
    print(f"Generated unique SWID: {did_key}")
    public_key_part = did_key.replace("did:key:", "")

    # Store the new user in the database
    db = connect_db()
    cursor = db.cursor()
    # The user is registering themselves, so `registered_by` is the user’s own DID
    registered_by = did_key
    # Insert user into did_keys table (REPLACE to avoid conflict on primary key DID)
    try:
        cursor.execute(
            "REPLACE INTO did_keys (did, public_key, metadata, registered_by, kafka_topic) VALUES (%s, %s, %s, %s, %s)",
            (did_key, public_key_part, json.dumps(data), registered_by, None)
        )
        db.commit()
    except mysql.Error as err:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database insert failed: {err}")
    finally:
        cursor.close()
        db.close()

    # Return the DID and private key to the user (private key should be handled securely by the client)
    return {"status": "success", "did": did_key, "private_key": private_key_pem, "hsml": data}

@router.post("/register-entity")
async def register_entity_endpoint(payload: dict):
    """
    Register a new HSML entity (Agent, Credential, etc.) after user login.
    Expected JSON payload should include:
      - "entity": HSML JSON object of the entity to register.
      - "registered_by": DID of the user performing the registration (optional for Person/Org).
      - For Credentials: "domain_pem": private key (PEM content) of the domain to verify access (required if entity is Credential).
    """
    if "entity" not in payload:
        raise HTTPException(status_code=400, detail="No entity data provided.")
    data = payload["entity"]
    registered_by = payload.get("registered_by")
    # Basic validation of entity JSON
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Entity data must be a JSON object.")
    if "@context" not in data or "hsml.jsonld" not in data["@context"]:
        raise HTTPException(status_code=400, detail="Not a valid HSML JSON (missing @context).")
    entity_type = data.get("@type")
    if not entity_type:
        raise HTTPException(status_code=400, detail="Missing '@type' in entity data.")
    # Define required fields for known entity types
    required_fields_map = {
        "Entity": ["name", "description"],
        "Person": ["name", "birthDate", "email"],
        "Agent": ["name", "creator", "dateCreated", "dateModified", "description"],
        "Credential": ["name", "description", "issuedBy", "accessAuthorization", "authorizedForDomain"],
        "Organization": ["name", "description", "url", "address", "logo", "foundingDate", "email"]
    }
    required_fields = required_fields_map.get(entity_type)
    if not required_fields:
        raise HTTPException(status_code=400, detail=f"Unsupported or unknown entity type: {entity_type}")
    missing_fields = [field for field in required_fields if field not in data or data[field] == ""]
    if missing_fields:
        raise HTTPException(status_code=400, detail=f"Missing required fields: {missing_fields}")
    
    # If this is a top-level user registration (Person/Organization), ensure only allowed if no registered_by (new user)
    if entity_type in ["Person", "Organization"] and not registered_by:
        # New user registration should use /register endpoint instead
        # If called here without a logged-in user, treat as self-registration
        registered_by = None

    # Check if registration requires an existing user (for non-Person/Org)
    if entity_type not in ["Person", "Organization"]:
        if not registered_by:
            raise HTTPException(status_code=401, detail="Authentication required to register this entity.")
        # Verify the registering user is allowed (should exist in DB and be Person/Org)
        db = connect_db()
        cur = db.cursor()
        cur.execute("SELECT metadata FROM did_keys WHERE did = %s", (registered_by,))
        reg_user = cur.fetchone()
        cur.close()
        db.close()
        if not reg_user:
            raise HTTPException(status_code=401, detail="Registered_by DID not found. Login required.")
        reg_user_data = json.loads(reg_user[0])
        if reg_user_data.get("@type") not in ["Person", "Organization"]:
            raise HTTPException(status_code=403, detail="Only a Person or Organization can register new entities.")
    
    # No SWID in JSON. Generate a unique DID and private key for the new user
    # If the HSML JSON already contains an SWID (DID), warn/overwrite it by generating a new one
    if "swid" in data:
        # Overwrite incoming SWID to ensure uniqueness (the client should normally not provide SWID)
        pass  # We simply ignore the provided swid and generate a new DID below

    # Generate a new DID and private key for this entity
    try:
        did_key, private_key_pem = generate_did_key()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate DID for entity: {e}")
    data["swid"] = did_key
    public_key_part = did_key.replace("did:key:", "")

    # Determine who is registering this entity
    if not registered_by:
        registered_by = did_key  # For initial user self-registration or if not provided

    # Special case: If the entity is an Agent, create a Kafka topic and send a welcome message
    topic_name = None
    if entity_type == "Agent":
        flagUniqueName = False
        while flagUniqueName == False:
            random_suffix = generate_random_string() # new
            topic_name = f"{data["name"].replace(" ", "_").lower()}_{random_suffix}" # new
            #topic_name = data["name"].replace(" ", "_").lower()
            # Check the name generated does not already exist
            db = connect_db()
            cursor = db.cursor()
            cursor.execute("SELECT COUNT(*) FROM did_keys WHERE kafka_topic = %s", (topic_name,))
            existing_topic = cursor.fetchone()
            if existing_topic[0]==0:
                flagUniqueName = True
                create_kafka_topic(topic_name)
                send_kafka_message(topic_name, {"message": f"New Agent registered: {data['name']}"})

    # Special case: If the entity is a Credential, handle special verification for authorized domain access
    if entity_type == "Credential":
        issued_by_did = data.get("issuedBy", {}).get("swid")
        auth_domain = data.get("authorizedForDomain", {})
        authorized_for_domain_did = auth_domain.get("swid")
        credential_domain_name = auth_domain.get("name")
        access_auth = data.get("accessAuthorization", {})
        access_auth_did = access_auth.get("swid")
        # Ensure all required swids are present
        if not (issued_by_did and authorized_for_domain_did and access_auth_did):
            raise HTTPException(status_code=400, detail="Credential is missing one of the required 'swid' fields in issuedBy, authorizedForDomain, or accessAuthorization.")
        if issued_by_did != registered_by:
            raise HTTPException(status_code=403, detail="The 'issuedBy.swid' must match the DID of the user registering this Credential.")
        # A private key for the authorized domain must be provided to prove access (pem content)
        domain_pem = payload.get("domain_pem")
        if not domain_pem:
            raise HTTPException(status_code=400, detail=f"Provide the private_key.pem content for '{credential_domain_name}' to verify domain access.")
        # Extract DID from provided domain private key to verify it matches the authorized_for_domain DID
        domain_pem_path = "/tmp/domain_key.pem"
        try:
            with open(domain_pem_path, "w") as f:
                f.write(domain_pem)
            credential_domain_did = extract_did_from_private_key(domain_pem_path)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid domain private key for '{credential_domain_name}': {e}")
        finally:
            try:
                os.remove(domain_pem_path)
            except OSError:
                pass
        if credential_domain_did != authorized_for_domain_did:
            raise HTTPException(status_code=403, detail="Domain private key does not match the provided authorizedForDomain DID.")
        # Update the domain's metadata in DB to include the new access authorization
        db = connect_db()
        cur = db.cursor()
        cur.execute("SELECT metadata, allowed_did FROM did_keys WHERE did = %s", (authorized_for_domain_did,))
        domain_rec = cur.fetchone()
        if not domain_rec:
            cur.close()
            db.close()
            raise HTTPException(status_code=400, detail=f"Domain DID {authorized_for_domain_did} not found. Register '{credential_domain_name}' first.")
        domain_metadata = json.loads(domain_rec[0])
        # Append accessAuthorization to domain's canAccess list
        if "canAccess" not in domain_metadata:
            domain_metadata["canAccess"] = [access_auth]
        else:
            # Ensure 'canAccess' is a list and add new access if not already present
            existing_access = domain_metadata.get("canAccess")
            if not isinstance(existing_access, list):
                existing_access = [existing_access]
            existing_swids = {entry.get("swid") for entry in existing_access if isinstance(entry, dict)}
            if access_auth_did not in existing_swids:
                existing_access.append(access_auth)
            domain_metadata["canAccess"] = existing_access
        # Update allowed_did list for the domain (users who can access this domain)
        allowed_did_list = []
        if domain_rec[1]:
            allowed_did_list = domain_rec[1].split(",")
        if access_auth_did and access_auth_did not in allowed_did_list:
            allowed_did_list.append(access_auth_did)
        allowed_did_str = ",".join(allowed_did_list)
        try:
            cur.execute("UPDATE did_keys SET metadata = %s, allowed_did = %s WHERE did = %s",
                        (json.dumps(domain_metadata), allowed_did_str, authorized_for_domain_did))
            db.commit()
        except mysql.Error as err:
            db.rollback()
            cur.close()
            db.close()
            raise HTTPException(status_code=500, detail=f"Failed to update domain access: {err}")
        cur.close()
        db.close()
        # No need to return anything special for credential; it will be handled in final return data

    # Insert the new Entity into the database
    db = connect_db()
    cur = db.cursor()
    try:
        cur.execute(
            "REPLACE INTO did_keys (did, public_key, metadata, registered_by, kafka_topic) VALUES (%s, %s, %s, %s, %s)",
            (did_key, public_key_part, json.dumps(data), registered_by, topic_name)
        )
        db.commit()
    except mysql.Error as err:
        db.rollback()
        cur.close()
        db.close()
        raise HTTPException(status_code=500, detail=f"Database insert failed: {err}")
    finally:
        cur.close()
        db.close()

    # Return success and the new entity's details (excluding private key for non-user entities)
    response_data = {
        "status": "success",
        "did": did_key,
        "entity_type": entity_type,
        "metadata": data
    }
    # Provide private key for the entity if needed (for Agents or others, user might want to download it)
    if entity_type not in ["Credential"]:
        # Note: For an Agent or other non-user entity, returning the private key here for user to store safely.
        response_data["private_key"] = private_key_pem
    return JSONResponse(status_code=200, content=response_data)

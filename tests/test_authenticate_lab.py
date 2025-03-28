import requests
import json

# API endpoints
API_URL = "http://192.168.1.55:8000/producer"

# Request data
private_key_path = "C:/Users/abarrio/OneDrive - JPL/Desktop/Digital Twin Interoperability/Codes/HSML codes/testing_lab_verification/registeredExamplesLabDB/private_key_example_agent_A.pem"
topic = "example_agent_a_kel2zq"
json_file_path = "C:/Users/abarrio/OneDrive - JPL/Desktop/Digital Twin Interoperability/Codes/HSML codes/testing_lab_verification/registeredExamplesLabDB/Example_Agent_A.json"

# Read private key
try:
    private_key_file = open(private_key_path, "rb")
except Exception as e:
    print(f"Error reading private key: {e}")
    exit(1)

# Read JSON message
try:
    with open(json_file_path, 'r') as f:
        json_message = json.load(f)  # Load JSON content from file
except Exception as e:
    print(f"Error reading JSON file: {e}")
    exit(1)

# Step 1: Authenticate - Make a POST request
#auth_response = requests.post(f"{API_URL}/authenticate", params={"private_key_path": private_key_path, "topic": topic})
auth_response = requests.post(f"{API_URL}/authenticate", params={"topic": topic}, files={"private_key": private_key_file})
# Print the response
print(auth_response.json()) # Should print success or failure

# Step 2: Start Producer - Send JSON Messages Continuously
start_response = requests.post(f"{API_URL}/start", params={"topic": topic}, json=json_message)
print(start_response.json()) # Should print confirmation that producer started

# (Optional) Step 3: Send JSON Messages on Demand - Manual Control (useful for SOS message)
with open(json_file_path, "r") as f:
    json_message = json.load(f)
   
send_response = requests.post(f"{API_URL}/send-message", params={"topic": topic}, json=json_message)
print(send_response.json())  # Should print "Message sent successfully"

input("Press Enter to stop the producer...")

# Stop Producer
stop_response = requests.post(f"{API_URL}/stop", params={"topic": topic})
print(stop_response.json())


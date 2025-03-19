import requests
import json

# API endpoints
API_URL = "http://127.0.0.1:8000/verification"

# Request data
private_key_path = "C:/Users/abarrio/OneDrive - JPL/Desktop/Digital Twin Interoperability/Codes/HSML Examples/registeredExamples/private_key_example_agent_4.pem"
topic = "example_agent_4"
json_file_path = "C:/Users/abarrio/OneDrive - JPL/Desktop/Digital Twin Interoperability/Codes/HSML_updated_platform_scripts/Unreal/kafkaUnrealProducer_hsml.json"

try:
    with open(json_file_path, 'r') as f:
        json_message = json.load(f)  # Load JSON content from file
except Exception as e:
    print(f"Error reading JSON file: {e}")

# Step 1: Authenticate - Make a POST request
auth_response = requests.post(f"{API_URL}/authenticate", params={"private_key_path": private_key_path, "topic": topic})
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
stop_response = requests.post(f"{API_URL}/stop", json={"topic": topic})
print(stop_response.json())


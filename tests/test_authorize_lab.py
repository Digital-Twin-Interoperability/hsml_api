import requests

# API endpoints
API_URL = "http://192.168.1.55:8000/consumer"

# Request data
private_key_path = "C:/Users/abarrio/OneDrive - JPL/Desktop/Digital Twin Interoperability/Codes/HSML codes/testing_lab_verification/registeredExamplesLabDB/private_key_example_agent_B.pem"
topic = "example_agent_a_kel2zq"

# Read private key
try:
    private_key_file = open(private_key_path, "rb")
except Exception as e:
    print(f"Error reading private key: {e}")
    exit(1)

# Step 1: Authorize - Make a POST request
#auth_response = requests.post(f"{API_URL}/authorize", params={"private_key_path": private_key_path, "topic": topic})
auth_response = requests.post(f"{API_URL}/authorize", params={"topic": topic}, files={"private_key": private_key_file})
# Print the response
print(auth_response.json()) # Should print success or failure

# Step 2: Start Consumer - Send JSON Messages Continuously
start_response = requests.post(f"{API_URL}/start", params={"topic": topic})
print(start_response.json()) # Should print confirmation that producer started

input("Press Enter to stop the consumer...")

# Stop Producer
stop_response = requests.post(f"{API_URL}/stop", params={"topic": topic})
print(stop_response.json())


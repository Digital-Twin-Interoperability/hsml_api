import requests

# API endpoints
API_URL = "http://127.0.0.1:8000/consumer"

# Request data
private_key_path = "C:/Users/abarrio/OneDrive - JPL/Desktop/Digital Twin Interoperability/Codes/HSML Examples/registeredExamples/private_key_example_agent_2.pem"
topic = "example_agent_4"

# Step 1: Authorize - Make a POST request
auth_response = requests.post(f"{API_URL}/authorize", params={"private_key_path": private_key_path, "topic": topic})
# Print the response
print(auth_response.json()) # Should print success or failure

# Step 2: Start Consumer - Send JSON Messages Continuously
start_response = requests.post(f"{API_URL}/start", params={"topic": topic})
print(start_response.json()) # Should print confirmation that producer started

input("Press Enter to stop the consumer...")

# Stop Producer
stop_response = requests.post(f"{API_URL}/stop", params={"topic": topic})
print(stop_response.json())


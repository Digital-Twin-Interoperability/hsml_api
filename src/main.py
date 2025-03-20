from fastapi import FastAPI, HTTPException, Depends
import uvicorn
from registration_api import router as registration_router # Import the router from registration_api
from authentication_kafka_producer import router as producer_router # Import the router from authentication_kafka_producer
from authorization_kafka_consumer import router as consumer_router # Import the router from authorization_kafka_producer

# Initialize the FastAPI app
app = FastAPI()

# Include registration router 
app.include_router(registration_router, prefix="/entity")

# Include authentication producer router
app.include_router(producer_router, prefix="/producer")

# Include authorization consumer router
app.include_router(consumer_router, prefix="/consumer")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

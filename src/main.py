from fastapi import FastAPI, HTTPException, Depends
import uvicorn
from registration_api import router as registration_router # Import the router from registration_api
from authentication_kafka_producer import router as producer_router # Import the router from authentication_kafka_producer
#from authorization_kafka_consumer import

# Initialize the FastAPI app
app = FastAPI()

# Include registration router 
app.include_router(registration_router)
#app.include_router(registration_router, prefix="/register", tags=["registration"])

# Include authentication producer routerlo
app.include_router(producer_router, prefix="/verification")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

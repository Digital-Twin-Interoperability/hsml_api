from fastapi import FastAPI, HTTPException, Depends
import uvicorn
from registration_api import router as registration_router
#from registration_api import functions
#from authentication_kafka_producer import functions
#from authorization_kafka_consumer import functions


app = FastAPI()

# Include registration router (with base route like /register)
app.include_router(registration_router, prefix="/register", tags=["registration"])


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

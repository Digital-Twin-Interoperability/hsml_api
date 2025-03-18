from fastapi import FastAPI, HTTPException, Depends
import uvicorn

app = FastAPI()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

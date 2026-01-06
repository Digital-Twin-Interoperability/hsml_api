# Use official Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy dependency files first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir uvicorn

# Copy the rest of the application
COPY . .

# Expose API port
EXPOSE 8000

# Ensure Python sees your src directory
ENV PYTHONPATH=/app/src:/app

# Run FastAPI app with Uvicorn
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

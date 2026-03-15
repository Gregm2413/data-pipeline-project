# Use slim Python image to keep container lightweight
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Install system dependencies needed for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first (layer caching)
# This layer only rebuilds if requirements.txt changes
COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source scripts
COPY src/ ./src/

# Default command - can be overridden in docker-compose.yml
CMD ["python", "src/ingest.py"]
FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy application code
COPY . .

# Install dependencies
# Note: we install the project in editable mode so changes in volume mount are reflected
RUN pip install --no-cache-dir -e .

# Default command
CMD ["python", "-m", "trend_miner", "run", "--config", "config.docker.yaml"]

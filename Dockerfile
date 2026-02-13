FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy project file
COPY pyproject.toml .

# Install dependencies
# Note: we install the project in editable mode so changes in volume mount are reflected
# But since we copy code later, editable install might point to /app/trend_miner which is correct
RUN pip install --no-cache-dir -e .

# Copy application code
COPY . .

# Default command
CMD ["python", "-m", "trend_miner", "run", "--config", "config.docker.yaml"]

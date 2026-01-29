# River Router API
FROM python:3.11-slim

# Install system dependencies for geospatial libs
RUN apt-get update && apt-get install -y \
    libgeos-dev \
    libproj-dev \
    libgdal-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/
COPY scripts/ ./scripts/

# Create data directories
RUN mkdir -p data/nhdplus data/graph data/cache

# Expose port
EXPOSE 8000

# Run the API
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

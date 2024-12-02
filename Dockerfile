FROM apache/airflow:latest

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create requirements.txt
COPY requirements.txt /requirements.txt

USER airflow

# Install Python packages
RUN pip install --no-cache-dir -r /requirements.txt

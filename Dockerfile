FROM python:3.8-slim-buster

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/app/airflow

# Install dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    lsb-release \
    gnupg \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install gsutil
RUN curl -sSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
    && echo "deb http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y google-cloud-sdk

WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN chmod +x scripts/entrypoint.sh

COPY pyproject.toml poetry.lock ./
COPY raw_data_processing ./raw_data_processing

# Install Python dependencies using poetry
RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install --only main

# Ensure raw_data_processing is in the PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/app/airflow/raw_data_processing:/app/airflow"

# Set JAVA_HOME
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Add Java to PATH
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Ensure gsutil is in the PATH
ENV PATH="/usr/lib/google-cloud-sdk/bin:${PATH}"

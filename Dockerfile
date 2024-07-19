FROM python:3.8-slim-buster

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED 1
ENV AIRFLOW_HOME=/app/airflow

# Install dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN chmod +x scripts/entrypoint.sh

COPY pyproject.toml poetry.lock ./
COPY raw_data_processing /app/airflow/raw_data_processing
COPY dags dags

# Install Python dependencies using poetry
RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install --only main

# Ensure raw_data_processing is in the PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/app/airflow/raw_data_processing"

# Set JAVA_HOME
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Add Java to PATH
ENV PATH="${JAVA_HOME}/bin:${PATH}"

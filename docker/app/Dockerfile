# Lightweight image for the Kafka producer and the Streamlit dashboard (no Java, no Spark).
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1
WORKDIR /opt/reviewlens
COPY docker/app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY pyproject.toml ./
COPY contracts ./contracts
COPY src ./src
COPY dashboard ./dashboard
RUN pip install --no-cache-dir --no-deps -e .
# Same owner as the Airflow user (uid 50000), whichever container mounts the lake volume first.
RUN mkdir -p /opt/lake && chown 50000:0 /opt/lake

FROM python:3.11.1-slim-buster

WORKDIR /app

COPY ingest_data.py ingest_data.py

RUN apt-get update && apt-get clean \
    && apt-get -y install libpq-dev gcc \
    && pip install --no-cache pandas sqlalchemy requests psycopg2 fastparquet \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["python", "ingest_data.py"]
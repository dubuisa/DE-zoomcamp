FROM python:3.11.1

WORKDIR /app
COPY ingest_data.py ingest_data.py
RUN pip install pandas sqlalchemy requests psycopg2 fastparquet
ENTRYPOINT ["python", "ingest_data.py"]
FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2
RUN pip install wget

WORKDIR /app
COPY ingest_trip_data.py ingest_trip_data.py
ENTRYPOINT [ "python" ,"-u", "ingest_trip_data.py" ]

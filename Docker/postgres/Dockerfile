FROM python:3.12.4
RUN pip install pandas sqlalchemy psycopg2 pyarrow argparse pyodbc
WORKDIR /app
COPY ingest-data.py ingest-data.py
ENTRYPOINT [ "python", "ingest-data.py" ]
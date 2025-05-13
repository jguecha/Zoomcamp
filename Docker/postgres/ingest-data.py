#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
from time import time
from sqlalchemy import create_engine

## This script downloads a CSV file from a given URL and ingests it into a PostgreSQL database. Create a function called main that takes a parameter called params.
def main(params):
    # Extract the parameters from the params object and assign them to variables.
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    # download the CSV file
    df = pd.read_parquet(url, engine="pyarrow")
    df.to_csv(csv_name, index=False)

    # Create a connection to the PostgreSQL database using SQLAlchemy. The connection string is constructed using the parameters provided.
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read the CSV file in chunks using pandas. The chunksize parameter specifies the number of rows to read at a time.
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    # Read the first chunk of the CSV file and convert the pickup and dropoff datetime columns to datetime objects.
    df = next(df_iter)

    # Convert the pickup and dropoff datetime columns to datetime objects using pandas.
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Create the table in the PostgreSQL database using the first chunk of data. The if_exists parameter specifies what to do if the table already exists. In this case, it will replace the existing table with the new one.
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Insert the first chunk of data into the PostgreSQL database. The if_exists parameter specifies what to do if the table already exists. In this case, it will append the new data to the existing table.
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # Loop through the remaining chunks of data and insert them into the PostgreSQL database. The while loop continues until there are no more chunks to read. The StopIteration exception is raised when there are no more chunks to read.
    # The time taken to insert each chunk is printed to the console.
    while True:
        try:
            t_start = time()

            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print('completed')
            break

# The main function is called when the script is run directly. The command line arguments are parsed using argparse. The parameters are passed to the main function.
# The script is designed to be run from the command line, and the parameters are passed as command line arguments.
if __name__ == '__main__':
    # Parse the command line arguments and calls the main program
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)
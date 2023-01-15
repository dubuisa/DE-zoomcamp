import pandas as pd
from sqlalchemy import engine

from io import StringIO

import requests
import logging
import csv
import argparse

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger("ingest_data")

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = f'{table.schema}.{table.name}'
        else:
            table_name = table.name

        sql = f'COPY {table_name} ({columns}) FROM STDIN WITH CSV'
        cur.copy_expert(sql=sql, file=s_buf)



def main(params):
    year = params.year
    month = params.month
    table_name = params.table_name

    host =params.host
    db = params.db
    user = params.user
    password = params.password
    port = params.port

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    url = f"{base_url}/yellow_tripdata_{year}-{month:02}.parquet"

    logger.info(f"Retrieving data: {url}")
    response = requests.get(url)

    if response.status_code != 200:
        logger.info(f"GET {url} returned {response.status_code}")
        exit(1)

    with open('./data.parquet', 'wb') as f:
        f.write(response.content)
        logger.info(f"file saved under ./data.parquet")


    e = engine.create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')
    df = pd.read_parquet('./data.parquet')
    df.head(0).to_sql(name=table_name, con=e, if_exists='replace')
    df.to_sql(name=table_name, con=e, if_exists="append", index=False, method=psql_insert_copy)
    logger.info(f"Storing {len(df)} rows under {db=}, {table_name=}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = "Download and ingest NY taxi fare dataset'")

    parser.add_argument("--year", help="Year of file", default=2022, required=False)
    parser.add_argument("--month", help="Month of file", default=10, required=False)
    parser.add_argument("--table_name", help="Table_name for postgress")
    parser.add_argument("--host", help="host for postgress")
    parser.add_argument("--db", help="database name for postgress")
    parser.add_argument("--user", help="User for postgress")
    parser.add_argument("--password", help="Password for postgress")
    parser.add_argument("--port", help="port for postgress", default=5432, required=False)

    args = parser.parse_args()

    main(args)
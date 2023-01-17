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
    table_name = params.table_name

    host =params.host
    db = params.db
    user = params.user
    password = params.password
    port = params.port

    url = params.url
    
    if url.endswith('.csv.gz'):
        fname = 'output.csv.gz'
    elif url.endswith('.csv'):
        fname = 'output.csv'
    elif url.endswith('.parquet'):
        fname = 'output.parquet'
    else:
        logger.error(f'Cannot process file extension for {url=}, only supports csv, csv.gz and parquet')
        exit(1)

    logger.info(f"Retrieving data: {url}")
    response = requests.get(url)
    
    if response.status_code != 200:
        logger.info(f"GET {url} returned {response.status_code}")
        exit(1)

    with open(fname, 'wb') as f:
        f.write(response.content)
        logger.info(f"file saved under {fname}")


    e = engine.create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')
    
    if fname == 'output.parquet' :
        df = pd.read_parquet(fname)
    else:
        df = pd.read_csv(fname, parse_dates=[1,2])

    df.head(0).to_sql(name=table_name, con=e, if_exists='replace')
    df.to_sql(name=table_name, con=e, if_exists="append", index=False, method=psql_insert_copy)
    logger.info(f"Storing {len(df)} rows under {db=}, {table_name=}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = "Download and ingest NY taxi fare dataset'")

    parser.add_argument("--url", help="url to download the file")
    parser.add_argument("--table_name", help="Table_name for postgress")
    parser.add_argument("--host", help="host for postgress")
    parser.add_argument("--db", help="database name for postgress")
    parser.add_argument("--user", help="User for postgress")
    parser.add_argument("--password", help="Password for postgress")
    parser.add_argument("--port", help="port for postgress", default=5432, required=False)

    args = parser.parse_args()

    main(args)
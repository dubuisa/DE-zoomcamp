from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


import pandas as pd

from io import StringIO
from datetime import timedelta
import csv
import os


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

@task(tags=['extract'], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1), log_prints=True)
def extract_data(url: str) -> pd.DataFrame:
    if url.endswith('.csv.gz'):
            fname = 'output.csv.gz'
    elif url.endswith('.csv'):
        fname = 'output.csv'
    elif url.endswith('.parquet'):
        fname = 'output.parquet'
    else:
        print(f'Cannot process file extension for {url=}, only supports csv, csv.gz and parquet')
        exit(1)
    os.system(f"wget {url} -O {fname}")
    
    if fname == 'output.parquet' :
        df = pd.read_parquet(fname)
    else:
        df = pd.read_csv(fname, parse_dates=[1,2])

    return df


@task(log_prints=True)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print(f"pre: cleaning data {len(df)}")

    df = df[df['passenger_count'] != 0]

    print(f"post: cleaning data {len(df)}")
    return df

@task(log_prints=True, retries=3)
def load_data(df, table_name):
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists="append" , method=psql_insert_copy)
    print(f"Storing {len(df)} under {table_name=}")

@flow(name="Ingest Data")
def main_flow(url, table_name='trips'): 
    df = extract_data(url)
    df = transform_data(df)
    load_data(df, table_name)

if __name__ == '__main__':
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet"
    main_flow(url, table_name='trips')
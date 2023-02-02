import pandas as pd

from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket

from prefect_gcp import GcpCredentials
from typing import List

from pathlib import Path


@task(retries=3)
def extract_from_gcs(color, year, month):
    path = f'./data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_bucket = GcsBucket.load("data-lake-bucket")
    gcs_bucket.get_directory(from_path=path, local_path='./')
    return Path(path)

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df: pd.DataFrame):

    creds = GcpCredentials.load('de-service-account')
    df.to_gbq(
        destination_table='trips_data_all.rides',
        credentials=creds.get_credentials_from_service_account(),
        project_id='data-engineering-course-374809',
        chunksize=500_000, 
        if_exists='append'
    )


@flow(log_prints=True)
def etl_gcs_to_bq(year, month, color):
    """Main ETL flow to load data from GCS into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    print(f"total len {len(df)}")


@flow(log_prints=True)
def gcs_to_bq_parent_flow(year: int = 2021, months: List[int]=[1,2,3], color : str ='yellow'):
    for month in months:
        etl_gcs_to_bq(year, month, color)


if __name__ == '__main__':
    gcs_to_bq_parent_flow(2019, [2,3], 'yellow')
from pathlib import Path
from typing import List
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash

import os
from datetime import timedelta

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1), log_prints=True)
def fetch_data(url: str) -> pd.DataFrame:
    """Read taxi data from web and returns pd.DataFrame"""
    return pd.read_parquet(url)


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    print(f"pre: cleaning data {len(df)}")
    #df = df[df['passenger_count'] != 0]
    print(f"post: cleaning data {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file : str) -> Path:
    """Write Dataframe locally as a parquet file"""
    os.makedirs(Path(f"./data/{color}"),exist_ok=True)
    path = Path(f"./data/{color}/{dataset_file}")
    df.to_parquet(path, compression='gzip')
    print(f'file written under {path=}')
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to gcs"""
    gcs_bucket = GcsBucket.load("data-lake-bucket")
    gcs_bucket.upload_from_path(path, path)


@flow(log_prints=True)
def etl_web_to_gcs(year, month, color) -> None:
    """Main ETL function"""
    dataset_file = f'{color}_tripdata_{year}-{month:02}.parquet'
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
    df = fetch_data(url)
    df = clean(df)
    path = write_local(df, color, dataset_file)
    write_gcs(path)


@flow()
def etl_web_to_gcs_parent_flow(year: int = 2021, months: List[int]=[1,2,3], color : str ='yellow' ):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == '__main__':
    etl_web_to_gcs_parent_flow(2021, [1,2,3], "yellow")
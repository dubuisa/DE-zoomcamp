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
    return pd.read_csv(url)


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file : str) -> Path:
    """Write Dataframe locally as a parquet file"""
    os.makedirs(Path(f"./data/{color}"),exist_ok=True)
    path = Path(f"./data/{color}/{dataset_file}.parquet")
    df.to_parquet(path)
    print(f'file written under {path=}')
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to gcs"""
    gcs_bucket = GcsBucket.load("data-lake-bucket")
    gcs_bucket.upload_from_path(path, path)


@flow(log_prints=True)
def etl_fhv_file(year, month) -> None:
    dataset_file = f'fhv_tripdata_{year}-{month:02}'
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    df = fetch_data(url)
    path = write_local(df, "fhv", dataset_file)
    write_gcs(path)


@flow()
def etl_fhv(year: int = 2019):
    for month in range(1,13):
        etl_fhv_file(year, month)


if __name__ == '__main__':
    etl_fhv(2019)
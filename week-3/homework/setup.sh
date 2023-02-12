#!/bin/bash

prefect deployment build ./etl_fhv.py:etl_fhv --name etl_fhv   --apply
prefect deployment build ./etl_fhv_parquet.py:etl_fhv --name etl_fhv_parquet   --apply

prefect deployment run etl-fhv/etl_fhv
prefect deployment run etl-fhv/etl_fhv_parquet
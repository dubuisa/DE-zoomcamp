-- Create an external table
CREATE OR REPLACE EXTERNAL TABLE `trips_data.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://data_lake_taxi/data/fhv/fhv_tripdata_2019*']
);
-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `trips_data.fhv_tripdata` AS
SELECT * FROM `trips_data.external_fhv_tripdata`;

--Q1: What is the count for fhv vehicle records for year 2019?
SELECT count(*) FROM `trips_data.external_fhv_tripdata`;

--Answer is : 43244696

-- Q2: Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
--What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT count(distinct Affiliated_base_number) from `trips_data.external_fhv_tripdata`;
--This query will process 0 B when run.
SELECT count(distinct Affiliated_base_number) from `trips_data.fhv_tripdata`;
--This query will process 317.94 MB when run.

--Answer is `0 MB for the External Table and 317.94MB for the BQ Table`


-- Q3 How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
SELECT count(*) FROM `trips_data.fhv_tripdata` where PUlocationID is null and DOlocationID is null;
--Answer is `717,748`

--Q4: What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
--Answer is: `Partition by pickup_datetime Cluster on affiliated_base_number`


--Q5: 

--Create table
CREATE OR REPLACE TABLE `trips_data.fhv_tripdata_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `trips_data.fhv_tripdata`;

Select distinct affiliated_base_number from `trips_data.fhv_tripdata` 
where DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
-- This query will process 647.87 MB when run.

Select distinct affiliated_base_number from `trips_data.fhv_tripdata_partitoned_clustered`
where DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
-- This query will process 23.05 MB when run.
--Answer is `647.87 MB for non-partitioned table and 23.06 MB for the partitioned table`


-- Q6: Where is the data stored in the External Table you created?
-- Answer is: `GCP Bucket`

-- Q7: It is best practice in Big Query to always cluster your data:
-- Answer is `False`

--Q8:

CREATE OR REPLACE EXTERNAL TABLE `trips_data.external_fhv_tripdata_parquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://data_lake_taxi/data/fhv/fhv_tripdata_2019*.parquet']
);

SELECT count(*) from trips_data.external_fhv_tripdata_parquet;

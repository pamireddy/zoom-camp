--get count of records in Green taxi trips 2022
SELECT COUNT(*) FROM `ny_taxi.green_tripdata_2022`;

--Create an external table 
CREATE OR REPLACE EXTERNAL TABLE ny_taxi.green_tripdata_2022_external
OPTIONS(
  format = 'parquet',
  uris = ['gs://zoomcamp_week3/green_tripdata_2022-*.parquet']
);

--2. to count the distinct number of PULocationIDs
--Internal Table
SELECT COUNT(DISTINCT PULocationID)
FROM ny_taxi.green_tripdata_2022;

--external Table
SELECT COUNT(DISTINCT PULocationID)
FROM ny_taxi.green_tripdata_2022_external;

--3. How many records have a fare_amount of 0?
SELECT COUNT(*)
FROM ny_taxi.green_tripdata_2022_external
WHERE fare_amount = 0;

--4. What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

--Partitioned by lpep_pickup_datetime and clustered by PUlocationID
CREATE OR REPLACE TABLE ny_taxi.green_tripdata_2022_partitioned_and_clustered
PARTITION BY DATE(lpep_pickup_datetime) 
CLUSTER BY PUlocationID
AS
SELECT * FROM `ny_taxi.green_tripdata_2022_external`;    

--5. query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
SELECT DISTINCT PUlocationID
FROM `ny_taxi.green_tripdata_2022`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

--from Partitoned table
SELECT DISTINCT PUlocationID
FROM `ny_taxi.green_tripdata_2022_partitioned_and_clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';


-- Create an external table referring to the Yellow Taxi trip data for 2024
CREATE OR REPLACE EXTERNAL TABLE `di-malrokh-sandbox-malrokh.home_work3.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://trip_data_week_home3/yellow_tripdata_2024-*.parquet']
);

-- question 1 
SELECT count(*) FROM `di-malrokh-sandbox-malrokh.home_work3.external_yellow_tripdata` ;


-- ===============================================================================================


CREATE OR REPLACE TABLE `di-malrokh-sandbox-malrokh.home_work3.yellow_tripdata_non_partitioned` AS
SELECT * FROM `di-malrokh-sandbox-malrokh.home_work3.external_yellow_tripdata`;




-- question 2 



-- Query to count the distinct number of PULocationIDs in the non-partitioned table
select count(distinct(PULocationID)) 
from `di-malrokh-sandbox-malrokh.home_work3.yellow_tripdata_non_partitioned`;




-- =====================================================================================================



-- question3 



select PULocationID  from `di-malrokh-sandbox-malrokh.home_work3.yellow_tripdata_non_partitioned`;

select PULocationID ,DOLocationID  from `di-malrokh-sandbox-malrokh.home_work3.yellow_tripdata_non_partitioned`;



-- =======================================================================================================================


--question4 

-- Query to count the records with fare_amount equal to 0

SELECT COUNT(*) AS zero_fare_count
FROM `di-malrokh-sandbox-malrokh.home_work3.yellow_tripdata_non_partitioned`
WHERE fare_amount = 0;

-- ===========================================================================================================================


-- Question 5: Best strategy to optimize table for filtering by tpep_dropoff_datetime and ordering by VendorID



-- Create a partitioned and clustered table based on tpep_dropoff_datetime and VendorID

CREATE OR REPLACE TABLE `di-malrokh-sandbox-malrokh.home_work3.yellow_tripdata_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `di-malrokh-sandbox-malrokh.home_work3.external_yellow_tripdata`;



-- ==================================================================================================


-- Question 6:

-- Retrieve distinct VendorIDs between March 1 and March 15, 2024

-- Query to retrieve distinct VendorIDs between the specified date range

SELECT DISTINCT VendorID
FROM `di-malrokh-sandbox-malrokh.home_work3.yellow_tripdata_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- Query using the partitioned table instead of the non-partitioned table
SELECT DISTINCT VendorID
FROM `di-malrokh-sandbox-malrokh.home_work3.yellow_tripdata_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

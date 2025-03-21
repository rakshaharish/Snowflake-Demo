
-- For the Demo, we are using an internal Stage called Github_Stage to host the original CSV files. 
-- Note: External stages like AWS S3, Azure ADLS and GCP data stores can be used, for these external data stores, we create a snowpipe to bridge the files from cloud to raw data layer in snowflake. 
-- For internal stage (like in our case), we can just copy file data from internal stage to the raw data table

-- (1) View all the stages in the database
SHOW STAGES IN DATABASE nyc_taxi_demo_db;

-- (2) Creating an internal stage for github data 
CREATE OR REPLACE STAGE public.github_stage;

-- (3) Create the base empty table in the raw_layer with schema
CREATE OR REPLACE TABLE nyc_taxi_demo_db.raw_layer.taxi_data (
    vendor_id STRING,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT
);

-- (4) Create a snowpipe to ingest CSV data file into a snowflake table
COPY INTO nyc_taxi_demo_db.raw_layer.taxi_data
FROM @github_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"');

-- (5) View the data in the raw data layer table
SELECT * FROM nyc_taxi_demo_db.raw_layer.taxi_data;

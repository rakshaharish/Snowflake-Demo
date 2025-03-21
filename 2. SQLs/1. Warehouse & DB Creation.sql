-- Run the SQL commands as "SYSADMIN"
-- (1) Create a Data Warehouse for the demo
CREATE OR REPLACE WAREHOUSE etl_wh WITH 
WAREHOUSE_SIZE = 'XSMALL' 
AUTO_SUSPEND = 60 
AUTO_RESUME = TRUE

-- (2) Create a Demo database
CREATE OR REPLACE DATABASE nyc_taxi_demo_db;

-- (3) Create 3 schemas for the ETL pipeline
CREATE OR REPLACE SCHEMA nyc_taxi_demo_db.raw_layer;
CREATE OR REPLACE SCHEMA nyc_taxi_demo_db.staging_layer;
CREATE OR REPLACE SCHEMA nyc_taxi_demo_db.serving_layer;




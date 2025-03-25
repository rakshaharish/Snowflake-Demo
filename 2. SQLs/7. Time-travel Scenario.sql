-- This SQL will demonstrate how records change with time in a SQL table
-- Note: This is not yet tested on SnowFlake Platform

-- Delete a record
DELETE FROM nyc_taxi_demo_db.serving_layer.final_taxi_data WHERE passenger_count = '2';

-- Recover using Time Travel
SELECT * FROM nyc_taxi_demo_db.serving_layer.final_taxi_data AT(OFFSET => -60);


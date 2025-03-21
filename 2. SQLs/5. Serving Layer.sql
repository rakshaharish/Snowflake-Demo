-- (1) To load processed data to serving layer
CREATE OR REPLACE TABLE nyc_taxi_demo_db.serving_layer.final_taxi_data AS
SELECT 
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    DATEDIFF('minute', pickup_datetime, dropoff_datetime) AS trip_duration,
    trip_distance,
    CASE 
        WHEN trip_distance < 2 THEN 'Short'
        WHEN trip_distance BETWEEN 2 AND 5 THEN 'Medium'
        ELSE 'Long'
    END AS trip_distance_category,
    fare_amount,
    passenger_count,
    CURRENT_TIMESTAMP() AS load_time
FROM nyc_taxi_demo_db.staging_layer.transformed_taxi_data;


-- (2) View the serving layer table
SELECT * FROM nyc_taxi_demo_db.serving_layer.final_taxi_data;

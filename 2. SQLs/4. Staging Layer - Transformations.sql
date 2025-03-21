-- (1) Here we do the Transformation part of ETL
CREATE OR REPLACE TABLE nyc_taxi_demo_db.staging_layer.transformed_taxi_data AS
SELECT 
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,

    -- Calculate trip duration in minutes, handling NULL values
    COALESCE(DATEDIFF('minute', pickup_datetime, dropoff_datetime), 0) AS trip_duration_minutes,

    -- Categorize fares based on simple thresholds
    CASE 
        WHEN fare_amount > 50 THEN 'High Fare'
        WHEN fare_amount BETWEEN 20 AND 50 THEN 'Medium Fare'
        ELSE 'Low Fare' 
    END AS fare_category,

    -- Standardize vendor names
    CASE 
        WHEN vendor_id = 'VTS' THEN 'Creative Mobile Technologies'
        WHEN vendor_id = 'CMT' THEN 'VeriFone Inc.'
        ELSE 'Unknown'
    END AS vendor_normalized,

    -- Extract date parts for easier analysis
    YEAR(pickup_datetime) AS pickup_year,
    MONTH(pickup_datetime) AS pickup_month,
    DAYOFWEEK(pickup_datetime) AS pickup_weekday,

    -- Load timestamp for tracking
    CURRENT_TIMESTAMP() AS load_time

FROM nyc_taxi_demo_db.raw_layer.taxi_data
WHERE passenger_count > 0 
AND trip_distance > 0 
AND fare_amount > 0;

-- (2) View the data in transformed table
SELECT * FROM nyc_taxi_demo_db.staging_layer.transformed_taxi_data;

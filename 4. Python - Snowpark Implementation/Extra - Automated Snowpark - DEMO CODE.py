# Step 1: Install Snowflake Connector & Snowpark
## pip install snowflake-connector-python snowflake-snowpark-python pandas

# Step 2: Python Script to Automate Data Processing
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import pandas as pd
import time

# Snowflake Connection Configuration
CONNECTION_PARAMS = {
    "account": "<your_snowflake_account>",
    "user": "<your_username>",
    "password": "<your_password>",
    "role": "<your_role>",
    "warehouse": "etl_wh",
    "database": "nyc_taxi_db",
    "schema": "RAW"
}

# Establish Snowpark Session
session = Session.builder.configs(CONNECTION_PARAMS).create()

# Load Raw Data from GitHub
file_url = "https://raw.githubusercontent.com/yourrepo/nyc_taxi_data/main/data.csv"
df = pd.read_csv(file_url)
session.write_pandas(df, "RAW_DATA", auto_create_table=True, overwrite=True)
print("Raw data loaded successfully!")

# Create Staging Layer (Transform Data)
session.sql("""
    CREATE OR REPLACE TABLE nyc_taxi_db.staging.taxi_data AS
    SELECT *, CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM nyc_taxi_db.raw.taxi_data;
""").collect()
print("Data transformed and moved to staging!")

# Create Serving Layer (Final Processed Data)
session.sql("""
    CREATE OR REPLACE TABLE nyc_taxi_db.serving.final_data AS
    SELECT *, UPPER(passenger_count) AS PROCESSED_PASSENGERS
    FROM nyc_taxi_db.staging.taxi_data;
""").collect()
print("Data moved to serving layer!")

# Simulate a Delete for Time Travel Demonstration
session.sql("DELETE FROM nyc_taxi_db.serving.final_data WHERE passenger_count = '2'").collect()
print("Data deleted for time travel demo!")

# Retrieve Deleted Data Using Time Travel (1 minute back)
time.sleep(60)
time_travel_query = "SELECT * FROM nyc_taxi_db.serving.final_data AT(OFFSET => -60)"
time_travel_df = session.sql(time_travel_query).to_pandas()
print("Recovered Data from Time Travel:")
print(time_travel_df)

# Close Session
session.close()

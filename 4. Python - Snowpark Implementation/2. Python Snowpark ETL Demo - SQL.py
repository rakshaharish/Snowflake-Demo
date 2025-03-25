# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col 
from snowflake.snowpark.functions import col, lit  # Added lit import


def main(session: snowpark.Session):
    # Define sample data
    data = [
        (1, 'Alice', 25, 'New York', 'Engineer', 75000),
        (2, 'Bob', 30, 'San Francisco', 'Analyst', 80000),
        (3, 'Charlie', 28, 'Chicago', 'Manager', 90000),
        (4, 'David', 35, 'Boston', 'Director', 120000),
        (5, 'Eve', 27, 'Seattle', 'Consultant', 85000)
    ]
    
    # Define column names
    columns = ['id', 'name', 'age', 'city', 'job_title', 'salary']
    
    # Create a Snowpark DataFrame
    df = session.create_dataframe(data, schema=columns)
    
    # Create the raw data table in "raw_layer" schema of the database
    create_table_query = """
    CREATE OR REPLACE TABLE nyc_taxi_demo_db.raw_layer.raw_employee_data (
        id INT, 
        name STRING, 
        age INT, 
        city STRING, 
        job_title STRING, 
        salary INT
    );
    """
    session.sql(create_table_query).collect()
    
    # Write DataFrame to the raw_layer table directly for Step-1 of ETL
    df.write.mode("overwrite").save_as_table("nyc_taxi_demo_db.raw_layer.raw_employee_data")
    print("Table 'raw_employee_data' created in the raw_layer")
    
    # Read raw_layer table into a DataFrame for step-2 of ETL
    raw_df = session.table("nyc_taxi_demo_db.raw_layer.raw_employee_data")
    
    # Apply transformations (e.g., increase salary by 10%)
    transformed_df = raw_df.with_column("updated_salary", col("salary") * 1.1)
    
    # Create the table in staging_layer schema
    create_staging_table_query = """
    CREATE OR REPLACE TABLE nyc_taxi_demo_db.staging_layer.transformed_employee_data AS 
    SELECT *, salary * 1.1 AS updated_salary FROM nyc_taxi_demo_db.raw_layer.raw_employee_data;
    """
    session.sql(create_staging_table_query).collect()
    print("Table 'transformed_employee_data' created in the staging_layer")
    
    # Read staging_layer table into a DataFrame for Step-3 of the ETL
    staging_df = session.table("nyc_taxi_demo_db.staging_layer.transformed_employee_data")
    
    # Final transformation: Add a timestamp column
    final_df = staging_df.with_column("processed_time", lit(session.sql("SELECT CURRENT_TIMESTAMP").collect()[0][0]))
    
    # Create the table in serving_layer schema
    create_serving_table_query = """
    CREATE OR REPLACE TABLE nyc_taxi_demo_db.serving_layer.final_employee_data AS 
    SELECT *, CURRENT_TIMESTAMP AS processed_time FROM nyc_taxi_demo_db.staging_layer.transformed_employee_data;
    """
    session.sql(create_serving_table_query).collect()
    print("Table 'final_employee_data' created in the serving_layer")
    
    return final_df

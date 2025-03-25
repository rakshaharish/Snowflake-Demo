import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit

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
    
    # Write DataFrame to the raw_layer table directly for Step-1 of ETL
    df.write.mode("overwrite").save_as_table("nyc_taxi_demo_db.raw_layer.raw_employee_data")
    print("Table 'raw_employee_data' created in the raw_layer")
    
    # Read raw_layer table into a DataFrame for step-2 of ETL
    raw_df = session.table("nyc_taxi_demo_db.raw_layer.raw_employee_data")
    
    # Apply transformations (e.g., increase salary by 10%)
    transformed_df = raw_df.with_column("updated_salary", col("salary") * 1.1)
    
    # Write transformed DataFrame to staging_layer table
    transformed_df.write.mode("overwrite").save_as_table("nyc_taxi_demo_db.staging_layer.transformed_employee_data")
    print("Table 'transformed_employee_data' created in the staging_layer")
    
    # Read staging_layer table into a DataFrame for Step-3 of the ETL
    staging_df = session.table("nyc_taxi_demo_db.staging_layer.transformed_employee_data")
    
    # Final transformation: Add a timestamp column
    final_df = staging_df.with_column("processed_time", lit(session.sql("SELECT CURRENT_TIMESTAMP").collect()[0][0]))
    
    # Write final DataFrame to serving_layer table
    final_df.write.mode("overwrite").save_as_table("nyc_taxi_demo_db.serving_layer.final_employee_data")
    print("Table 'final_employee_data' created in the serving_layer")
    
    # Return the final transformed DataFrame
    return final_df

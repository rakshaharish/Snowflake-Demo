------------------------------------------------------
SELECT CURRENT_ROLE(); -- shows as SYSADMIN
USE ROLE ACCOUNTADMIN; -- updated role to account admin
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE SYSADMIN; -- insufficient access
SHOW USERS; -- shows the list of all users 
SHOW GRANTS TO USER RHARISH; -- only SYSADMIN granted currently
-- GRANT ROLE ACCOUNTADMIN TO USER RHARISH; -- if AccountAdmin access is not granted already
-------------------------------------------------------

-- Note: To execute below SQL, the user must have "AccountAdmin" role assigned 
-- (1) Create roles to demo the Access Controls - This can be done only by account admin
CREATE OR REPLACE ROLE etl_admin;
CREATE OR REPLACE ROLE etl_developer;
CREATE OR REPLACE ROLE etl_analyst;

-- (2) Grant access to the roles created
GRANT USAGE ON WAREHOUSE etl_wh TO ROLE etl_admin;
GRANT USAGE ON DATABASE nyc_taxi_demo_db TO ROLE etl_admin;
GRANT USAGE, CREATE TABLE ON SCHEMA nyc_taxi_demo_db.raw_layer TO ROLE etl_developer;
GRANT USAGE, CREATE TABLE ON SCHEMA nyc_taxi_demo_db.staging_layer TO ROLE etl_developer;
GRANT USAGE ON SCHEMA nyc_taxi_demo_db.serving_layer TO ROLE etl_analyst;

-- (3) Assign roles to users
GRANT ROLE etl_admin TO USER RHARISH; -- set Raksha as etl_admin
GRANT ROLE etl_developer TO USER KRATLANI; -- set Karuna as etl_developer
GRANT ROLE etl_analyst TO USER AROY; -- set Arnab as etl_analyst

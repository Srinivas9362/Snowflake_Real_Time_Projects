import os
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables from .env file
load_dotenv('cred.env')

# Define your Snowflake connection parameters using environment variables
connection_params = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

# Initialize conn to None
conn = None

# Establish the connection
try:
    conn = snowflake.connector.connect(
        user=connection_params["user"],
        password=connection_params["password"],
        account=connection_params["account"],
        warehouse=connection_params["warehouse"],
        database=connection_params["database"],
        schema=connection_params["schema"],
        role=connection_params["role"]
    )
    print("Connection established successfully!")

    # Create a cursor object to execute SQL queries
    cur = conn.cursor()

    for i in range(0,5):
    # Write your SQL query
        query = "SELECT * FROM DEV_DB.CONSUMPTION_SCH.WEATHER_DATA LIMIT 10"

    # Execute the query
        cur.execute(query)

        query1 = "SELECT count(*) FROM DEV_DB.CONSUMPTION_SCH.WEATHER_DATA LIMIT 10"

        # Execute the query
        cur.execute(query1)

    # Fetch all results from the executed query
        results = cur.fetchall()

    # Print the results
    for row in results:
        print(row)

    # Close the cursor
    cur.close()

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if conn:
        conn.close()

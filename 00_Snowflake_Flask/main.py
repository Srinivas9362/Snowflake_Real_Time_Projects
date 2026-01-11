import pandas as pd
import snowflake.connector as sf
import json
import os
from dotenv import load_dotenv
from flask import Flask, request

app = Flask(__name__)

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

# Create a connection to Snowflake
try:
    conn = sf.connect(
        user=connection_params["user"],
        password=connection_params["password"],
        account=connection_params["account"],
        warehouse=connection_params["warehouse"],
        database=connection_params["database"],
        schema=connection_params["schema"],
        role=connection_params["role"],
        autocommit=True  # Optional but useful to ensure automatic commits
    )
    print("Connection established successfully!")
except Exception as e:
    print(f"Failed to connect to Snowflake: {e}")
    conn = None

# Function to run a query without returning results
def run_query(conn, query):
    print(f"Executing the query: {query}")
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully!")
    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        cursor.close()

# Function to run a query and return the first result
def run_query1(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        records = str(cursor.fetchone()[0])
    except Exception as e:
        print(f"Error fetching data: {e}")
        records = None
    finally:
        cursor.close()
    return records

@app.route('/')
def hello_world():
    return "Hello World, Welcome to Snowflake API"

@app.route('/aggregator', methods=['GET', 'POST'])
def stats_collector():
    query_to_be_executed = request.args.get("query")
    extracted_data = run_query1(conn, query_to_be_executed)
    if extracted_data:
        print(extracted_data)
        return extracted_data
    else:
        return "Failed to execute query or fetch data."

@app.route('/data_fetcher', methods=['GET', 'POST'])
def data_extractor():
    query_to_be_executed = request.args.get("query")
    try:
        data_from_table = pd.read_sql(query_to_be_executed, conn)
        return {"data": json.loads(data_from_table.to_json(orient='records'))}
    except Exception as e:
        return f"Error fetching data: {e}"

if __name__ == '__main__':
    # Run initialization queries to set up the environment
    if conn:
        run_query(conn, f"use warehouse {connection_params['warehouse']}")
        run_query(conn, f"alter warehouse {connection_params['warehouse']} resume")
        run_query(conn, f"use database {connection_params['database']}")
        run_query(conn, f"use role {connection_params['role']}")
    else:
        print("Connection is not established, skipping initialization queries.")

    app.run()

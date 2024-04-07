import psycopg2
import yaml
import logging
import schedule
import time
from datetime import datetime


# Configure logging
logging.basicConfig(level=logging.INFO)


# Function to read database configuration from config file
def read_db_config(filename='config.yml'):
    """Reads database configuration from a YAML file."""
    with open(filename, 'r') as file:
        config = yaml.safe_load(file)
    return config['database']


# Function to connect to PostgreSQL database
def connect_to_postgres(config):
    """Connects to a PostgreSQL database."""
    try:
        conn = psycopg2.connect(**config)
        logging.info("Connected to PostgreSQL database!")
        return conn
    except psycopg2.Error as e:
        logging.error("Error connecting to PostgreSQL database: %s", e)
        return None

# Function to execute the stored procedure
def execute_stored_proc(conn):
    try:
        cursor = conn.cursor()

        # Execute the stored procedure
        cursor.callproc('populate_dim_date')
        conn.commit()
        print("Stored procedure executed successfully!")
    except psycopg2.Error as e:
        print("Error executing stored procedure:", e)

# Main function
def scheduler():
    # Connect to PostgreSQL
    conn = connect_to_postgres()
    if conn is None:
        return


    # Execute the stored procedure
    execute_stored_proc(conn)

    # Close connection
    conn.close()
    print("Connection closed.")

# Schedule the execution of the script every day at 01:00
schedule.every().day.at("01:00").do(scheduler)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(1)
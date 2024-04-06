import psycopg2
import yaml
import logging
import pandas as pd

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


# Function to create tables
def create_tables(conn, sql_file='postgres/data_table.sql',
                  duplicated_data_table_sql='postgres/log_duplicated_table.sql',
                  dim_date_table='postgres/date_dim_table.sql'):
    """Creates tables in the Postgre database."""
    try:
        cursor = conn.cursor()

        # Read SQL commands from file
        with open(sql_file, 'r') as file:
            create_table_sql = file.read()

        # Execute SQL to create tables
        cursor.execute(create_table_sql)

        # Read SQL commands from file for the duplicated_data table
        with open(duplicated_data_table_sql, 'r') as file:
            duplicated_data_table_sql = file.read()

        # Execute SQL to create the duplicated_data table
        cursor.execute(duplicated_data_table_sql)

        # Read SQL commands from file
        with open(dim_date_table, 'r') as file:
            create_table_sql = file.read()

        # Execute SQL to create tables
        cursor.execute(create_table_sql)

        conn.commit()
        logging.info("Tables created successfully!")
    except psycopg2.Error as e:
        logging.error("Error creating tables: %s", e)


# Function to load data from CSV and insert into the database
def load_data(conn):
    try:
        cursor = conn.cursor()

        # Read the insert query from SQL file
        with open('postgres/insert_data.sql', 'r') as file:
            insert_query = file.read()

        # Read data from CSV file
        df = pd.read_csv('datasets/performed/Cleaned.csv')

        # Convert DataFrame to list of tuples
        data = df.values.tolist()

        # Execute SQL query to insert data into the database
        cursor.executemany(insert_query, data)
        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print("Error inserting data:", e)


def create_stored_procedure(conn):
    """Creates the stored procedure to populate the 'dim date' table."""
    try:
        cursor = conn.cursor()

        # Read SQL commands from file
        with open('postgres/populate_dim_date.sql', 'r') as file:
            sql_query = file.read()

        # Execute SQL query to create the stored procedure
        cursor.execute(sql_query)

        conn.commit()
        logging.info("Stored procedure created successfully!")
    except psycopg2.Error as e:
        logging.error("Error creating stored procedure: %s", e)


# Main function
def main(config_file='config.yml'):
    """Main entry point of the program."""
    # 1. Read database configuration from config file
    db_config = read_db_config(config_file)

    # 2. Connect to PostgreSQL
    conn = connect_to_postgres(db_config)
    if conn is None:
        return

    # 3. Create tables
    create_tables(conn)

    # 4. Load data into the database
    load_data(conn)

    # 5. Create stored procedure to populate "dim date" table
    create_stored_procedure(conn)

    # 6. Close connection
    conn.close()

    logging.info("Connection closed.")


if __name__ == "__main__":
    main()

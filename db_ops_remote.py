import logging
import os
import time

import config
import db_ops
from ConnType import DBLocation

logger = logging.getLogger(__name__)
import psycopg2
from sshtunnel import SSHTunnelForwarder
# Configuration
BASTION_IP = os.getenv("FINGREP_BASTION_IP")
BASTION_PORT = int(os.getenv("FINGREP_BASTION_PORT"))
BASTION_USER = os.getenv("FINGREP_BASTION_USER")
BASTION_KEY = os.getenv("FINGREP_BASTION_KEY")
REMOTE_DB_HOST = os.getenv("FINGREP_REMOTE_DB_HOST")
LOCAL_DB_HOST = os.getenv("FINGREP_LOCAL_DB_HOST")
DB_NAME = os.getenv("FINGREP_DB")
DB_USER = os.getenv("FINGREP_DB_USER")
DB_PASSWORD = os.getenv("FINGREP_DB_PASS")
DB_PORT = 5432

def postgresql_remote_connection():
    try:
        # Establish SSH tunnel
        tunnel = SSHTunnelForwarder(
            (BASTION_IP, BASTION_PORT),  # Bastion host and SSH port
            ssh_username=BASTION_USER,
            ssh_pkey=BASTION_KEY,
            remote_bind_address=(REMOTE_DB_HOST, DB_PORT)
        )

        tunnel.start()
        try:
            # Connect to the database through the tunnel
            connection = psycopg2.connect(
                host='127.0.0.1',  # Localhost for the tunnel
                port=tunnel.local_bind_port,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            return connection, tunnel

        except psycopg2.Error as db_error:
            logger.error(f"Remote db connection error: {db_error}")
            tunnel.close()  # Ensure the tunnel is closed if DB connection fails
            raise

    except Exception as ssh_error:
        logger.error(f"SSH tunnel error: {ssh_error}")
        raise  # Re-raise the exception after logging it



# Function to read data from the shares table
def read_shares_table():

    connection, tunnel = postgresql_remote_connection()
    try:
        cursor = connection.cursor()
        print("Connected to the database.")

        # Execute SELECT query
        query = "SELECT count(*) FROM shares;"
        cursor.execute(query)

        # Fetch and print all rows
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        connection.close()
        tunnel.stop()
        print("Database connection closed.")


# Run the function
if __name__ == "__main__":
    start_time = time.time()  # Start time in seconds
    # Perform your operation here
    time.sleep(1)  # Simulate some delay
    end_time = time.time()  # End time in seconds



import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

class TableNotFoundError(Exception):
    """Custom exception raised when a database table does not exist."""
    def __init__(self, table_name):
        self.table_name = table_name
        super().__init__(f"Table '{table_name}' does not exist.")

def get_db_connection():
    """
    Establishes and returns a connection to the PostgreSQL database.
    """
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    return conn

def execute_query(query, params=None):
    """
    Executes a given SQL query and raises TableNotFoundError if a table does not exist.
    
    :param query: The SQL query to execute.
    :param params: Optional parameters to pass into the SQL query.
    :return: Query results or raises TableNotFoundError if applicable.
    """
    conn = None
    results = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            if cursor.description:  # Check if the query returns any result
                results = cursor.fetchall()
        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

    return results


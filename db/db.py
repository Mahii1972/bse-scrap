import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def execute_query(query, params=None):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                conn.commit()
                return True
        except Exception as e:
            print(f"Error executing query: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    return False

def fetch_query(query, params=None):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                return cur.fetchall()
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
        finally:
            conn.close()
    return None

def execute_batch_query(query, params_list, page_size=100):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                execute_batch(cur, query, params_list, page_size=page_size)
                conn.commit()
                return True
        except Exception as e:
            print(f"Error executing batch query: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    return False
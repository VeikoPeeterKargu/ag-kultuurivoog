import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

def init_db():
    # Default connection parameters - adjust if needed or take from env
    db_params = {
        "user": "vpk", # Try current user first, often works on macOS with Homebrew
        "host": "localhost",
        "port": "5432"
    }

    try:
        # Connect to 'postgres' db to create new db
        print("Connecting to postgres database...")
        try:
            conn = psycopg2.connect(dbname="postgres", **db_params)
        except psycopg2.OperationalError:
             # Fallback to 'postgres' user if current user fails
            print("Connection with current user failed, trying 'postgres' user...")
            db_params["user"] = "postgres"
            conn = psycopg2.connect(dbname="postgres", **db_params)
            
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Check if database exists
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'kultuurivoog'")
        exists = cur.fetchone()
        
        if not exists:
            print("Creating database 'kultuurivoog'...")
            cur.execute("CREATE DATABASE kultuurivoog")
        else:
            print("Database 'kultuurivoog' already exists.")
        
        cur.close()
        conn.close()

        # Connect to the new database
        print("Connecting to 'kultuurivoog' database...")
        conn = psycopg2.connect(dbname="kultuurivoog", **db_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Read schema.sql
        print("Applying schema...")
        with open('schema.sql', 'r') as f:
            schema = f.read()
        
        cur.execute(schema)
        print("Schema applied successfully.")

        # Verify table creation
        cur.execute("SELECT to_regclass('public.events')")
        if cur.fetchone()[0]:
             print("Table 'events' verified.")
        else:
             print("Error: Table 'events' not found after schema application.")
             sys.exit(1)

        cur.close()
        conn.close()
        print("Database initialization complete.")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    init_db()

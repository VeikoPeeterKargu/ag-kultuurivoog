import psycopg2
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

# Get DATABASE_URL from env or default to localhost
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://vpk@localhost:5432/kultuurivoog")

def get_connection_params(url):
    # Very basic parser for the default case, relying on libpq for the rest if passed directly
    # But for creating the DB, we might need to connect to 'postgres' first.
    # We'll assume the URL is in a standard format.
    # For simplicity, if it's the default URL, we know the parts.
    if url == "postgresql://vpk@localhost:5432/kultuurivoog":
         return {"user": "vpk", "host": "localhost", "port": "5432", "dbname": "kultuurivoog"}
    return {"dsn": url}

def init_db():
    print(f"Initializing database using: {DATABASE_URL}")
    
    # 1. Ensure Database Exists
    # We need to connect to 'postgres' db to create 'kultuurivoog' if it doesn't exist
    conn_params = get_connection_params(DATABASE_URL)
    
    # Only try to create DB if we can parse the name from our default/simple params
    target_dbname = conn_params.get("dbname")
    
    if target_dbname:
        try:
            # Connect to postgres system db
            sys_params = conn_params.copy()
            sys_params["dbname"] = "postgres"
            if "dsn" in sys_params: del sys_params["dsn"] # Remove dsn if we are constructing manually
            
            print("Connecting to 'postgres' system database...")
            # Try to connect to postgres db to check/create target db
            # We might fail here if auth fails or server down.
            conn = psycopg2.connect(**sys_params)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            
            cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{target_dbname}'")
            if not cur.fetchone():
                print(f"Creating database '{target_dbname}'...")
                cur.execute(f"CREATE DATABASE {target_dbname}")
            else:
                print(f"Database '{target_dbname}' already exists.")
            
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Warning: Could not connect to 'postgres' db to check/create '{target_dbname}'. Assuming it exists or using provided credentials directly.\nError: {e}")

    # 2. Connect to Target DB and Create Table
    try:
        if "dsn" in conn_params:
            conn = psycopg2.connect(conn_params["dsn"])
        else:
             conn = psycopg2.connect(**conn_params)
             
        conn.autocommit = True
        cur = conn.cursor()
        
        print(f"Connected to '{target_dbname or 'target'}' database.")

        # Create Table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            genre TEXT,
            date DATE NOT NULL,
            time TIME NULL,
            venue TEXT,
            city TEXT,
            is_free BOOLEAN DEFAULT FALSE,
            is_kids_event BOOLEAN DEFAULT FALSE,
            description TEXT,
            image_url TEXT,
            ticket_url TEXT,
            canonical_event_id TEXT NOT NULL UNIQUE,
            source TEXT NOT NULL DEFAULT 'teater.ee',
            source_url TEXT,
            last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """
        cur.execute(create_table_sql)
        print("Table 'events' ensured.")

        # Create Indices
        indices = [
            "CREATE INDEX IF NOT EXISTS idx_events_date ON events(date);",
            "CREATE INDEX IF NOT EXISTS idx_events_genre ON events(genre);",
            "CREATE INDEX IF NOT EXISTS idx_events_is_kids ON events(is_kids_event);"
        ]
        
        for idx in indices:
            cur.execute(idx)
        print("Indices ensured.")
        
        # Check for missing columns (Simple ALTER Check - naive but effective for this task)
        # We checked strict list. If table existed from previous run, it might miss 'source', 'source_url', 'last_seen_at', 'updated_at'
        required_columns = {
            'source': "TEXT NOT NULL DEFAULT 'teater.ee'", 
            'source_url': "TEXT", 
            'last_seen_at': "TIMESTAMP NOT NULL DEFAULT NOW()", 
            'updated_at': "TIMESTAMP NOT NULL DEFAULT NOW()",
            'canonical_event_id': "TEXT" # previously VARCHAR(255), now TEXT. Postgres handles this mostly fine, but let's ensure.
        }
        
        for col, def_type in required_columns.items():
            try:
                cur.execute(f"ALTER TABLE events ADD COLUMN IF NOT EXISTS {col} {def_type}")
            except Exception as e:
                print(f"Notice: Could not alter table for column {col} (might already exist with different type): {e}")

        cur.close()
        conn.close()
        print("Database validation complete.")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    init_db()

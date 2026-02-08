import os
import sys
import psycopg2

def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("Error: DATABASE_URL environment variable is not set.")
        sys.exit(1)
    return psycopg2.connect(db_url)

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Create table with stricter schema
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                genre TEXT,
                date DATE NOT NULL,
                time TIME,
                venue TEXT,
                city TEXT,
                is_free INTEGER DEFAULT 0,
                free_reason TEXT,
                is_kids_event INTEGER DEFAULT 0,
                description TEXT,
                image_url TEXT,
                ticket_url TEXT,
                canonical_event_id TEXT NOT NULL UNIQUE,
                source TEXT NOT NULL DEFAULT 'teater.ee',
                source_url TEXT,
                last_seen_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_date ON events(date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_is_kids ON events(is_kids_event);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_genre ON events(genre);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_canonical ON events(canonical_event_id);")

        print("DB_INIT_OK: true")

        # Views
        # 1.1 v_events_clean (Only future events)
        cur.execute("""
            CREATE OR REPLACE VIEW v_events_clean AS
            SELECT
                id,
                date,
                time,
                title,
                genre,
                venue,
                city,
                is_free,
                is_kids_event,
                description,
                source,
                source_url,
                ticket_url,
                canonical_event_id
            FROM events
            WHERE date >= CURRENT_DATE
        """)
        
        # 1.2 v_events_clean_adults (Future + No Kids)
        cur.execute("""
            CREATE OR REPLACE VIEW v_events_clean_adults AS
            SELECT *
            FROM v_events_clean
            WHERE is_kids_event = 0
        """)

        conn.commit()
        print("VIEWS_OK: true")
        
    except Exception as e:
        print(f"DB_INIT_ERROR: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    init_db()

import sqlite3
import json

DB_FILE = "ag_kultuurivoog.db"

def create_views():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 1.1 v_events_clean
    cursor.execute("DROP VIEW IF EXISTS v_events_clean")
    cursor.execute("""
        CREATE VIEW v_events_clean AS
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
            source,
            source_url,
            ticket_url,
            canonical_event_id
        FROM events
    """)
    
    # 1.2 v_events_clean_adults
    cursor.execute("DROP VIEW IF EXISTS v_events_clean_adults")
    cursor.execute("""
        CREATE VIEW v_events_clean_adults AS
        SELECT *
        FROM v_events_clean
        WHERE is_kids_event = 0
    """)
    
    conn.commit()
    print("VIEWS_CREATED: v_events_clean, v_events_clean_adults")
    
    # Counts
    c_clean = cursor.execute("SELECT COUNT(*) FROM v_events_clean").fetchone()[0]
    c_adults = cursor.execute("SELECT COUNT(*) FROM v_events_clean_adults").fetchone()[0]
    print(f"VIEW_COUNTS: clean={c_clean}, adults={c_adults}")
    conn.close()

def cleanup_data():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    print("\nCLEANUP_STARTED")
    
    # 2.2 Reegel A (Keywords)
    cursor.execute("""
        DELETE FROM events
        WHERE (
            lower(title) LIKE '%galerii%'
            OR lower(title) LIKE '%foto%'
            OR lower(title) LIKE '%pildid%'
            OR lower(title) LIKE '%tähistas%'
            OR lower(title) LIKE '%tagasivaade%'
        )
    """)
    deleted_a = cursor.rowcount
    
    # 2.2 Reegel B (No time for concerts)
    # Check if time column has empty strings or NULL
    cursor.execute("""
        DELETE FROM events
        WHERE source = 'concert.ee'
        AND genre = 'Kontsert'
        AND (time IS NULL OR trim(time) = '')
    """)
    deleted_b = cursor.rowcount
    
    conn.commit()
    
    total_deleted = deleted_a + deleted_b
    print(f"DELETED_RECORDS: {total_deleted}")
    
    total_events = cursor.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    print(f"DB_TOTAL_EVENTS_AFTER_CLEANUP: {total_events}")
    
    # Sample concert.ee events
    concerts = cursor.execute("""
        SELECT date, time, title, venue, city, genre, source_url, canonical_event_id
        FROM events
        WHERE source = 'concert.ee'
        LIMIT 3
    """).fetchall()
    
    columns = ['date', 'time', 'title', 'venue', 'city', 'genre', 'source_url', 'canonical_event_id']
    
    if not concerts:
        print("CONCERT_EVENTS_REMAINING: 0")
    else:
        for row in concerts:
            print(json.dumps(dict(zip(columns, row)), ensure_ascii=False))
            
    conn.close()

if __name__ == "__main__":
    create_views()
    cleanup_data()

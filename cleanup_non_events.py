def ensure_views(db_path=None):
    if db_path is None: db_path = DB_FILE
    conn = sqlite3.connect(db_path)
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
    c_clean = cursor.execute("SELECT COUNT(*) FROM v_events_clean").fetchone()[0]
    c_adults = cursor.execute("SELECT COUNT(*) FROM v_events_clean_adults").fetchone()[0]
    conn.close()
    return {"clean": c_clean, "adults": c_adults}

def run_cleanup(db_path=None):
    if db_path is None: db_path = DB_FILE
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
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
    cursor.execute("""
        DELETE FROM events
        WHERE source = 'concert.ee'
        AND genre = 'Kontsert'
        AND (time IS NULL OR trim(time) = '')
    """)
    deleted_b = cursor.rowcount
    
    conn.commit()
    total_deleted = deleted_a + deleted_b
    total_events = cursor.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    conn.close()
    return {"deleted": total_deleted, "total_after": total_events}

if __name__ == "__main__":
    v_stats = ensure_views()
    print(f"VIEWS: {v_stats}")
    c_stats = run_cleanup()
    print(f"CLEANUP: {c_stats}")

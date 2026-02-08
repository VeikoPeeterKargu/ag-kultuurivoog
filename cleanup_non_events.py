import os
import sys
import psycopg2

def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("Error: DATABASE_URL not set")
        return None
    try:
        return psycopg2.connect(db_url)
    except Exception as e:
        print(f"DB Connection Error: {e}")
        return None

def ensure_views():
    # In Postgres, views are persistent, but we can recreate them just in case schema changed.
    # Actually, db_init does this. But we can call db_init logic here or just rely on db_init.
    # To be safe and follow the pattern:
    conn = get_db_connection()
    if not conn: return {}
    
    cur = conn.cursor()
    # Assuming views exist from db_init, just count.
    # Or should we recreate? db_init is better place. 
    # Let's just count here.
    try:
        cur.execute("SELECT COUNT(*) FROM v_events_clean")
        c_clean = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM v_events_clean_adults")
        c_adults = cur.fetchone()[0]
        conn.close()
        return {"clean": c_clean, "adults": c_adults}
    except:
        conn.close()
        return {"clean": 0, "adults": 0}

def run_cleanup():
    conn = get_db_connection()
    if not conn: return {"deleted": 0, "total_after": 0}
    
    cur = conn.cursor()
    
    # 2.2 Reegel A (Keywords)
    cur.execute("""
        DELETE FROM events
        WHERE (
            title ILIKE '%galerii%'
            OR title ILIKE '%foto%'
            OR title ILIKE '%pildid%'
            OR title ILIKE '%tähistas%'
            OR title ILIKE '%tagasivaade%'
        )
    """)
    deleted_a = cur.rowcount
    
    # 2.2 Reegel B (No time for concerts)
    cur.execute("""
        DELETE FROM events
        WHERE source = 'concert.ee'
        AND genre = 'Kontsert'
        AND (time IS NULL)
    """)
    deleted_b = cur.rowcount
    
    conn.commit()
    total_deleted = deleted_a + deleted_b
    
    print(f"DELETED_RECORDS: {total_deleted}")
    
    cur.execute("SELECT COUNT(*) FROM events")
    total_after = cur.fetchone()[0]
    print(f"DB_TOTAL_EVENTS_AFTER_CLEANUP: {total_after}")
    
    cur.close()
    conn.close()
    return {"deleted": total_deleted, "total_after": total_after}

if __name__ == "__main__":
    run_cleanup()
    ensure_views()

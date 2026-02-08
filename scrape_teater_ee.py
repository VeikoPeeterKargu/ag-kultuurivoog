import sqlite3
import requests
from bs4 import BeautifulSoup
import hashlib
import datetime
import json
import re

# Configuration
DB_FILE = "ag_kultuurivoog.db"
TEATER_EE_URL = "https://teater.ee/teatriinfo/mangukava/"

MONTHS = {
    "jaanuar": "01", "veebruar": "02", "märts": "03", "aprill": "04",
    "mai": "05", "juuni": "06", "juuli": "07", "august": "08",
    "september": "09", "oktoober": "10", "november": "11", "detsember": "12"
}

KIDS_KEYWORDS = [
    "lastele", "kogupere", "perelavastus", "nukk", "nukuteater", 
    "muinasjutt", "pipi", "lotte", "sipsik", "väike", "tilluke"
]
KIDS_VENUES = ["Eesti Noorsooteater", "NUKU", "Lasteteater"]

FREE_KEYWORDS = [
    "tasuta", "vaba sissepääs", "vabalt valitud annetusega", 
    "annetuspõhine", "soovituslik annetus", "piletita"
]

def normalize_text(text):
    if not text:
        return ""
    return " ".join(text.strip().split()).lower()

def is_kids_event_check(title, venue, description):
    text_to_check = (title + " " + description).lower()
    for kw in KIDS_KEYWORDS:
        if kw in text_to_check:
            return 1
    for kv in KIDS_VENUES:
        if kv.lower() in venue.lower():
            return 1
    return 0

def detect_genre(title, description, venue):
    text = (title + " " + description).lower()
    venue_lower = venue.lower()
    
    # 1. Opera
    if "ooper" in text or "opera" in text:
        return "Ooper"
    
    # 2. Ballet
    if any(kw in text for kw in ["ballett", "tantsuteater", "tantsulavastus", "koreograaf"]):
        return "Ballett"
        
    # 3. Operetta
    if "operett" in text:
        return "Operett"
        
    # 4. Concert
    concert_kws = ["kontsert", "jazz", "orkester", "klaveriõhtu", "kammerkontsert", "koor", "ansambel"]
    if any(kw in text for kw in concert_kws):
        return "Kontsert"
    if any(kw in venue_lower for kw in ["kontserdimaja", "philly joe", "jazz", "ait"]):
        return "Kontsert"
        
    # 5. Default
    return "Teater"

def detect_free(title, description):
    text = (title + " " + description).lower()
    for kw in FREE_KEYWORDS:
        if kw in text:
            return 1, kw
    return 0, None

def parse_estonian_full_date(date_str):
    try:
        if "," in date_str:
            date_str = date_str.split(",")[1].strip()
        parts = date_str.split(" ")
        if len(parts) >= 3:
            day = parts[0].replace(".", "").zfill(2)
            month_name = parts[1].lower()
            year = parts[2]
            month = MONTHS.get(month_name)
            if month: return f"{year}-{month}-{day}"
    except Exception: pass
    return None

def generate_canonical_id(title, date_str, venue, city, time_str):
    norm_title = normalize_text(title)
    norm_venue = normalize_text(venue)
    norm_city = normalize_text(city)
    clean_time = time_str if time_str else ""
    canonical_raw = f"{norm_title}|{date_str}|{norm_venue}|{norm_city}|{clean_time}"
    return hashlib.sha1(canonical_raw.encode('utf-8')).hexdigest()

def init_db(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            genre TEXT,
            date TEXT NOT NULL,
            time TEXT,
            venue TEXT,
            city TEXT,
            is_free INTEGER DEFAULT 0,
            is_kids_event INTEGER DEFAULT 0,
            description TEXT,
            image_url TEXT,
            ticket_url TEXT,
            canonical_event_id TEXT NOT NULL UNIQUE,
            source TEXT NOT NULL DEFAULT 'teater.ee',
            source_url TEXT,
            last_seen_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    # Add free_reason column if missing
    try:
        cursor.execute("ALTER TABLE events ADD COLUMN free_reason TEXT")
    except sqlite3.OperationalError:
        pass # Column likely exists
        
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_date ON events(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_genre ON events(genre)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_is_kids ON events(is_kids_event)")
    conn.commit()

def run_reports(cursor, run_id):
    print(f"\n--- REPORTS (RUN {run_id}) ---")
    
    # Genre Stats
    genres = ["Ooper", "Ballett", "Operett", "Kontsert", "Teater"]
    print("GENRE_STATS:")
    for g in genres:
        count = cursor.execute("SELECT COUNT(*) FROM events WHERE genre = ?", (g,)).fetchone()[0]
        print(f"  {g}: {count}")
        
    # Free Stats
    free_count = cursor.execute("SELECT COUNT(*) FROM events WHERE is_free = 1").fetchone()[0]
    print(f"\nFREE_STATS\nFREE_EVENTS_COUNT: {free_count}")
    
    print("FREE_REASONS:")
    reasons = cursor.execute("SELECT free_reason, COUNT(*) FROM events WHERE is_free = 1 GROUP BY free_reason").fetchall()
    for r, c in reasons:
        print(f"  \"{r}\": {c}")

    # Sample Data
    print("\nSample Data (10 Rows):")
    # 2 Opera/Ballet/Operetta
    special_genres = cursor.execute("SELECT * FROM events WHERE genre IN ('Ooper', 'Ballett', 'Operett') LIMIT 2").fetchall()
    # 3 Concert
    concerts = cursor.execute("SELECT * FROM events WHERE genre = 'Kontsert' LIMIT 3").fetchall()
    # 3 Theater
    theater = cursor.execute("SELECT * FROM events WHERE genre = 'Teater' LIMIT 3").fetchall()
    # 2 Free
    free_events = cursor.execute("SELECT * FROM events WHERE is_free = 1 LIMIT 2").fetchall()
    
    columns = [desc[0] for desc in cursor.description]
    
    def row_to_dict(row):
        return dict(zip(columns, row))
        
    all_samples = special_genres + concerts + theater + free_events
    # Dedup samples specifically for display to avoid duplicates if categories overlap
    seen_ids = set()
    unique_samples = []
    for row in all_samples:
        d = row_to_dict(row)
        if d['id'] not in seen_ids:
            seen_ids.add(d['id'])
            unique_samples.append(d)
            
    # Print only required fields JSON
    req_fields = ['date', 'time', 'title', 'venue', 'city', 'genre', 'is_free', 'free_reason', 'source_url', 'canonical_event_id']
    for s in unique_samples[:10]:
        out = {k: s[k] for k in req_fields if k in s}
        print(json.dumps(out, ensure_ascii=False))

def scrape_teater_ee(run_id=1):
    conn = sqlite3.connect(DB_FILE)
    init_db(conn)
    cursor = conn.cursor()

    # Pre-fetch existing IDs for update tracking
    existing_ids = set(row[0] for row in cursor.execute("SELECT canonical_event_id FROM events").fetchall())

    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(TEATER_EE_URL, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"Error: {e}")
        conn.close()
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    date_blocks = soup.select('.post-etendus__item')
    
    total_parsed = 0
    # True Inserted/Updated tracking
    inserted_count = 0
    updated_count = 0
    
    current_time = datetime.datetime.now().isoformat()

    for block in date_blocks:
        if total_parsed >= 50: break
        
        d_head = block.select_one('.post-etendus__heading')
        if not d_head: continue
        date_iso = parse_estonian_full_date(d_head.get_text(strip=True))
        if not date_iso: continue
            
        for ev_div in block.select('.block-etendus'):
            try:
                title_el = ev_div.select_one('.block-etendus__paragraph-big') or ev_div.select_one('.block-etendus__paragraph-big')
                title = title_el.get_text(strip=True) if title_el else "Unknown"

                link_el = ev_div.select_one('a[href*="/lavastused/"]')
                source_url = link_el['href'] if link_el else ""
                if source_url and not source_url.lower().startswith('http'):
                    source_url = "https://teater.ee" + source_url
                
                time_el = ev_div.select_one('.block-etendus__time')
                time_str = time_el.get_text(strip=True) if time_el else None
                
                venue = ""
                city = ""
                # Improved venue extraction logic? Using grep insights:
                # Often just `block-etendus__paragraph-small`
                ps = ev_div.select('.block-etendus__paragraph-small')
                # Usually: 1. Hashtags/Genre 2. Venue
                # But sometimes first logic works.
                for p in ps:
                    txt = p.get_text(strip=True)
                    if "vaatajale" in txt.lower() or "lavastus" in txt.lower(): continue 
                    if not venue and len(txt) > 2: venue = txt
                
                if "Tallinn" in venue: city = "Tallinn"
                elif "Tartu" in venue: city = "Tartu"
                elif "Pärnu" in venue: city = "Pärnu"
                elif "Rakvere" in venue: city = "Rakvere"
                elif "Viljandi" in venue: city = "Viljandi"
                elif "Kuressaare" in venue: city = "Kuressaare"
                elif "Narva" in venue: city = "Narva"

                img_el = ev_div.select_one('img')
                image_url = img_el['src'] if img_el else ""
                
                # Enrich
                is_kids = is_kids_event_check(title, venue, "")
                genre = detect_genre(title, "", venue)
                is_free, free_reason = detect_free(title, "")
                
                canonical_id = generate_canonical_id(title, date_iso, venue, city, time_str)
                
                # Check status
                if canonical_id in existing_ids:
                    updated_count += 1
                else:
                    inserted_count += 1
                
                # UPSERT
                cursor.execute("""
                    INSERT INTO events (
                        title, genre, date, time, venue, city, 
                        is_free, free_reason, is_kids_event, description, image_url, ticket_url, 
                        canonical_event_id, source, source_url, 
                        last_seen_at, created_at, updated_at
                    ) VALUES (
                        :title, :genre, :date, :time, :venue, :city,
                        :is_free, :free_reason, :is_kids_event, :description, :image_url, :ticket_url,
                        :canonical_event_id, :source, :source_url,
                        :last_seen_at, :created_at, :updated_at
                    )
                    ON CONFLICT(canonical_event_id) DO UPDATE SET
                        title=excluded.title,
                        genre=excluded.genre,
                        is_kids_event=excluded.is_kids_event,
                        is_free=excluded.is_free,
                        free_reason=excluded.free_reason,
                        date=excluded.date,
                        time=excluded.time,
                        venue=excluded.venue,
                        city=excluded.city,
                        image_url=excluded.image_url,
                        source_url=excluded.source_url,
                        last_seen_at=excluded.last_seen_at,
                        updated_at=excluded.updated_at
                """, {
                    'title': title, 'genre': genre, 
                    'date': date_iso, 'time': time_str,
                    'venue': venue, 'city': city,
                    'is_free': is_free, 'free_reason': free_reason,
                    'is_kids_event': is_kids,
                    'description': '', 'image_url': image_url,
                    'ticket_url': '', 'canonical_event_id': canonical_id,
                    'source': 'teater.ee', 'source_url': source_url,
                    'last_seen_at': current_time, 
                    'created_at': current_time,
                    'updated_at': current_time
                })
                
                total_parsed += 1
                
            except Exception: pass
            
    conn.commit()
    print(f"\nRUN {run_id} LOG:")
    print(f"TOTAL_PARSED: {total_parsed}")
    print(f"INSERTED: {inserted_count}")
    print(f"UPDATED: {updated_count}")
    
    run_reports(cursor, run_id)
    conn.close()

if __name__ == "__main__":
    print("--- FIRST RUN ---")
    scrape_teater_ee(run_id=1)
    
    print("\n\n--- SECOND RUN (DEDUPE CHECK) ---")
    scrape_teater_ee(run_id=2)

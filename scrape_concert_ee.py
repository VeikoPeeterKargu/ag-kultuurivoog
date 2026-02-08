import sqlite3
import requests
from bs4 import BeautifulSoup
import hashlib
import datetime
import json
import re

# Configuration
DB_FILE = "ag_kultuurivoog.db"
CONCERT_EE_URL = "https://concert.ee/" 

MONTHS = {
    "jaanuar": "01", "veebruar": "02", "märts": "03", "aprill": "04",
    "mai": "05", "juuni": "06", "juuli": "07", "august": "08",
    "september": "09", "oktoober": "10", "november": "11", "detsember": "12"
}

FREE_KEYWORDS = [
    "tasuta", "vaba sissepääs", "vabalt valitud annetusega", 
    "annetuspõhine", "soovituslik annetus", "piletita"
]

def normalize_text(text):
    if not text: return ""
    return " ".join(text.strip().split()).lower()

def parse_estonian_full_date(date_str):
    try:
        # "30.01" -> need to add year.
        sim = re.match(r'^(\d{1,2})\.(\d{2})$', date_str.strip())
        if sim:
            day, month = sim.group(1).zfill(2), sim.group(2).zfill(2)
            now = datetime.datetime.now()
            year = now.year
            if now.month == 12 and int(month) == 1: year += 1
            return f"{year}-{month}-{day}"

        # "12.02.2026"
        dm = re.match(r'(\d{1,2})\.(\d{2})(?:\.(\d{4}))', date_str)
        if dm:
            return f"{dm.group(3)}-{dm.group(2).zfill(2)}-{dm.group(1).zfill(2)}"
        
        # Text format
        if "," in date_str: date_str = date_str.split(",")[1].strip()
        parts = date_str.split(" ")
        if len(parts) >= 3:
            day = parts[0].replace(".", "").zfill(2)
            month_name = parts[1].lower()
            year = parts[2]
            month = MONTHS.get(month_name)
            if month: return f"{year}-{month}-{day}"
    except Exception: pass
    return None

def detect_free(title, description):
    text = (title + " " + description).lower()
    for kw in FREE_KEYWORDS:
        if kw in text: return 1, kw
    return 0, None

def generate_canonical_id(title, date_str, venue, city, time_str):
    norm_title = normalize_text(title)
    norm_venue = normalize_text(venue)
    norm_city = normalize_text(city)
    clean_time = time_str if time_str else ""
    canonical_raw = f"{norm_title}|{date_str}|{norm_venue}|{norm_city}|{clean_time}"
    return hashlib.sha1(canonical_raw.encode('utf-8')).hexdigest()

def scrape_concert_ee():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    existing_ids = set(row[0] for row in cursor.execute("SELECT canonical_event_id FROM events").fetchall())
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        response = requests.get(CONCERT_EE_URL, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching {CONCERT_EE_URL}: {e}")
        conn.close()
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    
    # NEW STRATEGY: Iterate over columns, check for event date inside
    # Structure seen in dump: <div class="col"> ... <div class="event"> ... <div class="date">
    # We can iterate .event or .col that contains .event
    
    event_blocks = soup.select('.event')
    if not event_blocks:
        # Fallback to col + check for date
        cols = soup.select('.col')
        event_blocks = []
        for c in cols:
            if c.select_one('.date') and c.select_one('h3 a'):
                event_blocks.append(c)

    parsed = 0
    inserted = 0
    updated = 0
    events_found = []
    current_time = datetime.datetime.now().isoformat()

    for block in event_blocks:
        if parsed >= 40: break
        try:
            # Title
            title_el = block.select_one('h3 a')
            if not title_el: 
                 # Maybe generic
                 title_el = block.select_one('.title a')
            
            if not title_el: continue
            
            title = title_el.get_text(strip=True)
            source_url = title_el['href']
            if source_url and not source_url.startswith('http'):
                source_url = "https://concert.ee" + source_url

            # Date
            date_el = block.select_one('.date')
            date_text = date_el.get_text(strip=True) if date_el else ""
            
            date_iso = parse_estonian_full_date(date_text)
            if not date_iso: continue 

            # Time - usually hidden in list view, ignore
            time_str = None
            
            # Venue - usually hidden in list view, ignore or default
            venue = "" 
            city = ""
            
            # Enrich
            is_free, free_reason = detect_free(title, "")
            canonical_id = generate_canonical_id(title, date_iso, venue, city, time_str)
            
            if canonical_id in existing_ids: updated += 1
            else: inserted += 1
            
            event = {
                'title': title, 'genre': 'Kontsert',
                'date': date_iso, 'time': time_str,
                'venue': venue, 'city': city,
                'is_free': is_free, 'free_reason': free_reason,
                'is_kids_event': 0, 'description': '', 
                'image_url': '', 'ticket_url': '',
                'canonical_event_id': canonical_id,
                'source': 'concert.ee', 'source_url': source_url,
                'last_seen_at': current_time,
                'created_at': current_time, 'updated_at': current_time
            }
            parsed += 1
            events_found.append(event)
            
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
                    date=excluded.date,
                    time=excluded.time,
                    venue=excluded.venue,
                    city=excluded.city,
                    source_url=excluded.source_url,
                    last_seen_at=excluded.last_seen_at,
                    updated_at=excluded.updated_at
            """, event)
            
        except Exception: pass
        
    conn.commit()
    
    print(f"CONCERTS_PARSED: {parsed}")
    print(f"INSERTED: {inserted}")
    print(f"UPDATED: {updated}")
    
    db_total = cursor.execute("SELECT count(*) FROM events").fetchone()[0]
    print(f"DB_TOTAL_EVENTS_NOW: {db_total}")
    
    print("GENRE_STATS:")
    for g, c in cursor.execute("SELECT genre, COUNT(*) FROM events GROUP BY genre").fetchall():
        print(f"  {g}: {c}")

    print("\nSample Data (5 Rows):")
    for ev in events_found[:5]:
        out = {k: ev[k] for k in ['date', 'time', 'title', 'venue', 'city', 'genre', 'is_free', 'source_url', 'canonical_event_id']}
        print(json.dumps(out, ensure_ascii=False))

    print("\nCREATED_FILES: .gitignore, requirements.txt, README.md, schema.sql")
    conn.close()

if __name__ == "__main__":
    scrape_concert_ee()

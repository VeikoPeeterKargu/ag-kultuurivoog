import os
import sys
import psycopg2
import requests
from bs4 import BeautifulSoup
import hashlib
import datetime
import json
import re

CONCERT_EE_URL = "https://concert.ee/"

MONTHS = {
    "jaanuar": "01", "veebruar": "02", "märts": "03", "aprill": "04", "mai": "05", "juuni": "06",
    "juuli": "07", "august": "08", "september": "09", "oktoober": "10", "november": "11", "detsember": "12"
}

def parse_estonian_full_date(date_str):
    if not date_str: return None
    s = date_str.lower().strip()
    match = re.search(r'(\d{1,2})\.\s+([a-z]+)\s+(\d{4})', s)
    if not match: return None
    
    day, month_name, year = match.groups()
    if len(day) == 1: day = '0' + day
    month = MONTHS.get(month_name)
    if not month: return None
    
    return f"{year}-{month}-{day}"

def normalize_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip().lower()

def generate_canonical_id(title, date_str, venue, city, time_str):
    clean_time = time_str.replace(":", "") if time_str else ""
    norm_title = normalize_text(title)
    norm_venue = normalize_text(venue)
    norm_city = normalize_text(city)
    
    canonical_raw = f"{norm_title}|{date_str}|{norm_venue}|{norm_city}|{clean_time}"
    return hashlib.sha1(canonical_raw.encode('utf-8')).hexdigest()

def detect_free(title, description):
    t = (title or "").lower()
    d = (description or "").lower()
    full_text = f"{t} {d}"
    
    keywords = ["tasuta", "vaba sissepääs", "vabalt valitud annetusega", "annetuspõhine", "soovituslik annetus", "piletita"]
    
    for k in keywords:
        if k in full_text:
            return 1, k
            
    return 0, None

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

def run_scraper():
    conn = get_db_connection()
    if not conn:
        return {"parsed": 0, "inserted": 0, "updated": 0, "error": "No DB connection"}
    
    cur = conn.cursor()
    
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(CONCERT_EE_URL, headers=HEADERS, timeout=20)
        # response.raise_for_status() 
    except Exception as e:
        print(f"Error fetching {CONCERT_EE_URL}: {e}")
        conn.close()
        return {"parsed": 0, "inserted": 0, "updated": 0, "error": str(e)}

    soup = BeautifulSoup(response.text, 'html.parser')
    
    event_blocks = soup.select('.event')
    if not event_blocks:
        cols = soup.select('.col')
        event_blocks = []
        for c in cols:
            if c.select_one('.date') and c.select_one('h3 a'):
                event_blocks.append(c)

    parsed = 0
    inserted = 0
    updated = 0
    current_time = datetime.datetime.now().isoformat()

    for block in event_blocks:
        if parsed >= 40: break
        try:
            title_el = block.select_one('h3 a')
            if not title_el: title_el = block.select_one('.title a')
            if not title_el: continue
            
            title = title_el.get_text(strip=True)
            source_url = title_el['href']
            if source_url and not source_url.startswith('http'):
                source_url = "https://concert.ee" + source_url

            date_el = block.select_one('.date')
            date_text = date_el.get_text(strip=True) if date_el else ""
            date_iso = parse_estonian_full_date(date_text)
            if not date_iso: continue 

            time_str = None
            venue = "" 
            city = ""
            
            is_free, free_reason = detect_free(title, "")
            canonical_id = generate_canonical_id(title, date_iso, venue, city, time_str)
            
            cur.execute("""
                INSERT INTO events (
                    title, genre, date, time, venue, city, 
                    is_free, free_reason, is_kids_event, description, image_url, ticket_url, 
                    canonical_event_id, source, source_url, 
                    last_seen_at, created_at, updated_at
                ) VALUES (
                    %(title)s, %(genre)s, %(date)s, %(time)s, %(venue)s, %(city)s,
                    %(is_free)s, %(free_reason)s, %(is_kids_event)s, %(description)s, %(image_url)s, %(ticket_url)s,
                    %(canonical_event_id)s, %(source)s, %(source_url)s,
                    %(last_seen_at)s, %(created_at)s, %(updated_at)s
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
                RETURNING (xmax = 0) AS inserted;
            """, {
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
            })
            
            is_inserted = cur.fetchone()[0]
            if is_inserted: inserted += 1
            else: updated += 1
            
            parsed += 1
            
        except Exception: pass
        
    conn.commit()
    print(f"CONCERTS_PARSED: {parsed}")
    print(f"INSERTED: {inserted}")
    print(f"UPDATED: {updated}")
    
    cur.close()
    conn.close()
    return {"parsed": parsed, "inserted": inserted, "updated": updated}

if __name__ == "__main__":
    run_scraper()

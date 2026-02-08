import os
import sys
import psycopg2
import requests
from bs4 import BeautifulSoup
import hashlib
import datetime
import json
import re
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Default URL, can be overridden by env
TEATER_EE_URL_DEFAULT = "https://teater.ee/teatriinfo/mangukava/"

MONTHS = {
    "jaanuar": "01", "veebruar": "02", "märts": "03", "aprill": "04", "mai": "05", "juuni": "06",
    "juuli": "07", "august": "08", "september": "09", "oktoober": "10", "november": "11", "detsember": "12"
}

def parse_estonian_full_date(date_str):
    if not date_str: return None
    parts = date_str.lower().split()
    if len(parts) < 3: return None
    
    day = parts[0].strip('.')
    month_name = parts[1]
    year = parts[2]
    
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

def detect_genre(title, description, venue):
    t = (title or "").lower()
    d = (description or "").lower()
    v = (venue or "").lower()
    full_text = f"{t} {d}"

    if "ooper" in full_text: return "Ooper"
    if "ballett" in full_text or "tantsuteater" in full_text or "tantsulavastus" in full_text or "koreograaf" in full_text: return "Ballett"
    if "operett" in full_text: return "Operett"
    
    concert_keywords = ["kontsert", "jazz", "orkester", "klaveriõhtu", "kammerkontsert", "koor", "ansambel"]
    if any(k in full_text for k in concert_keywords): return "Kontsert"
    
    venue_hints = ["kontserdimaja", "philly joe", "jazz", "ait"]
    if any(h in v for h in venue_hints): return "Kontsert"

    return "Teater"

def detect_free(title, description):
    t = (title or "").lower()
    d = (description or "").lower()
    full_text = f"{t} {d}"
    
    keywords = ["tasuta", "vaba sissepääs", "vabalt valitud annetusega", "annetuspõhine", "soovituslik annetus", "piletita"]
    
    for k in keywords:
        if k in full_text:
            return 1, k
            
    return 0, None

def is_kids_event_check(title, venue, description):
    t = (title or "").lower()
    v = (venue or "").lower()
    d = (description or "").lower()
    full_text = f"{t} {v} {d}"
    
    keywords = [
        "nukuteater", "noorsooteater", "lastele", " kogupere", "mudilastele", 
        "lastelavastus", "piparkoogi", "päkapiku", "jõuluvana", "lohe", 
        "muinasjutt", "tsirkus", "kloun", "buratino", "sipsik", "lotte", 
        "pipi", "karlsson", "bullerby"
    ]
    
    for k in keywords:
        if k in full_text:
            return 1
            
    return 0

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
    # Setup stats
    stats = {
        "parsed": 0, "inserted": 0, "updated": 0, 
        "error": None, "blocked": False, "status": 0
    }
    
    conn = get_db_connection()
    if not conn:
        stats["error"] = "No DB connection"
        return stats
    
    target_url = os.getenv("TEATER_URL", TEATER_EE_URL_DEFAULT)
    
    # Session setup with robust headers
    session = requests.Session()
    
    # Retry strategy (Backoff)
    retries = Retry(
        total=3,
        backoff_factor=2, # 2s, 4s, 8s
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "et-EE,et;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate", 
        "Sec-Fetch-Dest": "document",
        "Referer": "https://teater.ee/",
        "DNT": "1"
    }
    
    try:
        # Warm-up: Visit homepage first
        print("Scraper: Warming up (GET /)...")
        session.get("https://teater.ee", headers=HEADERS, timeout=10)
        time.sleep(1) # Be polite
        
        # Real request
        print(f"Scraper: Fetching {target_url}...")
        response = session.get(target_url, headers=HEADERS, timeout=20)
        
        status_code = response.status_code
        stats["status"] = status_code
        print(f"TEATER_HTTP_STATUS: {status_code}")
        
        # Check if actually blocked or error
        if status_code in [403, 429]:
            print(f"TEATER_BLOCKED: true (Status {status_code})")
            stats["blocked"] = True
            conn.close()
            return stats
            
        response.raise_for_status()
        
    except Exception as e:
        print(f"Error fetching: {e}")
        stats["error"] = str(e)
        if hasattr(e, 'response') and e.response:
             stats["status"] = e.response.status_code
             if e.response.status_code in [403, 429]:
                 stats["blocked"] = True
        conn.close()
        return stats

    # Parse logic
    soup = BeautifulSoup(response.text, 'html.parser')
    date_blocks = soup.select('.post-etendus__item')
    
    current_time = datetime.datetime.now().isoformat()
    cur = conn.cursor()
    
    sample_events = []
    inserted_count = 0
    updated_count = 0
    total_parsed = 0

    for block in date_blocks:
        if total_parsed >= 50: break
        
        d_head = block.select_one('.post-etendus__heading')
        if not d_head: continue
        date_iso = parse_estonian_full_date(d_head.get_text(strip=True))
        if not date_iso: continue
            
        for ev_div in block.select('.block-etendus'):
            try:
                title_el = ev_div.select_one('.block-etendus__paragraph-big')
                if not title_el: title_el = ev_div.select_one('.block-etendus__paragraph-big')
                title = title_el.get_text(strip=True) if title_el else "Unknown"

                link_el = ev_div.select_one('a[href*="/lavastused/"]')
                source_url = link_el['href'] if link_el else ""
                if source_url and not source_url.lower().startswith('http'):
                    source_url = "https://teater.ee" + source_url
                
                time_el = ev_div.select_one('.block-etendus__time')
                time_str = time_el.get_text(strip=True) if time_el else None
                
                venue = ""
                city = ""
                ps = ev_div.select('.block-etendus__paragraph-small')
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
                
                is_kids = is_kids_event_check(title, venue, "")
                genre = detect_genre(title, "", venue)
                is_free, free_reason = detect_free(title, "")
                
                canonical_id = generate_canonical_id(title, date_iso, venue, city, time_str)
                
                # UPSERT with RETURNING
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
                    RETURNING (xmax = 0) AS inserted;
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
                
                is_inserted = cur.fetchone()[0]
                if is_inserted: inserted_count += 1
                else: updated_count += 1
                
                total_parsed += 1
                
                if len(sample_events) < 5:
                    sample_events.append({
                        "date": date_iso, "time": time_str, "title": title, "venue": venue, "city": city,
                        "genre": genre, "is_free": is_free, "is_kids_event": is_kids,
                        "canonical_event_id": canonical_id, "source_url": source_url
                    })
                
            except Exception as e:
                # print(f"Parse error: {e}")
                pass
            
    conn.commit()
    print(f"TOTAL_PARSED: {total_parsed}")
    print(f"INSERTED: {inserted_count}")
    print(f"UPDATED: {updated_count}")
    
    # Update stats
    stats["parsed"] = total_parsed
    stats["inserted"] = inserted_count
    stats["updated"] = updated_count
        
    cur.close()
    conn.close()
    return stats

if __name__ == "__main__":
    run_scraper()

from fastapi import FastAPI, Query, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from contextlib import asynccontextmanager
import sqlite3
import datetime
import logging
from typing import Optional, List
from pydantic import BaseModel
import sys

# Import custom modules
import scrape_teater_ee
import scrape_concert_ee
import cleanup_non_events

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

DB_FILE = "ag_kultuurivoog.db"

def refresh_data():
    logger.info("--- STARTED: Scheduled Data Refresh ---")
    try:
        # 1. Scrape Teater.ee
        t_stats = scrape_teater_ee.run_scraper()
        logger.info(f"Teater.ee: {t_stats}")
        
        # 2. Scrape Concert.ee
        c_stats = scrape_concert_ee.run_scraper()
        logger.info(f"Concert.ee: {c_stats}")
        
        # 3. Cleanup & Views
        cl_stats = cleanup_non_events.run_cleanup()
        logger.info(f"Cleanup: {cl_stats}")
        
        v_stats = cleanup_non_events.ensure_views()
        logger.info(f"Views: {v_stats}")
        
    except Exception as e:
        logger.error(f"Refresh failed: {e}")
    logger.info("--- FINISHED: Data Refresh ---")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Server Startup: Initializing Scheduler")
    scheduler = BackgroundScheduler()
    
    # Run immediately on startup (in background to not block)
    scheduler.add_job(refresh_data, 'date', run_date=datetime.datetime.now() + datetime.timedelta(seconds=5))
    
    # Schedule every 60 minutes
    scheduler.add_job(refresh_data, IntervalTrigger(minutes=60))
    
    scheduler.start()
    yield
    # Shutdown
    logger.info("Server Shutdown: Stopping Scheduler")
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

class Event(BaseModel):
    id: int
    date: str
    time: Optional[str]
    title: str
    genre: Optional[str]
    venue: Optional[str]
    city: Optional[str]
    is_free: int
    is_kids_event: int
    source: str
    source_url: Optional[str]
    ticket_url: Optional[str]
    canonical_event_id: str

def get_db_connection():
    conn = sqlite3.connect(f"file:{DB_FILE}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn

def query_events(start_date: str, end_date: str, show_kids: bool):
    try:
        conn = get_db_connection()
        view = "v_events_clean" if show_kids else "v_events_clean_adults"
        
        query = f"""
            SELECT * FROM {view}
            WHERE date BETWEEN ? AND ?
            ORDER BY date ASC, time ASC
        """
        
        cursor = conn.execute(query, (start_date, end_date))
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return []

@app.get("/events/today")
def get_today(show_kids: bool = False):
    today = datetime.date.today().isoformat()
    return query_events(today, today, show_kids)

@app.get("/events/7days")
def get_7days(show_kids: bool = False):
    today = datetime.date.today()
    end = today + datetime.timedelta(days=7)
    return query_events(today.isoformat(), end.isoformat(), show_kids)

@app.get("/events/14days")
def get_14days(show_kids: bool = False):
    today = datetime.date.today()
    end = today + datetime.timedelta(days=14)
    return query_events(today.isoformat(), end.isoformat(), show_kids)

@app.get("/events/30days")
def get_30days(show_kids: bool = False):
    today = datetime.date.today()
    end = today + datetime.timedelta(days=30)
    return query_events(today.isoformat(), end.isoformat(), show_kids)

@app.get("/events/search")
def search_events(start: str, end: str, show_kids: bool = False):
    return query_events(start, end, show_kids)

@app.get("/events/{event_id}/ics")
def get_event_ics(event_id: int):
    conn = get_db_connection()
    event = conn.execute("SELECT * FROM v_events_clean WHERE id = ?", (event_id,)).fetchone()
    conn.close()
    
    if not event:
        return FileResponse("static/404.html") # Or just 404 text

    # Manual ICS Generation (Variant A)
    # 1. Format Dates
    date_str = event['date'].replace("-", "") 
    time_str = event['time'].replace(":", "") if event['time'] else "190000" # Default 19:00
    if len(time_str) == 4: time_str += "00" # HHMM -> HHMMSS
    
    dtstart = f"{date_str}T{time_str}"
    dtstamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    
    # 2. Prepare content
    summary = event['title']
    location = f"{event['venue']}, {event['city']}" if event['city'] else event['venue']
    if not location: location = ""
    
    description = event['description'] or ""
    if event['ticket_url']: description += f"\\n\\nPiletid: {event['ticket_url']}"
    description += f"\\n\\nAllikas: {event['source_url']}"
    
    uid = f"{event['canonical_event_id']}@ag-kultuurivoog"
    
    # 3. Assemble ICS
    ics_content = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//AG Kultuurivoog//ET",
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{dtstamp}",
        f"DTSTART;TZID=Europe/Tallinn:{dtstart}",
        f"SUMMARY:{summary}",
        f"LOCATION:{location}",
        f"DESCRIPTION:{description}",
        f"URL:{event['source_url'] or ''}",
        "END:VEVENT",
        "END:VCALENDAR"
    ]
    
    return Response(content="\\r\\n".join(ics_content), media_type="text/calendar", headers={"Content-Disposition": f"attachment; filename=event_{event_id}.ics"})

@app.get("/")
def root():
    return FileResponse("static/index.html")

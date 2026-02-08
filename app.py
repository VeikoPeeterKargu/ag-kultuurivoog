from fastapi import FastAPI, Query, Response, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from contextlib import asynccontextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime
import logging
import sys
import os

# Import custom modules
import db_init
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

def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL_MISSING: true")
        raise Exception("DATABASE_URL environment variable is not set")
    return psycopg2.connect(db_url, cursor_factory=RealDictCursor)

def refresh_data():
    logger.info("--- STARTED: Scheduled Data Refresh ---")
    try:
        # 1. Ensure DB/Views
        db_init.init_db()
        
        # 2. Scrape Teater.ee
        t_stats = scrape_teater_ee.run_scraper()
        logger.info(f"Teater.ee: {t_stats}")
        
        # 3. Scrape Concert.ee
        c_stats = scrape_concert_ee.run_scraper()
        logger.info(f"Concert.ee: {c_stats}")
        
        # 4. Cleanup
        cl_stats = cleanup_non_events.run_cleanup()
        logger.info(f"Cleanup: {cl_stats}")
        
        # Log final view counts
        v_stats = cleanup_non_events.ensure_views()
        logger.info(f"Views: {v_stats}")
        
    except Exception as e:
        logger.error(f"Refresh failed: {e}")
    logger.info("--- FINISHED: Data Refresh ---")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    scheduler_enabled = os.getenv("SCHEDULER_ENABLED", "1") == "1"
    
    if scheduler_enabled:
        logger.info("SCHEDULER_STARTED: true")
        logger.info("STARTUP_REFRESH_TRIGGERED: true")
        scheduler = BackgroundScheduler()
        
        # Run slightly delayed to allow server start
        scheduler.add_job(refresh_data, 'date', run_date=datetime.datetime.now() + datetime.timedelta(seconds=5))
        scheduler.add_job(refresh_data, IntervalTrigger(minutes=60))
        
        scheduler.start()
    else:
        logger.info("SCHEDULER_STARTED: false")
        
    yield
    
    if scheduler_enabled:
        scheduler.shutdown()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

def query_events(start_date: str, end_date: str, show_kids: bool):
    try:
        conn = get_db_connection()
        if not conn: return []
        
        view = "v_events_clean" if show_kids else "v_events_clean_adults"
        
        with conn.cursor() as cur:
            query = f"""
                SELECT * FROM {view}
                WHERE date BETWEEN %s AND %s
                ORDER BY date ASC, time ASC
            """
            cur.execute(query, (start_date, end_date))
            rows = cur.fetchall()
            result = [dict(row) for row in rows]
            
        conn.close()
        return result
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

def ics_escape(text):
    if not text: return ""
    return text.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\n", "\\n")

@app.get("/events/{event_id}/ics")
def get_event_ics(event_id: int):
    try:
        conn = get_db_connection()
    except Exception:
        return Response("Database connection failed", status_code=500)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM v_events_clean WHERE id = %s", (event_id,))
            event = cur.fetchone()
    finally:
        conn.close()
    
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    # Manual ICS Generation (Variant A) with Escaping
    d_obj = event['date']
    date_str = d_obj.strftime("%Y%m%d")
    
    t_obj = event['time']
    if t_obj:
        time_str = t_obj.strftime("%H%M%S")
    else:
        time_str = "190000"
    
    dtstart = f"{date_str}T{time_str}"
    dtstamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    
    summary = ics_escape(event['title'])
    
    v_parts = []
    if event['venue']: v_parts.append(event['venue'])
    if event['city']: v_parts.append(event['city'])
    location = ics_escape(", ".join(v_parts))
    
    desc_lines = []
    if event['description']: desc_lines.append(event['description'])
    if event['ticket_url']: desc_lines.append(f"Piletid: {event['ticket_url']}")
    desc_lines.append(f"Allikas: {event['source_url']}")
    description = ics_escape("\n\n".join(desc_lines))
    
    uid = f"{event['canonical_event_id']}@ag-kultuurivoog"
    
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
    
    print("ICS_ENDPOINT_OK: true")

    return Response(content="\r\n".join(ics_content), media_type="text/calendar", headers={"Content-Disposition": f"attachment; filename=event_{event_id}.ics"})

@app.get("/")
def root():
    return FileResponse("static/index.html")

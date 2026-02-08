from fastapi import FastAPI, Query, Response, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from contextlib import asynccontextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime
import logging
import sys
import os
import json

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

# Global State for Health Check
APP_STATE = {
    "db_ok": False,
    "last_refresh_started_at": None,
    "last_refresh_finished_at": None,
    "events_total": 0,
    "events_clean": 0,
    "events_adults": 0,
    "last_teater_status": 0,
    "last_teater_blocked": False
}

def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL_MISSING: true")
        raise Exception("DATABASE_URL environment variable is not set")
    return psycopg2.connect(db_url, cursor_factory=RealDictCursor)

def update_health_stats(conn=None):
    try:
        close_conn = False
        if not conn:
            conn = get_db_connection()
            close_conn = True
            
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM events")
            APP_STATE["events_total"] = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM v_events_clean")
            APP_STATE["events_clean"] = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM v_events_clean_adults")
            APP_STATE["events_adults"] = cur.fetchone()[0]
            
        APP_STATE["db_ok"] = True
        
        if close_conn:
            conn.close()
    except Exception as e:
        logger.error(f"Health stats update failed: {e}")
        APP_STATE["db_ok"] = False

def refresh_data():
    APP_STATE["last_refresh_started_at"] = datetime.datetime.now().isoformat()
    logger.info("--- STARTED: Scheduled Data Refresh ---")
    
    parsed_total = 0
    
    try:
        # 1. Ensure DB/Views
        try:
            db_init.init_db()
            APP_STATE["db_ok"] = True
        except Exception:
            APP_STATE["db_ok"] = False
        
        # 2. Scrape Teater.ee
        t_stats = scrape_teater_ee.run_scraper()
        logger.info(f"Teater.ee: {t_stats}")
        
        APP_STATE["last_teater_status"] = t_stats.get("status", 0)
        APP_STATE["last_teater_blocked"] = t_stats.get("blocked", False)
        
        if t_stats.get("blocked"):
            logger.info("TEATER_BLOCKED_FALLBACK: true")
        
        parsed_total += t_stats.get("parsed", 0)
        
        # 3. Scrape Concert.ee
        c_stats = scrape_concert_ee.run_scraper()
        logger.info(f"Concert.ee: {c_stats}")
        parsed_total += c_stats.get("parsed", 0)
        
        # 4. Cleanup (Safe Mode)
        # Only cleanup if we actually successfully parsed data OR if it's not a block scenario
        # If both scrapers failed/blocked (parsed=0), we might want to skip cleanup to avoid wiping out logic
        cl_stats = cleanup_non_events.run_cleanup(check_safety=True, parsed_count=parsed_total)
        logger.info(f"Cleanup: {cl_stats}")
        
        # Update view stats
        update_health_stats()
        
    except Exception as e:
        logger.error(f"Refresh failed: {e}")
    
    APP_STATE["last_refresh_finished_at"] = datetime.datetime.now().isoformat()
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

@app.get("/health")
def health_check():
    # Update DB stats on demand if needed, or rely on scheduler
    # Let's verify DB connection real-time for accuracy
    try:
        conn = get_db_connection()
        conn.close()
        APP_STATE["db_ok"] = True
    except:
        APP_STATE["db_ok"] = False
        
    return JSONResponse(content=APP_STATE)

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

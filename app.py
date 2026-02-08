from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import sqlite3
import datetime
from typing import Optional, List
from pydantic import BaseModel

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
DB_FILE = "ag_kultuurivoog.db"

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

@app.get("/")
def root():
    return FileResponse("static/index.html")

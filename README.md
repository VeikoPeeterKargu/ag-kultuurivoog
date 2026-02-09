# AG Kultuurivoog

Eesti kultuurisündmuste agregaator (MVP).
Hetkel toetab: `teater.ee`, `concert.ee`.

## Setup

1. `python3 -m venv .venv`
2. `source .venv/bin/activate`
3. `pip install -r requirements.txt`

## Scrapers

- `python3 scrape_teater_ee.py` - Scrapes Teater.ee events
- `python3 scrape_concert_ee.py` - Scrapes Concert.ee events

## Database

Uses SQLite `ag_kultuurivoog.db`.
Schema defined in `schema.sql`.

## Project Status: On Pause
See `docs/PAUSE_POINT.md` for resume instructions.

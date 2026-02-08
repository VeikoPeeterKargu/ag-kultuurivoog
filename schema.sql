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
    free_reason TEXT,
    description TEXT,
    image_url TEXT,
    ticket_url TEXT,
    canonical_event_id TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    source_url TEXT,
    last_seen_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_date ON events(date);
CREATE INDEX IF NOT EXISTS idx_events_genre ON events(genre);
CREATE INDEX IF NOT EXISTS idx_events_is_kids ON events(is_kids_event);

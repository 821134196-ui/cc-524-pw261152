"""SQLite storage layer. WAL mode allows concurrent readers with a single writer,
which is what the parallel batch runner needs."""
import os
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS datasets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    path TEXT NOT NULL,
    n_records INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(name, version)
);

CREATE TABLE IF NOT EXISTS feature_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id INTEGER NOT NULL REFERENCES datasets(id),
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    feature_config_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    path TEXT NOT NULL,
    n_records INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(name, version)
);

CREATE TABLE IF NOT EXISTS models (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    artifact_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',   -- active | rolled_back
    created_at TEXT NOT NULL,
    UNIQUE(name, version)
);

CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_key TEXT NOT NULL UNIQUE,          -- idempotency key: duplicate submits hit this
    dataset_id INTEGER NOT NULL REFERENCES datasets(id),
    snapshot_id INTEGER NOT NULL REFERENCES feature_snapshots(id),
    model_id INTEGER NOT NULL REFERENCES models(id),
    params_json TEXT NOT NULL,
    params_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending|running|success|partial_failed|failed
    total_items INTEGER NOT NULL DEFAULT 0,
    succeeded_items INTEGER NOT NULL DEFAULT 0,
    failed_items INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    rerun_of INTEGER REFERENCES batches(id),
    run_index INTEGER NOT NULL DEFAULT 1,
    note TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_rerun_of ON batches(rerun_of);

CREATE TABLE IF NOT EXISTS batch_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES batches(id),
    item_key TEXT NOT NULL,
    features_json TEXT,
    status TEXT NOT NULL,                    -- success | failed
    score REAL,
    label INTEGER,
    error TEXT,
    UNIQUE(batch_id, item_key)
);
CREATE INDEX IF NOT EXISTS idx_items_batch ON batch_items(batch_id);

CREATE TABLE IF NOT EXISTS results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES batches(id),
    version INTEGER NOT NULL,
    path TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    n_rows INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(batch_id, version)
);

CREATE TABLE IF NOT EXISTS publications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES batches(id),
    result_id INTEGER NOT NULL REFERENCES results(id),
    status TEXT NOT NULL DEFAULT 'published',  -- published | withdrawn
    published_at TEXT NOT NULL,
    withdrawn_at TEXT
);
-- At most ONE active publication per batch: duplicate publishes are impossible.
CREATE UNIQUE INDEX IF NOT EXISTS one_active_publication
    ON publications(batch_id) WHERE status = 'published';
"""

TERMINAL_STATES = ("success", "partial_failed", "failed")


def connect(db_path):
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(db_path):
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()

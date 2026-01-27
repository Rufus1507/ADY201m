import sqlite3
from datetime import datetime
import os

# ================= PATH CONFIG =================
RAW_DB_PATH = "data/raw/data_traffic_QN.db"
CLEAN_DIR = "data/clean"
CLEAN_DB_PATH = os.path.join(CLEAN_DIR, "data_traffic_clean.db")

os.makedirs(CLEAN_DIR, exist_ok=True)

# ================= SQLITE RAW SOURCE =================
raw_conn = sqlite3.connect(RAW_DB_PATH)
raw_cur = raw_conn.cursor()

raw_cur.execute("""
    SELECT
        id,
        timestamp,
        location,
        current_speed_kmh,
        free_flow_speed_kmh,
        confidence
    FROM traffic_data
""")

rows = raw_cur.fetchall()

# ================= SQLITE CLEAN TARGET =================
clean_conn = sqlite3.connect(CLEAN_DB_PATH)
clean_cur = clean_conn.cursor()

# ----- CREATE CLEAN TABLE (IF NOT EXISTS) -----
clean_cur.execute("""
    CREATE TABLE IF NOT EXISTS traffic_data_clean (
        id INTEGER,
        timestamp TEXT,
        location TEXT,
        current_speed_kmh REAL,
        free_flow_speed_kmh REAL,
        speed_ratio REAL,
        traffic_level TEXT,
        confidence REAL,
        PRIMARY KEY (id, timestamp)
    )
""")

# ================= CLEAN & INSERT =================
for row in rows:
    (
        rid,
        ts,
        location,
        current_speed,
        free_flow_speed,
        confidence
    ) = row

    # ----- VALIDATION -----
    if current_speed is None or free_flow_speed in (None, 0):
        continue

    # ----- DERIVED METRICS -----
    speed_ratio = round(current_speed / free_flow_speed, 2)

    if speed_ratio < 0.3:
        traffic_level = "SEVERE"
    elif speed_ratio < 0.5:
        traffic_level = "HEAVY"
    elif speed_ratio < 0.7:
        traffic_level = "MODERATE"
    else:
        traffic_level = "FREE"

    # ----- NORMALIZE TIMESTAMP -----
    ts = datetime.fromisoformat(ts).isoformat()

    # ----- IDEMPOTENT INSERT (SQLITE) -----
    clean_cur.execute("""
        INSERT OR IGNORE INTO traffic_data_clean (
            id,
            timestamp,
            location,
            current_speed_kmh,
            free_flow_speed_kmh,
            speed_ratio,
            traffic_level,
            confidence
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        rid,
        ts,
        location,
        current_speed,
        free_flow_speed,
        speed_ratio,
        traffic_level,
        confidence
    ))

clean_conn.commit()

# ================= CLOSE CONNECTIONS =================
raw_conn.close()
clean_conn.close()

print("Cleaned data stored into SQLite (clean zone) successfully")
print(f"Clean DB location: {CLEAN_DB_PATH}")
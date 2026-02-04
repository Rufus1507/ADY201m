import requests
import datetime
import time
import os
import sqlite3

# ================= TOMTOM CONFIG =================
API_KEY = "fR6oIACAyE0vwksnpXC7QfeQsA7FfPWt"

TOMTOM_FLOW_URL = (
    "https://api.tomtom.com/traffic/services/4/"
    "flowSegmentData/absolute/10/json"
)

# ================= FIND ADY201m ROOT =================
def find_project_root(marker="ADY201m"):
    path = os.path.abspath(__file__)
    while True:
        if os.path.basename(path) == marker:
            return path
        parent = os.path.dirname(path)
        if parent == path:
            raise RuntimeError(f"❌ Không tìm thấy thư mục {marker}")
        path = parent


PROJECT_ROOT = find_project_root("ADY201m")
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

DB_FILE = os.path.join(DATA_DIR, "data_traffic_QN.db")

# ================= DATABASE =================
def init_database():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            active INTEGER DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            location INTEGER NOT NULL,
            current_speed_kmh REAL,
            free_flow_speed_kmh REAL,
            speed_ratio REAL,
            traffic_level TEXT,
            confidence REAL,
            FOREIGN KEY (location) REFERENCES locations(id)
        )
    """)

    conn.commit()
    conn.close()
    print(f"📁 Database ready: {DB_FILE}")
# ================= LOAD LOCATIONS =================
def load_locations():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, lat, lon
        FROM locations
        WHERE active = 1
    """)

    rows = cur.fetchall()
    conn.close()

    return [
        {"id": r[0], "name": r[1], "lat": r[2], "lon": r[3]}
        for r in rows
    ]

# ================= TIME CONTROL =================
def is_active_hours():
    return 6 <= datetime.datetime.now().hour < 22


def get_seconds_until_6am():
    now = datetime.datetime.now()
    target = now.replace(hour=6, minute=0, second=0, microsecond=0)
    if now.hour >= 6:
        target += datetime.timedelta(days=1)
    return int((target - now).total_seconds())

# ================= TOMTOM =================
def get_traffic(lat, lon):
    r = requests.get(
        TOMTOM_FLOW_URL,
        params={"point": f"{lat},{lon}", "key": API_KEY},
        timeout=10
    )
    r.raise_for_status()
    return r.json()["flowSegmentData"]

# ================= COLLECT =================
def collect():
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for loc in load_locations():
        try:
            flow = get_traffic(loc["lat"], loc["lon"])
            cur = flow["currentSpeed"]
            free = flow["freeFlowSpeed"]
            conf = flow.get("confidence", 0)

            ratio = cur / free if free else 0
            level = "THOANG" if ratio > 0.8 else "DONG" if ratio > 0.5 else "KET_XE"

            rows.append((
                now,
                loc["id"],
                cur,
                free,
                round(ratio, 2),
                level,
                conf
            ))

        except Exception as e:
            print(f"❌ {loc['name']}: {e}")

    return rows

# ================= SAVE =================
def save(data):
    if not data:
        return

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.executemany("""
        INSERT INTO traffic_data (
            timestamp,
            location,
            current_speed_kmh,
            free_flow_speed_kmh,
            speed_ratio,
            traffic_level,
            confidence
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, data)

    conn.commit()
    conn.close()
    print(f"➕ Saved {len(data)} rows")

# ================= MAIN =================
if __name__ == "__main__":
    print("🚦 Traffic crawler started")
    init_database()


    current_date = None

    while True:
        try:
            now = datetime.datetime.now()

            if current_date != now.date():
                print(f"\n📅 {now.strftime('%d/%m/%Y')}")
                current_date = now.date()

            if not is_active_hours():
                sleep = get_seconds_until_6am()
                print("🌙 Outside active hours, sleeping...")
                time.sleep(sleep)
                continue

            print(f"\n⏰ {now.strftime('%H:%M:%S')} collecting...")
            data = collect()
            save(data)

            time.sleep(20 * 60)

        except KeyboardInterrupt:
            print("🛑 Stopped by user")
            break
        except Exception as e:
            print("❌ Error:", e)
            time.sleep(60)

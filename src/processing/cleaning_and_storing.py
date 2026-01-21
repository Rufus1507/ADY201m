import sqlite3
import pyodbc
from datetime import datetime

# ================= SQLITE (RAW SOURCE) =================
sqlite_conn = sqlite3.connect("data_traffic_QN.db")
sqlite_cur = sqlite_conn.cursor()

sqlite_cur.execute("""
    SELECT
        id,
        timestamp,
        location,
        current_speed_kmh,
        free_flow_speed_kmh,
        speed_ratio,
        traffic_level,
        confidence
    FROM traffic_raw
""")

rows = sqlite_cur.fetchall()

# ================= SQL SERVER (TARGET DB) =================
sqlserver_conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=TrafficDB;"
    "Trusted_Connection=yes;"
)
sql_cur = sqlserver_conn.cursor()

# ================= CLEAN & INSERT =================
for row in rows:
    (
        rid,
        ts,
        location,
        current_speed,
        free_flow_speed,
        speed_ratio,
        traffic_level,
        confidence
    ) = row

    # ----- BASIC VALIDATION -----
    if current_speed is None or free_flow_speed in (None, 0):
        continue

    # ----- RE-CALCULATE (CLEAN DATA) -----
    speed_ratio = round(current_speed / free_flow_speed, 2)

    if speed_ratio < 0.3:
        traffic_level = "SEVERE"
    elif speed_ratio < 0.5:
        traffic_level = "HEAVY"
    elif speed_ratio < 0.7:
        traffic_level = "MODERATE"
    else:
        traffic_level = "FREE"

    # ----- PARSE TIMESTAMP -----
    ts = datetime.fromisoformat(ts)

    # ----- INSERT SQL SERVER -----
    sql_cur.execute("""
        INSERT INTO traffic_data (
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

sqlserver_conn.commit()

# ================= CLOSE CONNECTIONS =================
sqlite_conn.close()
sqlserver_conn.close()

print("Cleaned data stored into SQL Server successfully")

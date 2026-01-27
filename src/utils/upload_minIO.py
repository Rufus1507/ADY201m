from minio import Minio
from datetime import datetime
import os

# ================= CONNECT MINIO (S3 API) =================
client = Minio(
    "localhost:9000",          # ✅ đúng port
    access_key="admin",
    secret_key="admin123",
    secure=False
)

# ================= CONFIG =================
BUCKET_NAME = "raw-traffic-data"
OBJECT_PREFIX = "traffic/sqlite"

# ================= CREATE BUCKET =================
if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)

# ================= FILE PATH (RAW DATA) =================
file_path = "data/raw/data_traffic_QN.db"

if not os.path.exists(file_path):
    raise FileNotFoundError(f"{file_path} not found")

# ================= OBJECT NAME (DATE PARTITION) =================
now = datetime.utcnow()

object_name = (
    f"{OBJECT_PREFIX}/"
    f"year={now.year}/"
    f"month={now.month:02d}/"
    f"day={now.day:02d}/"
    f"data_traffic_QN_{now.strftime('%H%M%S')}.db"
)

# ================= UPLOAD =================
client.fput_object(
    bucket_name=BUCKET_NAME,
    object_name=object_name,
    file_path=file_path,
    content_type="application/octet-stream"
)

print("✅ Raw SQLite database uploaded to MinIO")
print(f"📦 Bucket: {BUCKET_NAME}")
print(f"📁 Object: {object_name}")

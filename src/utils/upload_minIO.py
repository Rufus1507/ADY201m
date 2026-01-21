from minio import Minio
from datetime import datetime
import os

# ================= CONNECT MINIO =================
client = Minio(
    "localhost:9001",
    access_key="admin",
    secret_key="admin123",
    secure=False
)

# ================= CONFIG =================
BUCKET_NAME = "raw-traffic-data"
OBJECT_PREFIX = "traffic"

# ================= CREATE BUCKET =================
if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)

# ================= FILE PATH =================
file_path = "raw_traffic.json"

if not os.path.exists(file_path):
    raise FileNotFoundError("raw_traffic.json not found")

# ================= OBJECT NAME (partition by date) =================
object_name = (
    f"{OBJECT_PREFIX}/"
    f"year={datetime.utcnow().year}/"
    f"month={datetime.utcnow().month:02d}/"
    f"day={datetime.utcnow().day:02d}/"
    f"raw_traffic_{datetime.utcnow().strftime('%H%M%S')}.json"
)

# ================= UPLOAD =================
client.fput_object(
    bucket_name=BUCKET_NAME,
    object_name=object_name,
    file_path=file_path,
    content_type="application/json"
)

print(f"Uploaded {file_path} to MinIO as {object_name}")

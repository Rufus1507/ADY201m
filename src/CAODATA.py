import requests
import datetime
import pandas as pd
import time
import os

# ================= TOMTOM CONFIG =================
API_KEY = "fR6oIACAyE0vwksnpXC7QfeQsA7FfPWt"

TOMTOM_FLOW_URL = (
    "https://api.tomtom.com/traffic/services/4/"
    "flowSegmentData/absolute/10/json"
)

# ================= LOCATION POINTS (QUY NHƠN) =================
LOCATIONS = {
    "Nga_5_Dong_Da": (13.783255328622369, 109.21968988347302),
    "HOÀNG VĂN THỤ - TâY SƠN": (13.759429398523837, 109.20579782420032),
    "Vong_xoay_Nguyen_Tat_Thanh": (13.771844981726773, 109.222182156807),
    "VÒNG XOAY NGUYỄN THÁI HỌC": (13.775568025517046, 109.22246023281485),
    "Nga_3 THÁP ĐÔI": (13.785601361791992, 109.21037595228529)
}

DATA_DIR = "data_traffic_QN"
os.makedirs(DATA_DIR, exist_ok=True)

def get_data_file():
    return "data_traffic_QN.csv"



# ================= GET TRAFFIC DATA =================
def get_traffic(lat, lon):
    params = {
        "point": f"{lat},{lon}",
        "key": API_KEY
    }

    r = requests.get(TOMTOM_FLOW_URL, params=params, timeout=10)
    r.raise_for_status()
    print("DEBUG response:", r.json())

    return r.json()["flowSegmentData"]


# ================= COLLECT =================
def collect():
    now = datetime.datetime.now()
    rows = []

    for name, (lat, lon) in LOCATIONS.items():
        try:
            flow = get_traffic(lat, lon)

            if "currentSpeed" not in flow:
                print(f"⚠️ Không có dữ liệu speed cho {name}")
                continue

            current_speed = flow["currentSpeed"]
            free_speed = flow["freeFlowSpeed"]
            confidence = flow.get("confidence", 0)

            ratio = current_speed / free_speed if free_speed else 0

            if ratio > 0.8:
                level = "THOANG"
            elif ratio > 0.5:
                level = "DONG"
            else:
                level = "KET_XE"

            rows.append({
                "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                "location": name, 
                "current_speed_kmh": current_speed, # tốc độ hiện tại
                "free_flow_speed_kmh": free_speed, # tốc độ tự do
                "speed_ratio": round(ratio, 2), # tỷ lệ tốc độ hiện tại / tốc độ tự do
                "traffic_level": level, # mức độ giao thông
                "confidence": confidence # độ tin cậy của dữ liệu
            })

        except Exception as e:
            print(f"❌ Lỗi {name}: {e}")

    print(f"DEBUG: collected {len(rows)} rows")
    return rows


# ================= SAVE =================
def save(data):
    if not data:
        print("⚠️ Không có dữ liệu để lưu")
        return

    df = pd.DataFrame(data)
    file_path = get_data_file()

    if not os.path.exists(file_path):
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"📁 Tạo file mới: {file_path}")
    else:
        df.to_csv(file_path, mode="a", header=False, index=False, encoding="utf-8-sig")
        print(f"➕ Ghi thêm dữ liệu")



# ================= MAIN LOOP =================
print("🚦 Bắt đầu thu thập dữ liệu giao thông Quy Nhơn (TomTom)")
data = collect()

if not data:
    print("⚠️ Không có dữ liệu, tạo file rỗng nếu chưa tồn tại")
    save([])   # 🔥 ép tạo file
else:
    save(data)
    print(f"✅ Lưu {len(data)} dòng")

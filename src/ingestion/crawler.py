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

# ================= LOCATION POINTS (QUY NHƠN) =================
LOCATIONS = {
    "NGÃ 5 ĐỐNG ĐA": (13.783255328622369, 109.21968988347302),
    "HOÀNG VĂN THỤ - TâY SƠN": (13.759429398523837, 109.20579782420032),
    "VÒNG XOAY QUẢNG TRƯỜNG NTT": (13.771844981726773, 109.222182156807),
    "VÒNG XOAY NGUYỄN THÁI HỌC": (13.775568025517046, 109.22246023281485),
    "NGÃ 3 THÁP ĐÔI": (13.785601361791992, 109.21037595228529),
    "NGUYỄN THỊ ĐỊNH - TRẦN QUANG KHẢI":(13.752202170437391, 109.21080722510607),
    "NGUYỄN THÁI HỌC - LÊ DUẨN": (13.77463878500872, 109.22127839580803),
    "TĂNG BẠT HỔ - PHAN CHU TRINH": (13.772898931718016, 109.2372726265572),
    "PHỐ ẨM THỰC PHAN BỘI CHÂU":(13.77384491315607, 109.23493958486543),
    "HAI BÀ TRƯNG - LÊ THÁNH TÔNG":(13.771955140194756, 109.23491406725005),
    "NGUYỄN HUỆ":(13.765834411456693, 109.22515448438355),
    "CHU VĂN AN - NGUYỄN TẤT THÀNH":(13.770201352303607, 109.22234458030856),
    "HÀ HUY TẬP - ĐÔ ĐỐC BẢO":(13.768391068804707, 109.22316940484025),
    "ĐẠI HỌC QUY NHƠN":(13.75807095840719, 109.21880029916076),
    "VÕ THỊ YẾN - TRẦN VĂN ƠN":(13.756128607197843, 109.21502346499844),
    "NGUYỄN TRUNG TÍN - AN DƯƠNG VƯƠNG":(13.754119858006238, 109.21668564234275),
    "CHƯƠNG DƯƠNG - AN DƯƠNG VƯƠNG":(13.752156658686841, 109.21569818780243),
    "NGUYỄN THỊ ĐỊNH -TRẦN VĂN ƠN":(13.75681789454975, 109.21374240168403),
    "TRƯỜNG TIỂU HỌC NGÔ MÂY":(13.764730035480014, 109.2145872844767),
    "THPT NGUYỄN THÁI HỌC":(13.771292913356621, 109.21875741245276),
    "NGUYỄN TẤT THÀNH - LÊ HỒNG PHONG":(13.777801547413691, 109.22215583970028),
    "NGUYỄN THIỆP":(13.763030897981771, 109.22364563673754),
    "BỆNH VIỆN ĐA KHOA TỈNH BÌNH ĐỊNH":(13.767381082689928, 109.22720420210335),
    "NGỌC HÂN CÔNG CHÚA":(13.767085718423903, 109.22902972498773),
    "DIÊN HỒNG - TRƯỜNG CHINH":(13.771948895248373, 109.22046297981173)
}

# ================= DATABASE CONFIG =================
# Đường dẫn tuyệt đối đến thư mục chứa file code
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(SCRIPT_DIR, "data_traffic_QN.db")


def init_database():
    """Tạo file SQLite và bảng nếu chưa tồn tại"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS traffic_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            location TEXT NOT NULL,
            current_speed_kmh REAL,
            free_flow_speed_kmh REAL,
            speed_ratio REAL,
            traffic_level TEXT,
            confidence REAL
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"📁 Database đã sẵn sàng: {DB_FILE}")


def is_active_hours():
    """
    Kiểm tra xem có đang trong khung giờ thu thập dữ liệu không.
    Chỉ thu thập từ 6h sáng đến 22h tối.
    """
    current_hour = datetime.datetime.now().hour
    return 6 <= current_hour < 22


def get_seconds_until_6am():
    """
    Tính số giây từ bây giờ đến 6h sáng ngày hôm sau.
    """
    now = datetime.datetime.now()
    # Tính thời điểm 6h sáng ngày hôm sau
    tomorrow_6am = now.replace(hour=6, minute=0, second=0, microsecond=0)
    if now.hour >= 6:
        # Nếu đã qua 6h hôm nay, thì 6h sáng mai
        tomorrow_6am += datetime.timedelta(days=1)
    
    seconds_until = (tomorrow_6am - now).total_seconds()
    return int(seconds_until)


# ================= GET TRAFFIC DATA =================
def get_traffic(lat, lon):
    params = {
        "point": f"{lat},{lon}",
        "key": API_KEY
    }

    r = requests.get(TOMTOM_FLOW_URL, params=params, timeout=10)
    r.raise_for_status()
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
                "current_speed_kmh": current_speed,
                "free_flow_speed_kmh": free_speed,
                "speed_ratio": round(ratio, 2),
                "traffic_level": level,
                "confidence": confidence
            })

        except Exception as e:
            print(f"❌ Lỗi {name}: {e}")

    print(f"DEBUG: collected {len(rows)} rows")
    return rows


# ================= SAVE TO SQLITE =================
def save(data):
    if not data:
        print("⚠️ Không có dữ liệu để lưu")
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    for row in data:
        cursor.execute('''
            INSERT INTO traffic_data 
            (timestamp, location, current_speed_kmh, free_flow_speed_kmh, 
             speed_ratio, traffic_level, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            row["timestamp"],
            row["location"],
            row["current_speed_kmh"],
            row["free_flow_speed_kmh"],
            row["speed_ratio"],
            row["traffic_level"],
            row["confidence"]
        ))
    
    conn.commit()
    conn.close()
    print(f"➕ Đã lưu {len(data)} dòng vào SQLite")


# ================= MAIN LOOP =================
if __name__ == "__main__":
    print("🚦 Bắt đầu thu thập dữ liệu giao thông Quy Nhơn (TomTom)")
    print("📌 Lịch thu thập:")
    print("   • 6h - 22h: mỗi 20 phút")
    print("   • 22h - 6h: NGHỈ (chờ đến 6h sáng hôm sau)")
    print("-" * 50)
    
    # Khởi tạo database
    init_database()
    
    # Biến lưu ngày hiện tại để chỉ hiện ngày khi sang ngày mới
    current_date = None
    
    while True:
        try:
            now = datetime.datetime.now()
            today = now.date()
            
            # Chỉ hiện ngày khi sang ngày mới
            if current_date != today:
                print(f"\n📅 ===== {today.strftime('%d/%m/%Y')} =====")
                current_date = today
            
            # Kiểm tra có trong khung giờ hoạt động không
            if not is_active_hours():
                # Ngoài khung giờ 6h-22h => nghỉ đến 6h sáng hôm sau
                sleep_seconds = get_seconds_until_6am()
                hours = sleep_seconds // 3600
                minutes = (sleep_seconds % 3600) // 60
                print(f"\n🌙 [{now.strftime('%H:%M:%S')}] Ngoài khung giờ thu thập (22h-6h)")
                print(f"💤 Nghỉ {hours} tiếng {minutes} phút, chờ đến 6h sáng...")
                time.sleep(sleep_seconds)
                continue
            
            # Hiện giờ cho mỗi lần thu thập
            print(f"\n⏰ [{now.strftime('%H:%M:%S')}] Bắt đầu thu thập...")
            
            data = collect()
            
            if data:
                save(data)
                print(f"✅ Lưu {len(data)} dòng")
            else:
                print("⚠️ Không có dữ liệu trong lần thu thập này")
            
            # Khung giờ ban ngày: 20 phút
            print(f"💤 Chờ 20 phút đến lần thu thập tiếp theo...")
            time.sleep(20 * 60)
            
        except KeyboardInterrupt:
            print("\n🛑 Dừng chương trình theo yêu cầu người dùng")
            break
        except Exception as e:
            print(f"❌ Lỗi không mong đợi: {e}")
            print("🔄 Thử lại sau 60 giây...")
            time.sleep(60)

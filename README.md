# 🚦 HỆ THỐNG DỰ ĐOÁN ÙN TẮC GIAO THÔNG THÔNG MINH - TP. QUY NHƠN
### (Intelligent Traffic Prediction System for Quy Nhon City)

> **Dự án Học tập - Môn ADY201m (AI, Data Science with Python & SQL)**

## 👥 Thành viên thực hiện
| STT | Họ và Tên | Mã sinh viên | Vai trò |
|:---:|:---|:---|:---|
| 1 | **Lê Ngọc Phú** | [Điền mã SV] | [Ví dụ: Data Engineer, DevOps] |
| 2 | **Nguyễn Xuân Đỉnh** | [Điền mã SV] | [Ví dụ: Data Analyst, AI Modeler] |

## 📖 Giới thiệu dự án
Dự án này được xây dựng nhằm mục đích thu thập, lưu trữ và phân tích dữ liệu giao thông thời gian thực tại **Thành phố Quy Nhơn**. Bằng cách sử dụng dữ liệu từ **TomTom Traffic API**, hệ thống áp dụng quy trình ETL hiện đại và các mô hình Học máy (Machine Learning) để dự đoán mức độ tắc nghẽn giao thông, đặc biệt tập trung vào tác động của hoạt động du lịch và các khung giờ cao điểm.

Mục tiêu cuối cùng là cung cấp thông tin chi tiết giúp tối ưu hóa lộ trình di chuyển cho người dân và khách du lịch tại Quy Nhơn.

## 🛠️ Công nghệ sử dụng
Hệ thống được thiết kế theo kiến trúc Microservices sử dụng Docker:
- **Thu thập dữ liệu (Ingestion):** Python (Requests, Scheduler)
- **Data Lake:** MinIO (Lưu trữ Raw Data JSON)
- **Data Warehouse:** PostgreSQL (Lưu trữ Structured Data)
- **Phân tích & AI:** Jupyter Notebook, Pandas, Scikit-learn, Matplotlib
- **Triển khai:** Docker & Docker Compose

---
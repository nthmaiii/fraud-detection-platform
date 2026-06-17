# Real-Time Fraud Detection Platform

Hệ thống Microservice phát hiện gian lận giao dịch thời gian thực (Real-time Fraud Detection) đạt hiệu năng cao, tích hợp mô hình Học máy (Machine Learning) cùng kiến trúc xử lý sự kiện hướng đối tượng (Event-Driven Architecture). Hệ thống xử lý song song hai luồng dữ liệu: Đồng bộ qua REST API và Bất đồng bộ qua luồng Streaming sự kiện để chấm điểm rủi ro giao dịch chỉ trong **< 10ms**.

## 🚀 Tính Năng Cốt Lõi
- **Real-Time Feature Engineering (Sliding Window):** Sử dụng cấu trúc dữ liệu **Redis Sorted Sets (ZSET)** để triển khai bộ đếm tần suất đơn hàng trượt trong 10 phút (`orders_last_10m`) giúp tối ưu RAM và giảm tải tối đa cho Database chính.
- **Machine Learning Inference:** Nhúng trực tiếp mô hình phân lớp **XGBoost Classifier** lên bộ nhớ RAM để dự đoán rủi ro (Risk Score) tức thì và tự động đưa ra quyết định dựa trên Decision Engine Layer (BLOCK/MANUAL_REVIEW/APPROVE).
- **High-Throughput Streaming:** Triển khai **AIOKafka Consumer** chạy bất đồng bộ (Asynchronous) hoàn chỉnh dưới dạng Background Task, giúp tiêu thụ luồng sự kiện transaction tải lượng cao mà không làm gián đoạn luồng API chính.
- **Hệ thống Giám sát (Monitoring):** Tích hợp **Prometheus Metrics** thu thập các chỉ số hiệu năng hệ thống quan trọng theo thời gian thực bao gồm: Độ trễ xử lý P95/P99 (`latency`) và Tỷ lệ chặn gian lận (`fraud rate`).

## 🛠️ Công Nghệ Sử Dụng
- **Language:** Python 3.10+
- **Framework:** FastAPI, Uvicorn
- **Event Streaming:** Apache Kafka (aiokafka)
- **Caching & Velocity Counting:** Redis (redis-py)
- **Machine Learning:** XGBoost, NumPy
- **Monitoring:** Prometheus Client
- **Infrastructure:** Docker, Docker Compose

## 🏗️ Kiến Trúc Hệ Thống
1. **REST API Endpoint (`/fraud/check`):** Phục vụ luồng kiểm tra đồng bộ trực tiếp từ các dịch vụ cốt lõi (ví dụ: Order Service).
2. **Kafka Event Consumer:** Lắng nghe luồng sự kiện từ topic `orders`, tự động trích xuất đặc trưng và đẩy qua mô hình chấm điểm ngầm.
3. **Prometheus Endpoint (`/metrics`):** Phơi dữ liệu thô phục vụ việc thu thập và hiển thị biểu đồ Dashboard trực quan.

## 💻 Hướng Dẫn Cài Đặt & Chạy Khởi Động

### 1. Khởi động Hạ tầng (Kafka & Redis)
Đảm bảo bạn đã cài đặt Docker và Docker Compose trên máy, sau đó chạy lệnh:
```bash
docker-compose up -d
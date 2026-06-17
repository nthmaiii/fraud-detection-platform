import time
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from src.services.cache_service import cache_service
from src.services.ml_service import ml_service
from src.services.kafka_service import kafka_fraud_consumer

# ==============================================================================
# --- DEFINE PROMETHEUS METRICS ---
# ==============================================================================
# Bộ đếm tổng số lượng đơn hàng được xử lý qua REST API (Phục vụ tính toán Fraud Rate)
ORDER_PROCESS_COUNTER = Counter(
    "fraud_platform_orders_total", 
    "Tong so don hang he thong da kiem tra gian lan",
    ["decision"]  # Phân loại tag: BLOCK, APPROVE, MANUAL_REVIEW để vẽ đồ thị tỉ lệ
)

# Bộ đo độ trễ xử lý (Inference Latency) nhằm giám sát chỉ số P50, P95, P99
LATENCY_HISTOGRAM = Histogram(
    "fraud_platform_inference_latency_seconds",
    "Thoi gian xu ly tinh toan gian lan (Inference Latency)",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)  # Các mốc đo từ 5ms đến 1s
)

# ==============================================================================
# --- APPLICATION LIFECYCLE (LIFESPAN) ---
# ==============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Quản lý vòng đời khởi chạy của ứng dụng.
    Đăng ký Kafka Consumer chạy dưới dạng một Async Task ngầm (Background Task).
    Cơ chế này giúp Windows quản lý socket hợp lệ bên trong Event Loop của FastAPI.
    """
    # Khởi chạy Kafka Consumer bất đồng bộ nền
    kafka_task = asyncio.create_task(kafka_fraud_consumer.start())
    yield
    # Khi tắt Server FastAPI (Uvicorn tắt hoặc nhấn Ctrl + C), hủy task nền an toàn
    kafka_task.cancel()
    try:
        await kafka_task
    except asyncio.CancelledError:
        print("🛑 [Lifespan] Đã hủy và dọn dẹp Task ngầm Kafka thành công.")

# Khởi tạo FastAPI tích hợp tiến trình vòng đời quản lý luồng ngầm
app = FastAPI(title="Real-time Fraud Detection Platform", lifespan=lifespan)

# ==============================================================================
# --- DATA MODELS (PYDANTIC) ---
# ==============================================================================
class OrderCheckRequest(BaseModel):
    user_id: str
    device_id: str
    order_value: float
    discount_percent: float

class FraudResponse(BaseModel):
    risk_score: float
    decision: str
    reasons: list[str]

# ==============================================================================
# --- REST API ENDPOINTS ---
# ==============================================================================

@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    """
    Endpoint phơi dữ liệu dạng thô (Scrape Endpoint) cho hệ thống Prometheus Server cào dữ liệu.
    Mặc định trả về định dạng text/plain theo đúng chuẩn của Prometheus.
    """
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/fraud/check", response_model=FraudResponse)
async def check_order_fraud(request: OrderCheckRequest):
    """
    API đồng bộ kiểm tra gian lận thời gian thực (Synchronous Check Flow).
    Thực hiện: Đếm tần suất qua Redis -> Tổng hợp Feature -> XGBoost Predict -> Ghi nhận chỉ số đo lường.
    """
    # 1. Bấm giờ đo độ trễ hệ thống
    start_time = time.time()
    
    try:
        # 2. Ghi nhận transaction và tính toán khung giờ trượt Sliding Window 10 phút qua Redis
        orders_count = cache_service.log_order_and_get_velocity(user_id=request.user_id, window_seconds=600)
        
        # Giả lập tầng thu thập dấu chân phần cứng (Device Fingerprint Store)
        mock_accounts_per_device = 1 if request.device_id != "bot_device_123" else 10

        # 3. Gom toàn bộ ma trận đặc trưng (Feature Matrix)
        features = {
            "orders_last_10m": orders_count,
            "accounts_per_device": mock_accounts_per_device,
            "discount_percent": request.discount_percent
        }

        # 4. Đưa vector vào mô hình ML (XGBoost giả lập) để chấm điểm rủi ro
        risk_score, decision = ml_service.predict_risk(features)
        
        # 5. Khảo sát nguyên nhân đưa ra quyết định (Explainability Layer)
        reasons = []
        if orders_count >= 5: reasons.append("high_order_velocity_10m")
        if mock_accounts_per_device > 5: reasons.append("multi_account_per_device")
        if request.discount_percent >= 80: reasons.append("high_discount_abuse")

        # 6. Ghi nhận thời gian hoàn tất xử lý (Inference Latency) và tăng bộ đếm Metric
        duration = time.time() - start_time
        LATENCY_HISTOGRAM.observe(duration)
        ORDER_PROCESS_COUNTER.labels(decision=decision).inc()

        return FraudResponse(
            risk_score=risk_score, 
            decision=decision, 
            reasons=reasons
        )
        
    except Exception as e:
        # Đảm bảo bọc toàn bộ Exception để không làm đổ vỡ luồng chính của ứng dụng
        raise HTTPException(status_code=500, detail=str(e))
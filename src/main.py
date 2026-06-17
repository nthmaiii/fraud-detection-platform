from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from src.services.cache_service import cache_service
from src.services.ml_service import ml_service

app = FastAPI(title="Real-time Fraud Detection Platform")

# Định nghĩa cấu trúc Dữ liệu đầu vào (Request Body)
class OrderCheckRequest(BaseModel):
    user_id: str
    device_id: str
    order_value: float
    discount_percent: float

# Định nghĩa cấu trúc Dữ liệu trả về (Response Body)
class FraudResponse(BaseModel):
    risk_score: float
    decision: str
    reasons: list[str]

@app.post("/fraud/check", response_model=FraudResponse)
async def check_order_fraud(request: OrderCheckRequest):
    try:
        # Bước 1: Ghi nhận đơn hàng vào Redis và lấy ngay số đơn trong 10 phút qua (Sliding Window)
        orders_count = cache_service.log_order_and_get_velocity(user_id=request.user_id, window_seconds=600)

        # Giả lập việc truy vấn nhanh số account/device (thường lưu ở Redis)
        # Ở đây ta hardcode giả lập để test logic
        mock_accounts_per_device = 1 if request.device_id != "bot_device_123" else 10

        # Bước 2: Gom toàn bộ Feature thu thập được
        features = {
            "orders_last_10m": orders_count,
            "accounts_per_device": mock_accounts_per_device,
            "discount_percent": request.discount_percent
        }

        # Bước 3: Đưa vào Model ML tính toán
        risk_score, decision = ml_service.predict_risk(features)

        # Tạo lý do để hiển thị lên Dashboard sau này (Explainability)
        reasons = []
        if orders_count >= 5: reasons.append("high_order_velocity_10m")
        if mock_accounts_per_device > 5: reasons.append("multi_account_per_device")
        if request.discount_percent >= 80: reasons.append("high_discount_abuse")

        return FraudResponse(
            risk_score=risk_score,
            decision=decision,
            reasons=reasons
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
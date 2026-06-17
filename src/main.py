import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from src.services.cache_service import cache_service
from src.services.ml_service import ml_service
from src.services.kafka_service import kafka_fraud_consumer

# Định nghĩa Lifespan để quản lý tiến trình chạy ngầm (Background Task)
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Khởi chạy Kafka Consumer chạy ngầm dạng non-blocking task khi App start
    kafka_task = asyncio.create_task(kafka_fraud_consumer.start())
    yield
    # Khi App tắt, dọn dẹp task ngầm
    kafka_task.cancel()

app = FastAPI(title="Real-time Fraud Detection Platform", lifespan=lifespan)

class OrderCheckRequest(BaseModel):
    user_id: str
    device_id: str
    order_value: float
    discount_percent: float

class FraudResponse(BaseModel):
    risk_score: float
    decision: str
    reasons: list[str]

# API Endpoint phục vụ gọi trực tiếp (Synchronous Check)
@app.post("/fraud/check", response_model=FraudResponse)
async def check_order_fraud(request: OrderCheckRequest):
    try:
        orders_count = cache_service.log_order_and_get_velocity(user_id=request.user_id, window_seconds=600)
        mock_accounts_per_device = 1 if request.device_id != "bot_device_123" else 10

        features = {
            "orders_last_10m": orders_count,
            "accounts_per_device": mock_accounts_per_device,
            "discount_percent": request.discount_percent
        }

        risk_score, decision = ml_service.predict_risk(features)
        
        reasons = []
        if orders_count >= 5: reasons.append("high_order_velocity_10m")
        if mock_accounts_per_device > 5: reasons.append("multi_account_per_device")
        if request.discount_percent >= 80: reasons.append("high_discount_abuse")

        return FraudResponse(risk_score=risk_score, decision=decision, reasons=reasons)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
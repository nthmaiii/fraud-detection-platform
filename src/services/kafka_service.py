import json
import asyncio
from aiokafka import AIOKafkaConsumer
from src.config import config
from src.services.cache_service import cache_service
from src.services.ml_service import ml_service

class KafkaFraudConsumer:
    def __init__(self):
        self.consumer = None

    async def start(self):
        # Khởi tạo Kafka Consumer lắng nghe topic 'orders'
        self.consumer = AIOKafkaConsumer(
            config.ORDER_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id="fraud-detection-group",
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        await self.consumer.start()
        print("🚀 [Kafka Consumer] Đã kết nối tới Kafka và đang lắng nghe topic 'orders'...")

        try:
            # Vòng lặp vô tận để consume event theo thời gian thực
            async for msg in self.consumer:
                order_data = msg.value
                await self.process_order_event(order_data)
        except Exception as e:
            print(f"❌ Lỗi khi consume event: {e}")
        finally:
            await self.stop()

    async def process_order_event(self, order_data: dict):
        """
        Xử lý bất đồng bộ từng event đơn hàng nhận được từ luồng Streaming
        """
        user_id = str(order_data.get("user_id"))
        device_id = order_data.get("device_id", "unknown")
        discount_percent = order_data.get("discount_percent", 0)
        order_id = order_data.get("order_id")

        # 1. Pipeline tính Velocity qua Redis (Chạy mất <1ms)
        orders_count = cache_service.log_order_and_get_velocity(user_id=user_id, window_seconds=600)

        # 2. Chuẩn bị features
        mock_accounts_per_device = 1 if device_id != "bot_device_123" else 10
        features = {
            "orders_last_10m": orders_count,
            "accounts_per_device": mock_accounts_per_device,
            "discount_percent": discount_percent
        }

        # 3. Đưa vào Model chấm điểm
        risk_score, decision = ml_service.predict_risk(features)

        # 4. Log kết quả (Trong thực tế sẽ bắn kết quả này sang topic 'fraud_results' hoặc lưu DB)
        print(f"⚡ [REAL-TIME DETECTED] Order: {order_id} | User: {user_id} | Score: {risk_score} | Decision: {decision}")

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
            print("🛑 [Kafka Consumer] Đã ngắt kết nối an toàn.")

# Khởi tạo instance độc bản
kafka_fraud_consumer = KafkaFraudConsumer()
import json
import asyncio
from aiokafka import AIOKafkaConsumer
from src.config import config
from src.services.cache_service import cache_service
from src.services.ml_service import ml_service

class KafkaFraudConsumer:
    def __init__(self):
        self.consumer = None
        self.task = None

    async def start(self):
        """
        Khởi tạo và chạy vòng lặp lắng nghe Kafka
        """
        print("🚀 [Kafka Consumer] Đang khởi tạo kết nối bất đồng bộ tới Kafka...")
        
        self.consumer = AIOKafkaConsumer(
            config.ORDER_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id="fraud-detection-group",
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            # Thêm cấu hình tối ưu chống timeout trên Windows local
            heartbeat_interval_ms=3000,
            session_timeout_ms=10000
        )
        
        await self.consumer.start()
        print("✅ [Kafka Consumer] Đã lắng nghe thành công trên topic 'orders'.")

        try:
            # Vòng lặp đọc message async chuẩn ngành
            async for msg in self.consumer:
                order_data = msg.value
                # Chạy xử lý ngầm (background) không block luồng đọc tin nhắn kế tiếp
                asyncio.create_task(self.process_order_event(order_data))
        except asyncio.CancelledError:
            print("🛑 [Kafka Consumer] Tiến trình nhận tin nhắn bị hủy.")
        except Exception as e:
            print(f"❌ [Kafka Consumer] Gặp lỗi hệ thống: {e}")
        finally:
            await self.stop()

    async def process_order_event(self, order_data: dict):
        """
        Xử lý logic tính toán gian lận cho event từ Kafka
        """
        try:
            user_id = str(order_data.get("user_id"))
            device_id = order_data.get("device_id", "unknown")
            discount_percent = order_data.get("discount_percent", 0)
            order_id = order_data.get("order_id")

            # Gọi sang Redis đếm (Redis-py hỗ trợ chạy mượt trong môi trường async)
            orders_count = cache_service.log_order_and_get_velocity(user_id=user_id, window_seconds=600)

            mock_accounts_per_device = 1 if device_id != "bot_device_123" else 10
            features = {
                "orders_last_10m": orders_count,
                "accounts_per_device": mock_accounts_per_device,
                "discount_percent": discount_percent
            }

            risk_score, decision = ml_service.predict_risk(features)
            print(f"⚡ [KAFKA STREAM] Order: {order_id} | Score: {risk_score} | Decision: {decision}")
        except Exception as e:
            print(f"❌ [Kafka Consumer] Lỗi khi xử lý event đơn hàng: {e}")

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
            print("🛑 [Kafka Consumer] Đã ngắt kết nối an toàn.")

kafka_fraud_consumer = KafkaFraudConsumer()
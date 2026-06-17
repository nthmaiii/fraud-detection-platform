import redis
import time
from src.config import config

class CacheService:
    def __init__(self):
        # Kết nối tới Redis cluster/instance
        self.client = redis.Redis(
            host=config.REDIS_HOST, 
            port=config.REDIS_PORT, 
            decode_responses=True
        )

    def log_order_and_get_velocity(self, user_id: str, window_seconds: int = 600) -> int:
        """
        Ghi nhận một timestamp đơn hàng mới và trả về tổng số đơn trong khung giờ (window_seconds).
        Mặc định window_seconds = 600 (10 phút).
        """
        key = f"user_orders:{user_id}"
        current_time = time.time()
        cutoff_time = current_time - window_seconds

        # Tạo một Redis pipeline để tối ưu số lần gọi mạng (Network Round Trip)
        pipe = self.client.pipeline()
        
        # 1. Thêm đơn hàng hiện tại với score = timestamp hiện tại
        pipe.zadd(key, {str(current_time): current_time})
        
        # 2. Xóa tất cả các đơn hàng cũ nằm ngoài khung thời gian (từ 0 đến cutoff_time)
        pipe.zremrangebyscore(key, 0, cutoff_time)
        
        # 3. Đếm số lượng đơn hàng còn lại trong window
        pipe.zcard(key)
        
        # 4. Đặt thời gian hết hạn cho toàn bộ key để tránh rác bộ nhớ (TTL = 1 ngày)
        pipe.expire(key, 86400)
        
        # Thực thi pipeline
        results = pipe.execute()
        
        # Kết quả của lệnh zcard nằm ở vị trí index số 2 trong mảng trả về
        order_count_in_window = results[2]
        return order_count_in_window

cache_service = CacheService()
import numpy as np

class MLService:
    def __init__(self):
        # Trong thực tế: self.model = xgb.Booster() -> self.model.load_model("model.json")
        print("🤖 [ML Service] Đã tải model XGBoost lên RAM thành công.")

    def predict_risk(self, features: dict) -> tuple[float, str]:
        """
        Giả lập cơ chế chấm điểm dựa trên Feature Engineering.
        Trả về: (risk_score, decision)
        """
        # Giả lập công thức cây quyết định (Decision Tree) của XGBoost
        score = 0.05  # Base score mặc định cho đơn hàng sạch

        # 1. Kiểm tra Velocity Feature (Lấy từ Redis ở Bước 2)
        if features["orders_last_10m"] >= 5:
            score += 0.40  # Spam đơn liên tục
        elif features["orders_last_10m"] >= 2:
            score += 0.15

        # 2. Kiểm tra Device Feature
        if features["accounts_per_device"] > 5:
            score += 0.35  # Bot farm, 1 máy nhiều tài khoản
        elif features["accounts_per_device"] > 2:
            score += 0.15

        # 3. Kiểm tra Order Feature
        if features["discount_percent"] >= 80:
            score += 0.20  # Săn voucher quá đà

        # Giới hạn score trong khoảng [0.0, 1.0]
        risk_score = float(np.clip(score, 0.0, 1.0))

        # Decision Engine Layer (Kế thừa từ quy định Business)
        if risk_score >= 0.80:
            decision = "BLOCK"
        elif risk_score >= 0.50:
            decision = "MANUAL_REVIEW"
        else:
            decision = "APPROVE"

        return round(risk_score, 2), decision

ml_service = MLService()
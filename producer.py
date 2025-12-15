import requests
from datetime import datetime, timezone
import time
import csv
import os
import json
from kafka import KafkaProducer

# ==========================
# 1. API config Finnhub
# ==========================
API_KEY = "d4bh1fhr01qnomk4rv6gd4bh1fhr01qnomk4rv70"
BASE_URL = "https://finnhub.io/api/v1"

# ==========================
# 2. Kafka config
# ==========================
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC_NAME = "aapl_quotes"

# ==========================
# 3. CSV Log file
# ==========================
OUTPUT_CSV = "aapl_quotes.csv"

def append_to_csv(row: dict):
    file_exists = os.path.isfile(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

# ==========================
# 4. Hàm gọi API Finnhub
# ==========================
def get_quote(symbol: str):
    url = f"{BASE_URL}/quote"
    params = {"symbol": symbol, "token": API_KEY}

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    ts_unix = data.get("t")
    time_iso = datetime.fromtimestamp(ts_unix, tz=timezone.utc).isoformat() if ts_unix else None

    return {
        "symbol": symbol,
        "price": data.get("c"),
        "open": data.get("o"),
        "change": data.get("d"),
        "pct_change": data.get("dp"),
        "high": data.get("h"),
        "low": data.get("l"),
        "prev_close": data.get("pc"),
        "time_unix": ts_unix,
        "time_iso": time_iso,
    }

# ==========================
# 5. Main Loop
# ==========================
if __name__ == "__main__":
    symbol = "AAPL"

    while True:
        print("===== NEW SNAPSHOT =====")
        try:
            quote = get_quote(symbol)

            # In log
            print(
                f"{quote['time_iso']} | {quote['symbol']}: "
                f"Giá hiện tại = {quote['price']}, "
                f"🔺 Thay đổi = {quote['change']} ({quote['pct_change']}%), "
                f" Mở cửa = {quote['open']}, "
                f" Cao nhất = {quote['high']}, "
                f" Thấp nhất = {quote['low']}, "
                f" Đóng hôm qua = {quote['prev_close']}"
            )

            # Ghi CSV
            append_to_csv(quote)

            # 🔥 Gửi vào Kafka topic
            producer.send(TOPIC_NAME, value=quote)
            producer.flush()
            print(" Đã gửi vào Kafka!")

        except Exception as e:
            print(f"Lỗi khi lấy giá hoặc gửi Kafka: {e}")

        time.sleep(1)

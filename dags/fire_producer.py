from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
from random import randint, uniform, choice
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import threading
import time
import logging
from kafka.errors import NoBrokersAvailable

app = FastAPI(title="Fire Risk Kafka Stream")

KAFKA_BROKER = "localhost:29092"
TOPIC = "fire_sensors"
MAX_RETRIES = 20
RETRY_INTERVAL = 3  # seconds

producer = None
for attempt in range(1, MAX_RETRIES + 1):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=5000
        )
        # test sending empty message to ensure connection works
        producer.send(TOPIC, {"test": "ping"}).get(timeout=5)
        print(f"✅ Connected to Kafka at {KAFKA_BROKER}")
        break
    except (KafkaError, Exception) as e:
        print(f"⏳ Kafka not ready yet, retrying {attempt}/{MAX_RETRIES} in {RETRY_INTERVAL}s...")
        time.sleep(RETRY_INTERVAL)
else:
    raise RuntimeError(f"❌ Could not connect to Kafka at {KAFKA_BROKER} after {MAX_RETRIES} attempts")
# ✅ Updated schema (5 key features)
class SensorEvent(BaseModel):
    sensor_id: str
    temperature: float
    humidity: float
    wind_speed: float
    vegetation_type: str
    soil_moisture: float
    timestamp: str

# Possible vegetation types
vegetation_types = ["forest", "grassland", "shrubland", "desert"]

# 🔥 Generate realistic fire-risk data
def get_sensor_data():
    return {
        "sensor_id": f"S{randint(1, 5)}",
        "temperature": round(uniform(20, 45), 2),
        "humidity": round(uniform(10, 90), 2),
        "wind_speed": round(uniform(0, 40), 2),
        "vegetation_type": choice(vegetation_types),
        "soil_moisture": round(uniform(5, 50), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

# 🚀 DEMO MODE: ~50 events/sec
def stream_to_kafka():
    while True:
        try:
            for _ in range(50):   # batch of 50 events
                data = get_sensor_data()
                producer.send(TOPIC, data)

            producer.flush()
            print("🔥 Sent 50 fire risk events")

            time.sleep(1)  # 50 events/sec

        except Exception as e:
            logging.error(f"Error producing event: {e}")

# Start background streaming
@app.on_event("startup")
def start_streaming():
    threading.Thread(target=stream_to_kafka, daemon=True).start()

# Optional endpoint (manual trigger)
@app.get("/generate_event", response_model=SensorEvent)
def generate_event():
    data = get_sensor_data()
    producer.send(TOPIC, data)
    return data

# ✅ Uvicorn entrypoint for direct run
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("fire_producer:app", host="0.0.0.0", port=8000, reload=True)
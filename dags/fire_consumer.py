from kafka import KafkaConsumer
import json

TOPIC = "fire_sensors"
KAFKA_BROKER = "localhost:29092"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='fire_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def compute_fire_risk(event):
    score = 0
    if event["temperature"] > 40: score += 2
    if event["humidity"] < 20: score += 2
    if event["wind_speed"] > 30: score += 1
    if event["soil_moisture"] < 10: score += 1
    if score >= 5:
        return "🔥 HIGH RISK"
    elif score >= 3:
        return "⚠️ MODERATE RISK"
    else:
        return "✅ SAFE"

print("🎯 Fire Risk Consumer started...")

for msg in consumer:
    event = msg.value
    # Skip if keys are missing
    required_keys = ["temperature", "humidity", "wind_speed", "vegetation_type", "soil_moisture"]
    if not all(k in event for k in required_keys):
        print(f"⚠️ Skipping invalid event: {event}")
        continue

    risk = compute_fire_risk(event)
    print(f"🔥 Event {event['sensor_id']} risk level: {risk}")


# for message in consumer:
#     print("Raw message:", message)
#     event = message.value
#     print("Event dict:", event)
#     risk = compute_fire_risk(event)
#     print(f"Sensor {event['sensor_id']} | Temp: {event['temperature']}°C | Risk: {risk}")
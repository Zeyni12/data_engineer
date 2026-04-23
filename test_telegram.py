import requests

BOT_TOKEN = "8680424283:AAFeF4pyaw2ZCvTvwTKpMWKOm9TSLJ7YWjQ"
CHAT_ID = "6242105556"

def send_telegram_alert(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message
    }
    requests.post(url, data=payload)

send_telegram_alert("🔥 Test Alert: System is working!")    
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import joblib
import random

# ==============================
# 1. Reproducibility
# ==============================
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# ==============================
# 2. Generate training data
# ==============================
def generate_data(n_samples=1000):
    data = []

    for _ in range(n_samples):
        temp = random.uniform(10, 50)
        humidity = random.uniform(10, 100)
        wind = random.uniform(0, 30)
        soil = random.uniform(0, 50)

        # Rule-based labeling
        risk = 0
        if temp > 40:
            risk += 2
        if humidity < 20:
            risk += 2
        if wind > 20:
            risk += 1
        if soil < 20:
            risk += 1

        label = 1 if risk >= 4 else 0

        data.append([temp, humidity, wind, soil, label])

    return pd.DataFrame(data, columns=[
        "temperature", "humidity", "wind_speed", "soil_moisture", "label"
    ])

df = generate_data()

# ==============================
# 3. Train model
# ==============================
X = df[["temperature", "humidity", "wind_speed", "soil_moisture"]]
y = df["label"]

model = RandomForestClassifier(
    n_estimators=100,
    random_state=SEED
)

model.fit(X, y)

# ==============================
# 4. Save model (stable format)
# ==============================
MODEL_PATH = "fire_model_fix.pkl"

joblib.dump(model, MODEL_PATH, compress=3)

print(f"✅ Model trained and saved as {MODEL_PATH}")
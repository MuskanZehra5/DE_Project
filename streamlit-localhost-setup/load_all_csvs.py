

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
import joblib
from sqlalchemy import create_engine

# ─── DB Load ───────────────────────────────────────────────────────────────────
engine = create_engine("postgresql+psycopg2://umar22z:1234@localhost/car_sales")
query = """
    SELECT price, engine, mileage, year, city, transmission, time_to_sell
    FROM used_cars_sim
    WHERE price IS NOT NULL AND time_to_sell IS NOT NULL
"""
df = pd.read_sql(query, engine)

# ─── Filter and Clean ──────────────────────────────────────────────────────────
df = df.dropna(subset=["price", "engine", "mileage", "year", "city", "transmission", "time_to_sell"])
df = df[df["price"] > 100000]  # remove outliers if needed

# ─── Define Features and Target ────────────────────────────────────────────────
X = df[["price", "engine", "mileage", "year", "city", "transmission"]]
y = df["time_to_sell"]

# ─── Preprocessing ─────────────────────────────────────────────────────────────
categorical = ["city", "transmission"]
numeric = ["price", "engine", "mileage", "year"]

preprocessor = ColumnTransformer(transformers=[
    ("num", SimpleImputer(strategy="median"), numeric),
    ("cat", OneHotEncoder(handle_unknown="ignore"), categorical)
])

# ─── Model Pipeline ────────────────────────────────────────────────────────────
pipeline = Pipeline(steps=[
    ("pre", preprocessor),
    ("model", RandomForestRegressor(n_estimators=100, random_state=42))
])

# ─── Train ─────────────────────────────────────────────────────────────────────
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
pipeline.fit(X_train, y_train)

# ─── Evaluate (Optional) ───────────────────────────────────────────────────────
score = pipeline.score(X_test, y_test)
print(f"✅ Model R² on test set: {score:.2f}")

# ─── Save ──────────────────────────────────────────────────────────────────────
joblib.dump(pipeline, "revenue_optimizer_model.pkl")
print("💾 Model saved as revenue_optimizer_model.pkl")

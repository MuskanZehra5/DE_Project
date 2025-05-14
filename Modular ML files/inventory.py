# ml/inventory_mix.py

import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
MIN_LISTINGS = 2
FORECAST_MONTHS = 1

# ─── DB CONNECTION ──────────────────────────────────────────────────────────────
def get_engine():
    return create_engine("postgresql+psycopg2://umar22z:1234@localhost/car_sales")

# ─── LOAD AND CLEAN ─────────────────────────────────────────────────────────────
def load_data():
    engine = get_engine()
    df = pd.read_sql("SELECT city, title, date_posted, time_to_sell FROM used_cars_sim", engine)

    df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce")
    df = df.dropna(subset=["date_posted", "title", "city"])
    df["month"] = df["date_posted"].dt.to_period("M")
    return df

# ─── FORECAST DEMAND ────────────────────────────────────────────────────────────
def forecast_demand(df):
    grouped = df.groupby(["city", "title", "month"]).size().reset_index(name="listing_count")
    combo_counts = df.groupby(["city", "title"]).size().reset_index(name="count")

    st.info(f"📊 {len(combo_counts)} unique (city, model) combos")
    st.write("✅ Combos with ≥ 5 listings:", (combo_counts["count"] >= 5).sum())
    st.write("❌ Combos with < 5 listings:", (combo_counts["count"] < 5).sum())

    future_rows = []

    for (city, title), group in grouped.groupby(["city", "title"]):
        if len(group) < MIN_LISTINGS:
            continue

        group = group.sort_values("month")
        months = np.arange(len(group)).reshape(-1, 1)
        counts = group["listing_count"].values

        try:
            model = LinearRegression().fit(months, counts)
            forecast = model.predict([[len(group) + FORECAST_MONTHS - 1]])[0]
        except:
            forecast = counts[-1]

        avg_time_to_sell = df[(df["city"] == city) & (df["title"] == title)]["time_to_sell"].mean()

        future_rows.append({
            "city": city,
            "model": title,
            "forecasted_demand": max(forecast, 0),
            "avg_time_to_sell": round(avg_time_to_sell, 1)
        })

    return pd.DataFrame(future_rows)

# ─── SCORE & RANK MODELS ────────────────────────────────────────────────────────
def recommend_inventory(df_forecasted):
    df = df_forecasted.copy()
    df["demand_score"] = df["forecasted_demand"] / df["forecasted_demand"].max()
    df["turnover_score"] = 1 - (df["avg_time_to_sell"] / df["avg_time_to_sell"].max())
    df["stock_score"] = round((df["demand_score"] * 0.6 + df["turnover_score"] * 0.4), 3)
    return df.sort_values(["city", "stock_score"], ascending=[True, False])










    

# ─── STREAMLIT UI ───────────────────────────────────────────────────────────────
def run_inventory_ui():
    st.title("📦 Inventory Mix Recommendation")

    with st.spinner("Loading data..."):
        df = load_data()

    with st.spinner("Forecasting demand..."):
        forecast_df = forecast_demand(df)

    recommendations = recommend_inventory(forecast_df)

    cities = recommendations["city"].unique()
    city = st.selectbox("Select a city to view top model recommendations:", sorted(cities))

    top5 = recommendations[recommendations["city"] == city].head(5)
    st.subheader(f"🏆 Top 5 Inventory Picks for {city}")
    st.dataframe(top5[["model", "forecasted_demand", "avg_time_to_sell", "stock_score"]])

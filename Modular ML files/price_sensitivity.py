# ml/price_sensitivity.py

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import streamlit as st
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
SEGMENT_BY = "title"   # or use 'engine' or 'fuel' for coarser groups
MIN_LISTINGS = 2       # minimum listings required to analyze a model
PRICE_CHANGE_RANGE = [-0.05, 0, 0.05]  # simulate -5%, 0%, +5%

# ─── DB CONNECTION ──────────────────────────────────────────────────────────────
def get_engine():
    return create_engine("postgresql+psycopg2://umar22z:1234@localhost/car_sales")

# ─── LOAD & CLEAN ───────────────────────────────────────────────────────────────
def load_used_cars():
    engine = get_engine()
    query = """
        SELECT title, fuel, engine, price, time_to_sell
        FROM used_cars
        WHERE price IS NOT NULL
    """
    df = pd.read_sql(query, engine)

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["time_to_sell"] = pd.to_numeric(df["time_to_sell"], errors="coerce")

    # Simulate missing time_to_sell with 0–50 day random values
    missing = df["time_to_sell"].isna()
    df.loc[missing, "time_to_sell"] = np.random.randint(0, 51, size=missing.sum())

    return df.dropna(subset=["price", "time_to_sell"])

# ─── SIMULATE PRICE EFFECT ──────────────────────────────────────────────────────
def simulate(df: pd.DataFrame):
    global SEGMENT_BY
    st.info(f"📊 Trying segmentation by: `{SEGMENT_BY}`")

    segment_counts = df[SEGMENT_BY].value_counts()
    st.write("📦 Top 10 segment sizes:", segment_counts.head(10))

    eligible = segment_counts[segment_counts >= MIN_LISTINGS].index.tolist()

    if len(eligible) < 3:
        st.warning(f"⚠️ Too few segments with >= {MIN_LISTINGS} listings using '{SEGMENT_BY}'.")
        fallback = "fuel" if "fuel" in df.columns else "engine"
        SEGMENT_BY = fallback
        segment_counts = df[SEGMENT_BY].value_counts()
        eligible = segment_counts[segment_counts >= MIN_LISTINGS].index.tolist()
        st.info(f"🔁 Falling back to coarser segment: '{SEGMENT_BY}'")
        st.write("📦 New top 10 segments:", segment_counts.head(10))

    rows = []

    for segment in eligible:
        subset = df[df[SEGMENT_BY] == segment]
        X = subset[["price"]].values
        y = subset["time_to_sell"].values

        if len(np.unique(X)) < 2:
            continue

        model = LinearRegression()
        model.fit(X, y)
        base_price = np.mean(X)

        for pct in PRICE_CHANGE_RANGE:
            new_price = base_price * (1 + pct)
            predicted = model.predict([[new_price]])[0]
            rows.append({
                SEGMENT_BY: segment,
                "price_change": f"{int(pct*100)}%",
                "predicted_days_to_sell": round(predicted, 2)
            })

    return pd.DataFrame(rows)

# ─── PLOT ───────────────────────────────────────────────────────────────────────
def plot_heatmap(df_simulated: pd.DataFrame):
    pivot = df_simulated.pivot(index=SEGMENT_BY, columns="price_change", values="predicted_days_to_sell")
    fig, ax = plt.subplots(figsize=(12, max(6, len(pivot) * 0.4)))
    sns.heatmap(pivot, annot=True, fmt=".1f", cmap="coolwarm", linewidths=0.5, ax=ax)
    ax.set_title("Price Sensitivity Heatmap (Predicted Days to Sell)")
    ax.set_xlabel("Price Change")
    ax.set_ylabel(SEGMENT_BY.title())
    st.pyplot(fig)

# ─── STREAMLIT UI ──────────────────────────────────────────────────────────────
def run_price_sensitivity_ui():
    st.title("📉 Price Sensitivity Simulation")
    st.info(
        " **What you're seeing:**\n\n"
        "This simulation estimates how changes in price affect a car model’s time to sell.\n\n"
        "- The heatmap shows how markdowns (or markups) influence demand across different models.\n"
        "- Cooler colors (blue) mean faster selling; warmer colors (red) indicate slower turnover.\n"
        "- Use the raw data below to explore exact predictions for each price level.\n\n"
        "📊 This is especially useful for choosing markdown levels during campaigns or price tuning."
    )


    with st.spinner("Loading data..."):
        df = load_used_cars()

    with st.spinner("Running simulations..."):
        df_simulated = simulate(df)

    if df_simulated.empty:
        st.error("⚠️ Not enough data to generate insights. Try changing segment settings.")
    else:
        st.subheader("📊 Sensitivity Heatmap")
        plot_heatmap(df_simulated)
        st.subheader("📋 Raw Simulation Data")
        st.dataframe(df_simulated)

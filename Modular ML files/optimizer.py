

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import joblib
from sqlalchemy import create_engine

# ─── STREAMLIT UI ENTRY ───────────────────────────────────────────────────────
def run_optimizer_ui():
    st.title("💰 ML-Based Revenue & Margin Optimizer")
    st.info(
        " **What you're seeing:**\n\n"
        "This tool helps you find the *best markdown strategy* to sell a selected car model faster **without losing profit**.\n\n"
        "- Select a car model and enter your cost basis (what you paid for the car).\n"
        "- Choose how quickly you want it to sell (target days) and how much discount you're willing to offer.\n"
        "- The chart shows how markdowns impact **sell time** (blue) and **margin** (red).\n"
        "- You'll also get a recommendation if a markdown hits your target with a positive margin.\n\n"
        "📊 Use this to price competitively, move inventory faster, and protect your bottom line."
    )


    # Load model and DB
    model = joblib.load("revenue_optimizer_model.pkl")
    engine = create_engine("postgresql+psycopg2://umar22z:1234@localhost/car_sales")

    # Dropdown for model title
    model_list = pd.read_sql("SELECT DISTINCT title FROM used_cars_sim", engine)["title"]
    model_title = st.selectbox("Choose a car model:", sorted(model_list))

    # Inputs
    cost_basis = st.number_input("Your cost basis (PKR):", min_value=100000.0, step=50000.0)
    target_days = st.slider("🎯 Target sell time (days):", 10, 90, 30)
    max_md = st.slider("📉 Max markdown range (%):", 5, 30, 15)

    # Load and filter data
    df = pd.read_sql(f"SELECT * FROM used_cars_sim WHERE title = %s", engine, params=(model_title,))
    df = df.dropna(subset=["price", "engine", "mileage", "year", "city", "transmission", "time_to_sell"])

    if df.empty:
        st.error("No data available for this model.")
        return

    # Simulate markdown scenarios
    results = []
    for m in range(0, max_md + 1, 5):
        discount = m / 100.0
        test_df = df.copy()
        test_df["price"] = test_df["price"] * (1 - discount)
        test_df["cost_basis"] = cost_basis

        X = test_df[["price", "engine", "mileage", "year", "city", "transmission"]]
        predicted_sell_time = model.predict(X)
        margin = ((test_df["price"] - cost_basis) / cost_basis) * 100

        results.append({
            "markdown": m,
            "avg_sell_time": predicted_sell_time.mean(),
            "avg_margin": margin.mean()
        })

    # Results table
    res_df = pd.DataFrame(results)

    # Plotting
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.plot(res_df["markdown"], res_df["avg_sell_time"], "b-o", label="Sell Time")
    ax2.plot(res_df["markdown"], res_df["avg_margin"], "r-x", label="Margin")

    ax1.set_xlabel("Markdown %")
    ax1.set_ylabel("Sell Time (days)", color="b")
    ax2.set_ylabel("Margin %", color="r")
    plt.title(f"📊 Markdown Simulation: {model_title[:40]}")
    plt.tight_layout()
    st.pyplot(fig)

    # Recommendation
    valid = res_df[res_df["avg_sell_time"] <= target_days]
    if not valid.empty:
        best = valid.sort_values("avg_margin", ascending=False).iloc[0]
        st.success(
            f"✅ With ~{best['markdown']}% markdown, you can expect ~{int(best['avg_sell_time'])} days to sell "
            f"and ~{best['avg_margin']:.1f}% margin."
        )
    else:
        st.error("❌ No markdown level meets the target sell time with positive margin.")

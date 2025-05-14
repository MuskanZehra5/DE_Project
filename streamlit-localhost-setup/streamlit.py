import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from prophet import Prophet
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import numpy as np
import io

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
DB_URL = "postgresql+psycopg2://umar22z:1234@localhost/car_sales"
engine = create_engine(DB_URL)

st.set_page_config(page_title="Car Sales Analytics", layout="wide")
st.title("🚗 Car Showroom Intelligence Dashboard")

# ─── SIDEBAR NAVIGATION ─────────────────────────────────────────────────────────
page = st.sidebar.selectbox("Select Analysis", [
    "Fast Movers",
    "Price Sensitivity",
    "Inventory Mix",
    "Cluster Insights",
    "Seasonal Forecast",
    "Revenue & Margin Optimizer"
])

# ─── FAST MOVERS ────────────────────────────────────────────────────────────────
if page == "Fast Movers":
    st.header("⚡ Fast Movers by Month")
    df = pd.read_sql("SELECT * FROM used_cars_sim WHERE date_posted IS NOT NULL", engine)
    df["month"] = pd.to_datetime(df["date_posted"]).dt.to_period("M")
    df_grouped = df.groupby(["month", "title"])["time_to_sell"].mean().reset_index()
    df_grouped = df_grouped[df_grouped["time_to_sell"] > 0]
    top_titles = df_grouped.groupby("title")["time_to_sell"].mean().nsmallest(5).index
    df_filtered = df_grouped[df_grouped["title"].isin(top_titles)]
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.lineplot(data=df_filtered, x="month", y="time_to_sell", hue="title", marker="o", ax=ax)
    plt.title("Average Time to Sell (Top 5 Fastest Models)")
    st.pyplot(fig)

# ─── PRICE SENSITIVITY ──────────────────────────────────────────────────────────
elif page == "Price Sensitivity":
    st.header("💸 Price Sensitivity Heatmap")
    df = pd.read_sql("SELECT title, price, time_to_sell FROM used_cars_sim", engine)
    df = df.dropna()
    df = df[df["price"] > 100000]
    pivot = df.pivot_table(index="title", values="time_to_sell", aggfunc="mean")
    heatmap_data = pivot.head(30)  # limit for performance
    fig, ax = plt.subplots(figsize=(8, 10))
    sns.heatmap(heatmap_data, annot=True, cmap="coolwarm", fmt=".1f", ax=ax)
    plt.title("Avg Time to Sell by Model")
    st.pyplot(fig)

# ─── INVENTORY MIX ──────────────────────────────────────────────────────────────
elif page == "Inventory Mix":
    st.header("📦 Recommended Inventory Mix")
    df = pd.read_sql("SELECT city, title FROM used_cars_sim", engine)
    top = df.groupby(["city", "title"]).size().reset_index(name="count")
    top = top[top["count"] >= 3]
    st.dataframe(top.sort_values("count", ascending=False))

# ─── CLUSTER INSIGHTS ───────────────────────────────────────────────────────────
elif page == "Cluster Insights":
    st.header("🔍 Customer Cluster Insights")
    cluster_cols = st.multiselect("Select clustering features", ["price", "engine", "city", "transmission"], default=["price", "engine"])
    df = pd.read_sql("SELECT price, engine, city, transmission, fuel, time_to_sell FROM used_cars_sim", engine).dropna()

    cat_cols = [col for col in cluster_cols if col in ["city", "transmission"]]
    num_cols = [col for col in cluster_cols if col in ["price", "engine"]]

    transformer = ColumnTransformer([
        ("num", StandardScaler(), num_cols),
        ("cat", OneHotEncoder(drop="first"), cat_cols)
    ])

    pipe = Pipeline([
        ("prep", transformer),
        ("kmeans", KMeans(n_clusters=5, random_state=0))
    ])

    X = df[cluster_cols]
    pipe.fit(X)
    df["cluster"] = pipe.predict(X)
    st.dataframe(df.groupby("cluster").agg(
        count=("price", "count"),
        avg_price=("price", "mean"),
        avg_engine=("engine", "mean"),
        avg_time_to_sell=("time_to_sell", "mean"),
        top_fuel=("fuel", lambda x: x.mode().iloc[0])
    ))

# ─── SEASONAL FORECAST ──────────────────────────────────────────────────────────
elif page == "Seasonal Forecast":
    st.header("📅 Seasonal Sales Forecast")
    city = st.selectbox("Select city", pd.read_sql("SELECT DISTINCT city FROM used_cars_sim", engine)["city"])
    df = pd.read_sql(f"SELECT date_posted, price FROM used_cars_sim WHERE city = '{city}'", engine)
    df = df.dropna()
    df["week"] = pd.to_datetime(df["date_posted"]).dt.to_period("W").apply(lambda r: r.start_time)
    weekly = df.groupby("week").agg(avg_price=("price", "mean")).reset_index().rename(columns={"week": "ds", "avg_price": "y"})

    if len(weekly) >= 10:
        m = Prophet()
        m.fit(weekly)
        future = m.make_future_dataframe(periods=12, freq='W')
        forecast = m.predict(future)
        fig = m.plot(forecast)
        st.pyplot(fig)
    else:
        st.warning("Not enough data to forecast for this city.")

# ─── REVENUE & MARGIN OPTIMIZER ─────────────────────────────────────────────────
elif page == "Revenue & Margin Optimizer":
    st.header("📉 Revenue & Margin Optimizer")

    model_title = st.selectbox("Select car model", pd.read_sql("SELECT DISTINCT title FROM used_cars_sim", engine)["title"])
    target_days = st.slider("Target sell time (days)", min_value=10, max_value=90, value=30)
    markdown_range = st.slider("Max markdown (%)", min_value=5, max_value=30, step=5, value=15)

    df = pd.read_sql(f"SELECT * FROM used_cars_sim WHERE title = '{model_title}'", engine).dropna()
    df = df[df["price"] > 100000].copy()
    if "cost_basis" not in df.columns:
        np.random.seed(42)
        df["cost_basis"] = df["price"] * np.random.uniform(0.85, 0.95, size=len(df))

    results = []
    for md in np.linspace(0, markdown_range / 100, 6):
        new_price = df["price"] * (1 - md)
        sell_time = df["time_to_sell"] * (1 - md)**2
        profit = new_price - df["cost_basis"]
        margin = profit / new_price
        results.append({
            "markdown": md * 100,
            "avg_sell_time": sell_time.mean(),
            "avg_margin": margin.mean() * 100
        })

    res_df = pd.DataFrame(results)
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.plot(res_df["markdown"], res_df["avg_sell_time"], "b-o", label="Sell Time")
    ax2.plot(res_df["markdown"], res_df["avg_margin"], "r-x", label="Profit Margin")
    ax1.set_xlabel("Markdown (%)")
    ax1.set_ylabel("Avg Sell Time", color="b")
    ax2.set_ylabel("Avg Margin (%)", color="r")
    plt.title(f"Impact of Markdown for {model_title[:40]}...")
    st.pyplot(fig)

    recommended = res_df[res_df["avg_sell_time"] <= target_days].sort_values("avg_margin", ascending=False)
    if not recommended.empty:
        best = recommended.iloc[0]
        st.success(f"📌 For this model, to hit ~{target_days} days, mark down ~{best['markdown']:.0f}%. Expected margin: ~{best['avg_margin']:.1f}%")
    else:
        st.error("❌ No markdown level meets the target sell time with a positive margin.")

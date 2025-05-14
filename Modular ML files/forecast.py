import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st
from sqlalchemy import create_engine
from prophet import Prophet
from prophet.plot import plot_plotly
from datetime import timedelta
import plotly.graph_objs as go


# ─── CONFIG ─────────────────────────────────────────────────────────────────────
DB_URL = "postgresql+psycopg2://umar22z:1234@localhost/car_sales"
FORECAST_WEEKS = 12

# ─── LOAD DATA ──────────────────────────────────────────────────────────────────
def load_weekly_data():
    engine = create_engine(DB_URL)
    df = pd.read_sql("SELECT date_posted, price, city FROM used_cars_sim", engine)

    df = df[pd.to_numeric(df["price"], errors="coerce") > 100000].copy()
    df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce")
    df = df.dropna(subset=["date_posted", "price", "city"])

    df["week"] = df["date_posted"].dt.to_period("W").apply(lambda r: r.start_time)
    weekly = df.groupby(["week", "city"]).agg(
        listings=("price", "count"),
        avg_price=("price", "mean")
    ).reset_index()
    weekly = weekly.sort_values("week")
    return weekly

# ─── FORECASTING ────────────────────────────────────────────────────────────────
def forecast_prophet(df, y_col):
    df_prophet = df.rename(columns={"week": "ds", y_col: "y"})[["ds", "y"]]
    model = Prophet()
    model.fit(df_prophet)

    future = model.make_future_dataframe(periods=FORECAST_WEEKS, freq="W")
    forecast = model.predict(future)
    return forecast, model

# ─── STREAMLIT UI ───────────────────────────────────────────────────────────────
def run_forecast_ui():
    st.title("📅 Weekly Forecasting (Prophet)")
    st.info(
        " **What you're seeing:**\n\n"
        "This page gives you **city-wise weekly forecasts** for:\n"
        "- Number of listings (supply)\n"
        "- Average car prices (pricing trends)\n\n"
        "📈 Use this to predict how competitive the market will be in coming weeks, so you can:\n"
        "- Time your markdowns\n"
        "- Stock inventory smartly\n"
        "- Price your listings more accurately\n\n"
        "The shaded area around each forecast shows the confidence range — wider bands mean more uncertainty."
    )


    with st.spinner("Loading and aggregating data..."):
        weekly_df = load_weekly_data()

    cities = weekly_df["city"].unique()
    city = st.selectbox("Select city for forecast", sorted(cities))

    city_df = weekly_df[weekly_df["city"] == city]
    if len(city_df) < 8:
        st.warning("⚠️ Not enough data for this city. Try another.")
        return

    for metric in ["listings", "avg_price"]:
        st.subheader(f"🔮 Forecast: Weekly {metric.replace('_', ' ').title()}")

        forecast, model = forecast_prophet(city_df[["week", metric]].copy(), metric)
        fig = plot_plotly(model, forecast)
        st.plotly_chart(fig, use_container_width=True)

        # Show forecasted table
        st.write(forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(FORECAST_WEEKS).rename(columns={
            "ds": "Week", "yhat": "Forecast", "yhat_lower": "Lower Bound", "yhat_upper": "Upper Bound"
        }))

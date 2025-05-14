
from prefect import flow, task
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import joblib
import seaborn as sns
import matplotlib.pyplot as plt
from prophet import Prophet


@flow(name="train_cluster_model")
def run_cluster_model():
    ###########
        # ─── CONFIG ─────────────────────────────────────────────────────────────────────
    DB_URL = "postgresql+psycopg2://umar22z:1234@localhost/car_sales"
    CLUSTER_FEATURES = ["price", "engine", "city", "transmission"]
    N_CLUSTERS = 5

    CLUSTER_LABELS = {
        0: "Budget Hatchbacks",
        1: "Hybrid Sedans",
        2: "Premium SUVs",
        3: "Midrange Sedans",
        4: "Compact Imports"
    }

    # ─── LOAD + CLEAN ───────────────────────────────────────────────────────────────
    def load_data():
        engine = create_engine(DB_URL)
        df = pd.read_sql("SELECT * FROM used_cars_sim", engine)

        # Force types + clean junk
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["engine"] = pd.to_numeric(df["engine"], errors="coerce")
        df["time_to_sell"] = pd.to_numeric(df["time_to_sell"], errors="coerce")
        df = df[df["price"] > 100000]  # remove junk like 28.5

        return df.dropna(subset=["price", "engine", "transmission", "city", "time_to_sell", "fuel"])

    # ─── PREPROCESS FEATURES ────────────────────────────────────────────────────────
    def preprocess_data(df, features):
        df = df.copy()
        selected_df = df[features]
        meta_df = df[["title", "price", "engine", "fuel", "transmission", "city", "time_to_sell"]].copy()

        num_cols = [f for f in features if f in ["price", "engine"]]
        cat_cols = [f for f in features if f not in num_cols]

        transformers = []
        if num_cols:
            transformers.append(("num", StandardScaler(), num_cols))
        if cat_cols:
            transformers.append(("cat", OneHotEncoder(drop="first", sparse_output=False), cat_cols))

        preprocessor = ColumnTransformer(transformers)
        X = preprocessor.fit_transform(selected_df)

        return X, meta_df

    # ─── CLUSTER + SUMMARIZE ────────────────────────────────────────────────────────
    def cluster_and_summarize(X, meta_df):
        kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init="auto")
        cluster_labels = kmeans.fit_predict(X)
        meta_df["cluster"] = cluster_labels

        print("\n🔍 Sample listings with cluster labels:")
        print(meta_df[["title", "price", "engine", "fuel", "transmission", "city", "time_to_sell", "cluster"]].sample(5))

        summary = (
            meta_df.groupby("cluster")
            .agg(
                count=("title", "count"),
                avg_price=("price", "mean"),
                avg_engine=("engine", "mean"),
                avg_time_to_sell=("time_to_sell", "mean"),
                top_fuel=("fuel", lambda x: x.value_counts().idxmax()),
                top_city=("city", lambda x: x.value_counts().idxmax())
            )
            .reset_index()
        )

        summary["cluster_label"] = summary["cluster"].map(CLUSTER_LABELS)
        summary = summary[["cluster", "cluster_label", "count", "avg_price", "avg_engine", "avg_time_to_sell", "top_fuel", "top_city"]]
        summary = summary.sort_values("avg_price")

        # Round numeric columns
        summary["avg_price"] = summary["avg_price"].round(0).astype(int)
        summary["avg_engine"] = summary["avg_engine"].round(1)
        summary["avg_time_to_sell"] = summary["avg_time_to_sell"].round(1)

        return summary, meta_df



@flow(name="train_forecast_model")
def run_forecast_model():

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



@flow(name="train_price_sensitivity_model")
def run_price_sensitivity_model():

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



@flow(name="train_inventory_model")
def run_inventory_model():
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



@flow(name="train_fast_movers_model")
def run_fast_movers_model():
        # ─── CONFIG ─────────────────────────────────────────────────────────────────────
    DEFAULT_TOP_N = 3
    DEFAULT_MIN_LISTINGS = 1

    # ─── DB CONNECTION ──────────────────────────────────────────────────────────────
    def get_engine():
        return create_engine("postgresql+psycopg2://umar22z:1234@localhost/car_sales")

    # ─── LOAD DATA ──────────────────────────────────────────────────────────────────
    def load_used_cars():
        engine = get_engine()
        query = """
            SELECT title, transmission, fuel, engine, price, mileage,
                city, date_posted, time_to_sell
            FROM used_cars
            WHERE date_posted IS NOT NULL
        """
        df = pd.read_sql(query, engine)

        df["time_to_sell"] = pd.to_numeric(df["time_to_sell"], errors="coerce")
        missing_mask = df["time_to_sell"].isna()
        df.loc[missing_mask, "time_to_sell"] = np.random.randint(0, 61, size=missing_mask.sum())

        df["city"] = df["city"].fillna("Unknown").str.strip().str.title()

        return df

    # ─── PROCESS ─────────────────────────────────────────────────────────────────────
    def get_fastest(df: pd.DataFrame, top_n, min_listings, include_city):
        df["month"] = pd.to_datetime(df["date_posted"]).dt.to_period("M")
        df["model_trim"] = df["title"]

        group_cols = ["month", "model_trim"]
        if include_city:
            group_cols.insert(0, "city")

        grouped = (
            df.groupby(group_cols)
            .agg(avg_time_to_sell=("time_to_sell", "mean"), listings=("model_trim", "count"))
            .reset_index()
        )

        st.write(f"📦 Total groups before filtering: {len(grouped)}")
        grouped = grouped[grouped["listings"] >= min_listings]
        st.write(f"✅ Groups after filtering (≥ {min_listings} listings): {len(grouped)}")

        # Rank and filter top N
        rank_cols = ["month"]
        if include_city:
            rank_cols.insert(0, "city")

        grouped["rank"] = grouped.groupby(rank_cols)["avg_time_to_sell"].rank(method="dense")
        fastest = grouped[grouped["rank"] <= top_n]
        fastest["month"] = fastest["month"].dt.to_timestamp()
        fastest["avg_time_to_sell"] = pd.to_numeric(fastest["avg_time_to_sell"], errors="coerce")

        return fastest

    # ─── VISUALIZE ──────────────────────────────────────────────────────────────────
    def plot_fastest(fastest_df: pd.DataFrame, include_city: bool):
        if fastest_df.empty:
            st.warning("⚠️ No data to plot. Try lowering 'Min Listings'.")
            return

        plt.figure(figsize=(14, 7))

        if include_city:
            g = sns.FacetGrid(fastest_df, col="city", col_wrap=3, sharey=False, height=4)
            g.map_dataframe(sns.lineplot, x="month", y="avg_time_to_sell", hue="model_trim", marker="o")
            g.add_legend()
            g.set_titles("{col_name}")
            g.set_axis_labels("Month", "Avg Days to Sell")
            st.pyplot(g.figure)
        else:
            sns.lineplot(data=fastest_df, x="month", y="avg_time_to_sell", hue="model_trim", marker="o")
            plt.title("Fastest Selling Models by Month")
            plt.ylabel("Average Time to Sell (days)")
            plt.xlabel("Month")
            plt.xticks(rotation=45)
            plt.tight_layout()
            st.pyplot(plt.gcf())
        


@flow(name="train_revenue_optimizer_model")
def run_revenue_optimizer_model():
    DB_URL = "postgresql+psycopg2://umar22z:1234@localhost/car_sales"
    engine = create_engine(DB_URL)

    query = """
        SELECT price, engine, mileage, year, city, transmission, time_to_sell
        FROM used_cars_sim
        WHERE price IS NOT NULL AND time_to_sell IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    df = df.dropna(subset=["price", "engine", "mileage", "year", "city", "transmission", "time_to_sell"])
    df = df[df["price"] > 100000]

    X = df[["price", "engine", "mileage", "year", "city", "transmission"]]
    y = df["time_to_sell"]

    categorical = ["city", "transmission"]
    numeric = ["price", "engine", "mileage", "year"]

    preprocessor = ColumnTransformer(transformers=[
        ("num", SimpleImputer(strategy="median"), numeric),
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical)
    ])

    pipeline = Pipeline(steps=[
        ("pre", preprocessor),
        ("model", RandomForestRegressor(n_estimators=100, random_state=42))
    ])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    pipeline.fit(X_train, y_train)

    score = pipeline.score(X_test, y_test)
    print(f"✅ Revenue Optimizer R² on test set: {score:.2f}")

    joblib.dump(pipeline, "revenue_optimizer_model.pkl")
    print("💾 Model saved as revenue_optimizer_model.pkl")


    
if __name__ == "__main__":
    run_cluster_model()
    run_forecast_model()
    run_price_sensitivity_model()
    run_inventory_model()
    run_fast_movers_model()
    run_revenue_optimizer_model()

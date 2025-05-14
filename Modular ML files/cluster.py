import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

def run_cluster_ui():
    import streamlit as st

    st.title("🧠 Customer Cluster Insights")
    st.info(
        " **What you're seeing:**\n\n"
        "This page clusters car listings based on selected features like `price` and `engine`.\n\n"
        "- Each cluster shows trends like average price, engine size, and sell time.\n"
        "- You can identify market segments like *Budget Hatchbacks* or *Premium SUVs*.\n"
        "- Use this to plan inventory, adjust pricing, and understand buyer demand.\n"
        "- Scroll down to view real listings that match each cluster.\n"
    )

    df = load_data()

    features = st.multiselect(
        "Choose features for clustering:",
        options=["price", "engine", "city", "transmission"],
        default=["price", "engine"]
    )

    if len(features) < 2:
        st.warning("Please select at least two features.")
        return

    X, meta_df = preprocess_data(df, features)
    summary, full = cluster_and_summarize(X, meta_df)

    st.subheader("📊 Cluster Summary")
    st.dataframe(summary)

    st.subheader("🔍 Sample Listings with Cluster Labels")
    st.dataframe(full.sample(5)[["title", "price", "engine", "fuel", "transmission", "city", "time_to_sell", "cluster"]])


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

# # ─── MAIN ───────────────────────────────────────────────────────────────────────
# def main():
#     print(f"🔄 Loading used car data from DB...")
#     df = load_data()

#     print(f"⚙️  Clustering based on: {CLUSTER_FEATURES}")
#     X, meta_df = preprocess_data(df, CLUSTER_FEATURES)

#     print("🤖 Running KMeans...")
#     summary, full = cluster_and_summarize(X, meta_df)

#     summary.to_csv("cluster_summary.csv", index=False)
#     full.to_csv("cluster_assignments.csv", index=False)

#     print("\n📊 Cluster Summary:")
#     print(summary)
#     print("\n💾 Saved: cluster_summary.csv + cluster_assignments.csv")



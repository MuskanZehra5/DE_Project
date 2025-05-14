import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
DB_URL = "postgresql+psycopg2://umar22z:1234@localhost/car_sales"
TARGET_DAYS = 30  # desired max time_to_sell
MARKDOWN_STEPS = [0, 0.05, 0.10, 0.15, 0.20]  # % reductions in price

# ─── LOAD DATA ──────────────────────────────────────────────────────────────────
def load_data():
    engine = create_engine(DB_URL)
    df = pd.read_sql("SELECT * FROM used_cars_sim", engine)
    df = df.dropna(subset=["price", "time_to_sell"])
    df = df[df["price"] > 100000]  # filter valid prices

    # Simulate cost_basis if not already there
    if "cost_basis" not in df.columns:
        np.random.seed(42)
        df["cost_basis"] = df["price"] * np.random.uniform(0.85, 0.95, size=len(df))

    return df

# ─── SIMULATION ─────────────────────────────────────────────────────────────────
def simulate_markdowns(df):
    records = []
    for idx, row in df.iterrows():
        for md in MARKDOWN_STEPS:
            new_price = row["price"] * (1 - md)

            # Simulated effect: more discount = faster sale
            # We'll assume log decay: time_to_sell * (1 - md)^2
            adj_sell_time = row["time_to_sell"] * (1 - md)**2

            profit = new_price - row["cost_basis"]
            margin = profit / new_price if new_price > 0 else 0

            records.append({
                "title": row["title"],
                "original_price": row["price"],
                "markdown": md,
                "new_price": new_price,
                "simulated_sell_time": adj_sell_time,
                "profit": profit,
                "margin": margin
            })

    return pd.DataFrame(records)

# ─── OPTIMIZATION ───────────────────────────────────────────────────────────────
def find_best_options(df_sim):
    best = df_sim[df_sim["simulated_sell_time"] <= TARGET_DAYS]
    idxs = best.groupby("title")["margin"].idxmax()
    return best.loc[idxs]

# ─── VISUALIZATION ──────────────────────────────────────────────────────────────
def plot_for_sample(df_sim, title):
    subset = df_sim[df_sim["title"] == title]
    plt.figure(figsize=(8, 5))
    plt.plot(subset["markdown"] * 100, subset["simulated_sell_time"], label="Sell Time (days)", marker='o')
    plt.plot(subset["markdown"] * 100, subset["margin"] * 100, label="Profit Margin (%)", marker='x')
    plt.xlabel("Markdown (%)")
    plt.title(f"Impact of Markdown: {title[:50]}...")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("revenue_optimizer_sample.png")
    print("📊 Saved: revenue_optimizer_sample.png")

# ─── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    print("🔄 Loading data...")
    df = load_data()

    print("🔁 Simulating markdown effects...")
    df_sim = simulate_markdowns(df)

    print("📈 Finding optimal pricing options...")
    best_df = find_best_options(df_sim)
    best_df.to_csv("best_markdown_recommendations.csv", index=False)
    print("💾 Saved: best_markdown_recommendations.csv")

    print("🎯 Plotting example model...")
    sample_title = best_df.iloc[0]["title"]
    plot_for_sample(df_sim, sample_title)

    print("✅ Done.")

if __name__ == "__main__":
    main()

import streamlit as st

# ─── Import Modular ML Apps ───────────────────────────────────────────────────
import fast_movers #done
import price_sensitivity #done
import inventory #done
import cluster #done
import forecast #done
import optimizer

# ─── Page Settings ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Car Showroom Dashboard",
    layout="wide"
)

# ─── Sidebar Navigation ───────────────────────────────────────────────────────
st.sidebar.title("📊 Dashboard Navigation")
page = st.sidebar.radio("Choose analysis:", [
    "Fast Movers",
    "Price Sensitivity",
    "Inventory Mix",
    "Cluster Insights",
    "Seasonal Forecast",
    "Revenue Optimizer"
])

# ─── Dynamic Routing ──────────────────────────────────────────────────────────
if page == "Fast Movers":
    fast_movers.run_fast_movers_ui()

elif page == "Price Sensitivity":
    price_sensitivity.run_price_sensitivity_ui()

elif page == "Inventory Mix":
    inventory.run_inventory_ui()

elif page == "Cluster Insights":
    cluster.run_cluster_ui()

elif page == "Seasonal Forecast":
    forecast.run_forecast_ui()

elif page == "Revenue Optimizer":
    optimizer.run_optimizer_ui()

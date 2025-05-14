from prefect import flow
from datetime import datetime

# ─── INGESTION FLOWS ───────────────────────────────────────────────────────────
from ingestion_script import (
    ingest_pakwheels,
    ingest_petrol,
    ingest_pama,
    ingest_pbs
)

# ─── ML ANALYSIS FLOWS ─────────────────────────────────────────────────────────
from model_training import (
    run_cluster_model,
    run_forecast_model,
    run_price_sensitivity_model,
    run_inventory_model,
    run_fast_movers_model,
    run_revenue_optimizer_model,
)

# ─── CONDITIONAL LOGIC FOR SCHEDULES ───────────────────────────────────────────
def should_run_biweekly():
    return datetime.now().day % 14 == 0

def should_run_annually():
    return datetime.now().month == 1 and datetime.now().day == 1

# ─── MAIN FLOW ──────────────────────────────────────────────────────────────────
@flow(name="daily_orchestrator_flow")
def daily_pipeline():
    print("🚗 Starting daily data pipeline...")

    # Ingest daily
    ingest_pakwheels()

    # Ingest biweekly
    if should_run_biweekly():
        ingest_petrol()

    # Ingest annually
    if should_run_annually():
        ingest_pama()
        ingest_pbs()

    # Run all ML analyses daily
    run_cluster_model()
    run_forecast_model()
    run_price_sensitivity_model()
    run_inventory_model()
    run_fast_movers_model()
    run_revenue_optimizer_model()

    print("✅ Pipeline complete.")

# ─── LOCAL TEST ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    daily_pipeline()

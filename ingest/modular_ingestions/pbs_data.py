# ingest_pbs_registrations.py

# ingest_pbs_registrations.py

import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin
import pandas as pd
from requests.exceptions import HTTPError

# ───── CONFIG ────────────────────────────────────────────────────────────────
TARGET_DIR = Path(r"D:\LUMS\Spring 2025\DE\DE-final-project\pbs_data")
TARGET_DIR.mkdir(parents=True, exist_ok=True)

PBS_PAGE = "https://opendata.com.pk/dataset/motor-vehicles-registered-by-type-division-and-district-the-punjab-uptil-2021"
# ────────────────────────────────────────────────────────────────────────────────

def fetch_csv_url(page_url: str) -> str:
    """
    Load the PBS page and return the download URL of the CSV,
    by finding the <div class="resource-actions"> → <a href="...csv">.
    """
    r = requests.get(page_url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # find the CSV link inside the resource-actions container
    action_div = soup.select_one("div.resource-actions")
    if not action_div:
        raise RuntimeError("Could not find resource-actions container on PBS page")
    link = action_div.select_one("a[href$='.csv']")
    if not link:
        raise RuntimeError("Could not find CSV link inside resource-actions")
    return urljoin(page_url, link["href"])

def download_csv(csv_url: str, dest: Path) -> bool:
    """Download CSV if missing."""
    if dest.exists():
        print(f"📄 CSV already exists: {dest.name}")
        return True
    print(f"⬇️ Downloading CSV from:\n    {csv_url}")
    try:
        r = requests.get(csv_url)
        r.raise_for_status()
    except HTTPError as e:
        print(f"⚠️ Failed to download CSV: {e}")
        return False
    dest.write_bytes(r.content)
    print(f"✅ Saved CSV to {dest.name}")
    return True

def load_csv(dest: Path) -> pd.DataFrame:
    """Read the CSV into pandas."""
    print(f"📊 Loading {dest.name} into DataFrame")
    df = pd.read_csv(dest)
    print(f"   → {df.shape[0]} rows × {df.shape[1]} columns")
    return df

def main():
    csv_url = fetch_csv_url(PBS_PAGE)
    filename = Path(csv_url).name
    csv_path = TARGET_DIR / filename

    if download_csv(csv_url, csv_path):
        df = load_csv(csv_path)
        out = TARGET_DIR / "pbs_vehicle_registrations_2021.csv"
        df.to_csv(out, index=False)
        print("✅ Ingest complete. Sample:")
        print(df.head())

if __name__ == "__main__":
    main()

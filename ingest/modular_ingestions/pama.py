# ingest_pama.py

import os
import calendar
import datetime
from pathlib import Path

import requests
import pandas as pd
import camelot 

# ───── CONFIG ────────────────────────────────────────────────────────────────
# where to save PDFs and CSVs
TARGET_DIR = Path(r"D:\LUMS\Spring 2025\DE\DE-final-project")
TARGET_DIR.mkdir(parents=True, exist_ok=True)
# ────────────────────────────────────────────────────────────────────────────────

def build_pama_url_and_paths():
    """Return (url, pdf_path, csv_path) for the previous month's PAMA report."""
    today = datetime.date.today()
    # publication folder is this month
    pub_year  = today.year
    pub_month = today.month
    # report is for previous month
    prev = (today.replace(day=1) - datetime.timedelta(days=1))
    month_name = prev.strftime("%B")       # "March"
    year_str   = prev.year                 # 2025
    ym_str     = f"{prev.year}_{prev.month:02d}"  # "2025_03"

    url = (
        f"https://pama.org.pk/wp-content/uploads/"
        f"{pub_year}/{pub_month:02d}/"
        f"Production-Sales-{month_name}-{year_str}.pdf"
    )
    pdf_path = TARGET_DIR / f"PAMA_Production-Sales-{month_name}-{year_str}.pdf"
    csv_path = TARGET_DIR / f"pama_sales_{ym_str}.csv"
    return url, pdf_path, csv_path

def download_pdf(url, dest: Path):
    """Download a PDF from url to dest (if not already present)."""
    if dest.exists():
        print(f"📄 PDF already exists: {dest.name}")
        return
    print(f"⬇️  Downloading PDF from:\n    {url}")
    r = requests.get(url)
    r.raise_for_status()
    dest.write_bytes(r.content)
    print(f"✅ Saved PDF to {dest}")

def pdf_to_csv(pdf_path: Path, csv_path: Path):
    """Extract all tables from pdf_path with Camelot and write to csv_path."""
    print(f"📊 Reading tables from {pdf_path.name} …")
    tables = camelot.read_pdf(
        str(pdf_path),
        pages="all",
        flavor="lattice",      # try 'stream' if lattice misses cells
        edge_tol=300           # tweak if Camelot under- or over-segments
    )
    print(f"   → Found {len(tables)} tables.")

    # concatenate
    dfs = [t.df for t in tables]
    if not dfs:
        print("⚠️ No tables parsed—check Camelot settings.")
        return

    full = pd.concat(dfs, ignore_index=True)
    # assume first row is header
    header = full.iloc[0]
    df     = full[1:].copy()
    df.columns = header
    # optional: drop any completely-empty columns/rows
    df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")

    df.to_csv(csv_path, index=False)
    print(f"✅ Wrote combined CSV to {csv_path.name}")

def main():
    url, pdf_path, csv_path = build_pama_url_and_paths()
    download_pdf(url, pdf_path)
    pdf_to_csv(pdf_path, csv_path)

if __name__ == "__main__":
    main()

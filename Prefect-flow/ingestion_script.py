# from pathlib import Path
# from datetime import datetime, timedelta
# import pandas as pd
# import requests
# import re
# import argparse
# import camelot
# from bs4 import BeautifulSoup
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from urllib.parse import urljoin
# from sqlalchemy import create_engine
# from prefect import flow, task
# import os

# from sqlalchemy.dialects.postgresql import insert as pg_insert
# from sqlalchemy import create_engine, Table, MetaData
# # from prefect.schedules import IntervalSchedule


# # ─── POSTGRES HELPER ───────────────────────────────────────────────────────────


# # def insert_to_postgres(df: pd.DataFrame, table: str):
# #     engine = create_engine("postgresql+psycopg2://umar22z:1234@localhost/car_sales")
# #     with engine.begin() as conn:
# #         df.to_sql(table, con=conn, if_exists="append", index=False)

# # ─── POSTPROCESSING USED CARS ─────────────────────────────────────────────────

# def parse_last_updated(text):
#     if not isinstance(text, str): return timedelta(0)
#     m = re.search(r'(\d+(?:\.\d*)?)\s*(day|days|hr|hrs|hour|hours|min|mins|minute|minutes|week|weeks)', text, re.I)
#     if not m: return timedelta(0)
#     num = float(m.group(1))
#     unit = m.group(2).lower()
#     if unit.startswith('day'): return timedelta(days=num)
#     if unit.startswith('hr') or unit.startswith('hour'): return timedelta(hours=num)
#     if unit.startswith('min'): return timedelta(minutes=num)
#     if unit.startswith('week'): return timedelta(weeks=num)
#     return timedelta(days=num)

# def postprocess_used_cars(prev_path="used_cars_master.csv", curr_path="used_cars2.csv", out_path="used_cars_ready.csv"):
#     from datetime import datetime
#     import os

#     if not os.path.exists(curr_path):
#         raise FileNotFoundError(f"Current data file not found: {curr_path}")

#     if not os.path.exists(prev_path):
#         prev_df = pd.DataFrame(columns=[
#             "listing_id", "title", "price", "mileage", "fuel", "engine", "transmission",
#             "city", "last_updated", "year", "date_posted", "status", "time_to_sell"
#         ])
#         prev_df.to_csv(prev_path, index=False)

#     curr_date = datetime.now()
#     prev_mod_time = datetime.fromtimestamp(os.path.getmtime(prev_path))
#     prev_date = prev_mod_time

#     def load(path, date, is_master=False):
#         df = pd.read_csv(path)
#         df.columns = df.columns.str.strip()

#         if "listing_id" not in df.columns:
#             raise ValueError(f"{path} is missing 'listing_id'. Found columns: {df.columns.tolist()}")

#         df["listing_id"] = df["listing_id"].astype(str)
#         df["price"] = df["price"].astype(str).replace(r"[^\d.]", "", regex=True)
#         df["mileage"] = df["mileage"].astype(str).replace(r"[^\d.]", "", regex=True)
#         df["engine"] = df["engine"].astype(str).replace(r"[^\d.]", "", regex=True)

#         if is_master:
#             df["date_posted"] = pd.to_datetime(df.get("date_posted"), errors="coerce")
#             df["status"] = df.get("status", "")
#             df["time_to_sell"] = pd.to_numeric(df.get("time_to_sell"), errors="coerce")
#         else:
#             deltas = df["last_updated"].apply(parse_last_updated)
#             df["date_posted"] = date - deltas
#             df["status"] = ""
#             df["time_to_sell"] = None

#         return df.set_index("listing_id")

#     df_prev = load(prev_path, prev_date, is_master=True)
#     df_curr = load(curr_path, curr_date, is_master=False)

#     prev_ids = set(df_prev.index)
#     curr_ids = set(df_curr.index)

#     merged = []

#     for idx, row in df_curr.iterrows():
#         r = row.to_dict()
#         r["listing_id"] = idx  # ✅ make sure listing_id is in every row
#         if idx in prev_ids:
#             r["status"] = "present"
#             r["date_posted"] = df_prev.at[idx, "date_posted"]
#         else:
#             r["status"] = "new"
#         merged.append(r)

#     for idx in prev_ids - curr_ids:
#         row = df_prev.loc[idx]
#         r = row.to_dict()
#         r["listing_id"] = idx  # ✅ make sure listing_id is kept
#         r["status"] = "sold"
#         try:
#             posted_date = pd.to_datetime(r["date_posted"])
#             delta = curr_date - posted_date
#             r["time_to_sell"] = round(delta.total_seconds() / 86400, 2)
#         except:
#             r["time_to_sell"] = None
#         merged.append(r)

#     df_out = pd.DataFrame(merged)

#     df_out = df_out.drop(columns=["color", "body_type"], errors="ignore")

#     df_out["listing_id"] = df_out["listing_id"].astype("int64")
#     df_out["price"] = pd.to_numeric(df_out["price"], errors="coerce")
#     df_out["mileage"] = pd.to_numeric(df_out["mileage"], errors="coerce")
#     df_out["engine"] = pd.to_numeric(df_out["engine"], errors="coerce")
#     df_out["time_to_sell"] = pd.to_numeric(df_out["time_to_sell"], errors="coerce")
#     df_out["year"] = pd.to_numeric(df_out["year"], errors="coerce").astype("Int64")
#     df_out["date_posted"] = pd.to_datetime(df_out["date_posted"], errors="coerce")

#     final_columns = [
#         "listing_id", "title", "price", "mileage", "fuel", "engine", "transmission",
#         "city", "last_updated", "year", "date_posted", "status", "time_to_sell"
#     ]
#     df_out = df_out[final_columns]
#     ######################################################
#     #Fill missing date_posted using last_updated deltas
#     fallback_mask = df_out["date_posted"].isna() & df_out["last_updated"].notna()
#     if fallback_mask.any():
#         fallback_deltas = df_out.loc[fallback_mask, "last_updated"].apply(parse_last_updated)
#         df_out.loc[fallback_mask, "date_posted"] = curr_date - fallback_deltas
#         print(f"🛠️ Filled {fallback_mask.sum()} missing date_posted values from last_updated.")
#     ####################################################
#     df_out.to_csv(out_path, index=False, encoding="utf-8", lineterminator="\n")

#     return df_out



# def postprocess_petrol(df):
#     df = df.copy()
#     df["effective_date"] = df["effective_date"].str.replace("Effective From: ", "", regex=False)
#     df["effective_date"] = pd.to_datetime(df["effective_date"], errors="coerce")
#     df.rename(columns={
#         "PREMIER EURO 5": "premier_euro_5",
#         "HI-CETANE DIESEL EURO 5": "hi_cetane_diesel_euro_5"
#     }, inplace=True)
#     return df[["effective_date", "premier_euro_5", "hi_cetane_diesel_euro_5"]]

# def postprocess_pama(df):
#     tidy = []
#     for i in range(1, len(df)):
#         row = df.iloc[i]
#         year = row["Year"]
#         car_model = row["PASSENGER CARS"]
#         type_row = row["Unnamed: 2"]
#         if pd.notna(car_model) and type_row in ["Prod.", "Sale"]:
#             for col in df.columns[3:]:
#                 month = df.iloc[0][col]
#                 val = pd.to_numeric(row[col], errors="coerce")
#                 tidy.append({
#                     "year": year,
#                     "car_model": car_model,
#                     "type": type_row,
#                     "month": month,
#                     "units": val
#                 })
#     return pd.DataFrame(tidy)

# def postprocess_pbs(df):
#     rename = {
#         "Division/ District": "district",
#         "Total": "total",
#         "Motor Cars, Jeeps and Station Wagons": "cars_jeeps_station_wagons",
#         "Motor Cycles and Scoo- ters": "motorcycles_scooters",
#         "Trucks": "trucks",
#         "Pick- ups/ Deli- very Vans": "pickups_delivery_vans",
#         "Mini Buses/ Buses/ Flying/ Luxury Coaches": "buses_coaches",
#         "Taxis": "taxis",
#         "Auto Rick- shaws": "auto_rickshaws",
#         "Tractors": "tractors",
#         "Other Vehicles": "other_vehicles"
#     }
#     df = df.rename(columns=rename)
#     for col in df.columns[1:]:
#         df[col] = df[col].astype(str).str.replace(",", "").astype(float).astype("Int64")
#     return df


# # ─── PREFECT FLOWS ──────────────────────────────────────────────────────────────

# #@task
# def init_driver():
#     opts = webdriver.ChromeOptions()
#     driver = webdriver.Chrome(options=opts)
#     driver.set_page_load_timeout(60)
#     driver.implicitly_wait(10)
#     return driver

# #@task
# def close_popup(driver):
#     try:
#         WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, "onesignal-slidedown-cancel-button"))).click()
#     except Exception: pass

# #@task
# def safe_get(driver, url):
#     try: driver.get(url)
#     except Exception: driver.get(url)

# #@task
# def scrape_used_cars(driver):
#     BASE_URL = "https://www.pakwheels.com"
#     TEST_BODY = "sedan"
#     safe_get(driver, f"{BASE_URL}/used-cars/")
#     close_popup(driver)
#     WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//h2[text()='Used Cars by Body Type']")))
#     els = driver.find_elements(By.CSS_SELECTOR, "#browesBTSlider ul.browse-listing li a")
#     body_types = [(e.get_attribute("title"), e.get_attribute("href")) for e in els]
#     body_types = [bt for bt in body_types if bt[0].lower().startswith(TEST_BODY)]
#     rows = []

#     for bt_name, bt_url in body_types:
#         safe_get(driver, bt_url)
#         close_popup(driver)
#         try:
#             driver.find_element(By.CSS_SELECTOR, "a.more-link").click()
#         except:
#             pass

#         while True:
#             soup = BeautifulSoup(driver.page_source, "html.parser")
#             ads = soup.select("ul.search-results li[data-listing-id]")

#             for ad in ads:
#                 data = {"body_type": bt_name}
#                 data["listing_id"] = ad.get("data-listing-id")

#                 title_el = ad.select_one("a.car-name.ad-detail-path")
#                 data["title"] = title_el.get_text(" ", strip=True).replace("for Sale", "").strip()

#                 price_div = title_el.find_parent("div", class_="search-title-row").select_one(".price-details")
#                 data["price"] = price_div.get_text(" ", strip=True) if price_div else ""
#                 data["color"] = " ".join([c for c in (price_div or {}).get("class", []) if c != "price-details"]).replace("-", " ")

#                 info_ul = title_el.find_next("ul", class_="search-vehicle-info-2")
#                 vals = [li.get_text(strip=True) for li in (info_ul or []).find_all("li")] if info_ul else []
#                 data.update({
#                     "year": vals[0] if len(vals) > 0 else "",
#                     "mileage": vals[1] if len(vals) > 1 else "",
#                     "fuel": vals[2] if len(vals) > 2 else "",
#                     "engine": vals[3] if len(vals) > 3 else "",
#                     "transmission": vals[4] if len(vals) > 4 else "",
#                 })

#                 city_ul = ad.select_one("ul.search-vehicle-info.fs13")
#                 data["city"] = city_ul.select_one("li").get_text(strip=True) if city_ul else ""

#                 dated = ad.select_one("div.search-bottom.clearfix .pull-left.dated")
#                 data["last_updated"] = dated.get_text(strip=True) if dated else ""

#                 rows.append(data)

#             nxt = soup.select_one("a[rel='next']")
#             if not nxt:
#                 break
#             safe_get(driver, urljoin(BASE_URL, nxt['href']))
#             close_popup(driver)

#     return pd.DataFrame(rows)













# from sqlalchemy.dialects.postgresql import insert as pg_insert
# from sqlalchemy import create_engine, Table, MetaData

# def insert_to_postgres(df: pd.DataFrame, table_name: str, conflict_column: str = None):
#     engine = create_engine("postgresql+psycopg2://umar22z:1234@localhost/car_sales")

#     metadata = MetaData()
#     metadata.reflect(bind=engine)
#     table = metadata.tables.get(table_name)

#     if table is None:
#         raise ValueError(f"Table '{table_name}' does not exist in the database.")

#     with engine.begin() as conn:
#         for _, row in df.iterrows():
#             record = row.to_dict()

#             # 🔒 Skip invalid datetimes (NaT)
#             if any(str(v) == "NaT" for v in record.values()):
#                 continue

#             insert_stmt = pg_insert(table).values(**record)

#             # ✅ Apply ON CONFLICT only if explicitly specified
#             if conflict_column and conflict_column in record:
#                 stmt = insert_stmt.on_conflict_do_nothing(index_elements=[conflict_column])
#             else:
#                 stmt = insert_stmt

#             conn.execute(stmt)


# ###################################################################################################


# #@flow(name="ingest-pakwheels")
# def ingest_pakwheels():
#     try:
#         driver = init_driver()
#         df_scraped = scrape_used_cars(driver)
#         for col in df_scraped.columns:
#             df_scraped[col] = df_scraped[col].apply(
#             lambda x: list(x.values())[0] if isinstance(x, dict) and len(x) == 1 else x
#             )

#         driver.quit()

#         if df_scraped.empty or "listing_id" not in df_scraped.columns:
#             raise ValueError("Scraping failed — 'listing_id' column missing or no data returned.")

#         df_scraped.to_csv("used_cars2.csv", index=False)

#         df_merged = postprocess_used_cars()
#         insert_to_postgres(df_merged, "used_cars", conflict_column="listing_id")


#         df_master_next = df_merged[df_merged["status"] != "sold"].copy()
#         df_master_next.to_csv("used_cars_master.csv", index=False)

#         print("✅ Ingestion and update completed successfully.")

#     except Exception as e:
#         print(f"❌ Error in ingest_pakwheels flow: {e}")


# #@task
# def fetch_pso_prices():
#     URL = "https://psopk.com/en/fuels/fuel-prices"
#     r = requests.get(URL); r.raise_for_status()
#     soup = BeautifulSoup(r.text, "html.parser")
#     tbody = soup.select_one("section.accordSec table.uk-table tbody")
#     wanted = {"PREMIER EURO 5", "HI-CETANE DIESEL EURO 5"}
#     prices = {}
#     for tr in tbody.find_all("tr"):
#         name = tr.find_all("td")[0].get_text(strip=True)
#         if name in wanted:
#             txt = tr.find_all("td")[1].get_text(strip=True)
#             val = float(txt.replace("Rs.", "").replace("/Ltr", ""))
#             prices[name] = val
#     return prices

# #@flow(name="ingest-petrol-prices")
# def ingest_petrol():
#     prices = fetch_pso_prices()
#     today = datetime.now().strftime("%B %d, %Y")
#     row = {"effective_date": f"Effective From: {today}", **prices}
#     arch = Path("psopk_petrol_archives.csv")
#     df_new = pd.DataFrame([row])
#     df_old = pd.read_csv(arch) if arch.exists() else pd.DataFrame()
#     df_all = pd.concat([df_old, df_new], ignore_index=True)
#     df_all.to_csv(arch, index=False)
#     df_clean = postprocess_petrol(df_all)
#     insert_to_postgres(df_clean, "fuel_prices", conflict_column="effective_date")


# #@flow(name="ingest-pama")

# def ingest_pama():
#     today = datetime.today()
#     prev = (today.replace(day=1) - timedelta(days=1))
#     mname = prev.strftime("%B")
#     year = prev.year

#     url = f"https://pama.org.pk/wp-content/uploads/{today.year}/{today.month:02d}/Production-Sales-{mname}-{year}.pdf"
#     pdf = Path(f"PAMA_Production-Sales-{mname}-{year}.pdf")
#     csv = Path(f"pama_sales_{year}_{prev.month:02d}.csv")

#     if not pdf.exists():
#         try:
#             r = requests.get(url)
#             r.raise_for_status()
#             pdf.write_bytes(r.content)
#         except requests.exceptions.HTTPError:
#             print(f"⚠️ PAMA report not available for {mname} {year}. Skipping ingestion.")
#             return

#     try:
#         tables = camelot.read_pdf(str(pdf), pages="all", flavor="lattice")
#         df_raw = pd.concat([t.df for t in tables], ignore_index=True)
#         df_raw.columns = df_raw.iloc[0]
#         df_clean = postprocess_pama(df_raw)
#         df_clean.to_csv(csv, index=False)
#         insert_to_postgres(df_clean, "pama_sales", conflict_column=None)

#         print(f"✅ PAMA data ingested for {mname} {year}")
#     except Exception as e:
#         print(f"❌ Error processing PAMA PDF: {e}")

# #@task
# def fetch_csv_url(page):
#     r = requests.get(page); r.raise_for_status()
#     soup = BeautifulSoup(r.text, "html.parser")
#     return soup.select_one("div.resource-actions a[href$='.csv']")['href']

# #@task
# def download_csv(csv_url, dest):
#     if not dest.exists():
#         r = requests.get(csv_url); r.raise_for_status()
#         dest.write_bytes(r.content)

# #@flow(name="ingest-pbs")
# def ingest_pbs():
#     url = fetch_csv_url("https://opendata.com.pk/dataset/motor-vehicles-registered-by-type-division-and-district-the-punjab-uptil-2021")
#     csv_path = Path("pbs_vehicle_registrations.csv")
#     download_csv(url, csv_path)
#     df_raw = pd.read_csv(csv_path)
#     df_clean = postprocess_pbs(df_raw)
#     df_clean.to_csv("pbs_vehicle_registrations_clean.csv", index=False)
#     insert_to_postgres(df_clean, "vehicle_registrations", conflict_column=None)




# os.environ["PREFECT_API_DATABASE_CONNECTION_URL"] = "memory"


# if __name__ == "__main__":
#     ingest_pakwheels()
#     ingest_petrol()
#     ingest_pama()
#     ingest_pbs()






######################################################################################################
######################################################################################################
######################################################################################################
######################################################################################################
######################################################################################################

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import requests
import re
import argparse
import camelot
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin
from sqlalchemy import create_engine
from prefect import flow, task
import os

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import create_engine, Table, MetaData
# from prefect.schedules import IntervalSchedule


# ─── POSTGRES HELPER ───────────────────────────────────────────────────────────


# def insert_to_postgres(df: pd.DataFrame, table: str):
#     engine = create_engine("postgresql+psycopg2://umar22z:1234@localhost/car_sales")
#     with engine.begin() as conn:
#         df.to_sql(table, con=conn, if_exists="append", index=False)

# ─── POSTPROCESSING USED CARS ─────────────────────────────────────────────────

def parse_last_updated(text):
    if not isinstance(text, str): return timedelta(0)
    m = re.search(r'(\d+(?:\.\d*)?)\s*(day|days|hr|hrs|hour|hours|min|mins|minute|minutes|week|weeks)', text, re.I)
    if not m: return timedelta(0)
    num = float(m.group(1))
    unit = m.group(2).lower()
    if unit.startswith('day'): return timedelta(days=num)
    if unit.startswith('hr') or unit.startswith('hour'): return timedelta(hours=num)
    if unit.startswith('min'): return timedelta(minutes=num)
    if unit.startswith('week'): return timedelta(weeks=num)
    return timedelta(days=num)

def postprocess_used_cars(prev_path="used_cars_master.csv", curr_path="used_cars2.csv", out_path="used_cars_ready.csv"):
    from datetime import datetime
    import os

    if not os.path.exists(curr_path):
        raise FileNotFoundError(f"Current data file not found: {curr_path}")

    if not os.path.exists(prev_path):
        prev_df = pd.DataFrame(columns=[
            "listing_id", "title", "price", "mileage", "fuel", "engine", "transmission",
            "city", "last_updated", "year", "date_posted", "status", "time_to_sell"
        ])
        prev_df.to_csv(prev_path, index=False)

    curr_date = datetime.now()
    prev_mod_time = datetime.fromtimestamp(os.path.getmtime(prev_path))
    prev_date = prev_mod_time

    def load(path, date, is_master=False):
        df = pd.read_csv(path)
        df.columns = df.columns.str.strip()

        if "listing_id" not in df.columns:
            raise ValueError(f"{path} is missing 'listing_id'. Found columns: {df.columns.tolist()}")

        df["listing_id"] = df["listing_id"].astype(str)
        df["price"] = df["price"].astype(str).replace(r"[^\d.]", "", regex=True)
        df["mileage"] = df["mileage"].astype(str).replace(r"[^\d.]", "", regex=True)
        df["engine"] = df["engine"].astype(str).replace(r"[^\d.]", "", regex=True)

        if is_master:
            df["date_posted"] = pd.to_datetime(df.get("date_posted"), errors="coerce")
            df["status"] = df.get("status", "")
            df["time_to_sell"] = pd.to_numeric(df.get("time_to_sell"), errors="coerce")
        else:
            deltas = df["last_updated"].apply(parse_last_updated)
            df["date_posted"] = date - deltas
            df["status"] = ""
            df["time_to_sell"] = None

        return df.set_index("listing_id")

    df_prev = load(prev_path, prev_date, is_master=True)
    df_curr = load(curr_path, curr_date, is_master=False)

    prev_ids = set(df_prev.index)
    curr_ids = set(df_curr.index)

    merged = []

    for idx, row in df_curr.iterrows():
        r = row.to_dict()
        r["listing_id"] = idx  # ✅ make sure listing_id is in every row
        if idx in prev_ids:
            r["status"] = "present"
            r["date_posted"] = df_prev.at[idx, "date_posted"]
        else:
            r["status"] = "new"
        merged.append(r)

    for idx in prev_ids - curr_ids:
        row = df_prev.loc[idx]
        r = row.to_dict()
        r["listing_id"] = idx  # ✅ make sure listing_id is kept
        r["status"] = "sold"
        try:
            posted_date = pd.to_datetime(r["date_posted"])
            delta = curr_date - posted_date
            r["time_to_sell"] = round(delta.total_seconds() / 86400, 2)
        except:
            r["time_to_sell"] = None
        merged.append(r)

    df_out = pd.DataFrame(merged)

    df_out = df_out.drop(columns=["color", "body_type"], errors="ignore")

    df_out["listing_id"] = df_out["listing_id"].astype("int64")
    df_out["price"] = pd.to_numeric(df_out["price"], errors="coerce")
    df_out["mileage"] = pd.to_numeric(df_out["mileage"], errors="coerce")
    df_out["engine"] = pd.to_numeric(df_out["engine"], errors="coerce")
    df_out["time_to_sell"] = pd.to_numeric(df_out["time_to_sell"], errors="coerce")
    df_out["year"] = pd.to_numeric(df_out["year"], errors="coerce").astype("Int64")
    df_out["date_posted"] = pd.to_datetime(df_out["date_posted"], errors="coerce")

    final_columns = [
        "listing_id", "title", "price", "mileage", "fuel", "engine", "transmission",
        "city", "last_updated", "year", "date_posted", "status", "time_to_sell"
    ]
    df_out = df_out[final_columns]

    df_out.to_csv(out_path, index=False, encoding="utf-8", lineterminator="\n")

    return df_out



def postprocess_petrol(df):
    df = df.copy()
    df["effective_date"] = df["effective_date"].str.replace("Effective From: ", "", regex=False)
    df["effective_date"] = pd.to_datetime(df["effective_date"], errors="coerce")
    df.rename(columns={
        "PREMIER EURO 5": "premier_euro_5",
        "HI-CETANE DIESEL EURO 5": "hi_cetane_diesel_euro_5"
    }, inplace=True)
    return df[["effective_date", "premier_euro_5", "hi_cetane_diesel_euro_5"]]

def postprocess_pama(df):
    tidy = []
    for i in range(1, len(df)):
        row = df.iloc[i]
        year = row["Year"]
        car_model = row["PASSENGER CARS"]
        type_row = row["Unnamed: 2"]
        if pd.notna(car_model) and type_row in ["Prod.", "Sale"]:
            for col in df.columns[3:]:
                month = df.iloc[0][col]
                val = pd.to_numeric(row[col], errors="coerce")
                tidy.append({
                    "year": year,
                    "car_model": car_model,
                    "type": type_row,
                    "month": month,
                    "units": val
                })
    return pd.DataFrame(tidy)

def postprocess_pbs(df):
    rename = {
        "Division/ District": "district",
        "Total": "total",
        "Motor Cars, Jeeps and Station Wagons": "cars_jeeps_station_wagons",
        "Motor Cycles and Scoo- ters": "motorcycles_scooters",
        "Trucks": "trucks",
        "Pick- ups/ Deli- very Vans": "pickups_delivery_vans",
        "Mini Buses/ Buses/ Flying/ Luxury Coaches": "buses_coaches",
        "Taxis": "taxis",
        "Auto Rick- shaws": "auto_rickshaws",
        "Tractors": "tractors",
        "Other Vehicles": "other_vehicles"
    }
    df = df.rename(columns=rename)
    for col in df.columns[1:]:
        df[col] = df[col].astype(str).str.replace(",", "").astype(float).astype("Int64")
    return df


# ─── PREFECT FLOWS ──────────────────────────────────────────────────────────────

#@task
def init_driver():
    opts = webdriver.ChromeOptions()
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(10)
    return driver

#@task
def close_popup(driver):
    try:
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, "onesignal-slidedown-cancel-button"))).click()
    except Exception: pass

#@task
def safe_get(driver, url):
    try: driver.get(url)
    except Exception: driver.get(url)

#@task
def scrape_used_cars(driver):
    BASE_URL = "https://www.pakwheels.com"
    TEST_BODY = "sedan"
    safe_get(driver, f"{BASE_URL}/used-cars/")
    close_popup(driver)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//h2[text()='Used Cars by Body Type']")))
    els = driver.find_elements(By.CSS_SELECTOR, "#browesBTSlider ul.browse-listing li a")
    body_types = [(e.get_attribute("title"), e.get_attribute("href")) for e in els]
    body_types = [bt for bt in body_types if bt[0].lower().startswith(TEST_BODY)]
    rows = []

    for bt_name, bt_url in body_types:
        safe_get(driver, bt_url)
        close_popup(driver)
        try:
            driver.find_element(By.CSS_SELECTOR, "a.more-link").click()
        except:
            pass

        while True:
            soup = BeautifulSoup(driver.page_source, "html.parser")
            ads = soup.select("ul.search-results li[data-listing-id]")

            for ad in ads:
                data = {"body_type": bt_name}
                data["listing_id"] = ad.get("data-listing-id")

                title_el = ad.select_one("a.car-name.ad-detail-path")
                data["title"] = title_el.get_text(" ", strip=True).replace("for Sale", "").strip()

                price_div = title_el.find_parent("div", class_="search-title-row").select_one(".price-details")
                data["price"] = price_div.get_text(" ", strip=True) if price_div else ""
                data["color"] = " ".join([c for c in (price_div or {}).get("class", []) if c != "price-details"]).replace("-", " ")

                info_ul = title_el.find_next("ul", class_="search-vehicle-info-2")
                vals = [li.get_text(strip=True) for li in (info_ul or []).find_all("li")] if info_ul else []
                data.update({
                    "year": vals[0] if len(vals) > 0 else "",
                    "mileage": vals[1] if len(vals) > 1 else "",
                    "fuel": vals[2] if len(vals) > 2 else "",
                    "engine": vals[3] if len(vals) > 3 else "",
                    "transmission": vals[4] if len(vals) > 4 else "",
                })

                city_ul = ad.select_one("ul.search-vehicle-info.fs13")
                data["city"] = city_ul.select_one("li").get_text(strip=True) if city_ul else ""

                dated = ad.select_one("div.search-bottom.clearfix .pull-left.dated")
                data["last_updated"] = dated.get_text(strip=True) if dated else ""

                rows.append(data)

            nxt = soup.select_one("a[rel='next']")
            if not nxt:
                break
            safe_get(driver, urljoin(BASE_URL, nxt['href']))
            close_popup(driver)

    return pd.DataFrame(rows)













from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import create_engine, Table, MetaData

def insert_to_postgres(df: pd.DataFrame, table_name: str, conflict_column: str = None):
    engine = create_engine("postgresql+psycopg2://umar22z:1234@localhost/car_sales")

    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables.get(table_name)

    if table is None:
        raise ValueError(f"Table '{table_name}' does not exist in the database.")

    with engine.begin() as conn:
        for _, row in df.iterrows():
            record = row.to_dict()

            # 🔒 Skip invalid datetimes (NaT)
            if any(str(v) == "NaT" for v in record.values()):
                continue

            insert_stmt = pg_insert(table).values(**record)

            # ✅ Apply ON CONFLICT only if explicitly specified
            if conflict_column and conflict_column in record:
                stmt = insert_stmt.on_conflict_do_nothing(index_elements=[conflict_column])
            else:
                stmt = insert_stmt

            conn.execute(stmt)


###################################################################################################








#@flow(name="ingest-pakwheels")
#@flow(name="ingest-pakwheels")
#@flow(name="ingest-pakwheels")
def ingest_pakwheels():
    try:
        driver = init_driver()
        df_scraped = scrape_used_cars(driver)
        driver.quit()

        if df_scraped.empty or "listing_id" not in df_scraped.columns:
            raise ValueError("Scraping failed — 'listing_id' column missing or no data returned.")

        df_scraped.to_csv("used_cars2.csv", index=False)

        df_merged = postprocess_used_cars()
        insert_to_postgres(df_merged, "used_cars", conflict_column="listing_id")


        df_master_next = df_merged[df_merged["status"] != "sold"].copy()
        df_master_next.to_csv("used_cars_master.csv", index=False)

        print("✅ Ingestion and update completed successfully.")

    except Exception as e:
        print(f"❌ Error in ingest_pakwheels flow: {e}")


#@task
def fetch_pso_prices():
    URL = "https://psopk.com/en/fuels/fuel-prices"
    r = requests.get(URL); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    tbody = soup.select_one("section.accordSec table.uk-table tbody")
    wanted = {"PREMIER EURO 5", "HI-CETANE DIESEL EURO 5"}
    prices = {}
    for tr in tbody.find_all("tr"):
        name = tr.find_all("td")[0].get_text(strip=True)
        if name in wanted:
            txt = tr.find_all("td")[1].get_text(strip=True)
            val = float(txt.replace("Rs.", "").replace("/Ltr", ""))
            prices[name] = val
    return prices

#@flow(name="ingest-petrol-prices")
def ingest_petrol():
    prices = fetch_pso_prices()
    today = datetime.now().strftime("%B %d, %Y")
    row = {"effective_date": f"Effective From: {today}", **prices}
    arch = Path("psopk_petrol_archives.csv")
    df_new = pd.DataFrame([row])
    df_old = pd.read_csv(arch) if arch.exists() else pd.DataFrame()
    df_all = pd.concat([df_old, df_new], ignore_index=True)
    df_all.to_csv(arch, index=False)
    df_clean = postprocess_petrol(df_all)
    insert_to_postgres(df_clean, "fuel_prices", conflict_column="effective_date")


#@flow(name="ingest-pama")

def ingest_pama():
    today = datetime.today()
    prev = (today.replace(day=1) - timedelta(days=1))
    mname = prev.strftime("%B")
    year = prev.year

    url = f"https://pama.org.pk/wp-content/uploads/{today.year}/{today.month:02d}/Production-Sales-{mname}-{year}.pdf"
    pdf = Path(f"PAMA_Production-Sales-{mname}-{year}.pdf")
    csv = Path(f"pama_sales_{year}_{prev.month:02d}.csv")

    if not pdf.exists():
        try:
            r = requests.get(url)
            r.raise_for_status()
            pdf.write_bytes(r.content)
        except requests.exceptions.HTTPError:
            print(f"⚠️ PAMA report not available for {mname} {year}. Skipping ingestion.")
            return

    try:
        tables = camelot.read_pdf(str(pdf), pages="all", flavor="lattice")
        df_raw = pd.concat([t.df for t in tables], ignore_index=True)
        df_raw.columns = df_raw.iloc[0]
        df_clean = postprocess_pama(df_raw)
        df_clean.to_csv(csv, index=False)
        insert_to_postgres(df_clean, "pama_sales", conflict_column=None)

        print(f"✅ PAMA data ingested for {mname} {year}")
    except Exception as e:
        print(f"❌ Error processing PAMA PDF: {e}")

#@task
def fetch_csv_url(page):
    r = requests.get(page); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    return soup.select_one("div.resource-actions a[href$='.csv']")['href']

#@task
def download_csv(csv_url, dest):
    if not dest.exists():
        r = requests.get(csv_url); r.raise_for_status()
        dest.write_bytes(r.content)

#@flow(name="ingest-pbs")
def ingest_pbs():
    url = fetch_csv_url("https://opendata.com.pk/dataset/motor-vehicles-registered-by-type-division-and-district-the-punjab-uptil-2021")
    csv_path = Path("pbs_vehicle_registrations.csv")
    download_csv(url, csv_path)
    df_raw = pd.read_csv(csv_path)
    df_clean = postprocess_pbs(df_raw)
    df_clean.to_csv("pbs_vehicle_registrations_clean.csv", index=False)
    insert_to_postgres(df_clean, "vehicle_registrations", conflict_column=None)




os.environ["PREFECT_API_DATABASE_CONNECTION_URL"] = "memory"


if __name__ == "__main__":
    ingest_pakwheels()
    ingest_petrol()
    ingest_pama()
    ingest_pbs()

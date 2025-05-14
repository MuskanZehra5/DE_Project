# import time
# from urllib.parse import urljoin

# import pandas as pd
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from bs4 import BeautifulSoup


# # ──────── CONFIG ──────────
# TEST_BODY_TYPES = ["sedan"]    # change this to whichever body type you want to test
# BASE_URL         = "https://www.pakwheels.com"
# # ──────────────────────────

# def init_driver():
#     options = webdriver.ChromeOptions()
#     # options.add_argument("--headless")
#     driver = webdriver.Chrome(options=options)
#     driver.command_executor._client_config.timeout = 120
#     driver.set_page_load_timeout(60)
#     driver.implicitly_wait(10)
#     driver.maximize_window()
#     return driver

# def close_popup(driver):
#     try:
#         btn = WebDriverWait(driver, 5).until(
#             EC.element_to_be_clickable((By.ID, "onesignal-slidedown-cancel-button"))
#         )
#         btn.click()
#     except Exception:
#         pass

# def safe_get(driver, url):
#     try:
#         driver.get(url)
#     except Exception:
#         print(f"⚠️ Timeout on {url}, retrying…")
#         driver.get(url)

# def scrape_used_cars(driver):
#     safe_get(driver, f"{BASE_URL}/used-cars/")
#     close_popup(driver)
#     WebDriverWait(driver, 10).until(
#         EC.presence_of_element_located((By.XPATH, "//h2[text()='Used Cars by Body Type']"))
#     )

#     # 1) collect all body-type links
#     els = driver.find_elements(
#         By.CSS_SELECTOR,
#         "#browesBTSlider ul.browse-listing li a"
#     )
#     body_types = [(e.get_attribute("title"), e.get_attribute("href")) for e in els]
#     # filter to test run
#     body_types = [bt for bt in body_types if bt[0].lower().startswith(TEST_BODY_TYPES[0].lower())]
#     print("→ Testing body types:", body_types)

#     rows = []
#     for bt_name, bt_url in body_types:
#         print(f"[USED] scraping body type {bt_name} → {bt_url}")
#         safe_get(driver, bt_url)
#         close_popup(driver)
#         time.sleep(1)

#         # click “View All” to get every listing
#         try:
#             view_all = driver.find_element(By.CSS_SELECTOR, "a.more-link")
#             print("  → clicking View All")
#             view_all.click()
#             time.sleep(1)
#         except Exception:
#             print("  ⚠️ No View All button found—continuing with current page")

#         while True:
#             soup = BeautifulSoup(driver.page_source, "html.parser")
#             ads = soup.select("ul.search-results li[data-listing-id]")
#             print(f"  → found {len(ads)} ads on this page")

#             for ad in ads:
#                 # grab the built-in listing ID
#                 listing_id = ad.get("data-listing-id", "").strip()

#                 data = {
#                     "id": listing_id,
#                     "body_type": bt_name
#                 }

#                 # title
#                 title_el = ad.select_one("a.car-name.ad-detail-path")
#                 data["title"] = title_el.get_text(" ", strip=True).replace("for Sale", "").strip()

#                 # price & color
#                 price_div = title_el.find_parent("div", class_="search-title-row")\
#                                     .select_one(".price-details")
#                 data["price"] = price_div.get_text(" ", strip=True) if price_div else ""
#                 if price_div:
#                     cls = [c for c in price_div.get("class", []) if c!="price-details"]
#                     data["color"] = " ".join(cls).replace("-", " ")
#                 else:
#                     data["color"] = ""

#                 # DETAILS list (year, mileage, fuel, engine, transmission)
#                 info_ul = title_el.find_next("ul", class_="search-vehicle-info-2")
#                 if info_ul:
#                     vals = [li.get_text(strip=True) for li in info_ul.find_all("li")]
#                     data.update({
#                         "year":         vals[0] if len(vals)>0 else "",
#                         "mileage":      vals[1] if len(vals)>1 else "",
#                         "fuel":         vals[2] if len(vals)>2 else "",
#                         "engine":       vals[3] if len(vals)>3 else "",
#                         "transmission": vals[4] if len(vals)>4 else "",
#                     })

#                 # city
#                 city_ul = ad.select_one("ul.search-vehicle-info.fs13")
#                 data["city"] = city_ul.select_one("li").get_text(strip=True) if city_ul else ""

#                 # last-updated
#                 dated = ad.select_one("div.search-bottom.clearfix .pull-left.dated")
#                 data["last_updated"] = dated.get_text(strip=True) if dated else ""

#                 print("    →", data)
#                 rows.append(data)

#             # next page?
#             nxt = soup.select_one("a[rel='next']")
#             if not nxt:
#                 break
#             next_url = urljoin(BASE_URL, nxt["href"])
#             print("  → moving to next page:", next_url)
#             safe_get(driver, next_url)
#             close_popup(driver)
#             time.sleep(1)

#     df = pd.DataFrame(rows)
#     df.to_csv("used_cars2.csv", index=False)
#     print("✅ used_cars2.csv written with columns:", df.columns.tolist())

# def scrape_new_cars(driver):
#     safe_get(driver, f"{BASE_URL}/new-cars/")
#     close_popup(driver)
#     time.sleep(2)  # give the page a moment to settle
    
#     # debug: dump page title and first 200 chars of source
#     print("📋 Page title:", driver.title)
#     snippet = driver.page_source[:500].replace("\n", " ")
#     print("📋 Page source snippet:", snippet, "…")
    
#     # wait for the body-type list to appear
#     try:
#         WebDriverWait(driver, 10).until(
#             EC.presence_of_element_located((By.CSS_SELECTOR, "ul.browse-listing.row.mb0.clearfix li a"))
#         )
#     except Exception as e:
#         print("⚠️ Timeout waiting for new-cars body-type links:", e)
#         # proceed anyway; if selector is wrong we'll catch zero links below
    
#     # 1) collect body-type links (SUV, Sedan, etc.)
#     els = driver.find_elements(
#         By.CSS_SELECTOR,
#         "ul.browse-listing.row.mb0.clearfix li a"
#     )
#     if not els:
#         print("❌ No body-type links found on /new-cars/. Check the snippet above.")
#         return  # bail out early so you don’t scrape empty lists
#     ##################################################
#     # capture all body-type titles & URLs
#     all_body_types = [(e.get_attribute("title"), e.get_attribute("href")) for e in els]
#     print("🛠️  Found body types:", all_body_types)

#     # filter by substring match
#     body_types = [
#         (title, url)
#         for title, url in all_body_types
#         if TEST_BODY_TYPES[0].lower() in title.lower()
#     ]
#     print("→ Testing body types:", body_types)


#     ##############################################
    
#     # … rest of scrape_new_cars unchanged …

#     all_rows = []
#     for bt_name, bt_url in body_types:
#         print(f"[NEW] scraping body type {bt_name} → {bt_url}")
#         safe_get(driver, bt_url)
#         close_popup(driver)
#         time.sleep(1)

#         # click “Show More” if present
#         try:
#             more = driver.find_element(By.ID, "moreimported_cars_link")
#             print("  → clicking Show More")
#             more.click()
#             time.sleep(1)
#         except Exception:
#             print("  ⚠️ No Show More for new cars")

#         # parse each model listing
#         soup = BeautifulSoup(driver.page_source, "html.parser")
#         models = soup.select("ul.list-unstyled.model-list.row.item.clearfix li a")
#         print(f"  → found {len(models)} models")
#         for a in models:
#             data = {
#                 "body_type": bt_name,
#                 "make":      "",               # still blank since page grouped by body type
#                 "model":     a.get_text(" ", strip=True),
#                 "price":     "",
#                 "used_listing_title": "",
#                 "mileage":      "", 
#                 "transmission": "",
#                 "fuel":         "",
#                 "engine":       "",
#                 "city":         "",            # new-cars: leave blank
#                 "year":         "",            # new-cars: leave blank
#                 "last_updated": ""             # new-cars: not available
#             }

#             detail_url = urljoin(BASE_URL, a["href"])
#             print("    → fetching model detail:", detail_url)
#             safe_get(driver, detail_url)
#             close_popup(driver)
#             time.sleep(1)
#             det = BeautifulSoup(driver.page_source, "html.parser")

#             # specs extraction (same logic)
#             for sp in det.select("ul.engine-specs li"):
#                 txt = sp.get_text(" ", strip=True).lower()
#                 if "km" in txt:
#                     data["mileage"] = txt
#                 elif "transmission" in txt:
#                     data["transmission"] = sp.select_one("strong").get_text(strip=True)
#                 elif "fuel type" in txt:
#                     data["fuel"] = sp.select_one("strong").get_text(strip=True)
#                 elif "engine" in txt:
#                     data["engine"] = sp.select_one("strong").get_text(strip=True)

#             price_lbl = det.select_one("label.generic-green.nomargin.mt20.show")
#             data["price"] = price_lbl.get_text(" ", strip=True) if price_lbl else ""
#             used_btn = det.select_one("a.btn.btn-link-outline.fs12.mt10")
#             data["used_listing_title"] = used_btn.get("title", "") if used_btn else ""
#             print("    →", data)
#             all_rows.append(data)

#     df2 = pd.DataFrame(all_rows)
#     df2.to_csv("new_cars2.csv", index=False)
#     print("✅ new_cars2.csv written with columns:", df2.columns.tolist())

# def main():
#     driver = init_driver()
#     try:
#         scrape_used_cars(driver)
#         #scrape_new_cars(driver)
#     finally:
#         driver.quit()

# if __name__ == "__main__":
#     main()



###############################################################################################################################
################################################################################################################################
import time
import re
from urllib.parse import urljoin
from datetime import datetime, timedelta

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# ──────── CONFIG ──────────
TEST_BODY_TYPES = ["sedan"]
BASE_URL = "https://www.pakwheels.com"
SNAPSHOT_DATE = datetime(2025, 5, 6)
# ──────────────────────────

def init_driver():
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(options=options)
    driver.command_executor._client_config.timeout = 120
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(10)
    driver.maximize_window()
    return driver

def close_popup(driver):
    try:
        btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "onesignal-slidedown-cancel-button"))
        )
        btn.click()
    except Exception:
        pass

def safe_get(driver, url):
    try:
        driver.get(url)
    except Exception:
        print(f"⚠️ Timeout on {url}, retrying…")
        driver.get(url)

def parse_last_updated(text):
    if not isinstance(text, str): return timedelta(0)
    m = re.search(r'(\d+(?:\.\d*)?)\s*(day|days|hr|hrs|hour|hours|min|mins|minute|minutes|week|weeks)', text, re.I)
    if not m: return timedelta(0)
    num, unit = float(m.group(1)), m.group(2).lower()
    return timedelta(**{
        'day': num, 'days': num,
        'hr': 0, 'hrs': 0, 'hour': 0, 'hours': 0,
        'min': 0, 'mins': 0, 'minute': 0, 'minutes': 0,
        'week': num * 7, 'weeks': num * 7
    })

def scrape_used_cars(driver):
    safe_get(driver, f"{BASE_URL}/used-cars/")
    close_popup(driver)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//h2[text()='Used Cars by Body Type']"))
    )

    els = driver.find_elements(By.CSS_SELECTOR, "#browesBTSlider ul.browse-listing li a")
    body_types = [(e.get_attribute("title"), e.get_attribute("href")) for e in els]
    body_types = [bt for bt in body_types if bt[0].lower().startswith(TEST_BODY_TYPES[0].lower())]
    print("→ Testing body types:", body_types)

    rows = []
    for bt_name, bt_url in body_types:
        print(f"[USED] scraping body type {bt_name} → {bt_url}")
        safe_get(driver, bt_url)
        close_popup(driver)
        time.sleep(1)

        try:
            view_all = driver.find_element(By.CSS_SELECTOR, "a.more-link")
            print("  → clicking View All")
            view_all.click()
            time.sleep(1)
        except Exception:
            print("  ⚠️ No View All button found—continuing with current page")

        while True:
            soup = BeautifulSoup(driver.page_source, "html.parser")
            ads = soup.select("ul.search-results li[data-listing-id]")
            print(f"  → found {len(ads)} ads on this page")

            for ad in ads:
                listing_id = ad.get("data-listing-id", "").strip()

                data = {
                    "id": listing_id,
                    "body_type": bt_name
                }

                title_el = ad.select_one("a.car-name.ad-detail-path")
                data["title"] = title_el.get_text(" ", strip=True).replace("for Sale", "").strip()

                price_div = title_el.find_parent("div", class_="search-title-row")\
                                    .select_one(".price-details")
                data["price"] = price_div.get_text(" ", strip=True) if price_div else ""
                if price_div:
                    cls = [c for c in price_div.get("class", []) if c != "price-details"]
                    data["color"] = " ".join(cls).replace("-", " ")
                else:
                    data["color"] = ""

                info_ul = title_el.find_next("ul", class_="search-vehicle-info-2")
                if info_ul:
                    vals = [li.get_text(strip=True) for li in info_ul.find_all("li")]
                    data.update({
                        "year":         vals[0] if len(vals) > 0 else "",
                        "mileage":      vals[1] if len(vals) > 1 else "",
                        "fuel":         vals[2] if len(vals) > 2 else "",
                        "engine":       vals[3] if len(vals) > 3 else "",
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
            next_url = urljoin(BASE_URL, nxt["href"])
            print("  → moving to next page:", next_url)
            safe_get(driver, next_url)
            close_popup(driver)
            time.sleep(1)

    df = pd.DataFrame(rows)
    return df

def clean_used_cars(df, snapshot_date, out_path):
    df["id"] = df["id"].astype(str)
    df["price"] = df["price"].astype(str).replace(r"[^\d.\-]", "", regex=True).replace("", pd.NA)
    df["mileage"] = df["mileage"].astype(str).replace(r"[^\d]", "", regex=True).replace("", pd.NA)
    df["engine"] = df["engine"].astype(str).replace(r"[^\d]", "", regex=True).replace("", pd.NA)
    deltas = df["last_updated"].apply(parse_last_updated)
    df["date_posted"] = snapshot_date - deltas
    df["status"] = "present"
    df["time_to_sell"] = pd.NA
    df.drop(columns=["color", "body_type"], inplace=True, errors="ignore")
    df.to_csv(out_path, index=False)
    print("✅ Cleaned and saved as", out_path)

def main():
    driver = init_driver()
    try:
        df_raw = scrape_used_cars(driver)
        df_raw.to_csv("used_cars2.csv", index=False)
        clean_used_cars(df_raw, SNAPSHOT_DATE, "used_cars_master.csv")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()

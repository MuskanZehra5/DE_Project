import requests
from bs4 import BeautifulSoup
import pandas as pd

URL = "https://psopk.com/en/fuels/fuel-prices"

def fetch_pso_prices():
    r = requests.get(URL)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # locate the table body
    tbody = soup.select_one(
        "section.accordSec table.uk-table tbody"
    )
    if tbody is None:
        raise RuntimeError("Could not find the fuel‐prices table")

    # iterate through rows and pick out the two we care about
    rows = tbody.find_all("tr")
    wanted = {"PREMIER EURO 5", "HI-CETANE DIESEL EURO 5"}
    data = []
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        fuel_name = tds[0].get_text(strip=True)
        if fuel_name in wanted:
            price_text = tds[1].get_text(strip=True)
            # strip off “Rs.” and “/Ltr”
            price_val = price_text.replace("Rs.", "").replace("/Ltr", "").strip()
            data.append({
                "Fuel": fuel_name,
                "Price_Rs_per_Ltr": float(price_val)
            })

    if not data:
        raise RuntimeError("Did not find any matching fuel rows")

    return pd.DataFrame(data)


def main():
    df = fetch_pso_prices()
    out_path = "pso_fuel_prices.csv"
    df.to_csv(out_path, index=False)
    print(f"✅ Saved prices to {out_path}\n")
    print(df)


if __name__ == "__main__":
    main()

###################################################################################################################
#!/usr/bin/env python3
# import time
# import pandas as pd
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC

# # ───── CONFIG ────────────────────────────────────────────────────────────────
# BASE_URL   = "https://psopk.com/fuel-prices/pol/archives"
# OUTPUT_CSV = "psopk_petrol_archives.csv"
# # ────────────────────────────────────────────────────────────────────────────────

# # 1) start browser
# driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
# wait   = WebDriverWait(driver, 15)

# # 2) navigate & wait for the accordSec container
# driver.get(BASE_URL)
# wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "section.accordSec")))

# # 3) pull out each accordion <li> under the uk-accordion list
# items = driver.find_elements(
#     By.CSS_SELECTOR,
#     "section.accordSec ul.uk-accordion > li"
# )

# rows = []
# for item in items:
#     # header text like "Effective from: March 16, 2025"
#     header_el      = item.find_element(By.CSS_SELECTOR, "a.uk-accordion-title")
#     effective_date = header_el.text.strip()

#     # if not already open, click to expand
#     if "uk-open" not in item.get_attribute("class"):
#         header_el.click()
#         time.sleep(0.3)

#     # now the table is visible in div.uk-accordion-content
#     tbody = item.find_element(
#         By.CSS_SELECTOR,
#         "div.uk-accordion-content table.uk-table tbody"
#     )

#     def read_price(label):
#         try:
#             cell = tbody.find_element(
#                 By.XPATH,
#                 f".//td[normalize-space(text())='{label}']/following-sibling::td[1]"
#             )
#             return cell.text.strip()
#         except:
#             return None

#     rows.append({
#         "effective_date":           effective_date,
#         "PREMIER_EURO_5":           read_price("PREMIER EURO 5"),
#         "HI_CETANE_DIESEL_EURO_5":  read_price("HI-CETANE DIESEL EURO 5")
#     })

# driver.quit()

# # 4) build DataFrame & write out
# df = pd.DataFrame(rows)
# df.to_csv(OUTPUT_CSV, index=False)
# print(f"✅ Extracted {len(df)} archives → {OUTPUT_CSV}")

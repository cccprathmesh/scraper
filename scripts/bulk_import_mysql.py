#!/usr/bin/env python3
"""
Bulk Ingestion Script: Imports all scraped competitor data and URL lists
into MySQL tables: `competitor_scraped_products` and `competitor_urls`.
"""

import os
import sys
import csv
import json
import pymysql
from datetime import datetime

MYSQL_HOST = os.getenv("MYSQL_HOST", "2.24.198.101")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "Root@123456")
MYSQL_DB = os.getenv("MYSQL_DB", "google_shopping")

COMPETITORS_CONFIG = [
    {
        "competitor_name": "TV Stands Outlet",
        "competitor_id": 22109,
        "scraped_csv": "output/tvstandsoutlet/FinalScrappedData_TVS_COMPLETE_20261104_160615.csv",
        "master_url_file": "urls/all_urls_tvstandsoutlet_remaining.txt", # Has the original master list
    },
    {
        "competitor_name": "Bedroom Furniture Discounts",
        "competitor_id": 20604,
        "scraped_csv": "output/bedroomfurniturediscounts/FinalScrappedData_BFD_COMPLETE_20261105_093648.csv",
        "master_url_file": "urls/all_urls_bedroomfurniturediscounts_remaining.txt",
    },
    {
        "competitor_name": "Discount Living Rooms",
        "competitor_id": 15311,
        "scraped_csv": "output/discountlivingrooms/FinalScrappedData_DRL_PARTIAL_20261105_093845.csv",
        "master_url_file": "urls/all_urls_discountlivingrooms_remaining.txt",
    },
    {
        "competitor_name": "Dining Rooms Outlet",
        "competitor_id": 19434,
        "scraped_csv": "archive/old/dro.csv",
        "master_url_file": "all_urls_diningroomsoutlet.txt",
    },
    {
        "competitor_name": "Coleman Furniture",
        "competitor_id": 17028,
        "scraped_csv": "archive/merge/Coleman - Jackson.csv",
        "master_url_file": None,
    },
    {
        "competitor_name": "Coleman Furniture",
        "competitor_id": 17028,
        "scraped_csv": "archive/merge/coleman - Catnapper.csv",
        "master_url_file": None,
    }
]

def clean_val(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None

def clean_price(v):
    if not v:
        return None
    s = str(v).replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return None

def clean_date(v):
    if not v:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    s = str(v).strip()
    # Common formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y", "%Y%m%d_%H%M%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def run_import():
    print(f"Connecting to MySQL ({MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB})...")
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=False
    )

    try:
        for comp in COMPETITORS_CONFIG:
            comp_name = comp["competitor_name"]
            comp_id = comp["competitor_id"]
            csv_path = comp["scraped_csv"]
            master_url_file = comp["master_url_file"]

            print("\n" + "=" * 60)
            print(f"IMPORTING COMPETITOR: {comp_name} (ID: {comp_id})")
            print("=" * 60)

            scraped_urls = set()

            # 1. Ingest Scraped Products
            if csv_path and os.path.exists(csv_path):
                print(f"Reading scraped CSV: {csv_path}...")
                with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
                    reader = csv.reader(f)
                    header = [h.strip() for h in next(reader, [])]
                    
                    # Normalize header lookup
                    header_lower = [h.lower() for h in header]
                    def get_idx(*keys):
                        for k in keys:
                            for idx, h in enumerate(header_lower):
                                if k == h or (k in h and len(h) < len(k) + 8):
                                    return idx
                        return None

                    url_idx = get_idx("ref product url", "product_url", "product url", "url")
                    pid_idx = get_idx("ref product id", "product_id", "id")
                    var_idx = get_idx("ref variant id", "ref varient id", "variant_id")
                    cat_idx = get_idx("ref category", "category")
                    cat_url_idx = get_idx("ref category url", "category_url")
                    brand_idx = get_idx("ref brand name", "brand", "ref brand")
                    name_idx = get_idx("ref product name", "product_name", "title", "name")
                    sku_idx = get_idx("ref sku", "sku")
                    mpn_idx = get_idx("ref mpn", "mpn")
                    gtin_idx = get_idx("ref gtin", "gtin", "upc")
                    price_idx = get_idx("ref price", "price", "sale_price")
                    img_idx = get_idx("ref main image", "main_image", "image")
                    qty_idx = get_idx("ref quantity", "quantity", "qty")
                    attr1_idx = get_idx("ref group attr 1", "group_attr_1")
                    attr2_idx = get_idx("ref group attr 2", "group_attr_2")
                    status_idx = get_idx("ref status", "status")
                    add_data_idx = get_idx("additional product data", "ref highlights", "additional_data")
                    date_idx = get_idx("date scrapped", "date scraped", "scraped_at", "date")

                    if url_idx is None:
                        url_idx = 0

                    batch_products = []
                    batch_urls = []
                    row_count = 0

                    prod_insert_sql = """
                    INSERT INTO competitor_scraped_products (
                        competitor_name, competitor_id, product_url, product_id, variant_id,
                        category, category_url, brand_name, product_name, sku, mpn, gtin,
                        price, main_image, quantity, group_attr_1, group_attr_2, status,
                        additional_data, raw_row, date_scraped
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s
                    ) ON DUPLICATE KEY UPDATE
                        product_name = VALUES(product_name),
                        sku = COALESCE(VALUES(sku), competitor_scraped_products.sku),
                        price = COALESCE(VALUES(price), competitor_scraped_products.price),
                        status = VALUES(status),
                        main_image = COALESCE(VALUES(main_image), competitor_scraped_products.main_image),
                        date_scraped = VALUES(date_scraped),
                        updated_at = CURRENT_TIMESTAMP
                    """

                    url_insert_sql = """
                    INSERT INTO competitor_urls (
                        competitor_name, competitor_id, url, status, last_scraped_at
                    ) VALUES (
                        %s, %s, %s, 'scraped', %s
                    ) ON DUPLICATE KEY UPDATE
                        status = 'scraped',
                        last_scraped_at = VALUES(last_scraped_at)
                    """

                    with conn.cursor() as cur:
                        for row in reader:
                            if not row or len(row) <= url_idx:
                                continue
                            raw_url = row[url_idx].strip()
                            if not raw_url.startswith("http"):
                                continue

                            scraped_urls.add(raw_url)

                            def val(i):
                                return clean_val(row[i]) if (i is not None and i < len(row)) else None

                            p_url = raw_url
                            p_id = val(pid_idx)
                            p_var = val(var_idx)
                            p_cat = val(cat_idx)
                            p_cat_url = val(cat_url_idx)
                            p_brand = val(brand_idx)
                            p_name = val(name_idx) or "Product"
                            p_sku = val(sku_idx)
                            p_mpn = val(mpn_idx)
                            p_gtin = val(gtin_idx)
                            p_price = clean_price(val(price_idx))
                            p_img = val(img_idx)
                            p_qty = val(qty_idx)
                            p_attr1 = val(attr1_idx)
                            p_attr2 = val(attr2_idx)
                            p_status = val(status_idx) or "In Stock"
                            p_add_data = val(add_data_idx)
                            p_add_json = json.dumps({"data": p_add_data}) if p_add_data else None
                            p_raw_json = json.dumps(row)
                            p_date = clean_date(val(date_idx))

                            batch_products.append((
                                comp_name, comp_id, p_url, p_id, p_var,
                                p_cat, p_cat_url, p_brand, p_name, p_sku, p_mpn, p_gtin,
                                p_price, p_img, p_qty, p_attr1, p_attr2, p_status,
                                p_add_json, p_raw_json, p_date
                            ))

                            batch_urls.append((
                                comp_name, comp_id, p_url, p_date
                            ))

                            row_count += 1
                            if len(batch_products) >= 2000:
                                cur.executemany(prod_insert_sql, batch_products)
                                cur.executemany(url_insert_sql, batch_urls)
                                conn.commit()
                                print(f"  -> Ingested {row_count:,} products for {comp_name}...")
                                batch_products.clear()
                                batch_urls.clear()

                        if batch_products:
                            cur.executemany(prod_insert_sql, batch_products)
                            cur.executemany(url_insert_sql, batch_urls)
                            conn.commit()
                            print(f"  -> Ingested {row_count:,} products for {comp_name} (COMPLETE)")

            # 2. Ingest Pending URLs from master URL list
            if master_url_file and os.path.exists(master_url_file):
                print(f"Reading master URL file: {master_url_file}...")
                pending_urls = []
                with open(master_url_file, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        u = line.strip()
                        if u.startswith("http") and u not in scraped_urls:
                            pending_urls.append((comp_name, comp_id, u, "pending"))

                print(f"Found {len(pending_urls):,} pending (unscraped) URLs for {comp_name}. Inserting...")
                pending_insert_sql = """
                INSERT INTO competitor_urls (
                    competitor_name, competitor_id, url, status
                ) VALUES (
                    %s, %s, %s, %s
                ) ON DUPLICATE KEY UPDATE
                    -- Do not overwrite if already marked scraped
                    status = IF(competitor_urls.status = 'scraped', 'scraped', VALUES(status))
                """
                with conn.cursor() as cur:
                    for i in range(0, len(pending_urls), 2000):
                        chunk = pending_urls[i:i+2000]
                        cur.executemany(pending_insert_sql, chunk)
                        conn.commit()
                        print(f"  -> Inserted {min(i+2000, len(pending_urls)):,} / {len(pending_urls):,} pending URLs...")

        print("\n" + "=" * 60)
        print("IMPORT FINISHED SUCCESSFULLY!")
        print("=" * 60)

        # Print summary
        with conn.cursor() as cur:
            cur.execute("""
            SELECT competitor_name, status, COUNT(*) 
            FROM competitor_urls 
            GROUP BY competitor_name, status 
            ORDER BY competitor_name, status;
            """)
            rows = cur.fetchall()
            print("\nURL QUEUE BREAKDOWN:")
            for r in rows:
                print(f"  {r[0]:<30} | {r[1]:<10} : {r[2]:,}")

            cur.execute("""
            SELECT competitor_name, COUNT(*) 
            FROM competitor_scraped_products 
            GROUP BY competitor_name 
            ORDER BY competitor_name;
            """)
            rows_p = cur.fetchall()
            print("\nSCRAPED PRODUCTS TABLE BREAKDOWN:")
            for r in rows_p:
                print(f"  {r[0]:<30} : {r[1]:,}")

    finally:
        conn.close()

if __name__ == "__main__":
    run_import()


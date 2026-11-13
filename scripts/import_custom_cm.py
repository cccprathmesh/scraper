#!/usr/bin/env python3
"""
Importer for scrapers/custom/cm competitor CSV files into:
- competitor_scraped_products
- competitor_urls
"""

import os
import sys
import csv
import json
import time
import argparse
from datetime import datetime
import pymysql

MYSQL_HOST = os.getenv("MYSQL_HOST", "2.24.198.101")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "Root@123456")
MYSQL_DB = os.getenv("MYSQL_DB", "google_shopping")

COMPETITORS_CM = [
    {
        "file": "franceandson.csv",
        "name": "France & Son",
        "competitor_id": 16169,
    },
    {
        "file": "unlimitedfurnituregroup.csv",
        "name": "Unlimited Furniture",
        "competitor_id": 18096,
    },
    {
        "file": "coleman.csv",
        "name": "Coleman Furniture",
        "competitor_id": 17028,
    },
    {
        "file": "hgs.csv",
        "name": "Home Gallery Stores",
        "competitor_id": 15177,
    },
    {
        "file": "graysonliving.csv",
        "name": "Grayson Living",
        "competitor_id": 20013,
    },
    {
        "file": "cymax.csv",
        "name": "Cymax",
        "competitor_id": 20612,
    },
    {
        "file": "homesquare.csv",
        "name": "Homesquare",
        "competitor_id": 17442,
    },
    {
        "file": "emmamason.csv",
        "name": "Emma Mason",
        "competitor_id": 16060,
    },
    {
        "file": "afastore.csv",
        "name": "AFA Stores",
        "competitor_id": 19894,
    },
    {
        "file": "englishelm.csv",
        "name": "English Elm",
        "competitor_id": 20492,
    },
    {
        "file": "bisonoffice.csv",
        "name": "BisonOffice.com",
        "competitor_id": 17927,
    },
    {
        "file": "bbb2.csv",
        "name": "Bed Bath & Beyond",
        "competitor_id": 19034,
    },
    {
        "file": "Overstock.com.csv",
        "name": "Overstock",
        "competitor_id": 16167,
    },
]

class DBImporter:
    def __init__(self):
        self.conn = None
        self.connect()

    def connect(self):
        for attempt in range(15):
            try:
                if self.conn:
                    try:
                        self.conn.close()
                    except Exception:
                        pass
                self.conn = pymysql.connect(
                    host=MYSQL_HOST,
                    port=MYSQL_PORT,
                    user=MYSQL_USER,
                    password=MYSQL_PASS,
                    database=MYSQL_DB,
                    charset="utf8mb4",
                    autocommit=True,
                    connect_timeout=25,
                    read_timeout=90,
                    write_timeout=90
                )
                return
            except Exception as e:
                wait_t = min(30, (attempt + 1) * 2)
                print(f"Connection attempt {attempt+1} failed: {e}. Retrying in {wait_t}s...", flush=True)
                time.sleep(wait_t)
        raise RuntimeError("Failed to connect to MySQL after 15 attempts.")

    def execute_with_retry(self, sql):
        for attempt in range(15):
            try:
                with self.conn.cursor() as cur:
                    cur.execute(sql)
                return
            except (pymysql.OperationalError, pymysql.InternalError, Exception) as e:
                wait_t = min(30, (attempt + 1) * 2)
                print(f"DB Error on attempt {attempt+1}: {e}. Reconnecting in {wait_t}s...", flush=True)
                time.sleep(wait_t)
                try:
                    self.connect()
                except Exception:
                    pass
        raise RuntimeError("Failed executing SQL after 15 attempts.")

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

def escape_str(s):
    if s is None:
        return "NULL"
    escaped = s.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r")
    return f"'{escaped}'"

def escape_num(n):
    return str(n) if n is not None else "NULL"

def clean_val(v, max_len=None):
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    if max_len and len(s) > max_len:
        s = s[:max_len]
    return s

def clean_price(v):
    if not v:
        return None
    s = str(v).replace("$", "").replace(",", "").strip()
    try:
        val = float(s)
        if val < 0 or val > 99999999.99:
            return None
        return val
    except Exception:
        return None

def clean_date(v):
    if not v:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    s = str(v).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y", "%Y%m%d_%H%M%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def import_csv(db, item, base_dir="scrapers/custom/cm", batch_size=1000, skip_rows=0):
    comp_name = item["name"]
    comp_id = item["competitor_id"]
    csv_file = item["file"]
    csv_path = os.path.join(base_dir, csv_file)

    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}, skipping.", flush=True)
        return

    print(f"\n==================================================", flush=True)
    print(f"Starting import for: {comp_name} (ID: {comp_id})", flush=True)
    print(f"File: {csv_path}", flush=True)
    print(f"==================================================", flush=True)

    start_time = time.time()
    total_imported = 0

    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        header = [h.strip() for h in next(reader, [])]
        header_lower = [h.lower() for h in header]

        if skip_rows > 0:
            print(f"[{comp_name}] Skipping first {skip_rows:,} rows to resume...", flush=True)
            for _ in range(skip_rows):
                next(reader, None)
            total_imported = skip_rows

        def get_idx(*keys):
            for k in keys:
                for idx, h in enumerate(header_lower):
                    if k == h or (k in h and len(h) < len(k) + 8):
                        return idx
            return None

        url_idx = get_idx("ref product url", "product_url", "product url", "url")
        if url_idx is None:
            # Fallback search for any header with url
            for idx, h in enumerate(header_lower):
                if "url" in h:
                    url_idx = idx
                    break
        if url_idx is None:
            url_idx = 0

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

        cur_p_values = []
        cur_u_values = []

        for row in reader:
            if not row or len(row) <= url_idx:
                continue
            raw_url = row[url_idx].strip()
            if not raw_url.startswith("http"):
                continue

            def val(i, max_len=None):
                return clean_val(row[i], max_len) if (i is not None and i < len(row)) else None

            p_url = raw_url
            p_id = val(pid_idx, 128)
            p_var = val(var_idx, 128)
            p_cat = val(cat_idx, 256)
            p_cat_url = val(cat_url_idx, 512)
            p_brand = val(brand_idx, 256)
            p_name = val(name_idx, 512) or "Product"
            p_sku = val(sku_idx, 256)
            p_mpn = val(mpn_idx, 256)
            p_gtin = val(gtin_idx, 128)
            p_price = clean_price(val(price_idx))
            p_img = val(img_idx, 1024)
            p_qty = val(qty_idx, 64)
            p_attr1 = val(attr1_idx, 256)
            p_attr2 = val(attr2_idx, 256)
            p_status = val(status_idx, 64) or "In Stock"
            p_add_data = val(add_data_idx)
            p_add_json = json.dumps({"data": p_add_data}) if p_add_data else None
            p_raw_json = json.dumps(row)
            p_date = clean_date(val(date_idx))

            row_p_str = f"""(
                {escape_str(comp_name)}, {escape_num(comp_id)}, {escape_str(p_url)}, {escape_str(p_id)}, {escape_str(p_var)},
                {escape_str(p_cat)}, {escape_str(p_cat_url)}, {escape_str(p_brand)}, {escape_str(p_name)}, {escape_str(p_sku)},
                {escape_str(p_mpn)}, {escape_str(p_gtin)}, {escape_num(p_price)}, {escape_str(p_img)}, {escape_str(p_qty)},
                {escape_str(p_attr1)}, {escape_str(p_attr2)}, {escape_str(p_status)}, {escape_str(p_add_json)}, {escape_str(p_raw_json)},
                {escape_str(p_date)}
            )"""
            cur_p_values.append(row_p_str)

            row_u_str = f"({escape_str(comp_name)}, {escape_num(comp_id)}, {escape_str(p_url)}, 'scraped', {escape_str(p_date)})"
            cur_u_values.append(row_u_str)

            if len(cur_p_values) >= batch_size:
                sql_p = f"""
                INSERT INTO competitor_scraped_products (
                    competitor_name, competitor_id, product_url, product_id, variant_id,
                    category, category_url, brand_name, product_name, sku, mpn, gtin,
                    price, main_image, quantity, group_attr_1, group_attr_2, status,
                    additional_data, raw_row, date_scraped
                ) VALUES {', '.join(cur_p_values)}
                ON DUPLICATE KEY UPDATE
                    product_name = VALUES(product_name),
                    sku = COALESCE(VALUES(sku), competitor_scraped_products.sku),
                    price = COALESCE(VALUES(price), competitor_scraped_products.price),
                    status = VALUES(status),
                    main_image = COALESCE(VALUES(main_image), competitor_scraped_products.main_image),
                    date_scraped = VALUES(date_scraped),
                    updated_at = CURRENT_TIMESTAMP;
                """
                sql_u = f"""
                INSERT INTO competitor_urls (competitor_name, competitor_id, url, status, last_scraped_at)
                VALUES {', '.join(cur_u_values)}
                ON DUPLICATE KEY UPDATE
                    status = 'scraped',
                    last_scraped_at = VALUES(last_scraped_at);
                """
                db.execute_with_retry(sql_p)
                db.execute_with_retry(sql_u)
                total_imported += len(cur_p_values)
                cur_p_values.clear()
                cur_u_values.clear()
                if (total_imported - skip_rows) % 2000 == 0 or total_imported % 5000 == 0:
                    elapsed = time.time() - start_time
                    new_rows = total_imported - skip_rows
                    rate = new_rows / elapsed if elapsed > 0 else 0
                    print(f"[{comp_name}] Ingested {total_imported:,} rows (+{new_rows:,} new, {rate:.0f} rows/s)...", flush=True)

        if cur_p_values:
            sql_p = f"""
            INSERT INTO competitor_scraped_products (
                competitor_name, competitor_id, product_url, product_id, variant_id,
                category, category_url, brand_name, product_name, sku, mpn, gtin,
                price, main_image, quantity, group_attr_1, group_attr_2, status,
                additional_data, raw_row, date_scraped
            ) VALUES {', '.join(cur_p_values)}
            ON DUPLICATE KEY UPDATE
                product_name = VALUES(product_name),
                sku = COALESCE(VALUES(sku), competitor_scraped_products.sku),
                price = COALESCE(VALUES(price), competitor_scraped_products.price),
                status = VALUES(status),
                main_image = COALESCE(VALUES(main_image), competitor_scraped_products.main_image),
                date_scraped = VALUES(date_scraped),
                updated_at = CURRENT_TIMESTAMP;
            """
            sql_u = f"""
            INSERT INTO competitor_urls (competitor_name, competitor_id, url, status, last_scraped_at)
            VALUES {', '.join(cur_u_values)}
            ON DUPLICATE KEY UPDATE
                status = 'scraped',
                last_scraped_at = VALUES(last_scraped_at);
            """
            db.execute_with_retry(sql_p)
            db.execute_with_retry(sql_u)
            total_imported += len(cur_p_values)

    duration = time.time() - start_time
    print(f"[{comp_name}] COMPLETE: Ingested {total_imported:,} products in {duration:.1f}s", flush=True)

def main():
    parser = argparse.ArgumentParser(description="Import scrapers/custom/cm CSV files into MySQL")
    parser.add_argument("--competitor", type=str, help="Specific competitor name or file name to import")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for INSERT statements")
    parser.add_argument("--skip-done", action="store_true", help="Skip competitors that already have completed rows in MySQL")
    parser.add_argument("--skip-rows", type=int, default=0, help="Manually skip N rows for the specified competitor")
    args = parser.parse_args()

    db = DBImporter()
    try:
        targets = COMPETITORS_CM
        if args.competitor:
            c_low = args.competitor.lower()
            targets = [c for c in COMPETITORS_CM if c_low in c["name"].lower() or c_low in c["file"].lower()]
            if not targets:
                print(f"No matching competitor found for '{args.competitor}'.", flush=True)
                return

        for item in targets:
            skip_count = args.skip_rows
            if args.skip_done:
                with db.conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM competitor_scraped_products WHERE competitor_name = %s", (item["name"],))
                    cnt = cur.fetchone()[0]
                    fpath = os.path.join("scrapers/custom/cm", item["file"])
                    file_rows = 0
                    if os.path.exists(fpath):
                        # Approximate row count or check
                        pass

                    if item["name"] in ["France & Son", "Unlimited Furniture", "Coleman Furniture", "Home Gallery Stores", "Grayson Living", "Cymax", "Homesquare", "Emma Mason", "AFA Stores", "English Elm"]:
                        if cnt >= 50000:
                            print(f"[{item['name']}] Already complete ({cnt:,} records in DB), skipping (--skip-done).", flush=True)
                            continue
                    elif item["name"] == "BisonOffice.com":
                        if cnt >= 640000:
                            print(f"[{item['name']}] Already complete ({cnt:,} records in DB), skipping (--skip-done).", flush=True)
                            continue
                        elif cnt > 0:
                            print(f"[{item['name']}] Auto-resuming from row {cnt:,}...", flush=True)
                            skip_count = cnt
                    elif item["name"] == "Bed Bath & Beyond":
                        if cnt >= 850000:
                            print(f"[{item['name']}] Already complete ({cnt:,} records in DB), skipping (--skip-done).", flush=True)
                            continue
                        elif cnt > 0:
                            print(f"[{item['name']}] Auto-resuming from row {cnt:,}...", flush=True)
                            skip_count = cnt
                    elif item["name"] == "Overstock":
                        if cnt >= 1040000:
                            print(f"[{item['name']}] Already complete ({cnt:,} records in DB), skipping (--skip-done).", flush=True)
                            continue
                        elif cnt > 0:
                            print(f"[{item['name']}] Auto-resuming from row {cnt:,}...", flush=True)
                            skip_count = cnt

            import_csv(db, item, batch_size=args.batch_size, skip_rows=skip_count)
    finally:
        db.close()

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Robust, persistent-connection bulk importer with automatic reconnect & retries.
"""

import os
import sys
import csv
import json
import time
import pymysql
from datetime import datetime

MYSQL_HOST = os.getenv("MYSQL_HOST", "2.24.198.101")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "Root@123456")
MYSQL_DB = os.getenv("MYSQL_DB", "google_shopping")

class DBImporter:
    def __init__(self):
        self.conn = None
        self.connect()

    def connect(self):
        for attempt in range(5):
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
                    connect_timeout=20,
                    read_timeout=60,
                    write_timeout=60
                )
                return
            except Exception as e:
                print(f"Connection attempt {attempt+1} failed: {e}. Retrying in 2s...")
                time.sleep(2)
        raise RuntimeError("Failed to connect to MySQL after 5 attempts.")

    def execute_with_retry(self, sql):
        for attempt in range(5):
            try:
                with self.conn.cursor() as cur:
                    cur.execute(sql)
                return
            except (pymysql.OperationalError, pymysql.InternalError) as e:
                print(f"DB Error on attempt {attempt+1}: {e}. Reconnecting...")
                time.sleep(2)
                self.connect()
        raise RuntimeError("Failed executing SQL after 5 attempts.")

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
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y", "%Y%m%d_%H%M%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def import_scraped_file(db, comp_name, comp_id, csv_path):
    if not csv_path or not os.path.exists(csv_path):
        return set()

    print(f"\n[{comp_name}] Reading scraped CSV: {csv_path}...")
    scraped_urls = set()

    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        header = [h.strip() for h in next(reader, [])]
        header_lower = [h.lower() for h in header]

        def get_idx(*keys):
            for k in keys:
                for idx, h in enumerate(header_lower):
                    if k == h or (k in h and len(h) < len(k) + 8):
                        return idx
            return None

        url_idx = get_idx("ref product url", "product_url", "product url", "url") or 0
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
        total_p = 0

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

            if len(cur_p_values) >= 500:
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
                total_p += len(cur_p_values)
                print(f"[{comp_name}] Ingested {total_p:,} products & URLs...")
                cur_p_values.clear()
                cur_u_values.clear()

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
            total_p += len(cur_p_values)

    print(f"✓ [{comp_name}] Total scraped products ingested: {total_p:,}")
    return scraped_urls

def import_pending_urls(db, comp_name, comp_id, master_file, scraped_urls):
    if not master_file or not os.path.exists(master_file):
        return

    print(f"[{comp_name}] Reading master URL file: {master_file}...")
    cur_u_values = []
    total_u = 0

    with open(master_file, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            u = line.strip()
            if not u.startswith("http") or u in scraped_urls:
                continue

            row_u_str = f"({escape_str(comp_name)}, {escape_num(comp_id)}, {escape_str(u)}, 'pending', NULL)"
            cur_u_values.append(row_u_str)

            if len(cur_u_values) >= 1000:
                sql_u = f"""
                INSERT INTO competitor_urls (competitor_name, competitor_id, url, status, last_scraped_at)
                VALUES {', '.join(cur_u_values)}
                ON DUPLICATE KEY UPDATE
                    status = IF(competitor_urls.status = 'scraped', 'scraped', VALUES(status));
                """
                db.execute_with_retry(sql_u)
                total_u += len(cur_u_values)
                print(f"[{comp_name}] Ingested {total_u:,} pending URLs...")
                cur_u_values.clear()

    if cur_u_values:
        sql_u = f"""
        INSERT INTO competitor_urls (competitor_name, competitor_id, url, status, last_scraped_at)
        VALUES {', '.join(cur_u_values)}
        ON DUPLICATE KEY UPDATE
            status = IF(competitor_urls.status = 'scraped', 'scraped', VALUES(status));
        """
        db.execute_with_retry(sql_u)
        total_u += len(cur_u_values)

    print(f"✓ [{comp_name}] Total pending URLs ingested: {total_u:,}")

def main():
    db = DBImporter()
    targets = [
        ("Discount Living Rooms", 15311, "output/discountlivingrooms/FinalScrappedData_DRL_PARTIAL_20261105_093845.csv", "urls/all_urls_discountlivingrooms_remaining.txt"),
        ("Dining Rooms Outlet", 19434, "archive/old/dro.csv", "all_urls_diningroomsoutlet.txt"),
        ("Coleman Furniture", 17028, "archive/merge/Coleman - Jackson.csv", None),
        ("Coleman Furniture", 17028, "archive/merge/coleman - Catnapper.csv", None),
    ]

    for comp_name, comp_id, sc_csv, url_file in targets:
        sc_urls = import_scraped_file(db, comp_name, comp_id, sc_csv)
        import_pending_urls(db, comp_name, comp_id, url_file, sc_urls)

    print("\n" + "="*60)
    print("ALL TARGET COMPETITORS IMPORTED SUCCESSFULLY!")
    print("="*60)
    db.close()

if __name__ == "__main__":
    main()


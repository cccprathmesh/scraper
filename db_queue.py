#!/usr/bin/env python3
"""
MySQL URL Queue & Scraped Data Manager for Competitor Scrapers.
Integrates directly with:
- `competitor_urls` (URL registry & status tracking)
- `competitor_scraped_products` (Direct product ingestion)
"""

import os
import sys
import json
import time
from datetime import datetime
import pymysql

# Database Configuration with standard defaults
MYSQL_HOST = os.getenv("MYSQL_HOST", "2.24.198.101")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "Root@123456")
MYSQL_DB = os.getenv("MYSQL_DB", "google_shopping")

# Default Scrape Mode: 'remaining' or 'full'
SCRAPE_MODE = os.getenv("SCRAPE_MODE", "remaining").lower()

COMPETITOR_ID_MAP = {
    "discount living rooms": 15311,
    "tv stands outlet": 22109,
    "dining rooms outlet": 19434,
    "bedroom furniture discounts": 20604,
    "cymax": 20612,
    "homesquare": 17442,
    "bisonoffice.com": 17927,
    "bisonoffice": 17927,
    "coleman furniture": 17028,
    "emma mason": 16060,
    "home gallery stores": 15177,
    "unlimited furniture": 18096,
    "france & son": 16169,
    "english elm": 20492,
    "afa stores": 19894,
    "bed bath & beyond": 19034,
    "overstock": 16167,
    "grayson living": 20013,
}

def resolve_competitor_id(competitor_name, competitor_id=None):
    if competitor_id is not None and competitor_id > 0:
        return competitor_id
    if not competitor_name:
        return None
    name_clean = str(competitor_name).strip().lower()
    for k, v in COMPETITOR_ID_MAP.items():
        if k in name_clean or name_clean in k:
            return v
    return None

def get_connection():
    for attempt in range(5):
        try:
            return pymysql.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASS,
                database=MYSQL_DB,
                charset="utf8mb4",
                autocommit=True,
                connect_timeout=15,
                read_timeout=60,
                write_timeout=60
            )
        except Exception as e:
            if attempt < 4:
                time.sleep(2)
            else:
                raise e

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

def filter_pending_urls(competitor_name, urls, competitor_id=None, mode=None):
    """
    Given a list of URLs for a competitor:
    1. Registers all new incoming URLs into `competitor_urls` as 'pending'.
    2. Depending on `mode` ('remaining' vs 'full'):
       - 'remaining' (default): returns ONLY URLs that have NOT yet been marked 'scraped' in DB.
       - 'full': returns ALL URLs (for full re-scraping).
    """
    if not urls:
        return []

    if mode is None:
        mode = os.getenv("SCRAPE_MODE", "remaining").lower()
    else:
        mode = str(mode).lower()

    cid = resolve_competitor_id(competitor_name, competitor_id)

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # 1. Register newly discovered URLs into competitor_urls as 'pending'
            # (INSERT IGNORE preserves existing URLs and their statuses)
            batch_size = 1000
            for i in range(0, len(urls), batch_size):
                chunk = urls[i:i + batch_size]
                values = [(competitor_name, cid, u, "pending") for u in chunk]
                insert_sql = """
                INSERT IGNORE INTO competitor_urls (competitor_name, competitor_id, url, status)
                VALUES (%s, %s, %s, %s)
                """
                cur.executemany(insert_sql, values)

            # 2. If 'full' scraping requested, return all URLs
            if mode in ("full", "all", "force_all"):
                print(f"[{competitor_name}] Scrape Mode: FULL. Total URLs to scrape: {len(urls):,}")
                return urls

            # 3. If 'remaining' mode (default), fetch URLs already marked 'scraped'
            cur.execute("""
                SELECT url FROM competitor_urls 
                WHERE competitor_name = %s AND status = 'scraped'
            """, (competitor_name,))
            scraped_set = set(row[0] for row in cur.fetchall())

            pending_urls = [u for u in urls if u not in scraped_set]
            print(f"[{competitor_name}] Scrape Mode: REMAINING. Total: {len(urls):,} | Already Scraped: {len(scraped_set):,} | Pending: {len(pending_urls):,}")
            return pending_urls

    except Exception as e:
        print(f"[{competitor_name}] DB Queue Warning: {e}. Falling back to input URLs.")
        return urls
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def save_scraped_product(competitor_name, competitor_id, product_data):
    """
    Saves a single scraped product dictionary to `competitor_scraped_products`
    and updates `competitor_urls` status to 'scraped'.
    """
    if not product_data:
        return False
    return save_scraped_products_batch(competitor_name, competitor_id, [product_data])

def save_scraped_products_batch(competitor_name, competitor_id, products_list):
    """
    Directly saves a batch of scraped product dictionaries to `competitor_scraped_products`
    and marks their URLs as 'scraped' in `competitor_urls`.
    """
    if not products_list:
        return True

    cid = resolve_competitor_id(competitor_name, competitor_id)

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            prod_values = []
            url_values = []

            for p in products_list:
                url = (
                    p.get("url") or p.get("Ref Product URL") or p.get("product_url") or
                    p.get("link") or p.get("item_url")
                )
                if not url or not str(url).startswith("http"):
                    continue
                url = str(url).strip()

                name = clean_val(p.get("name") or p.get("Ref Product Name") or p.get("product_name") or p.get("title") or "Product", 512)
                p_id = clean_val(p.get("product_id") or p.get("Ref Product ID") or p.get("id"), 128)
                v_id = clean_val(p.get("variant_id") or p.get("Ref Variant ID") or p.get("Ref Varient ID"), 128)
                sku = clean_val(p.get("sku") or p.get("Ref SKU"), 256)
                brand = clean_val(p.get("brand") or p.get("Ref Brand Name") or p.get("brand_name"), 256)
                cat = clean_val(p.get("category") or p.get("Ref Category"), 256)
                cat_url = clean_val(p.get("category_url") or p.get("Ref Category URL"), 512)
                mpn = clean_val(p.get("mpn") or p.get("Ref MPN"), 256)
                gtin = clean_val(p.get("gtin") or p.get("Ref GTIN") or p.get("upc"), 128)
                price = clean_price(p.get("price") or p.get("Ref Price") or p.get("sale_price"))
                image = clean_val(p.get("image") or p.get("Ref Main Image") or p.get("main_image"), 1024)
                qty = clean_val(p.get("quantity") or p.get("Ref Quantity") or p.get("qty"), 64)
                attr1 = clean_val(p.get("group_attr_1") or p.get("Ref Group Attr 1"), 256)
                attr2 = clean_val(p.get("group_attr_2") or p.get("Ref Group Attr 2"), 256)
                status = clean_val(p.get("status") or p.get("Ref Status") or "In Stock", 64)
                date_scraped = clean_date(p.get("date_scraped") or p.get("Date Scrapped") or p.get("date"))

                raw_json = json.dumps(p)

                prod_values.append((
                    competitor_name, cid, url, p_id, v_id,
                    cat, cat_url, brand, name, sku, mpn, gtin,
                    price, image, qty, attr1, attr2, status,
                    raw_json, date_scraped
                ))
                url_values.append((competitor_name, cid, url, "scraped", date_scraped))

            if not prod_values:
                return True

            # Insert/update competitor_scraped_products
            sql_p = """
            INSERT INTO competitor_scraped_products (
                competitor_name, competitor_id, product_url, product_id, variant_id,
                category, category_url, brand_name, product_name, sku, mpn, gtin,
                price, main_image, quantity, group_attr_1, group_attr_2, status,
                raw_row, date_scraped
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                product_name = VALUES(product_name),
                sku = COALESCE(VALUES(sku), competitor_scraped_products.sku),
                price = COALESCE(VALUES(price), competitor_scraped_products.price),
                status = VALUES(status),
                main_image = COALESCE(VALUES(main_image), competitor_scraped_products.main_image),
                date_scraped = VALUES(date_scraped),
                updated_at = CURRENT_TIMESTAMP;
            """
            cur.executemany(sql_p, prod_values)

            # Insert/update competitor_urls to 'scraped'
            sql_u = """
            INSERT INTO competitor_urls (competitor_name, competitor_id, url, status, last_scraped_at)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                status = 'scraped',
                last_scraped_at = VALUES(last_scraped_at);
            """
            cur.executemany(sql_u, url_values)

            return True
    except Exception as e:
        print(f"[{competitor_name}] Error saving scraped products batch to DB: {e}")
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def get_pending_urls_from_db(competitor_name, limit=0):
    """
    Directly query pending URLs for a competitor from `competitor_urls`.
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            sql = "SELECT url FROM competitor_urls WHERE competitor_name = %s AND status = 'pending'"
            if limit > 0:
                sql += f" LIMIT {int(limit)}"
            cur.execute(sql, (competitor_name,))
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Error fetching pending URLs for {competitor_name}: {e}")
        return []
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="DB Queue & Scraped Data Manager")
    parser.add_argument("--competitor", type=str, help="Competitor name")
    parser.add_argument("--get-pending", action="store_true", help="Print or export pending URLs from DB")
    parser.add_argument("--output", type=str, help="Output file to write pending URLs")
    args = parser.parse_args()

    if args.competitor and args.get_pending:
        urls = get_pending_urls_from_db(args.competitor)
        print(f"[{args.competitor}] Found {len(urls):,} pending URLs in DB.")
        if args.output:
            with open(args.output, "w") as f:
                for u in urls:
                    f.write(u + "\n")
            print(f"Exported to {args.output}")
        else:
            for u in urls[:20]:
                print(u)

#!/usr/bin/env python3
"""
MySQL URL Queue Manager for Competitor Scrapers.
Integrates with `competitor_urls` and `competitor_scraped_products` tables.
"""

import os
import sys
import json
import pymysql
from datetime import datetime

MYSQL_HOST = os.getenv("MYSQL_HOST", "2.24.198.101")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "Root@123456")
MYSQL_DB = os.getenv("MYSQL_DB", "google_shopping")

def get_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=True
    )

def filter_pending_urls(competitor_name, urls):
    """
    Given a list of URLs for a competitor, returns only the URLs that have NOT
    been successfully scraped in MySQL.
    Also registers any newly discovered URLs into competitor_urls as 'pending'.
    """
    if not urls:
        return []

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # 1. Fetch all URLs already marked 'scraped' for this competitor
            # Use chunks if large
            cur.execute("""
                SELECT url FROM competitor_urls 
                WHERE competitor_name = %s AND status = 'scraped'
            """, (competitor_name,))
            scraped_set = set(row[0] for row in cur.fetchall())

            # 2. Filter incoming list
            pending_urls = [u for u in urls if u not in scraped_set]

            # 3. Register any new URLs into competitor_urls
            # Insert IGNORE to prevent overhead
            new_urls = [(competitor_name, u, "pending") for u in pending_urls]
            if new_urls:
                insert_sql = """
                INSERT IGNORE INTO competitor_urls (competitor_name, url, status)
                VALUES (%s, %s, %s)
                """
                for i in range(0, len(new_urls), 1000):
                    cur.executemany(insert_sql, new_urls[i:i+1000])

            print(f"[{competitor_name}] Queue Filter: Total URLs: {len(urls):,} | Already Scraped: {len(scraped_set):,} | Pending to Scrape: {len(pending_urls):,}")
            return pending_urls
    except Exception as e:
        print(f"[{competitor_name}] Error querying database queue: {e}. Falling back to input URLs.")
        return urls
    finally:
        conn.close()

def save_scraped_product(competitor_name, competitor_id, product_data):
    """
    Saves a scraped product dictionary to `competitor_scraped_products`
    and updates `competitor_urls` status to 'scraped'.
    """
    url = product_data.get("url") or product_data.get("Ref Product URL")
    if not url:
        return False

    name = product_data.get("name") or product_data.get("Ref Product Name") or "Product"
    sku = product_data.get("sku") or product_data.get("Ref SKU")
    brand = product_data.get("brand") or product_data.get("Ref Brand Name")
    category = product_data.get("category") or product_data.get("Ref Category")
    price = product_data.get("price") or product_data.get("Ref Price")
    image = product_data.get("image") or product_data.get("Ref Main Image")
    status = product_data.get("status") or product_data.get("Ref Status") or "In Stock"

    # Clean price
    if price:
        try:
            price = float(str(price).replace("$", "").replace(",", "").strip())
        except Exception:
            price = None

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO competitor_scraped_products (
                competitor_name, competitor_id, product_url, product_name,
                sku, brand_name, category, price, main_image, status,
                raw_row, date_scraped
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
                product_name = VALUES(product_name),
                sku = COALESCE(VALUES(sku), competitor_scraped_products.sku),
                price = COALESCE(VALUES(price), competitor_scraped_products.price),
                status = VALUES(status),
                main_image = COALESCE(VALUES(main_image), competitor_scraped_products.main_image),
                date_scraped = VALUES(date_scraped),
                updated_at = CURRENT_TIMESTAMP;
            """, (
                competitor_name, competitor_id, url, name,
                sku, brand, category, price, image, status,
                json.dumps(product_data)
            ))

            cur.execute("""
            INSERT INTO competitor_urls (competitor_name, competitor_id, url, status, last_scraped_at)
            VALUES (%s, %s, %s, 'scraped', CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
                status = 'scraped',
                last_scraped_at = CURRENT_TIMESTAMP;
            """, (competitor_name, competitor_id, url))
            return True
    except Exception as e:
        print(f"[{competitor_name}] Error saving product {url} to database: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    print("Testing db_queue module...")
    test_urls = ["https://www.tvstandsoutlet.com/test-url-1", "https://www.tvstandsoutlet.com/test-url-2"]
    filtered = filter_pending_urls("TV Stands Outlet", test_urls)
    print("Filtered test result:", len(filtered))

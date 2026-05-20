import sys
import json
import random
import os
import time
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import csv
import traceback
import pandas as pd
import argparse
import re
import shutil
from urllib.parse import parse_qsl, unquote, urlencode, urlparse, urlunparse
import psycopg2
from psycopg2.extras import execute_values
import ftplib

try:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import NoSuchElementException, WebDriverException, SessionNotCreatedException, StaleElementReferenceException, TimeoutException
    import undetected_chromedriver as uc
    from selenium.webdriver.common.keys import Keys
except ImportError:
    # Selenium/Chrome dependencies are optional for non-scraping tasks like report exporting
    pass

CLAIM_STATUS = "claimed"
PENDING_STATUS = "pending"

def upload_to_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, local_file, remote_filename):
    """Upload a file to the FTP server securely."""
    try:
        print(f"Uploading {remote_filename} to FTP server {ftp_host}...")
        ftp = ftplib.FTP()
        ftp.connect(ftp_host, 21, timeout=30)
        ftp.login(ftp_user, ftp_pass)
        ftp.set_pasv(True)
        
        # Navigate or create directories recursively
        if ftp_path and ftp_path != '/':
            try:
                ftp.cwd(ftp_path)
            except Exception:
                dirs = ftp_path.strip('/').split('/')
                current_path = ''
                for directory in dirs:
                    current_path += '/' + directory
                    try:
                        ftp.cwd(current_path)
                    except Exception:
                        ftp.mkd(current_path)
                        ftp.cwd(current_path)
                        
        with open(local_file, 'rb') as f:
            ftp.storbinary(f'STOR {remote_filename}', f)
        ftp.quit()
        print(f"✓ Successfully uploaded {remote_filename} to FTP.")
        return True
    except Exception as e:
        print(f"Error uploading to FTP: {e}")
        traceback.print_exc()
        return False

def parse_price(price_str):
    try:
        val = re.sub(r'[^\d.]', '', str(price_str))
        if val: return float(val)
        return None
    except: return None

def insert_to_postgres(product_results, seller_results):
    try:
        pg_host = os.environ.get("PG_HOST")
        pg_port = os.environ.get("PG_PORT", "5432")
        pg_user = os.environ.get("PG_USER")
        pg_pass = os.environ.get("PG_PASS")
        pg_db = os.environ.get("PG_DB")

        if not all([pg_host, pg_user, pg_pass, pg_db]):
            print("Skipping PostgreSQL insert: Missing credentials")
            return

        conn = psycopg2.connect(
            host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db
        )
        cursor = conn.cursor()

        # Identify retryable product IDs (completed but missing a valid product_url)
        retry_product_ids = set()
        valid_product_results = []
        for r in product_results or []:
            status_lower = str(r.get("status", "")).strip().lower()
            p_url = str(r.get("product_url", "")).strip()
            is_completed = status_lower == 'completed' or status_lower == 'product_found'
            is_valid_url = p_url.startswith("https://www.google.com/search?ibp=oshop") or p_url.startswith("https://share.google/")
            
            if is_completed and not is_valid_url:
                retry_product_ids.add(str(r.get("product_id", "")).strip())
            else:
                valid_product_results.append(r)

        # Gather all product_ids to clean up pre-existing competitor/seller records
        if product_results:
            prod_ids = [str(r.get("product_id", "")).strip() for r in product_results if r.get("product_id")]
            if prod_ids:
                # Delete existing sellers for these products to prevent duplicate or stale entries
                cursor.execute("DELETE FROM product_sellers WHERE product_code = ANY(%s)", (prod_ids,))

        # 1. Upsert product_scraping_results (1-to-1 relationship)
        if valid_product_results:
            prod_insert = """
                INSERT INTO product_scraping_results (
                    product_id, web_id, name, mpn_sku, gtin, brand, category,
                    keyword, url, osb_url, last_response, osb_url_match, product_url,
                    seller, product_name, cid, pid, last_fetched_date, osb_position,
                    osb_id, seller_count, status, product_about_info, main_image,
                    description, attributes
                ) VALUES %s
                ON CONFLICT (product_id) DO UPDATE SET
                    web_id = EXCLUDED.web_id,
                    name = EXCLUDED.name,
                    mpn_sku = EXCLUDED.mpn_sku,
                    gtin = EXCLUDED.gtin,
                    brand = EXCLUDED.brand,
                    category = EXCLUDED.category,
                    keyword = EXCLUDED.keyword,
                    url = EXCLUDED.url,
                    osb_url = EXCLUDED.osb_url,
                    last_response = EXCLUDED.last_response,
                    osb_url_match = EXCLUDED.osb_url_match,
                    product_url = EXCLUDED.product_url,
                    seller = EXCLUDED.seller,
                    product_name = EXCLUDED.product_name,
                    cid = EXCLUDED.cid,
                    pid = EXCLUDED.pid,
                    last_fetched_date = EXCLUDED.last_fetched_date,
                    osb_position = EXCLUDED.osb_position,
                    osb_id = EXCLUDED.osb_id,
                    seller_count = EXCLUDED.seller_count,
                    status = EXCLUDED.status,
                    product_about_info = EXCLUDED.product_about_info,
                    main_image = EXCLUDED.main_image,
                    description = EXCLUDED.description,
                    attributes = EXCLUDED.attributes,
                    updated_at = CURRENT_TIMESTAMP
            """
            prod_values = []
            for r in valid_product_results:
                prod_values.append((
                    str(r.get("product_id", "")),
                    str(r.get("web_id", "")),
                    str(r.get("name", "")),
                    str(r.get("mpn_sku", "")),
                    str(r.get("gtin", "")),
                    str(r.get("brand", "")),
                    str(r.get("category", "")),
                    str(r.get("keyword", "")),
                    str(r.get("url", "")),
                    str(r.get("osb_url", "")),
                    str(r.get("last_response", "")),
                    str(r.get("osb_url_match", "")),
                    str(r.get("product_url", "")),
                    str(r.get("seller", "")),
                    str(r.get("product_name", "")),
                    str(r.get("cid", "")),
                    str(r.get("pid", "")),
                    str(r.get("last_fetched_date", "")),
                    int(r.get("osb_position", 0) or 0),
                    str(r.get("osb_id", "")),
                    int(r.get("seller_count", 0) or 0),
                    str(r.get("status", "")),
                    str(r.get("product_about_info", "{}")),
                    str(r.get("main_image", "")),
                    str(r.get("description", "")),
                    str(r.get("attributes", "{}"))
                ))
            execute_values(cursor, prod_insert, prod_values)

        # 2. Upsert product_sellers (1-to-many relationship)
        valid_seller_results = []
        for r in seller_results or []:
            p_code = str(r.get("product_id", r.get("product_code", ""))).strip()
            if p_code not in retry_product_ids:
                valid_seller_results.append(r)

        if valid_seller_results:
            seller_insert = """
                INSERT INTO product_sellers (
                    product_code, seller_name, seller_price, seller_url, seller_product_name, stock_status
                ) VALUES %s
                ON CONFLICT (product_code, seller_name) DO UPDATE SET
                    seller_price = EXCLUDED.seller_price,
                    seller_url = EXCLUDED.seller_url,
                    seller_product_name = EXCLUDED.seller_product_name,
                    stock_status = EXCLUDED.stock_status,
                    updated_at = CURRENT_TIMESTAMP
            """
            # Deduplicate multiple offers from the same seller to avoid CardinalityViolation
            best_offers = {}
            for r in valid_seller_results:
                p_code = str(r.get("product_id", r.get("product_code", ""))).strip()
                s_name = str(r.get("seller", r.get("seller_name", ""))).strip()
                price = parse_price(r.get("seller_price"))
                
                if not p_code or not s_name:
                    continue
                    
                key = (p_code, s_name)
                current_offer = (
                    p_code,
                    s_name,
                    price,
                    r.get("seller_url", ""),
                    r.get("seller_product_name", ""),
                    r.get("stock_status", "In Stock")
                )
                
                if key not in best_offers:
                    best_offers[key] = current_offer
                else:
                    existing_price = best_offers[key][2]
                    # Keep the offer with the lowest valid price
                    if price is not None and (existing_price is None or price < existing_price):
                        best_offers[key] = current_offer
                        
            seller_values = list(best_offers.values())
            execute_values(cursor, seller_insert, seller_values)

        # 3. Transactionally update scraping_status in products_to_scrape table
        if product_results:
            status_update_query_fallback = """
                UPDATE products_to_scrape
                SET scraping_status = %s,
                    last_attempt = CURRENT_TIMESTAMP,
                    error_message = %s
                WHERE product_id = %s
            """
            status_update_query_with_claim_clear = """
                UPDATE products_to_scrape
                SET scraping_status = %s,
                    last_attempt = CURRENT_TIMESTAMP,
                    error_message = %s,
                    claimed_by = NULL,
                    claimed_at = NULL
                WHERE product_id = %s
            """
            supports_claims = False
            try:
                cursor.execute(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = 'products_to_scrape'
                      AND column_name IN ('claimed_by', 'claimed_at')
                    GROUP BY table_name
                    HAVING COUNT(*) = 2
                    """
                )
                supports_claims = cursor.fetchone() is not None
            except Exception:
                supports_claims = False

            for r in product_results:
                p_id = str(r.get("product_id", "")).strip()
                status_lower = str(r.get("status", "")).strip().lower()
                
                # Determine correct scraping status and error message
                if p_id in retry_product_ids:
                    scr_status = 'pending'
                    err_msg = 'Invalid product URL, retrying'
                elif status_lower == 'completed' or status_lower == 'product_found':
                    scr_status = 'completed'
                    err_msg = None
                elif status_lower == 'captcha_failed':
                    scr_status = 'pending'
                    err_msg = 'Captcha failed'
                elif status_lower in ('no_products', 'no_match'):
                    scr_status = status_lower
                    err_msg = r.get('last_response', 'No products found')
                else:
                    scr_status = 'error'
                    err_msg = r.get('last_response', 'Scrape failed to return data')
                
                cursor.execute(
                    status_update_query_with_claim_clear if supports_claims else status_update_query_fallback,
                    (scr_status, err_msg, p_id),
                )

        conn.commit()
        cursor.close()
        conn.close()
        print(f"✓ Transaction committed: Upserted {len(product_results)} products and {len(seller_results)} sellers into PostgreSQL.")

    except Exception as e:
        print(f"Error inserting into PostgreSQL: {e}")
        traceback.print_exc()

def sync_csv_to_db(csv_path):
    """Import CSV data into products_to_scrape table in fast batches if not already present."""
    try:
        pg_host = os.environ.get("PG_HOST")
        pg_port = os.environ.get("PG_PORT", "5432")
        pg_user = os.environ.get("PG_USER")
        pg_pass = os.environ.get("PG_PASS")
        pg_db = os.environ.get("PG_DB")

        if not all([pg_host, pg_user, pg_pass, pg_db]):
            print("Skipping DB sync: Missing credentials")
            return

        conn = psycopg2.connect(
            host=pg_host, 
            port=pg_port, 
            user=pg_user, 
            password=pg_pass, 
            dbname=pg_db,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        cursor = conn.cursor()

        # Optimization: check if products are already present in DB
        cursor.execute("SELECT COUNT(*) FROM products_to_scrape")
        existing_count = cursor.fetchone()[0]
        if existing_count > 0:
            print(f"✓ Database already contains {existing_count} products. Skipping CSV sync to save time.")
            cursor.close()
            conn.close()
            return

        print(f"Reading CSV {csv_path} for sync...")
        df = pd.read_csv(csv_path)
        initial_len = len(df)
        df = df.drop_duplicates(subset=['product_id'])
        if len(df) < initial_len:
            print(f"Removed {initial_len - len(df)} duplicate product_ids from CSV.")
            
        print(f"Syncing {len(df)} products from CSV to DB in batches...")

        insert_query = """
            INSERT INTO products_to_scrape (
                product_id, web_id, name, mpn_sku, gtin, brand, category, keyword, url, osb_url, status, "30daymfrsales"
            )
            VALUES %s
            ON CONFLICT (product_id) DO UPDATE SET
                status = EXCLUDED.status,
                "30daymfrsales" = EXCLUDED."30daymfrsales",
                name = EXCLUDED.name,
                keyword = EXCLUDED.keyword
        """
        
        batch_size = 5000
        total_rows = len(df)
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i : i + batch_size]
            records = batch.to_dict('records')
            values = []
            
            for row_dict in records:
                sales = row_dict.get('30daymfrsales', 0)
                if pd.isna(sales) or sales == '': sales = 0
                
                p_status = row_dict.get('status', 1)
                if pd.isna(p_status) or p_status == '': p_status = 1

                def clean(val):
                    return '' if pd.isna(val) else str(val)

                values.append((
                    clean(row_dict.get('product_id')),
                    clean(row_dict.get('web_id')),
                    clean(row_dict.get('name')),
                    clean(row_dict.get('mpn_sku')),
                    clean(row_dict.get('gtin')),
                    clean(row_dict.get('brand')),
                    clean(row_dict.get('category')),
                    clean(row_dict.get('keyword')),
                    clean(row_dict.get('url')),
                    clean(row_dict.get('osb_url')),
                    int(p_status),
                    int(sales)
                ))
            
            try:
                execute_values(cursor, insert_query, values)
                conn.commit()
            except Exception as batch_error:
                conn.rollback()
                print(f"Error in batch starting at row {i}: {batch_error}")
                # Fallback to smaller sub-batches
                for j in range(0, len(values), 1000):
                    sub_values = values[j : j + 1000]
                    try:
                        execute_values(cursor, insert_query, sub_values)
                        conn.commit()
                    except Exception as sub_err:
                        conn.rollback()
                        print(f"Failed to import sub-batch starting at index {i+j}: {sub_err}")
        
        cursor.close()
        conn.close()
        print("✓ CSV sync to DB completed.")
    except Exception as e:
        print(f"Error syncing CSV to DB: {e}")
        traceback.print_exc()


def get_pending_count_from_db():
    """Get the lightweight count of enabled products with 'pending' scraping_status."""
    try:
        pg_host = os.environ.get("PG_HOST")
        pg_port = os.environ.get("PG_PORT", "5432")
        pg_user = os.environ.get("PG_USER")
        pg_pass = os.environ.get("PG_PASS")
        pg_db = os.environ.get("PG_DB")

        conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM products_to_scrape WHERE scraping_status = 'pending' AND status = 1")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except Exception as e:
        print(f"Error getting pending count from DB: {e}")
        return 0

def _get_pg_conn():
    pg_host = os.environ.get("PG_HOST")
    pg_port = os.environ.get("PG_PORT", "5432")
    pg_user = os.environ.get("PG_USER")
    pg_pass = os.environ.get("PG_PASS")
    pg_db = os.environ.get("PG_DB")
    return psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db)

def _get_worker_id(explicit_worker_id=None):
    if explicit_worker_id:
        return str(explicit_worker_id)
    for key in ("SCRAPER_WORKER_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_RUN_ID", "GITHUB_JOB", "HOSTNAME"):
        val = os.environ.get(key)
        if val:
            return f"{key}:{val}"
    return f"pid:{os.getpid()}"

def release_expired_claims(ttl_minutes=60):
    """Release old claims so another runner can pick them up."""
    try:
        conn = _get_pg_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE products_to_scrape
            SET scraping_status = %s,
                claimed_by = NULL,
                claimed_at = NULL
            WHERE scraping_status = %s
              AND claimed_at IS NOT NULL
              AND claimed_at < (NOW() - (%s || ' minutes')::interval)
            """,
            (PENDING_STATUS, CLAIM_STATUS, int(ttl_minutes)),
        )
        released = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        if released:
            print(f"✓ Released {released} expired claimed products (TTL={ttl_minutes}m).")
        return released
    except Exception as e:
        print(f"Error releasing expired claims: {e}")
        return 0

def claim_pending_products_from_db(limit=30, worker_id=None, ttl_minutes=60):
    """
    Atomically claim up to `limit` pending products using row locks (FOR UPDATE SKIP LOCKED).
    Requires columns: claimed_by, claimed_at; and supports scraping_status='claimed'.
    """
    try:
        worker_id = _get_worker_id(worker_id)
        conn = _get_pg_conn()
        conn.autocommit = False
        cursor = conn.cursor()
        # Release expired claims first (best-effort)
        try:
            cursor.execute(
                """
                UPDATE products_to_scrape
                SET scraping_status = %s,
                    claimed_by = NULL,
                    claimed_at = NULL
                WHERE scraping_status = %s
                  AND claimed_at IS NOT NULL
                  AND claimed_at < (NOW() - (%s || ' minutes')::interval)
                """,
                (PENDING_STATUS, CLAIM_STATUS, int(ttl_minutes)),
            )
        except Exception:
            conn.rollback()
            # If schema doesn't have claim columns yet, let the caller know via empty df.
            cursor.close()
            conn.close()
            return pd.DataFrame()

        cursor.execute(
            """
            WITH picked AS (
                SELECT product_id
                FROM products_to_scrape
                WHERE scraping_status = %s AND status = 1
                ORDER BY "30daymfrsales" DESC NULLS LAST, product_id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            UPDATE products_to_scrape p
            SET scraping_status = %s,
                claimed_by = %s,
                claimed_at = NOW(),
                last_attempt = NOW(),
                error_message = NULL
            FROM picked
            WHERE p.product_id = picked.product_id
            RETURNING p.*
            """,
            (PENDING_STATUS, int(limit), CLAIM_STATUS, worker_id),
        )
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description] if cursor.description else []
        conn.commit()
        cursor.close()
        conn.close()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        print(f"Error claiming pending products from DB: {e}")
        return pd.DataFrame()

def get_pending_chunk_from_db(limit, offset):
    """Fetch only a specific partitioned slice of pending products using SQL LIMIT and OFFSET."""
    try:
        pg_host = os.environ.get("PG_HOST")
        pg_port = os.environ.get("PG_PORT", "5432")
        pg_user = os.environ.get("PG_USER")
        pg_pass = os.environ.get("PG_PASS")
        pg_db = os.environ.get("PG_DB")

        conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db)
        # Fetch only the assigned chunk's slice, ordered by sales descending
        query = """
            SELECT * FROM products_to_scrape 
            WHERE scraping_status = 'pending' AND status = 1
            ORDER BY "30daymfrsales" DESC NULLS LAST, product_id ASC
            LIMIT %s OFFSET %s
        """
        df = pd.read_sql(query, conn, params=(int(limit), int(offset)))
        conn.close()
        return df
    except Exception as e:
        print(f"Error fetching pending chunk from DB: {e}")
        return pd.DataFrame()

def verify_and_claim_product(product_id, worker_id=None, ttl_minutes=60):
    """
    Verify that a product is still available or claimed by us, and atomically claim/renew it.
    Returns True if we successfully claimed/renewed it and can scrape it.
    Returns False if it is completed, failed, or claimed by someone else.
    """
    try:
        pg_host = os.environ.get("PG_HOST")
        pg_port = os.environ.get("PG_PORT", "5432")
        pg_user = os.environ.get("PG_USER")
        pg_pass = os.environ.get("PG_PASS")
        pg_db = os.environ.get("PG_DB")

        if not all([pg_host, pg_user, pg_pass, pg_db]):
            # Standalone mode: assume it is safe to scrape
            return True

        worker_id = _get_worker_id(worker_id)
        conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db)
        cursor = conn.cursor()

        # Check if the schema supports claims columns
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'products_to_scrape'
              AND column_name IN ('claimed_by', 'claimed_at')
            GROUP BY table_name
            HAVING COUNT(*) = 2
            """
        )
        supports_claims = cursor.fetchone() is not None

        if not supports_claims:
            # Fallback simple check/claim
            cursor.execute(
                "SELECT scraping_status FROM products_to_scrape WHERE product_id = %s",
                (str(product_id),)
            )
            row = cursor.fetchone()
            if not row:
                cursor.close()
                conn.close()
                return False
            status = row[0]
            if status != PENDING_STATUS:
                cursor.close()
                conn.close()
                return False
            
            # Atomically set to claimed
            cursor.execute(
                "UPDATE products_to_scrape SET scraping_status = %s, last_attempt = NOW() WHERE product_id = %s AND scraping_status = %s",
                (CLAIM_STATUS, str(product_id), PENDING_STATUS)
            )
            claimed = cursor.rowcount > 0
            conn.commit()
            cursor.close()
            conn.close()
            return claimed

        # With claims support, atomically claim or renew our claim
        cursor.execute(
            """
            UPDATE products_to_scrape
            SET scraping_status = %s,
                claimed_by = %s,
                claimed_at = NOW(),
                last_attempt = NOW()
            WHERE product_id = %s
              AND (
                  scraping_status = %s
                  OR (scraping_status = %s AND claimed_by = %s)
                  OR (scraping_status = %s AND claimed_at < (NOW() - (%s || ' minutes')::interval))
              )
            """,
            (CLAIM_STATUS, worker_id, str(product_id), PENDING_STATUS, CLAIM_STATUS, worker_id, CLAIM_STATUS, int(ttl_minutes))
        )
        claimed = cursor.rowcount > 0
        conn.commit()
        cursor.close()
        conn.close()
        return claimed
    except Exception as e:
        print(f"Error verifying/claiming product {product_id} in DB: {e}")
        return True

def update_product_status(product_id, scraping_status, error_message=None):
    """Update the scraping_status of a product in the products_to_scrape table."""
    try:
        conn = _get_pg_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE products_to_scrape
                SET scraping_status = %s,
                    last_attempt = CURRENT_TIMESTAMP,
                    error_message = %s,
                    claimed_by = NULL,
                    claimed_at = NULL
                WHERE product_id = %s
                """,
                (scraping_status, error_message, str(product_id)),
            )
        except Exception:
            conn.rollback()
            cursor.execute(
                "UPDATE products_to_scrape SET scraping_status = %s, last_attempt = CURRENT_TIMESTAMP, error_message = %s WHERE product_id = %s",
                (scraping_status, error_message, str(product_id)),
            )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error updating status for {product_id}: {e}")

def reset_error_products_to_pending():
    """Reset all products with 'error' scraping_status to 'pending' to retry them."""
    try:
        pg_host = os.environ.get("PG_HOST")
        pg_port = os.environ.get("PG_PORT", "5432")
        pg_user = os.environ.get("PG_USER")
        pg_pass = os.environ.get("PG_PASS")
        pg_db = os.environ.get("PG_DB")

        conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE products_to_scrape SET scraping_status = 'pending' WHERE scraping_status = 'error'"
        )
        conn.commit()
        affected_rows = cursor.rowcount
        cursor.close()
        conn.close()
        if affected_rows > 0:
            print(f"✓ Reset {affected_rows} failed products from 'error' to 'pending' for retry.")
        return affected_rows
    except Exception as e:
        print(f"Error resetting error products to pending: {e}")
        return 0

def reset_invalid_url_products_for_retry():
    """Reset all products that completed but had invalid URLs back to pending and delete their scraped results."""
    try:
        pg_host = os.environ.get("PG_HOST")
        pg_port = os.environ.get("PG_PORT", "5432")
        pg_user = os.environ.get("PG_USER")
        pg_pass = os.environ.get("PG_PASS")
        pg_db = os.environ.get("PG_DB")

        conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db)
        cursor = conn.cursor()
        
        # Select target product IDs that have invalid URLs (e.g. 1stopbedrooms or not ibp/share URLs)
        # Exclude products that failed with 'no_products' or 'no_match' status
        select_query = """
            SELECT product_id
            FROM product_scraping_results
            WHERE product_url NOT LIKE 'https://www.google.com/search?ibp=oshop%%'
              AND product_url NOT LIKE 'https://share.google/%%'
              AND status NOT IN ('no_products', 'no_match')
        """
        cursor.execute(select_query)
        target_ids = [row[0] for row in cursor.fetchall()]
        
        if target_ids:
            # 1. Delete seller records for these products
            cursor.execute("DELETE FROM product_sellers WHERE product_code = ANY(%s)", (target_ids,))
            
            # 2. Delete the scraping results for these products
            cursor.execute("DELETE FROM product_scraping_results WHERE product_id = ANY(%s)", (target_ids,))
            
            # 3. Update products_to_scrape status to pending and clear claims/errors
            cursor.execute(
                """
                UPDATE products_to_scrape
                SET scraping_status = 'pending',
                    error_message = NULL,
                    claimed_by = NULL,
                    claimed_at = NULL
                WHERE product_id = ANY(%s)
                """,
                (target_ids,)
            )
            
            conn.commit()
            print(f"✓ Reset {len(target_ids)} products with invalid URLs back to pending and deleted their scraped results.")
        else:
            print("No completed products with invalid URLs to reset.")
            
        cursor.close()
        conn.close()
        return len(target_ids)
    except Exception as e:
        print(f"Error resetting invalid URL products for retry: {e}")
        return 0

def generate_reconciliation_report(output_path):
    """Query the database and compile the detailed flat reconciliation CSV report."""
    try:
        pg_host = os.environ.get("PG_HOST")
        pg_port = os.environ.get("PG_PORT", "5432")
        pg_user = os.environ.get("PG_USER")
        pg_pass = os.environ.get("PG_PASS")
        pg_db = os.environ.get("PG_DB")

        conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db)
        
        # Fetch only products that have been scraped (non-pending status)
        products_df = pd.read_sql("SELECT * FROM products_to_scrape WHERE scraping_status != 'pending'", conn)
        results_df = pd.read_sql("SELECT * FROM product_scraping_results", conn)
        sellers_df = pd.read_sql("SELECT * FROM product_sellers", conn)
        conn.close()

        if products_df.empty:
            print("No products found in DB to generate report.")
            return None

        print(f"Generating report from {len(products_df)} products, {len(results_df)} scraped results, and {len(sellers_df)} competitor offers...")

        # Build a dictionary of scraped results for fast lookup
        results_dict = {row['product_id']: row for _, row in results_df.iterrows()}
        
        # Group sellers by product_code for summary stats
        sellers_by_prod = {}
        for _, s_row in sellers_df.iterrows():
            p_code = s_row['product_code']
            if p_code not in sellers_by_prod:
                sellers_by_prod[p_code] = []
            sellers_by_prod[p_code].append(s_row)

        report_rows = []

        for _, p_row in products_df.iterrows():
            p_id = p_row['product_id']
            r_row = results_dict.get(p_id, {})
            
            # Product level fields
            prod_name = r_row.get('product_name') or p_row.get('name') or ''
            barcode = p_row.get('gtin') or ''
            brand = p_row.get('brand') or ''
            category = p_row.get('category') or ''
            tags = p_row.get('keyword') or ''
            num_matches = int(r_row.get('seller_count') or 0)
            my_position = int(r_row.get('osb_position') or 0)
            my_index = my_position - 1 if my_position > 0 else -1
            
            # Competitor stats for this product
            p_sellers = sellers_by_prod.get(p_id, [])
            prices = []
            for s in p_sellers:
                if s['seller_price'] is not None:
                    try:
                        prices.append(float(s['seller_price']))
                    except:
                        pass
            
            min_price = min(prices) if prices else 0.00
            max_price = max(prices) if prices else 0.00
            avg_price = sum(prices) / len(prices) if prices else 0.00
            
            # Find cheapest and highest site
            cheapest_site = ''
            highest_site = ''
            if p_sellers:
                valid_sellers = [s for s in p_sellers if s['seller_price'] is not None]
                if valid_sellers:
                    try:
                        cheapest_item = min(valid_sellers, key=lambda x: float(x['seller_price']))
                        highest_item = max(valid_sellers, key=lambda x: float(x['seller_price']))
                        cheapest_site = cheapest_item['seller_name']
                        highest_site = highest_item['seller_name']
                    except:
                        pass

            # Find My Price (1StopBedrooms)
            my_price = 0.00
            for s in p_sellers:
                if s['seller_name'] == '1StopBedrooms':
                    try:
                        my_price = float(s['seller_price']) if s['seller_price'] is not None else 0.00
                    except:
                        my_price = 0.00
                    break
            
            my_total_price = my_price
            my_product_cost = 0.00  # Default
            additional_cost = 0.00  # Default
            
            # Calculate SmartPrice
            # Rule: price below the cheapest competitor (excluding 1StopBedrooms)
            competitor_prices = []
            for s in p_sellers:
                if s['seller_name'] != '1StopBedrooms' and s['seller_price'] is not None:
                    try:
                        competitor_prices.append(float(s['seller_price']))
                    except:
                        pass
            
            if competitor_prices:
                cheapest_competitor_price = min(competitor_prices)
                smart_price = cheapest_competitor_price - 0.01
            else:
                smart_price = my_price if my_price > 0 else min_price
                
            # Keep SmartPrice sane (e.g. not negative)
            smart_price = max(0.00, smart_price)
            
            last_update_cycle = r_row.get('updated_at')
            if pd.isna(last_update_cycle) or not last_update_cycle:
                last_update_cycle = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                if isinstance(last_update_cycle, str):
                    pass
                else:
                    last_update_cycle = last_update_cycle.strftime("%Y-%m-%d %H:%M:%S")

            scrapped_url = r_row.get('url') or p_row.get('url') or ''
            osb_url_match = r_row.get('osb_url_match') or 'No'

            # If there are NO sellers, we still write 1 row for the product so it is represented in the file
            if not p_sellers:
                report_rows.append({
                    'Product Name': prod_name,
                    'Product Code': p_id,
                    'Barcode': barcode,
                    'Brand': brand,
                    'Category': category,
                    'Product Tags': tags,
                    'Number of Matches': 0,
                    'My Index': -1,
                    'My Position': 0,
                    'Cheapest Site': '',
                    'Highest Site': '',
                    'Minimum Price (Total Price)': 0.00,
                    'Maximum Price (Total Price)': 0.00,
                    'Average Price (Total Price)': 0.00,
                    'My Price': 0.00,
                    'My Total Price': 0.00,
                    'My Product Cost': 0.00,
                    'Additional Cost': 0.00,
                    'SmartPrice': 0.00,
                    'Last Update Cycle': last_update_cycle,
                    'Site': '',
                    'Site Index': -1,
                    'Total Price': 0.00,
                    'Change direction': 'N/A',
                    'Stock': '',
                    'URL': '',
                    'OSB URL match': osb_url_match,
                    'Scrapped url(google url)': scrapped_url
                })
            else:
                # Add a row for EACH competitor offer
                for idx, s in enumerate(p_sellers):
                    s_name = s['seller_name']
                    try:
                        s_price = float(s['seller_price']) if s['seller_price'] is not None else 0.00
                    except:
                        s_price = 0.00
                    s_url = s['seller_url']
                    s_stock = s['stock_status']
                    
                    s_osb_match = 'Yes' if s_name == '1StopBedrooms' else 'No'
                    
                    # Determine Change direction
                    change_dir = 'N/A'
                    if my_price > 0 and s_price > 0:
                        if s_price < my_price:
                            change_dir = 'Lower'
                        elif s_price > my_price:
                            change_dir = 'Higher'
                        else:
                            change_dir = 'Equal'
                            
                    report_rows.append({
                        'Product Name': prod_name,
                        'Product Code': p_id,
                        'Barcode': barcode,
                        'Brand': brand,
                        'Category': category,
                        'Product Tags': tags,
                        'Number of Matches': num_matches,
                        'My Index': my_index,
                        'My Position': my_position,
                        'Cheapest Site': cheapest_site,
                        'Highest Site': highest_site,
                        'Minimum Price (Total Price)': min_price,
                        'Maximum Price (Total Price)': max_price,
                        'Average Price (Total Price)': avg_price,
                        'My Price': my_price,
                        'My Total Price': my_total_price,
                        'My Product Cost': my_product_cost,
                        'Additional Cost': additional_cost,
                        'SmartPrice': smart_price,
                        'Last Update Cycle': last_update_cycle,
                        'Site': s_name,
                        'Site Index': idx + 1, # 1-based index in offers list
                        'Total Price': s_price,
                        'Change direction': change_dir,
                        'Stock': s_stock,
                        'URL': s_url,
                        'OSB URL match': s_osb_match,
                        'Scrapped url(google url)': scrapped_url
                    })

        report_df = pd.DataFrame(report_rows)
        # Reorder to guarantee exactly matching requested columns order
        COLUMNS_ORDER = [
            'Product Name', 'Product Code', 'Barcode', 'Brand', 'Category',
            'Product Tags', 'Number of Matches', 'My Index', 'My Position',
            'Cheapest Site', 'Highest Site', 'Minimum Price (Total Price)',
            'Maximum Price (Total Price)', 'Average Price (Total Price)',
            'My Price', 'My Total Price', 'My Product Cost', 'Additional Cost',
            'SmartPrice', 'Last Update Cycle', 'Site', 'Site Index', 'Total Price',
            'Change direction', 'Stock', 'URL', 'OSB URL match', 'Scrapped url(google url)'
        ]
        report_df = report_df[COLUMNS_ORDER]
        report_df.to_csv(output_path, index=False)
        print(f"✓ Reconciliation report saved successfully to: {output_path} ({len(report_rows)} rows)")
        
        # Automatically upload to FTP if credentials are provided in the environment
        ftp_host = os.environ.get("FTP_HOST")
        ftp_user = os.environ.get("FTP_USER")
        ftp_pass = os.environ.get("FTP_PASS")
        ftp_path = os.environ.get("FTP_PATH", "/scrap")
        
        if ftp_host and ftp_user and ftp_pass:
            remote_filename = os.path.basename(output_path)
            upload_to_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, output_path, remote_filename)
        else:
            print("Skipping FTP upload: Missing credentials (FTP_HOST, FTP_USER, or FTP_PASS)")
            
        return output_path
    except Exception as e:
        print(f"Error generating reconciliation report: {e}")
        traceback.print_exc()
        return None

PRODUCT_FINAL_COLUMNS = [
    "product_id",
    "web_id",
    "name",
    "mpn_sku",
    "gtin",
    "brand",
    "category",
    "keyword",
    "url",
    "osb_url",
    "last_response",
    "osb_url_match",
    "product_url",
    "seller",
    "product_name",
    "cid",
    "pid",
    "last_fetched_date",
    "osb_position",
    "osb_id",
    "seller_count",
    "status",
    "product_about_info",
    "main_image",
    "description",
    "attributes"
]
# Import the existing captcha solving functions
try:
    from solvecaptcha import solve_recaptcha_audio
except ImportError:
    # If solvecaptcha is not in same directory, try to import from current directory
    import importlib.util
    import sys
    
    # Add current directory to path
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    try:
        from solvecaptcha import solve_recaptcha_audio
    except ImportError:
        print("Warning: solvecaptcha module not found. Captcha solving will be disabled.")
        
        # Define a dummy function if module is not available
        def solve_recaptcha_audio(driver):
            print("Captcha solving module not available. Please install solvecaptcha.")
            return "failed"

def parse_platform_from_user_agent(user_agent):
    ua = (user_agent or "").lower()
    if "windows" in ua:
        return "Windows", "Win32"
    if "mac os x" in ua or "macintosh" in ua:
        return "macOS", "MacIntel"
    return "Linux", "Linux x86_64"

def build_user_agent_metadata(user_agent, platform_name):
    match = re.search(r"Chrome/(\d+)\.(\d+)\.(\d+)\.(\d+)", user_agent or "")
    if not match:
        return None

    major = match.group(1)
    full_version = ".".join(match.groups())
    return {
        "brands": [
            {"brand": "Not/A)Brand", "version": "8"},
            {"brand": "Chromium", "version": major},
            {"brand": "Google Chrome", "version": major},
        ],
        "fullVersionList": [
            {"brand": "Not/A)Brand", "version": "8.0.0.0"},
            {"brand": "Chromium", "version": full_version},
            {"brand": "Google Chrome", "version": full_version},
        ],
        "fullVersion": full_version,
        "platform": platform_name,
        "platformVersion": "10.0.0" if platform_name == "Windows" else "0.0.0",
        "architecture": "x86",
        "model": "",
        "mobile": False,
        "bitness": "64",
        "wow64": False,
    }

def normalize_driver_fingerprint(driver):
    accept_language = os.environ.get("BROWSER_ACCEPT_LANGUAGE", "en-US,en;q=0.9")
    timezone_id = os.environ.get("BROWSER_TIMEZONE", "America/New_York")
    locale = accept_language.split(",")[0].strip() or "en-US"

    try:
        browser_version = driver.execute_cdp_cmd("Browser.getVersion", {})
    except Exception as exc:
        print(f"Fingerprint normalization skipped: {exc}")
        return

    raw_user_agent = browser_version.get("userAgent", "") or ""
    user_agent = raw_user_agent.replace("HeadlessChrome/", "Chrome/")
    platform_name, navigator_platform = parse_platform_from_user_agent(user_agent)
    metadata = build_user_agent_metadata(user_agent, platform_name)

    try:
        driver.execute_cdp_cmd("Network.enable", {})
    except Exception:
        pass

    ua_override = {
        "userAgent": user_agent,
        "acceptLanguage": accept_language,
        "platform": navigator_platform,
    }
    if metadata:
        ua_override["userAgentMetadata"] = metadata

    try:
        driver.execute_cdp_cmd("Network.setUserAgentOverride", ua_override)
    except Exception as exc:
        print(f"User agent override skipped: {exc}")

    try:
        driver.execute_cdp_cmd("Emulation.setLocaleOverride", {"locale": locale})
    except Exception as exc:
        print(f"Locale override skipped: {exc}")

    try:
        driver.execute_cdp_cmd("Emulation.setTimezoneOverride", {"timezoneId": timezone_id})
    except Exception as exc:
        print(f"Timezone override skipped: {exc}")

    script = f"""
Object.defineProperty(navigator, 'webdriver', {{
  get: () => undefined,
}});
Object.defineProperty(navigator, 'languages', {{
  get: () => ['en-US', 'en'],
}});
Object.defineProperty(navigator, 'platform', {{
  get: () => '{navigator_platform}',
}});
Object.defineProperty(navigator, 'hardwareConcurrency', {{
  get: () => 8,
}});
Object.defineProperty(navigator, 'deviceMemory', {{
  get: () => 8,
}});
Object.defineProperty(navigator, 'plugins', {{
  get: () => [1, 2, 3, 4, 5],
}});
window.chrome = window.chrome || {{
  runtime: {{}},
  app: {{}},
}};
"""
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": script})
    except Exception as exc:
        print(f"Preload fingerprint script skipped: {exc}")

def accept_google_consent_if_present(driver):
    consent_selectors = [
        (By.XPATH, "//button[.//div[normalize-space()='Accept all'] or normalize-space()='Accept all']"),
        (By.XPATH, "//button[.//div[normalize-space()='I agree'] or normalize-space()='I agree']"),
        (By.XPATH, "//div[@role='button'][normalize-space()='Accept all' or normalize-space()='I agree']"),
    ]
    for by, selector in consent_selectors:
        try:
            button = WebDriverWait(driver, 4).until(EC.element_to_be_clickable((by, selector)))
            driver.execute_script("arguments[0].click();", button)
            time.sleep(random.uniform(1.0, 2.0))
            return True
        except Exception:
            continue
    return False

def warm_google_session(driver):
    try:
        driver.get("https://www.google.com/ncr")
        time.sleep(random.uniform(2.0, 3.5))
        accept_google_consent_if_present(driver)

        try:
            search_box = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.NAME, "q"))
            )
            search_box.click()
            time.sleep(random.uniform(0.4, 0.9))
            search_box.send_keys("furniture")
            time.sleep(random.uniform(0.3, 0.7))
            search_box.send_keys(Keys.ENTER)
            time.sleep(random.uniform(2.0, 3.5))
        except Exception:
            pass

        try:
            driver.execute_script("window.scrollBy(0, Math.max(300, window.innerHeight * 0.35));")
            time.sleep(random.uniform(0.8, 1.4))
        except Exception:
            pass
    except Exception as exc:
        print(f"Session warm-up skipped: {exc}")

def get_chrome_major_version():
    try:
        env_ver = os.environ.get("CHROME_VERSION_MAIN")
        if env_ver:
            return int(env_ver)
            
        import subprocess
        commands = [
            ["google-chrome", "--version"],
            ["chrome", "--version"],
            ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "--version"],
            ["google-chrome-stable", "--version"]
        ]
        
        chrome_bin = os.environ.get("CHROME_BIN")
        if chrome_bin:
            commands.insert(0, [chrome_bin, "--version"])
            
        for cmd in commands:
            try:
                cmd_str = cmd if isinstance(cmd, str) else " ".join([f'"{c}"' for c in cmd])
                output = subprocess.check_output(cmd_str, shell=True, stderr=subprocess.DEVNULL).decode().strip()
                match = re.search(r'Chrome\s+(\d+)\.', output, re.IGNORECASE)
                if match:
                    val = int(match.group(1))
                    print(f"✓ Detected Google Chrome major version: {val}")
                    return val
            except:
                continue
    except Exception as e:
        print(f"Error detecting Chrome version: {e}")
    return None

def setup_driver(max_attempts=3, base_delay=4):
    last_err = None
    
    def build_chrome_options():
        options = uc.ChromeOptions()
        chrome_bin = os.environ.get("CHROME_BIN")
        if chrome_bin:
            options.binary_location = chrome_bin
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-logging")
        options.add_argument("--log-level=3")
        options.add_argument("--window-size=1366,768")
        options.add_argument("--lang=en-US")
        options.add_argument("--disable-notifications")
        return options

    for attempt in range(1, max_attempts + 1):
        driver = None
        try:
            time.sleep(2)
            options = build_chrome_options()
            chrome_bin = os.environ.get("CHROME_BIN")
            chromedriver_bin = os.environ.get("CHROMEDRIVER_BIN")
            
            major_ver = get_chrome_major_version()
            
            uc_kwargs = {
                "options": options
            }
            
            if major_ver:
                uc_kwargs["version_main"] = major_ver
            
            if chromedriver_bin and os.path.exists(chromedriver_bin):
                uc_kwargs["driver_executable_path"] = chromedriver_bin
                print(f"✓ Using pre-configured ChromeDriver binary: {chromedriver_bin}")
                
            if chrome_bin and os.path.exists(chrome_bin):
                uc_kwargs["browser_executable_path"] = chrome_bin
                print(f"✓ Using pre-configured Chrome binary: {chrome_bin}")

            driver = uc.Chrome(**uc_kwargs)
            normalize_driver_fingerprint(driver)
            warm_google_session(driver)
            return driver

        except Exception as e:
            last_err = e
            print(f"Driver start failed (attempt {attempt}/{max_attempts}): {str(e)}")
            try:
                if driver:
                    driver.quit()
            except Exception:
                pass
            if attempt < max_attempts:
                time.sleep(base_delay * attempt + random.uniform(0, 2))
                
    if last_err:
        raise last_err
    raise RuntimeError("Driver start failed with unknown error")

def is_driver_connectivity_error(err):
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return (
        "chrome not reachable" in msg
        or "cannot connect to chrome" in msg
        or "disconnected" in msg
        or "session not created" in msg
    )

def build_error_result(product_id, keyword, url, message, status="error"):
    return {
        'product_id': product_id,
        'keyword': keyword,
        'url': url,
        'last_response': message,
        'status': status,
        'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'product_url': '',
        'seller': '',
        'product_name': '',
        'cid': '',
        'pid': '',
        'osb_position': 0,
        'osb_id': '',
        'seller_count': 0,
        'competitors': [],
        'product_about_info': json.dumps({}),
        'main_image': '',
        'description': '',
        'attributes': json.dumps({})
    }

def save_remaining_df(df, chunk_id, round_id, output_dir, reason=None):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(output_dir, exist_ok=True)
    csv3_filename = f"gshopping_remaining_round{round_id}_chunk{chunk_id}_{timestamp}.csv"
    csv3_path = os.path.join(output_dir, csv3_filename)
    df.to_csv(csv3_path, index=False)
    if reason:
        print(f"✓ Saved remaining rows: {csv3_filename} ({reason})")
    else:
        print(f"✓ Saved remaining rows: {csv3_filename}")
    return csv3_path, len(df)

def detects_recaptcha(driver):
    """Detect if reCAPTCHA is present on the page"""
    try:
        if driver.find_elements(By.CLASS_NAME, "rc-imageselect-challenge"):
            print("Puzzle reCAPTCHA detected!")
            return True
        iframe_sources = []
        for iframe in driver.find_elements(By.TAG_NAME, "iframe"):
            try:
                iframe_sources.append((iframe.get_attribute("src") or "").lower())
            except StaleElementReferenceException:
                continue

        if any("recaptcha" in src for src in iframe_sources):
            print("reCAPTCHA iframe detected!")
            return True

        print("No reCAPTCHA found.")
        return False
    except StaleElementReferenceException:
        print("reCAPTCHA check skipped due to transient stale iframe")
        return False
    except Exception as e:
        print(f"Error detecting reCAPTCHA: {e}")
        return False

# In your main gscrapperci.py, update the handle_captcha function:

def handle_captcha(driver, url):
    """Handle captcha if detected with retry logic"""
    max_retries = 1

    for attempt in range(max_retries):
        recaptcha = detects_recaptcha(driver)
        if recaptcha:
            print(f"Attempt {attempt + 1}/{max_retries} to solve captcha...")
            result = solve_recaptcha_audio(driver)
            
            if result == "solved":
                print("Captcha solved successfully!")
                driver.switch_to.default_content()
                return "solved"
            else:
                print(f"Captcha solving attempt {attempt + 1} failed")
                
                # if attempt < max_retries - 1:
                #     # Try refreshing the page
                #     print("Refreshing page and retrying...")
                #     driver.refresh()
                #     time.sleep(5)
                # else:
                #     print("All captcha solving attempts failed")
                #     return "failed"
                print("All captcha solving attempts failed")
                return "failed"
        else:
            print("No reCAPTCHA found.")
            return "no_captcha"
    
    return "failed"

def start_new_driver(search_url):
    """Start a new driver and handle captcha if present"""
    while True:
        try:
            driver.quit()
        except:
            pass
        
        driver = setup_driver()
        driver.get(search_url)
        
        # Handle captcha
        captcha_result = handle_captcha(driver, search_url)
        
        if captcha_result == "solved":
            return driver
        elif captcha_result == "no_captcha":
            return driver
        else:
            # Captcha solving failed, retry with new driver
            print("Captcha solving failed, retrying with new driver...")
            try:
                driver.quit()
            except:
                pass
            time.sleep(random.uniform(5, 8))


def split_csv(input_csv, output_dir, chunk_id, total_chunks):
    """Split CSV into chunks and return specific chunk"""
    try:
        df = pd.read_csv(input_csv)
        
        if df.empty:
            print("CSV file is empty")
            return None
        
        total_rows = len(df)
        rows_per_chunk = total_rows // total_chunks
        
        start_idx = (chunk_id - 1) * rows_per_chunk
        end_idx = chunk_id * rows_per_chunk if chunk_id < total_chunks else total_rows
        
        chunk_df = df.iloc[start_idx:end_idx]
        
        os.makedirs(output_dir, exist_ok=True)
        chunk_filename = f"chunk_{chunk_id}.csv"
        chunk_path = os.path.join(output_dir, chunk_filename)
        
        chunk_df.to_csv(chunk_path, index=False)
        
        print(f"Chunk {chunk_id}: Rows {start_idx+1} to {end_idx} ({len(chunk_df)} rows)")
        return chunk_path
        
    except Exception as e:
        print(f"Error splitting CSV: {str(e)}")
        return None

def get_product_options(driver):
    """Extract product variant options from the product panel"""
    scraped_data = {}
    
    try:
        panel = driver.find_element(By.XPATH, "//div[@jsname='Ql2bfc']")
    except NoSuchElementException:
        try:
            panel = driver.find_element(By.XPATH, "//div[@jsname='jzfSje']")
        except NoSuchElementException:
            print("Error: Could not find any product panel container.")
            return json.dumps({}, indent=2)

    # Scrape Swatch-style Filters
    swatch_groups = panel.find_elements(By.XPATH, ".//div[@jsname='iaBacd']")
    
    for group in swatch_groups:
        try:
            title = group.find_element(By.XPATH, ".//span[@class='ZMOBjc']").text
            if not title:
                continue
            
            options = []
            swatches = group.find_elements(By.XPATH, ".//a[@jsname='dbgGYd']")
            for swatch in swatches:
                label = swatch.get_attribute('data-label')
                if label:
                    options.append(label)
            
            if title and options:
                scraped_data[title] = list(dict.fromkeys(options))
                
        except Exception as e:
            print(f"Warning: Could not parse a swatch group. Error: {e}")
            continue

    # Scrape Dropdown-style Filters
    dropdown_groups = panel.find_elements(By.XPATH, ".//div[@data-attrid='variant_picker_chip']")

    for group in dropdown_groups:
        try:
            title_text_element = group.find_element(By.XPATH, ".//div[contains(@class, 'PQev6c')]")
            title_text = title_text_element.get_attribute('textContent').strip()
            
            if ":" in title_text:
                title = title_text.split(":")[0].strip()
            else:
                title = title_text.strip()
                
            if not title:
                continue
                
            options = []
            menu_items = group.find_elements(By.XPATH, ".//g-menu/g-menu-item")
            if menu_items:
                for item in menu_items:
                    try:
                        item_text = item.find_element(By.XPATH, ".//span").get_attribute('textContent').strip()
                        if item_text:
                            options.append(item_text)
                    except NoSuchElementException:
                        continue
            else:
                popup_items = group.find_elements(By.XPATH, ".//g-popup//div[@role='menuitemradio']")
                for item in popup_items:
                    try:
                        item_text = item.find_element(By.XPATH, ".//div[@class='PQev6c']").get_attribute('textContent').strip()
                        if item_text:
                            options.append(item_text)
                    except NoSuchElementException:
                        continue

            if title and options:
                scraped_data[title] = list(dict.fromkeys(options))

        except Exception as e:
            print(f"Warning: Could not parse a dropdown group ('{title}'). Error: {e}")
            continue
    
    return json.dumps(scraped_data, indent=2)

def get_product_about_info(driver):
    """
    Extract the 'About this product' section including description and attributes.
    Expands the "More details" button if needed to get all data.
    Returns a JSON string with description and attributes.
    """
    product_info = {
        'description': '',
        'attributes': {},
        'main_image': ''
    }
    
    try:
        print("Extracting 'About this product' information...")
        
        # Find the About this product section
        about_section = None
        try:
            about_section = driver.find_element(By.XPATH, "//div[@jsname='HhYL2b']")
        except:
            try:
                about_section = driver.find_element(By.XPATH, "//h3[contains(text(),'About this product')]/ancestor::div[1]")
            except:
                print("Could not find 'About this product' section")
                return json.dumps(product_info)
        
        # Extract description
        try:
            desc_element = about_section.find_element(By.XPATH, ".//div[@jsname='yKDmZd']")
            product_info['description'] = desc_element.text.strip()
        except:
            pass

        # Extract main image
        try:
            print("Attempting to extract main image...")
            # Comprehensive list of selectors for main image in Google Shopping panel
            # Based on user-provided HTML: img class "KfAt4d" inside div class "DqsAAd"
            image_selectors = [
                "//img[@class='KfAt4d']",
                "//div[@class='DqsAAd']//img",
                "//div[@jsname='figiqf']//img",
                "//div[@jsname='SAt90e']//img",
                "//div[contains(@class,'m8U2Z')]//img",
                "//div[@class='hB4fJb']//img",
                "//img[contains(@class,'sh-div__image')]",
                "//div[@class='L8S89c']//img",
                "//div[@class='B08B9']//img",
                "//div[contains(@class,'sh-div__image-container')]//img",
                "//div[@id='sh-div__image-container']//img",
                "//img[@id='sh-div__main-image']",
                "//img[contains(@class, 'r429ob')]",
                "//div[contains(@class, 'FLY67')]//img",
                "//div[contains(@class, 'sh-ds__image-container')]//img"
            ]
            
            for selector in image_selectors:
                try:
                    # Increase wait slightly for high-res images
                    img_element = WebDriverWait(driver, 4).until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )
                    
                    # Try srcset first for higher quality, then src
                    img_url = img_element.get_attribute('srcset')
                    if img_url:
                        # Pick the first URL in srcset
                        img_url = img_url.split(',')[0].split(' ')[0]
                    
                    if not img_url:
                        img_url = img_element.get_attribute('src')
                        
                    if img_url and not img_url.startswith('data:'):
                        product_info['main_image'] = img_url
                        print(f"✓ Found main image: {img_url[:60]}...")
                        break
                except:
                    continue
            
            # Final fallback: scan all images in the panel and pick the largest one
            if not product_info['main_image']:
                print("Using aggressive image fallback...")
                try:
                    # Look for images in the main panel area
                    all_imgs = driver.find_elements(By.XPATH, "//div[@jsname='HhYL2b']//img | //div[@jsname='SAt90e']//img | //div[contains(@class, 'm8U2Z')]//img | //div[@class='DqsAAd']//img")
                    print(f"Found {len(all_imgs)} images in panel area")
                    best_img = None
                    max_area = 0
                    
                    for img in all_imgs:
                        try:
                            src = img.get_attribute('src')
                            if not src or src.startswith('data:'):
                                continue
                                
                            w = int(img.get_attribute('width') or 0)
                            h = int(img.get_attribute('height') or 0)
                            area = w * h
                            print(f"  - Image: {src[:40]}... (Size: {w}x{h})")
                            
                            if area > max_area and w > 100:
                                max_area = area
                                best_img = src
                        except:
                            continue
                    
                    if best_img:
                        product_info['main_image'] = best_img
                        print(f"✓ Found main image via aggressive fallback: {best_img[:60]}...")
                except Exception as fe:
                    print(f"Aggressive fallback failed: {str(fe)}")
        except Exception as e:
            print(f"Error extracting main image: {str(e)}")
        
        # Check if "More details" button exists and click it
        try:
            # Look for collapsed state first
            more_button = about_section.find_element(By.XPATH, ".//div[@role='button' and contains(., 'More details')]")
            
            # Check if it's the collapsed version (aria-expanded="false")
            aria_expanded = more_button.get_attribute('aria-expanded')
            
            if aria_expanded == 'false' or not aria_expanded:
                print("Clicking 'More details' button to expand attributes...")
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", more_button)
                time.sleep(1)
                more_button.click()
                time.sleep(2)  # Wait for expansion
        except:
            print("No 'More details' button found or already expanded")
        
        # Extract all attributes
        try:
            # Find all attribute rows
            attribute_rows = about_section.find_elements(By.XPATH, ".//div[@role='row' and contains(@class,'YU1Fsb')]")
            
            for row in attribute_rows:
                try:
                    # Get attribute name
                    name_element = row.find_element(By.XPATH, ".//div[contains(@class,'TCzUld')]")
                    attr_name = name_element.text.strip()
                    
                    # Get attribute value
                    value_element = row.find_element(By.XPATH, ".//div[contains(@class,'uAwmIf')]//div")
                    attr_value = value_element.text.strip()
                    
                    if attr_name and attr_value:
                        product_info['attributes'][attr_name] = attr_value
                        
                except Exception as e:
                    continue
                    
        except Exception as e:
            print(f"Error extracting attributes: {str(e)}")
        
        print(f"Extracted {len(product_info['attributes'])} attributes")
        
    except Exception as e:
        print(f"Error in get_product_about_info: {str(e)}")
    
    return json.dumps(product_info)


def normalize_url_path_slug(raw_url):
    """Return normalized last path segment (slug), removing query/fragment."""
    try:
        if not raw_url:
            return ""
        cleaned = str(raw_url).strip()
        if not cleaned or cleaned.lower() == "n/a":
            return ""
        if "://" not in cleaned and cleaned.startswith("www."):
            cleaned = f"https://{cleaned}"

        parsed = urlparse(cleaned)
        path = unquote(parsed.path or "").strip()
        path = re.sub(r"/+", "/", path).rstrip("/")
        if not path:
            return ""
        return path.split("/")[-1].strip().lower()
    except:
        return ""

MAX_PRODUCT_TRIES = 5
PRODUCT_CLICK_RETRIES = 2
PANEL_WAIT_SECONDS = 8
OFFERS_WAIT_SECONDS = 8
OFFERS_RETRIES = 2

def build_retry_search_url(search_url):
    """Remove the 1stopbedrooms+ prefix from the q query parameter when present."""
    try:
        parsed = urlparse(search_url)
        query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        updated_pairs = []
        changed = False
        for key, value in query_pairs:
            if key == "q":
                new_value = re.sub(r"(?i)^1stopbedrooms(?:\s|\+)+", "", value or "")
                if new_value != value:
                    value = new_value
                    changed = True
            updated_pairs.append((key, value))
        if not changed:
            return search_url
        return urlunparse(parsed._replace(query=urlencode(updated_pairs)))
    except Exception:
        return search_url

def log_matching(product_id, message):
    print(f"[PID {os.getpid()}] {message}")

def wait_for_product_container(driver, timeout=10):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CLASS_NAME, "dURPMd"))
    )

def get_visible_product_cards(driver):
    mains = wait_for_product_container(driver, timeout=10)
    return mains.find_elements(By.CLASS_NAME, "MtXiu")

def product_matches_keyword(product_name, keyword):
    normalized_keyword = re.sub(r'\bset\s+of\b', '', keyword or '', flags=re.IGNORECASE)
    normalized_product_name = re.sub(r'\bset\s+of\b', '', product_name or '', flags=re.IGNORECASE)

    def has_set_word(text):
        return bool(re.search(r'\bset\b', text or '', flags=re.IGNORECASE))

    return has_set_word(normalized_product_name) == has_set_word(normalized_keyword)

def extract_product_card_meta(product):
    try:
        product_name = product.find_element(By.XPATH, ".//div[contains(@class,'gkQHve')]").text
    except Exception:
        product_name = ""

    try:
        seller = product.find_element(By.XPATH, ".//span[contains(@class,'WJMUdc')]").text
    except Exception:
        seller = ""

    try:
        cid = product.get_attribute('id')
    except Exception:
        cid = ""

    return {
        'product_name': product_name,
        'seller': seller,
        'cid': cid,
    }

def extract_share_url(driver):
    share_url = ""
    try:
        share_button = WebDriverWait(driver, PANEL_WAIT_SECONDS).until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//div[contains(@class,'RSNrZe') and @role='button' and @aria-label='Share']"
            ))
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", share_button)
        share_button.click()

        share_dialog = WebDriverWait(driver, PANEL_WAIT_SECONDS).until(
            EC.visibility_of_element_located((By.XPATH, "//div[@role='dialog' and @aria-label='Share']"))
        )

        try:
            share_input = share_dialog.find_element(By.CSS_SELECTOR, "input[aria-label='Share link'][type='url']")
            share_url = (share_input.get_attribute("value") or "").strip()
        except Exception:
            share_url = ""

        if not share_url:
            try:
                share_url = share_dialog.find_element(By.CSS_SELECTOR, "div[jsname='tQ9n1c']").text.strip()
            except Exception:
                share_url = ""

        try:
            close_button = share_dialog.find_element(By.CSS_SELECTOR, "[jsname='tqp7ud']")
            close_button.click()
        except Exception:
            try:
                ActionChains(driver).send_keys(u'\ue00c').perform()  # ESC
            except Exception:
                pass
    except Exception:
        share_url = ""
    return share_url

def expand_more_stores(driver):
    clicks = 0
    while clicks < 2:
        try:
            more_stores = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'duf-h')]//div[@role='button']"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", more_stores)
            more_stores.click()
            time.sleep(random.uniform(1.5, 2.5))
            clicks += 1
        except Exception:
            break

def populate_offers_for_selected_product(driver, result, product_id, osb_url):
    result['competitors'] = []
    raw_url = (extract_share_url(driver) or driver.current_url or "").strip()
    if raw_url.startswith("https://www.google.com/search?ibp=oshop") or raw_url.startswith("https://share.google/"):
        result['product_url'] = raw_url
    else:
        result['product_url'] = ""

    expand_more_stores(driver)

    last_error = None
    offers_grid = None
    for offer_attempt in range(OFFERS_RETRIES):
        try:
            offers_grid = WebDriverWait(driver, OFFERS_WAIT_SECONDS).until(
                EC.presence_of_element_located((By.XPATH, "//div[@jsname='RSFNod' and @data-attrid='organic_offers_grid']"))
            )
            break
        except Exception as exc:
            last_error = exc
            if offer_attempt + 1 < OFFERS_RETRIES:
                time.sleep(1)

    if offers_grid is None:
        raise last_error or Exception("Offers grid not found")

    exists = len(driver.find_elements(
        By.XPATH,
        "//div[contains(@class,'iI1aN')]//div[@class='EDblX kjqWgb']"
    )) > 0

    if exists > 0:
        result['options'] = get_product_options(driver)

    # ★ NEW: Add product about info scraping
    try:
        about_data_json = get_product_about_info(driver)
        about_data = json.loads(about_data_json)
        result['product_about_info'] = about_data_json
        result['description'] = about_data.get('description', '')
        result['attributes'] = json.dumps(about_data.get('attributes', {}))
        result['main_image'] = about_data.get('main_image', '')
        print("✓ Product about info, description, attributes and image extracted")
    except Exception as e:
        print(f"Error extracting product about info: {str(e)}")
        result['product_about_info'] = json.dumps({'description': '', 'attributes': {}, 'main_image': ''})
        result['description'] = ''
        result['attributes'] = json.dumps({})
        result['main_image'] = ''

    offer_elements = offers_grid.find_elements(By.CLASS_NAME, 'R5K7Cb')
    print(f"Found {len(offer_elements)} offers")

    competitors = []
    for seller_html in offer_elements:
        try:
            store_name = seller_html.find_element(By.CSS_SELECTOR, "div.hP4iBf.gUf0b.uWvFpd").text.strip()
        except Exception:
            store_name = "N/A"

        try:
            seller_product_name = seller_html.find_element(By.CSS_SELECTOR, "div.Rp8BL").text.strip()
        except Exception:
            seller_product_name = "N/A"

        try:
            seller_url = seller_html.find_element(By.CSS_SELECTOR, "a.P9159d").get_attribute('href')
        except Exception:
            seller_url = "N/A"

        try:
            seller_price_element = seller_html.find_element(By.CSS_SELECTOR, "div.QcEgce span[aria-hidden='true']")
            seller_price = seller_price_element.text.strip()
        except Exception:
            try:
                seller_price_element = seller_html.find_element(By.CSS_SELECTOR, "div.GBgquf span")
                seller_price = seller_price_element.text.strip()
            except Exception:
                seller_price = "N/A"

        try:
            row_text = seller_html.text.lower()
            if "out of stock" in row_text:
                stock_status = "Out of Stock"
            elif "in stock" in row_text:
                stock_status = "In Stock"
            else:
                stock_status = "In Stock" # Default stock status
        except Exception:
            stock_status = "In Stock"

        competitor_data = {
            'product_id': product_id,
            'seller': store_name,
            'seller_product_name': seller_product_name,
            'seller_url': seller_url,
            'seller_price': seller_price,
            'stock_status': stock_status,
            'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        competitors.append(competitor_data)
        result['competitors'].append(competitor_data)

    search_seller = '1StopBedrooms'
    sellers = [c['seller'] for c in competitors]
    osb_position = 0
    seller_count = len(sellers)
    osb_id = ''
    osb_url_match = False

    if search_seller in sellers:
        osb_position = sellers.index(search_seller) + 1
        for competitor in competitors:
            if competitor['seller'] == search_seller:
                seller_slug = normalize_url_path_slug(competitor.get('seller_url', ''))
                osb_id = seller_slug
                target_slug = normalize_url_path_slug(osb_url)
                if seller_slug and target_slug:
                    osb_url_match = seller_slug == target_slug
                break

    result.update({
        'osb_position': osb_position,
        'seller_count': seller_count,
        'osb_id': osb_id,
        'status': 'completed',
        'osb_url_match': f'{"Yes" if osb_url_match else "No"}',
        'last_response': f'Completed - OSB Position: {osb_position}, Total Sellers: {seller_count}'
    })
    return result

def try_click_product(driver, cid):
    last_error = None
    for click_attempt in range(PRODUCT_CLICK_RETRIES):
        try:
            element = WebDriverWait(driver, PANEL_WAIT_SECONDS).until(
                EC.element_to_be_clickable((By.XPATH, f'//div[@id="{cid}"]'))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
            time.sleep(0.8)
            try:
                element.click()
            except Exception:
                driver.execute_script("arguments[0].click();", element)
            WebDriverWait(driver, PANEL_WAIT_SECONDS).until(
                lambda d: (
                    len(d.find_elements(By.XPATH, "//div[contains(@class,'RSNrZe') and @role='button' and @aria-label='Share']")) > 0
                    or len(d.find_elements(By.XPATH, "//div[@jsname='RSFNod' and @data-attrid='organic_offers_grid']")) > 0
                )
            )
            time.sleep(random.uniform(0.8, 1.5))
            return True
        except Exception as exc:
            last_error = exc
            time.sleep(1)
    raise last_error or Exception("Product click failed")

def attempt_selected_product(driver, base_result, product_meta, osb_url):
    attempt_result = dict(base_result)
    attempt_result['competitors'] = []
    attempt_result.update({
        'product_name': product_meta.get('product_name', ''),
        'seller': product_meta.get('seller', ''),
        'cid': product_meta.get('cid', ''),
        'pid': '',
        'status': 'product_found',
    })

    if not attempt_result['cid']:
        attempt_result['status'] = 'product_not_clickable'
        attempt_result['last_response'] = 'Missing product CID'
        return attempt_result

    try:
        try_click_product(driver, attempt_result['cid'])
        attempt_result['last_response'] = "Clicked on product successfully"
    except Exception as exc:
        attempt_result['status'] = 'product_not_clickable'
        attempt_result['last_response'] = f'Could not click product element: {str(exc)}'
        return attempt_result

    try:
        return populate_offers_for_selected_product(driver, attempt_result, base_result['product_id'], osb_url)
    except Exception as exc:
        attempt_result['status'] = 'no_offers_found'
        attempt_result['last_response'] = f'No offers found: {str(exc)}'
        return attempt_result

def run_product_selection_phase(driver, product_id, phase_name, search_url, base_result, osb_url, fallback_first=False):
    log_matching(product_id, f"{phase_name} started")
    driver.get(search_url)
    try:
        wait_for_product_container(driver, timeout=10)
        time.sleep(random.uniform(1.5, 2.5))
    except Exception as exc:
        phase_result = dict(base_result)
        phase_result['url'] = search_url
        phase_result['status'] = 'no_products'
        phase_result['last_response'] = f'No products found on page: {str(exc)}'
        log_matching(product_id, f"{phase_name} no products on page")
        return phase_result, False

    products = get_visible_product_cards(driver)
    if not products:
        phase_result = dict(base_result)
        phase_result['url'] = search_url
        phase_result['status'] = 'no_products'
        phase_result['last_response'] = 'No products found in container'
        return phase_result, False

    limit = min(MAX_PRODUCT_TRIES, len(products))
    log_matching(product_id, f"Found {len(products)} products -> trying {limit if len(products) >= MAX_PRODUCT_TRIES else 'all'}")

    matching_products = []
    for product in products:
        meta = extract_product_card_meta(product)
        if fallback_first or product_matches_keyword(meta.get('product_name', ''), base_result['keyword']):
            matching_products.append(meta)

    if fallback_first:
        matching_products = matching_products[:1]
    else:
        matching_products = matching_products[:limit]

    if not matching_products:
        phase_result = dict(base_result)
        phase_result['url'] = search_url
        phase_result['status'] = 'no_match'
        phase_result['last_response'] = 'No matching product found'
        return phase_result, False

    fallback_result = None
    for index, product_meta in enumerate(matching_products, start=1):
        log_matching(product_id, f"Trying product {index}")
        attempt_result = attempt_selected_product(driver, base_result, product_meta, osb_url)
        attempt_result['url'] = search_url

        if fallback_result is None:
            fallback_result = attempt_result

        if attempt_result.get('osb_position', 0) <= 0:
            log_matching(product_id, "OSB seller not present")
        elif attempt_result.get('osb_url_match') == 'Yes':
            log_matching(product_id, "OSB URL MATCHED -> stopping")
            return attempt_result, True
        else:
            log_matching(product_id, "OSB URL mismatch")

        if index < len(matching_products):
            try:
                driver.back()
                wait_for_product_container(driver, timeout=10)
                time.sleep(random.uniform(1.0, 2.0))
            except Exception:
                driver.get(search_url)
                wait_for_product_container(driver, timeout=10)
                time.sleep(random.uniform(1.0, 2.0))

    return fallback_result or dict(base_result), False

def scrape_product(driver, product_id, keyword, url, osb_url=""):
    """Scrape individual product from Google Shopping"""
    try:
        print(f"\nScraping Product ID: {product_id}")
        print(f"Keyword: {keyword}")
        
        driver.get(url)
        
        # Handle captcha before proceeding
        captcha_result = handle_captcha(driver, url)
        if captcha_result == "failed":
            return {
                'product_id': product_id,
                'keyword': keyword,
                'url': url,
                'last_response': 'Captcha solving failed',
                'status': 'captcha_failed',
                'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'product_url': '',  # ADD THIS LINE
                'seller': '',  # ADD THIS LINE
                'product_name': '',  # ADD THIS LINE
                'cid': '',  # ADD THIS LINE
                'pid': '',  # ADD THIS LINE
                'osb_position': 0,  # ADD THIS LINE
                'osb_id': '',  # ADD THIS LINE
                'seller_count': 0,  # ADD THIS LINE
                'competitors': [],  # Already present
                'product_about_info': json.dumps({}),
                'main_image': '',
                'description': '',
                'attributes': json.dumps({})
            }
        
        time.sleep(random.uniform(4, 8))
        
        # Initialize result structure
        result = {
            'product_id': product_id,
            'keyword': keyword,
            'url': url,
            'last_response': '',
            'product_url': '',
            'seller': '',
            'product_name': '',
            'cid': '',
            'pid': '',
            'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'osb_position': 0,
            'osb_id': '',
            'seller_count': 0,
            'status': '',
            'competitors': [],
            'product_about_info': '',
            'main_image': '',
            'description': '',
            'attributes': ''
        }
        
        try:
            phase_result, matched = run_product_selection_phase(
                driver, product_id, "Original search", url, result, osb_url
            )
            if matched:
                return phase_result

            retry_url = build_retry_search_url(url)
            final_result = phase_result
            if retry_url != url:
                log_matching(product_id, "Retry search without 1stopbedrooms prefix")
                phase_result, matched = run_product_selection_phase(
                    driver, product_id, "Retry search", retry_url, result, osb_url
                )
                final_result = phase_result
                if matched:
                    return phase_result
                if phase_result.get('status') == 'completed':
                    return phase_result

            log_matching(product_id, "Fallback -> using first product from original search")
            fallback_result, _ = run_product_selection_phase(
                driver, product_id, "Fallback", url, result, osb_url, fallback_first=True
            )
            if fallback_result.get('status') in {'completed', 'product_found', 'product_not_clickable', 'no_offers_found'}:
                return fallback_result
            return final_result
        except Exception as e:
            result['last_response'] = f"Product selection failed: {str(e)}"
            result['status'] = "selection_error"
            return result
        
    except TimeoutException as e:
        print(f"Timeout error scraping product {product_id}: {str(e)}")
        traceback.print_exc()
        return {
            'product_id': product_id,
            'keyword': keyword,
            'url': url,
            'last_response': f'Timeout Error: {str(e)}',
            'status': 'timeout_error',
            'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'product_url': '',
            'seller': '',
            'product_name': '',
            'cid': '',
            'pid': '',
            'osb_position': 0,
            'osb_id': '',
            'seller_count': 0,
            'competitors': [],
            'product_about_info': json.dumps({}),
            'main_image': '',
            'description': '',
            'attributes': json.dumps({})
        }
    except Exception as e:
        print(f"Error scraping product {product_id}: {str(e)}")
        traceback.print_exc()
        return {
            'product_id': product_id,
            'keyword': keyword,
            'url': url,
            'last_response': f'Error: {str(e)}',
            'status': 'error',
            'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'product_url': '',
            'seller': '',
            'product_name': '',
            'cid': '',
            'pid': '',
            'osb_position': 0,
            'osb_id': '',
            'seller_count': 0,
            'competitors': [],
            'product_about_info': json.dumps({}),
            'main_image': '',
            'description': '',
            'attributes': json.dumps({})
        }

def merge_csv_files(file_paths, output_path, sort_columns=None, expected_columns=None):
    """Merge CSV files into one output CSV."""
    valid_files = [p for p in file_paths if p and os.path.exists(p) and os.path.getsize(p) > 0]
    if not valid_files:
        return None, 0

    frames = []
    for path in valid_files:
        try:
            df = pd.read_csv(path)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"Warning: Could not read {path}: {e}")

    if not frames:
        return None, 0

    merged_df = pd.concat(frames, ignore_index=True)
    if expected_columns:
        for col in expected_columns:
            if col not in merged_df.columns:
                merged_df[col] = ""
        merged_df = merged_df.loc[:, expected_columns]
    if sort_columns:
        available_cols = [c for c in sort_columns if c in merged_df.columns]
        if available_cols:
            merged_df = merged_df.sort_values(available_cols)

    merged_df.to_csv(output_path, index=False)
    return output_path, len(merged_df)


def split_dataframe_to_chunk_files(df, output_dir, total_chunks, prefix):
    """Split DataFrame into up to total_chunks chunk files and return file paths."""
    os.makedirs(output_dir, exist_ok=True)
    total_rows = len(df)
    if total_rows == 0:
        return []

    chunk_count = max(1, min(int(total_chunks), total_rows))
    base_size = total_rows // chunk_count
    remainder = total_rows % chunk_count

    chunk_files = []
    start_idx = 0
    for i in range(chunk_count):
        extra = 1 if i < remainder else 0
        end_idx = start_idx + base_size + extra
        chunk_df = df.iloc[start_idx:end_idx]
        if chunk_df.empty:
            start_idx = end_idx
            continue

        chunk_file = os.path.join(output_dir, f"{prefix}_chunk_{i + 1}.csv")
        chunk_df.to_csv(chunk_file, index=False)
        chunk_files.append(chunk_file)
        print(f"Prepared chunk {i + 1}/{chunk_count}: rows {start_idx + 1}-{end_idx}")
        start_idx = end_idx

    return chunk_files


def process_chunk(df, chunk_id, total_chunks, round_id=1, output_dir='output', worker_id=None, ttl_minutes=60):
    """Process a chunk of products"""
    try:
        if df is None or df.empty:
            print(f"Chunk {chunk_id} is empty, skipping")
            return {
                "success": True,
                "product_file": None,
                "seller_file": None,
                "remaining_file": None,
                "product_rows": 0,
                "seller_rows": 0,
                "remaining_rows": 0,
            }
        df = df.reset_index(drop=True)
        consecutive_timeouts = 0
        resolved_worker_id = _get_worker_id(worker_id)

        print(f"Processing {len(df)} products from chunk {chunk_id}")
        
        # Initialize results
        product_results = []
        seller_results = []
        remaining_results = []
        
        # Setup driver with retry
        driver = None
        try:
            driver = setup_driver(max_attempts=3, base_delay=5)
        except Exception as e:
            print(f"Driver setup failed for chunk {chunk_id}: {str(e)}")
            traceback.print_exc()
            if is_driver_connectivity_error(e):
                remaining_path, remaining_rows = save_remaining_df(
                    df, chunk_id, round_id, output_dir, reason="driver_setup_failed"
                )
                return {
                    "success": True,
                    "product_file": None,
                    "seller_file": None,
                    "remaining_file": remaining_path,
                    "product_rows": 0,
                    "seller_rows": 0,
                    "remaining_rows": remaining_rows,
                }
            raise
        
        try:
            # Process each product
            for index, row in df.iterrows():
                product_id = row['product_id']
                web_id = row['web_id']
                keyword = row['keyword']
                url = row['url']
                osb_url = row['osb_url']
                name = row['name']
                mpnsku = row['mpn_sku']
                gtin = row['gtin']
                brand = row['brand']
                cat = row['category']
                
                print(f"\nProcessing {index+1}/{len(df)}: Product ID {product_id}")
                
                # Check database status and claim the product atomically before scraping
                if not verify_and_claim_product(product_id, resolved_worker_id, ttl_minutes):
                    print(f"Skipping product {product_id} - already claimed/completed by another worker.")
                    continue
                
                # Scrape product
                try:
                    scraped_data = scrape_product(driver, product_id, keyword, url, osb_url)
                except Exception as e:
                    print(f"Error scraping product {product_id}: {str(e)}")
                    traceback.print_exc()
                    scraped_data = None
                
                if not scraped_data:
                    scraped_data = {
                        'product_id': product_id,
                        'status': 'error',
                        'last_response': 'Scrape failed to return data'
                    }

                # Add original fields back
                scraped_data['web_id'] = web_id
                scraped_data['keyword'] = keyword
                scraped_data['osb_url'] = osb_url
                scraped_data['name'] = name
                scraped_data['mpn_sku'] = mpnsku
                scraped_data['gtin'] = gtin
                scraped_data['brand'] = brand
                scraped_data['category'] = cat
                
                # Add to results
                product_results.append(scraped_data)
                seller_results.extend(scraped_data.get('competitors', []))
                
                status_lower = str(scraped_data.get('status', '')).strip().lower()
                if status_lower == 'timeout_error':
                    consecutive_timeouts += 1
                else:
                    consecutive_timeouts = 0

                if status_lower == 'captcha_failed':
                    print(f"!!! CAPTCHA DETECTED on Product {product_id}. Skipping remaining {len(df) - index} products in this chunk to avoid further blocks.")
                    # Add current product and all subsequent products in the chunk to remaining_results
                    remaining_df_part = df.iloc[index:]
                    for _, r_row in remaining_df_part.iterrows():
                        remaining_results.append({
                            col: ('' if pd.isna(r_row[col]) else r_row[col])
                            for col in df.columns
                        })
                    break  # Stop processing this chunk
                elif consecutive_timeouts >= 2:
                    print(f"!!! TIMEOUT PERSISTS ({consecutive_timeouts} consecutive timeouts) on Product {product_id}. Skipping remaining {len(df) - index} products in this chunk.")
                    # Add current product and all subsequent products in the chunk to remaining_results
                    remaining_df_part = df.iloc[index:]
                    for _, r_row in remaining_df_part.iterrows():
                        remaining_results.append({
                            col: ('' if pd.isna(r_row[col]) else r_row[col])
                            for col in df.columns
                        })
                    break  # Stop processing this chunk
                elif status_lower in ['error', 'timeout_error']:
                    remaining_row = {
                        col: ('' if pd.isna(row[col]) else row[col])
                        for col in df.columns
                    }
                    remaining_results.append(remaining_row)
                
                # Sleep between products
                if index < len(df) - 1:
                    time.sleep(random.uniform(1,3))
        finally:
            try:
                if driver:
                    driver.quit()
            except Exception:
                pass
        
        # Store ALL results (even failures like captcha_failed and error) as requested by the user
        completed_product_results = product_results

        # Create CSV 1: Product Information
        csv1_data = []
        for result in completed_product_results:
            csv1_row = {
                'product_id': result.get('product_id', ''),
                'web_id': result.get('web_id', ''),
                'name' : result.get('name',''),
                'mpn_sku' : result.get('mpn_sku',''),
                'gtin' : result.get('gtin',''),
                'brand' : result.get('brand',''),
                'category': result.get('category', ''),
                'keyword': result.get('keyword', ''),
                'url': result.get('url', ''),
                'osb_url': result.get('osb_url', ''),
                'last_response': result.get('last_response', ''),
                'osb_url_match' : result.get('osb_url_match', ''),
                'product_url': result.get('product_url', ''),
                'seller': result.get('seller', ''),
                'product_name': result.get('product_name', ''),
                'cid': result.get('cid', ''),
                'pid': result.get('pid', ''),
                'last_fetched_date': result.get('last_fetched_date', ''),
                'osb_position': result.get('osb_position', 0),
                'osb_id': result.get('osb_id', ''),
                'seller_count': result.get('seller_count', 0),
                'status': result.get('status', 'error'),
                'product_about_info': result.get('product_about_info', json.dumps({})),
                'main_image': result.get('main_image', ''),
                'description': result.get('description', ''),
                'attributes': result.get('attributes', json.dumps({}))
            }
            csv1_data.append(csv1_row)
        
        # Create CSV 2: Seller Information
        csv2_data = []
        completed_product_ids = {str(r.get('product_id', '')).strip() for r in completed_product_results}
        for seller in seller_results:
            if str(seller.get('product_id', '')).strip() not in completed_product_ids:
                continue
            csv2_row = {
                'product_id': seller.get('product_id', ''),
                'seller': seller.get('seller', ''),
                'seller_product_name': seller.get('seller_product_name', ''),
                'seller_url': seller.get('seller_url', ''),
                'seller_price': seller.get('seller_price', ''),
                'stock_status': seller.get('stock_status', 'In Stock'),
                'last_fetched_date': seller.get('last_fetched_date', '')
            }
            csv2_data.append(csv2_row)
        
        # Save CSV files locally
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(output_dir, exist_ok=True)
        
        csv1_filename = f"product_info_round{round_id}_chunk{chunk_id}_{timestamp}.csv"
        csv2_filename = f"seller_info_round{round_id}_chunk{chunk_id}_{timestamp}.csv"
        csv3_filename = f"gshopping_remaining_round{round_id}_chunk{chunk_id}_{timestamp}.csv"
        
        csv1_path = os.path.join(output_dir, csv1_filename)
        csv2_path = os.path.join(output_dir, csv2_filename)
        csv3_path = os.path.join(output_dir, csv3_filename)
        
        if csv1_data:
            pd.DataFrame(csv1_data, columns=PRODUCT_FINAL_COLUMNS).to_csv(csv1_path, index=False)
            print(f"✓ Saved product info: {csv1_filename}")
        
        if csv2_data:
            pd.DataFrame(csv2_data).to_csv(csv2_path, index=False)
            print(f"✓ Saved seller info: {csv2_filename}")

        if remaining_results:
            pd.DataFrame(remaining_results).to_csv(csv3_path, index=False)
            print(f"✓ Saved remaining rows: {csv3_filename}")
        
        # Upload to FTP STOPPED TO AVOID UNNECESSARY FTP USAGE DURING TESTING
        insert_to_postgres(csv1_data, csv2_data)
        
        print(f"\n✓ Chunk {chunk_id} processing completed")
        return {
            "success": True,
            "product_file": csv1_path if csv1_data else None,
            "seller_file": csv2_path if csv2_data else None,
            "remaining_file": csv3_path if remaining_results else None,
            "product_rows": len(csv1_data),
            "seller_rows": len(csv2_data),
            "remaining_rows": len(remaining_results),
        }
        
    except Exception as e:
        print(f"Error processing chunk {chunk_id}: {str(e)}")
        traceback.print_exc()
        if df is not None and is_driver_connectivity_error(e):
            remaining_path, remaining_rows = save_remaining_df(
                df, chunk_id, round_id, output_dir, reason="driver_connectivity_error"
            )
            return {
                "success": True,
                "product_file": None,
                "seller_file": None,
                "remaining_file": remaining_path,
                "product_rows": 0,
                "seller_rows": 0,
                "remaining_rows": remaining_rows,
            }
        return {
            "success": False,
            "product_file": None,
            "seller_file": None,
            "remaining_file": None,
            "product_rows": 0,
            "seller_rows": 0,
            "remaining_rows": 0,
        }




def main():
    parser = argparse.ArgumentParser(description='Google Shopping Scraper with Captcha Solving')
    parser.add_argument('--chunk-id', type=int, default=1, help='Chunk ID (1-based)')
    parser.add_argument('--total-chunks', type=int, required=False, default=1, help='Total number of chunks')
    parser.add_argument('--input-file', type=str, required=False, default=None, help='Input CSV filename (optional, ignored)')
    parser.add_argument('--recursive', action='store_true', help='Run recursive chunk processing until remaining is empty')
    parser.add_argument('--max-rounds', type=int, default=10, help='Maximum recursive rounds')
    parser.add_argument('--reset-errors', action='store_true', help='Reset all error products to pending and exit')
    parser.add_argument('--export-report', type=str, required=False, default=None, help='Generate reconciliation report CSV at specified path and exit')
    parser.add_argument('--claim-mode', action='store_true', help='Use DB claiming queue (recommended for parallel accounts)')
    parser.add_argument('--claim-limit', type=int, default=int(os.environ.get("CLAIM_LIMIT", "30")), help='How many products to claim and scrape')
    parser.add_argument('--claim-ttl-minutes', type=int, default=int(os.environ.get("CLAIM_TTL_MINUTES", "60")), help='Release claims older than this TTL')
    parser.add_argument('--worker-id', type=str, default=os.environ.get("SCRAPER_WORKER_ID", None), help='Worker identifier stored in DB claims')
    
    args = parser.parse_args()
    
    # Handlers for dedicated utility commands
    if args.reset_errors:
        reset_error_products_to_pending()
        reset_invalid_url_products_for_retry()
        sys.exit(0)
        
    if args.export_report:
        generate_reconciliation_report(args.export_report)
        sys.exit(0)
        
    print("=" * 60)
    print("Google Shopping Scraper with Captcha Solving")
    print(f"Chunk: {args.chunk_id} of {args.total_chunks}")
    print(f"Recursive mode: {'Yes' if args.recursive else 'No'}")
    print("=" * 60)
    
    # If this is the first chunk, automatically reset previous error products and invalid URL products to pending so they are retried in this run
    if args.chunk_id == 1:
        reset_error_products_to_pending()
        reset_invalid_url_products_for_retry()
    
    # Fetch total pending products count first (extremely fast)
    total_pending = get_pending_count_from_db()
    if total_pending == 0:
        print("No pending products found in DB. All done!")
        sys.exit(0)

    print(f"Total pending products in DB: {total_pending}")

    if args.claim_mode:
        print(f"Claim mode enabled: worker={_get_worker_id(args.worker_id)} limit={args.claim_limit} ttl={args.claim_ttl_minutes}m")
        chunk_df = claim_pending_products_from_db(limit=args.claim_limit, worker_id=args.worker_id, ttl_minutes=args.claim_ttl_minutes)
        if chunk_df.empty:
            print("No claimable pending products found (or claim columns missing).")
            sys.exit(0)
        chunk_result = process_chunk(chunk_df, 1, 1, worker_id=args.worker_id, ttl_minutes=args.claim_ttl_minutes)
        success = chunk_result.get("success", False)
        sys.exit(0 if success else 1)

    # Calculate balanced limit and offset for this chunk
    chunk_id = int(args.chunk_id)
    total_chunks = int(args.total_chunks)
    
    # Bound chunk_id and total_chunks to actual total_pending size
    total_chunks = max(1, min(total_chunks, total_pending))
    if chunk_id > total_chunks:
        print(f"Chunk {chunk_id} is out of bounds (total chunks adjusted to {total_chunks}).")
        sys.exit(0)
        
    base_rows = total_pending // total_chunks
    remainder = total_pending % total_chunks
    
    limit = base_rows + (1 if chunk_id <= remainder else 0)
    offset = (chunk_id - 1) * base_rows + min(chunk_id - 1, remainder)
    
    print(f"Chunk {chunk_id} of {total_chunks}: Limit={limit}, Offset={offset}")
    
    # Query database using SQL LIMIT and OFFSET
    chunk_df = get_pending_chunk_from_db(limit, offset)
    
    if chunk_df.empty:
        print(f"Chunk {args.chunk_id} has no products to process.")
        sys.exit(0)

    chunk_result = process_chunk(chunk_df, args.chunk_id, args.total_chunks, worker_id=args.worker_id, ttl_minutes=args.claim_ttl_minutes)
    success = chunk_result.get("success", False)
    
    if success:
        print("\n✓ Chunk processing completed successfully")
        sys.exit(0)
    else:
        print("\n✗ Chunk processing failed")
        sys.exit(1)

if __name__ == "__main__":
    main()

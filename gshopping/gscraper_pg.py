import sys
import warnings
warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy connectable.*")
import json
import random
import os
import time
from datetime import datetime
import threading
import queue
from concurrent.futures import ThreadPoolExecutor

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
import math
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
DEFAULT_PRODUCTS_PER_HOUR = 30
DEFAULT_MAX_RUNTIME_HOURS = 5
DEFAULT_CLAIM_TTL_MINUTES = 480
DEFAULT_DB_BATCH_SIZE = 50
DEFAULT_DB_WRITE_PAGE_SIZE = 500
_CLAIM_COLUMN_SUPPORT = None

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


def _decode_html_entities(text):
    """Decode HTML entities from product names (mirrors the SQL REPLACE chain)."""
    if not text:
        return ''
    text = str(text)
    text = text.replace('&quot;', '"')
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    return text


def build_keyword(name, mpn=None, color=None, bed_size_measure=None, mattress_size=None):
    """
    Build the plain-text search keyword — mirrors the SQL `keyword` column:
      1stopbedrooms {name} "{mpn}" "{color}" "{bed_size_measure}" "{mattress_size}"
    """
    parts = ['1stopbedrooms', _decode_html_entities(name)]
    if mpn and str(mpn).strip():
        parts.append(f'"{str(mpn).strip()}"')
    if color and str(color).strip():
        parts.append(f'"{str(color).strip()}"')
    if bed_size_measure and str(bed_size_measure).strip():
        parts.append(f'"{str(bed_size_measure).strip()}"')
    if mattress_size and str(mattress_size).strip():
        parts.append(f'"{str(mattress_size).strip()}"')
    return ' '.join(parts)


def build_search_url(name, mpn=None, color=None, bed_size_measure=None, mattress_size=None):
    """
    Build the Google Shopping search URL — mirrors the SQL `url` column:
      https://www.google.com/search?q=1stopbedrooms+{name}+"{mpn}"+...&udm=28&gl=US&hl=en&pws=0

    Replicates the SQL logic:
      CONCAT(
        'https://www.google.com/search?q=',
        REPLACE(REPLACE(CONCAT('1stopbedrooms ', name, ' ', '"mpn"', ...), ' ', '+'), '#', ''),
        '&udm=28&gl=US&hl=en&pws=0'
      )
    """
    keyword = build_keyword(name, mpn, color, bed_size_measure, mattress_size)
    # Replace spaces with + then strip # (matches SQL REPLACE chain)
    query = keyword.replace(' ', '+').replace('#', '')
    return f'https://www.google.com/search?q={query}&udm=28&gl=US&hl=en&pws=0'


def initialize_product_result(product_id, keyword, product_url):
    return {
        'product_id': product_id,
        'keyword': keyword,
        'url': product_url,
        'last_response': '',
        'product_url': product_url,
        'seller': '',
        'product_name': '',
        'cid': '',
        'pid': '',
        'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'osb_position': 0,
        'osb_id': '',
        'seller_count': 0,
        'status': 'pending',
        'competitors': [],
        'product_about_info': json.dumps({'description': '', 'attributes': {}, 'main_image': '', 'gs_images': []}),
        'main_image': '',
        'description': '',
        'attributes': json.dumps({}),
        'gs_images': json.dumps([]),
        'rating_star': None,
        'rating_count': None,
        'typical_price_low': None,
        'typical_price_high': None,
        'best_price_url': '',
        'popular_url': '',
        'brand': None,
        'color': None,
        'width': None,
        'height': None,
        'depth': None,
        'style': None,
        'material': None,
        'shape': None,
        'assembly_required': None,
        'weight': None
    }


def extract_mapped_attributes(attributes):
    """
    Extract specific fields from the attributes dictionary.
    Keys are case-insensitive.
    """
    if not attributes or not isinstance(attributes, dict):
        return {
            'brand': None, 'color': None, 'width': None, 'height': None, 'depth': None,
            'style': None, 'material': None, 'shape': None, 'assembly_required': None, 'weight': None
        }
        
    mapped = {}
    
    # Helper to look up key case-insensitively
    def get_val(keys):
        for key in keys:
            if key in attributes:
                return str(attributes[key]).strip()
            for k, v in attributes.items():
                if k.lower() == key.lower():
                    return str(v).strip()
        return None

    mapped['brand'] = get_val(['brand'])
    mapped['color'] = get_val(['color'])
    mapped['style'] = get_val(['style'])
    mapped['material'] = get_val(['material'])
    mapped['shape'] = get_val(['shape', 'product shape'])
    mapped['assembly_required'] = get_val(['assembly required', 'assembly_required', 'assembly'])
    mapped['weight'] = get_val(['weight', 'product weight', 'item weight', 'assembled weight'])

    # Width, Height, Depth
    width = get_val(['width', 'assembled width', 'product width'])
    height = get_val(['height', 'assembled height', 'product height'])
    depth = get_val(['depth', 'assembled depth', 'product depth', 'length', 'assembled length'])
    
    if not (width and height and depth):
        dims_str = get_val(['dimensions', 'product dimensions', 'assembled dimensions'])
        if dims_str:
            match = re.search(
                r"(\d+(?:\.\d+)?)\s*(?:in|inch|inches|cm|mm|')?\s*[xX*]\s*(\d+(?:\.\d+)?)\s*(?:in|inch|inches|cm|mm|')?\s*[xX*]\s*(\d+(?:\.\d+)?)\s*(?:in|inch|inches|cm|mm|')?",
                dims_str
            )
            if match:
                w_match = re.search(r"(\d+(?:\.\d+)?)\s*(?:in|inch|inches|'|\")?\s*[wW]", dims_str)
                h_match = re.search(r"(\d+(?:\.\d+)?)\s*(?:in|inch|inches|'|\")?\s*[hH]", dims_str)
                d_match = re.search(r"(\d+(?:\.\d+)?)\s*(?:in|inch|inches|'|\")?\s*[dD]", dims_str)
                
                if w_match:
                    width = w_match.group(1) + " in"
                if h_match:
                    height = h_match.group(1) + " in"
                if d_match:
                    depth = d_match.group(1) + " in"
                
                if not (width and height and depth):
                    if not width:
                        width = match.group(1) + " in"
                    if not depth:
                        depth = match.group(2) + " in"
                    if not height:
                        height = match.group(3) + " in"

    mapped['width'] = width
    mapped['height'] = height
    mapped['depth'] = depth
    
    return mapped


def insert_to_postgres(product_results, seller_results):
    def parse_jsonb_field(val):
        if not val:
            return {}
        if isinstance(val, str):
            try:
                return json.loads(val)
            except Exception:
                return {"raw_value": val}
        return val

    pg_host = os.environ.get("PG_HOST")
    pg_port = os.environ.get("PG_PORT", "5432")
    pg_user = os.environ.get("PG_USER")
    pg_pass = os.environ.get("PG_PASS")
    pg_db = os.environ.get("PG_DB")

    if not all([pg_host, pg_user, pg_pass, pg_db]):
        print("Skipping PostgreSQL insert: Missing credentials")
        return

    db_page_size = _env_int("DB_WRITE_PAGE_SIZE", DEFAULT_DB_WRITE_PAGE_SIZE)
    if db_page_size <= 0:
        db_page_size = DEFAULT_DB_WRITE_PAGE_SIZE

    def execute_transaction(conn, cursor):

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
            prod_ids = sorted({str(r.get("product_id", "")).strip() for r in product_results if r.get("product_id")})
            if prod_ids:
                # Delete existing sellers for these products to prevent duplicate or stale entries
                cursor.execute("DELETE FROM google_shopping_sellers WHERE product_id = ANY(%s)", (prod_ids,))

        # 1. Upsert google_shopping_results (1-to-1 relationship)
        if valid_product_results:
            prod_insert = """
                INSERT INTO google_shopping_results (
                    product_id, google_title, google_description, gs_main_image, gs_images,
                    brand, color, width, height, depth, style, material, shape, assembly_required, weight,
                    rating_star, rating_count, typical_price_low, typical_price_high,
                    best_price_url, popular_url, other_attributes,
                    last_response, osb_url_match, google_seller_page_url, cid, pid,
                    osb_position, osb_id, seller_count, status, scraped_at, updated_at
                ) VALUES %s
                ON CONFLICT (product_id) DO UPDATE SET
                    google_title = EXCLUDED.google_title,
                    google_description = EXCLUDED.google_description,
                    gs_main_image = EXCLUDED.gs_main_image,
                    gs_images = EXCLUDED.gs_images,
                    brand = EXCLUDED.brand,
                    color = EXCLUDED.color,
                    width = EXCLUDED.width,
                    height = EXCLUDED.height,
                    depth = EXCLUDED.depth,
                    style = EXCLUDED.style,
                    material = EXCLUDED.material,
                    shape = EXCLUDED.shape,
                    assembly_required = EXCLUDED.assembly_required,
                    weight = EXCLUDED.weight,
                    rating_star = EXCLUDED.rating_star,
                    rating_count = EXCLUDED.rating_count,
                    typical_price_low = EXCLUDED.typical_price_low,
                    typical_price_high = EXCLUDED.typical_price_high,
                    best_price_url = EXCLUDED.best_price_url,
                    popular_url = EXCLUDED.popular_url,
                    other_attributes = EXCLUDED.other_attributes,
                    last_response = EXCLUDED.last_response,
                    osb_url_match = EXCLUDED.osb_url_match,
                    google_seller_page_url = EXCLUDED.google_seller_page_url,
                    cid = EXCLUDED.cid,
                    pid = EXCLUDED.pid,
                    osb_position = EXCLUDED.osb_position,
                    osb_id = EXCLUDED.osb_id,
                    seller_count = EXCLUDED.seller_count,
                    status = EXCLUDED.status,
                    updated_at = CURRENT_TIMESTAMP
            """
            prod_values = []
            for r in valid_product_results:
                gs_images_raw = r.get("gs_images", [])
                if isinstance(gs_images_raw, str):
                    try:
                        gs_images_val = json.loads(gs_images_raw)
                    except:
                        gs_images_val = []
                else:
                    gs_images_val = gs_images_raw if isinstance(gs_images_raw, list) else []

                rating_star_val = r.get("rating_star")
                if rating_star_val is not None:
                    try:
                        rating_star_val = float(rating_star_val)
                    except:
                        rating_star_val = None
                        
                rating_count_val = r.get("rating_count")
                if rating_count_val is not None:
                    try:
                        rating_count_val = int(rating_count_val)
                    except:
                        rating_count_val = None
                        
                typical_price_low_val = r.get("typical_price_low")
                if typical_price_low_val is not None:
                    try:
                        typical_price_low_val = float(typical_price_low_val)
                    except:
                        typical_price_low_val = None
                        
                typical_price_high_val = r.get("typical_price_high")
                if typical_price_high_val is not None:
                    try:
                        typical_price_high_val = float(typical_price_high_val)
                    except:
                        typical_price_high_val = None

                prod_values.append((
                    str(r.get("product_id", "")),
                    str(r.get("google_title", r.get("product_name", ""))),
                    str(r.get("google_description", r.get("description", ""))),
                    str(r.get("gs_main_image", r.get("main_image", ""))),
                    psycopg2.extras.Json(gs_images_val),
                    r.get("brand"),
                    r.get("color"),
                    r.get("width"),
                    r.get("height"),
                    r.get("depth"),
                    r.get("style"),
                    r.get("material"),
                    r.get("shape"),
                    r.get("assembly_required"),
                    r.get("weight"),
                    rating_star_val,
                    rating_count_val,
                    typical_price_low_val,
                    typical_price_high_val,
                    str(r.get("best_price_url", "")),
                    str(r.get("popular_url", "")),
                    psycopg2.extras.Json(parse_jsonb_field(r.get("attributes"))),
                    str(r.get("last_response", "")),
                    str(r.get("osb_url_match", "")),
                    str(r.get("product_url", "")),
                    str(r.get("cid", "")),
                    str(r.get("pid", "")),
                    int(r.get("osb_position", 0) or 0),
                    str(r.get("osb_id", "")),
                    int(r.get("seller_count", 0) or 0),
                    str(r.get("status", "")),
                    datetime.now(),
                    datetime.now()
                ))
            execute_values(cursor, prod_insert, prod_values, page_size=db_page_size)

        # 2. Upsert google_shopping_sellers (1-to-many relationship)
        valid_seller_results = []
        for r in seller_results or []:
            p_code = str(r.get("product_id", r.get("product_code", ""))).strip()
            if p_code not in retry_product_ids:
                valid_seller_results.append(r)

        if valid_seller_results:
            # Upsert into competitors first to get their IDs and base URLs
            competitor_data = {}
            for r in valid_seller_results:
                s_name = str(r.get("seller", r.get("seller_name", ""))).strip()
                if not s_name:
                    continue
                s_url = str(r.get("seller_url", "")).strip()
                base_url = ""
                if s_url:
                    try:
                        from urllib.parse import urlparse
                        parsed = urlparse(s_url)
                        if parsed.scheme and parsed.netloc:
                            base_url = f"{parsed.scheme}://{parsed.netloc}"
                    except Exception:
                        pass
                
                # Keep the non-empty base url if we find one
                if s_name not in competitor_data or (base_url and not competitor_data[s_name]):
                    competitor_data[s_name] = base_url

            if competitor_data:
                # 1. Query existing competitors first to minimize locks/deadlocks
                cursor.execute(
                    "SELECT competitor_id, competitor_name, base_url FROM competitors WHERE competitor_name = ANY(%s)",
                    (list(competitor_data.keys()),)
                )
                existing_competitors = {row[1]: (row[0], row[2] or '') for row in cursor.fetchall()}
                
                # 2. Filter list: we only insert or update if they don't exist OR if we have a new base_url and the existing one is empty
                competitors_to_insert = []
                competitor_map = {}
                
                for name, base_url in competitor_data.items():
                    if name in existing_competitors:
                        cid, existing_base = existing_competitors[name]
                        competitor_map[name] = cid
                        # If the existing base_url is empty but our scraped base_url is not, we want to update it
                        if not existing_base and base_url:
                            competitors_to_insert.append((name, base_url))
                    else:
                        # Competitor doesn't exist, we must insert it
                        competitors_to_insert.append((name, base_url))
                
                # 3. Only run the INSERT/UPDATE if there's actually something to write
                if competitors_to_insert:
                    competitor_insert = """
                        INSERT INTO competitors (competitor_name, base_url)
                        VALUES %s
                        ON CONFLICT (competitor_name) DO UPDATE
                        SET base_url = EXCLUDED.base_url
                        WHERE competitors.base_url IS NULL OR competitors.base_url = ''
                    """
                    # Sort alphabetically by competitor_name to prevent database deadlocks under concurrency
                    sorted_competitor_tuples = sorted(competitors_to_insert, key=lambda x: x[0])
                    execute_values(cursor, competitor_insert, sorted_competitor_tuples, page_size=db_page_size)
                    
                    # Refresh the competitor_map for the newly inserted competitors
                    new_names = [x[0] for x in competitors_to_insert]
                    cursor.execute("SELECT competitor_id, competitor_name FROM competitors WHERE competitor_name = ANY(%s)", (new_names,))
                    for cid, name in cursor.fetchall():
                        competitor_map[name] = cid
            else:
                competitor_map = {}

            seller_insert = """
                INSERT INTO google_shopping_sellers (
                    product_id, competitor_id, seller_name, seller_product_name, seller_url, price,
                    original_price, discount_amount, coupon_code, coupon_remark, stock_status,
                    seller_rating, delivery_tagline, google_position
                ) VALUES %s
                ON CONFLICT (product_id, competitor_id, (md5(seller_url))) DO UPDATE SET
                    seller_name = EXCLUDED.seller_name,
                    seller_product_name = EXCLUDED.seller_product_name,
                    price = EXCLUDED.price,
                    original_price = EXCLUDED.original_price,
                    discount_amount = EXCLUDED.discount_amount,
                    coupon_code = EXCLUDED.coupon_code,
                    coupon_remark = EXCLUDED.coupon_remark,
                    stock_status = EXCLUDED.stock_status,
                    seller_rating = EXCLUDED.seller_rating,
                    delivery_tagline = EXCLUDED.delivery_tagline,
                    google_position = EXCLUDED.google_position,
                    scraped_at = NOW()
            """
            seller_values = []
            seen_sellers = set()
            for r in valid_seller_results:
                p_code = str(r.get("product_id", r.get("product_code", ""))).strip()
                s_name = str(r.get("seller", r.get("seller_name", ""))).strip()
                price = parse_price(r.get("seller_price"))
                
                if not p_code or not s_name:
                    continue
                
                comp_id = competitor_map.get(s_name)
                if not comp_id:
                    continue
                
                seller_url = str(r.get("seller_url") or "").strip()
                
                import hashlib
                url_hash = hashlib.md5(seller_url.encode('utf-8', errors='ignore')).hexdigest()
                
                # Check for duplicates in the current batch to avoid:
                # "ON CONFLICT DO UPDATE command cannot affect row a second time"
                seen_key = (p_code, comp_id, url_hash)
                if seen_key in seen_sellers:
                    continue
                seen_sellers.add(seen_key)
                
                orig_price = r.get("original_price")
                if orig_price is not None:
                    try:
                        orig_price = float(orig_price)
                    except:
                        orig_price = None
                
                disc_amount = r.get("discount_amount")
                if disc_amount is not None:
                    try:
                        disc_amount = float(disc_amount)
                    except:
                        disc_amount = None
                
                sel_rating = r.get("seller_rating")
                if sel_rating is not None:
                    try:
                        sel_rating = float(sel_rating)
                    except:
                        sel_rating = None

                google_pos = r.get("google_position")
                if google_pos is not None:
                    try:
                        google_pos = int(google_pos)
                    except:
                        google_pos = None

                seller_values.append((
                    p_code,
                    comp_id,
                    s_name,
                    r.get("seller_product_name", ""),
                    seller_url,
                    price,
                    orig_price,
                    disc_amount,
                    r.get("coupon_code", ""),
                    r.get("coupon_remark", ""),
                    r.get("stock_status", "In Stock"),
                    sel_rating,
                    r.get("delivery_tagline", ""),
                    google_pos
                ))

            if seller_values:
                execute_values(cursor, seller_insert, seller_values, page_size=db_page_size)

        # 3. Transactionally update scraping_status in osb_products table
        if product_results:
            status_values = []
            for r in product_results:
                p_id = str(r.get("product_id", "")).strip()
                if not p_id:
                    continue
                status_lower = str(r.get("status", "")).strip().lower()
                
                if p_id in retry_product_ids:
                    scr_status = 'pending'
                    err_msg = 'Invalid product URL, retrying'
                elif status_lower == 'completed' or status_lower == 'product_found':
                    scr_status = 'completed'
                    err_msg = None
                else:
                    scr_status = 'error'
                    err_msg = r.get('last_response') or f"Scrape ended with status: {status_lower}"
                
                status_values.append((p_id, scr_status, err_msg))

            if status_values:
                if _supports_claim_columns(cursor):
                    status_update_query = """
                        UPDATE osb_products AS p
                        SET scraping_status = v.scraping_status,
                            last_attempt = CURRENT_TIMESTAMP,
                            error_message = v.error_message,
                            claimed_by = NULL,
                            claimed_at = NULL
                        FROM (VALUES %s) AS v(product_id, scraping_status, error_message)
                        WHERE p.product_id = v.product_id
                    """
                else:
                    status_update_query = """
                        UPDATE osb_products AS p
                        SET scraping_status = v.scraping_status,
                            last_attempt = CURRENT_TIMESTAMP,
                            error_message = v.error_message
                        FROM (VALUES %s) AS v(product_id, scraping_status, error_message)
                        WHERE p.product_id = v.product_id
                    """
                execute_values(cursor, status_update_query, status_values, page_size=db_page_size)

        conn.commit()
        cursor.close()
        conn.close()
        print(f"✓ Transaction committed: Upserted {len(product_results)} products and {len(seller_results)} sellers into PostgreSQL.")

    max_attempts = 5
    base_delay = 0.5
    for attempt in range(1, max_attempts + 1):
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(
                host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db
            )
            cursor = conn.cursor()
            execute_transaction(conn, cursor)
            return
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

            is_transient = any(word in str(e).lower() for word in ["deadlock", "lock", "serialization", "concurrent", "timeout", "connection", "aborted"])
            if is_transient and attempt < max_attempts:
                import random
                sleep_time = base_delay * (2 ** (attempt - 1)) + random.uniform(0.1, 1.0)
                print(f"Database concurrency conflict or deadlock detected ({e}). Retrying in {sleep_time:.2f}s (Attempt {attempt}/{max_attempts})...")
                time.sleep(sleep_time)
            else:
                print(f"Error inserting into PostgreSQL after {attempt} attempts: {e}")
                traceback.print_exc()
                break

def sync_csv_to_db(csv_path):
    """Import CSV data into osb_products table in fast batches if not already present."""
    conn = None
    cursor = None
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
        cursor.execute("SELECT COUNT(*) FROM osb_products")
        existing_count = cursor.fetchone()[0]
        if existing_count > 0:
            print(f"✓ Database already contains {existing_count} products. Skipping CSV sync to save time.")
            return

        print(f"Reading CSV {csv_path} for sync...")
        df = pd.read_csv(csv_path)
        initial_len = len(df)
        df = df.drop_duplicates(subset=['product_id'])
        if len(df) < initial_len:
            print(f"Removed {initial_len - len(df)} duplicate product_ids from CSV.")
            
        print(f"Syncing {len(df)} products from CSV to DB in batches...")

        insert_query = """
            INSERT INTO osb_products (
                product_id, web_id, name, sku, mpn, gtin, brand, product_type, keyword, url, osb_url, status, mfr_sales_30d
            )
            VALUES %s
            ON CONFLICT (product_id) DO UPDATE SET
                status = EXCLUDED.status,
                mfr_sales_30d = EXCLUDED.mfr_sales_30d,
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
                    clean(row_dict.get('mpn_sku')),  # sku
                    clean(row_dict.get('mpn_sku')),  # mpn
                    clean(row_dict.get('gtin')),
                    clean(row_dict.get('brand')),
                    clean(row_dict.get('category')), # product_type
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
        
        print("✓ CSV sync to DB completed.")
    except Exception as e:
        print(f"Error syncing CSV to DB: {e}")
        traceback.print_exc()
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def get_pending_count_from_db():
    """Get the lightweight count of enabled products with 'pending' scraping_status."""
    conn = None
    cursor = None
    try:
        pg_host = os.environ.get("PG_HOST")
        pg_port = os.environ.get("PG_PORT", "5432")
        pg_user = os.environ.get("PG_USER")
        pg_pass = os.environ.get("PG_PASS")
        pg_db = os.environ.get("PG_DB")

        conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM osb_products WHERE scraping_status = 'pending' AND status = 1")
        count = cursor.fetchone()[0]
        return count
    except Exception as e:
        print(f"Error getting pending count from DB: {e}")
        return 0
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

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

def _env_int(name, default):
    val = os.environ.get(name)
    if val is None or str(val).strip() == "":
        return default
    try:
        return int(val)
    except ValueError:
        return default

def _env_float(name, default):
    val = os.environ.get(name)
    if val is None or str(val).strip() == "":
        return default
    try:
        return float(val)
    except ValueError:
        return default

def _supports_claim_columns(cursor):
    global _CLAIM_COLUMN_SUPPORT
    if _CLAIM_COLUMN_SUPPORT is not None:
        return _CLAIM_COLUMN_SUPPORT

    cursor.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'osb_products'
          AND column_name IN ('claimed_by', 'claimed_at')
        GROUP BY table_name
        HAVING COUNT(*) = 2
        """
    )
    _CLAIM_COLUMN_SUPPORT = cursor.fetchone() is not None
    return _CLAIM_COLUMN_SUPPORT

def calculate_parallel_claim_limit(claim_limit=None, products_per_hour=DEFAULT_PRODUCTS_PER_HOUR, max_runtime_hours=DEFAULT_MAX_RUNTIME_HOURS):
    if claim_limit is not None and int(claim_limit) > 0:
        return int(claim_limit)
    return max(1, int(math.ceil(float(products_per_hour) * float(max_runtime_hours))))

def release_expired_claims(ttl_minutes=60):
    """Release old claims so another runner can pick them up."""
    conn = None
    cursor = None
    try:
        conn = _get_pg_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE osb_products
            SET scraping_status = %s,
                claimed_by = NULL,
                claimed_at = NULL
            WHERE product_id IN (
                SELECT product_id FROM osb_products
                WHERE scraping_status = %s
                  AND claimed_at IS NOT NULL
                  AND claimed_at < (NOW() - (%s || ' minutes')::interval)
                FOR UPDATE SKIP LOCKED
            )
            """,
            (PENDING_STATUS, CLAIM_STATUS, int(ttl_minutes)),
        )
        released = cursor.rowcount
        conn.commit()
        if released:
            print(f"✓ Released {released} expired claimed products (TTL={ttl_minutes}m).")
        return released
    except Exception as e:
        print(f"Error releasing expired claims: {e}")
        return 0
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def claim_pending_products_from_db(limit=30, worker_id=None, ttl_minutes=60):
    """
    Atomically claim up to `limit` pending products using row locks (FOR UPDATE SKIP LOCKED).
    Requires columns: claimed_by, claimed_at; and supports scraping_status='claimed'.
    """
    conn = None
    cursor = None
    try:
        worker_id = _get_worker_id(worker_id)
        conn = _get_pg_conn()
        conn.autocommit = False
        cursor = conn.cursor()
        # Release expired claims first (best-effort)
        try:
            cursor.execute(
                """
                UPDATE osb_products
                SET scraping_status = %s,
                    claimed_by = NULL,
                    claimed_at = NULL
                WHERE product_id IN (
                    SELECT product_id FROM osb_products
                    WHERE scraping_status = %s
                      AND claimed_at IS NOT NULL
                      AND claimed_at < (NOW() - (%s || ' minutes')::interval)
                    FOR UPDATE SKIP LOCKED
                )
                """,
                (PENDING_STATUS, CLAIM_STATUS, int(ttl_minutes)),
            )
        except Exception:
            conn.rollback()
            # If schema doesn't have claim columns yet, let the caller know via empty df.
            return pd.DataFrame()

        cursor.execute(
            """
            WITH picked AS (
                SELECT product_id
                FROM osb_products
                WHERE scraping_status = %s AND status = 1
                ORDER BY mfr_sales_30d DESC NULLS LAST, product_id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            UPDATE osb_products p
            SET scraping_status = %s,
                claimed_by = %s,
                claimed_at = NOW(),
                last_attempt = NOW(),
                error_message = NULL
            FROM picked
            WHERE p.product_id = picked.product_id
            RETURNING p.product_id, p.web_id, p.name, p.sku AS mpn_sku, p.gtin, p.brand, p.product_type AS category, p.keyword, p.url, p.osb_url, p.status, p.mfr_sales_30d AS "30daymfrsales", p.scraping_status, p.claimed_by, p.claimed_at, p.last_attempt, p.error_message, p.created_at, p.updated_at, p.color, p.bed_size_measure, p.mattress_size
            """,
            (PENDING_STATUS, int(limit), CLAIM_STATUS, worker_id),
        )
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description] if cursor.description else []
        conn.commit()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        print(f"Error claiming pending products from DB: {e}")
        return pd.DataFrame()
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def get_product_ids_in_boundary(start_sales, start_id, end_sales, end_id):
    """Fetch all active products (status = 1) in the sorted boundary, regardless of scraping_status."""
    conn = None
    cursor = None
    try:
        conn = _get_pg_conn()
        cursor = conn.cursor()
        
        params = []
        clauses = ["status = 1"]
        if start_id:
            sales_val = int(start_sales) if (start_sales is not None and start_sales != '') else 0
            params.extend([sales_val, sales_val, str(start_id)])
            clauses.append("(mfr_sales_30d < %s OR (mfr_sales_30d = %s AND product_id >= %s))")
        if end_id:
            sales_val = int(end_sales) if (end_sales is not None and end_sales != '') else 0
            params.extend([sales_val, sales_val, str(end_id)])
            clauses.append("(mfr_sales_30d > %s OR (mfr_sales_30d = %s AND product_id <= %s))")
            
        query = f"""
            SELECT product_id 
            FROM osb_products 
            WHERE {" AND ".join(clauses)}
            ORDER BY mfr_sales_30d DESC NULLS LAST, product_id ASC
        """
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        print(f"Error fetching product IDs in boundary: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def claim_specific_products_from_db(product_ids, worker_id=None, limit=30, ttl_minutes=60):
    """Claim only from the pre-selected static list of product IDs."""
    if not product_ids:
        return pd.DataFrame()
        
    conn = None
    cursor = None
    try:
        worker_id = _get_worker_id(worker_id)
        conn = _get_pg_conn()
        conn.autocommit = False
        cursor = conn.cursor()
        
        # Release expired claims first (best-effort)
        try:
            cursor.execute(
                """
                UPDATE osb_products
                SET scraping_status = %s,
                    claimed_by = NULL,
                    claimed_at = NULL
                WHERE product_id IN (
                    SELECT product_id FROM osb_products
                    WHERE scraping_status = %s
                      AND claimed_at IS NOT NULL
                      AND claimed_at < (NOW() - (%s || ' minutes')::interval)
                    FOR UPDATE SKIP LOCKED
                )
                """,
                (PENDING_STATUS, CLAIM_STATUS, int(ttl_minutes)),
            )
        except Exception:
            conn.rollback()
            return pd.DataFrame()

        # Now claim from specific list of product IDs that are still pending
        cursor.execute(
            """
            WITH picked AS (
                SELECT product_id
                FROM osb_products
                WHERE product_id = ANY(%s)
                  AND scraping_status = %s 
                  AND status = 1
                ORDER BY mfr_sales_30d DESC NULLS LAST, product_id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            UPDATE osb_products p
            SET scraping_status = %s,
                claimed_by = %s,
                claimed_at = NOW(),
                last_attempt = NOW(),
                error_message = NULL
            FROM picked
            WHERE p.product_id = picked.product_id
            RETURNING p.product_id, p.web_id, p.name, p.sku AS mpn_sku, p.gtin, p.brand, p.product_type AS category, p.keyword, p.url, p.osb_url, p.status, p.mfr_sales_30d AS "30daymfrsales", p.scraping_status, p.claimed_by, p.claimed_at, p.last_attempt, p.error_message, p.created_at, p.updated_at, p.color, p.bed_size_measure, p.mattress_size
            """,
            (product_ids, PENDING_STATUS, int(limit), CLAIM_STATUS, worker_id),
        )
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description] if cursor.description else []
        conn.commit()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error claiming specific products from DB: {e}")
        return pd.DataFrame()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_pending_chunk_from_db(limit, offset):
    """Fetch only a specific partitioned slice of pending products using SQL LIMIT and OFFSET."""
    conn = None
    try:
        pg_host = os.environ.get("PG_HOST")
        pg_port = os.environ.get("PG_PORT", "5432")
        pg_user = os.environ.get("PG_USER")
        pg_pass = os.environ.get("PG_PASS")
        pg_db = os.environ.get("PG_DB")

        conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db)
        # Fetch only the assigned chunk's slice, ordered by sales descending
        query = """
            SELECT product_id, web_id, name, sku AS mpn_sku, gtin, brand, product_type AS category, keyword, url, osb_url, status, mfr_sales_30d AS "30daymfrsales", scraping_status, claimed_by, claimed_at, last_attempt, error_message, created_at, updated_at, color, bed_size_measure, mattress_size
            FROM osb_products 
            WHERE scraping_status = 'pending' AND status = 1
            ORDER BY mfr_sales_30d DESC NULLS LAST, product_id ASC
            LIMIT %s OFFSET %s
        """
        df = pd.read_sql(query, conn, params=(int(limit), int(offset)))
        return df
    except Exception as e:
        print(f"Error fetching pending chunk from DB: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def verify_and_claim_product(product_id, worker_id=None, ttl_minutes=60):
    """
    Verify that a product is still available or claimed by us, and atomically claim/renew it.
    Returns True if we successfully claimed/renewed it and can scrape it.
    Returns False if it is completed, failed, or claimed by someone else.
    """
    conn = None
    cursor = None
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

        if not _supports_claim_columns(cursor):
            # Fallback simple check/claim
            cursor.execute(
                "SELECT scraping_status FROM osb_products WHERE product_id = %s AND status = 1",
                (str(product_id),)
            )
            row = cursor.fetchone()
            if not row:
                return False
            status = row[0]
            if status != PENDING_STATUS:
                return False
            
            # Atomically set to claimed
            cursor.execute(
                "UPDATE osb_products SET scraping_status = %s, last_attempt = NOW() WHERE product_id = %s AND scraping_status = %s AND status = 1",
                (CLAIM_STATUS, str(product_id), PENDING_STATUS)
            )
            claimed = cursor.rowcount > 0
            conn.commit()
            return claimed

        # With claims support, atomically claim or renew our claim
        cursor.execute(
            """
            UPDATE osb_products
            SET scraping_status = %s,
                claimed_by = %s,
                claimed_at = NOW(),
                last_attempt = NOW()
            WHERE product_id = %s
              AND status = 1
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
        return claimed
    except Exception as e:
        print(f"Error verifying/claiming product {product_id} in DB: {e}")
        return False
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def update_product_status(product_id, scraping_status, error_message=None):
    """Update the scraping_status of a product in the osb_products table."""
    conn = None
    cursor = None
    try:
        conn = _get_pg_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE osb_products
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
                "UPDATE osb_products SET scraping_status = %s, last_attempt = CURRENT_TIMESTAMP, error_message = %s WHERE product_id = %s",
                (scraping_status, error_message, str(product_id)),
            )
        conn.commit()
    except Exception as e:
        print(f"Error updating status for {product_id}: {e}")
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def release_claimed_products(product_ids, worker_id=None, reason="not_processed"):
    """Return unprocessed rows claimed by this worker to the pending queue."""
    product_ids = [str(pid).strip() for pid in product_ids if str(pid).strip()]
    if not product_ids:
        return 0

    conn = None
    cursor = None
    try:
        resolved_worker_id = _get_worker_id(worker_id)
        conn = _get_pg_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE osb_products
            SET scraping_status = %s,
                claimed_by = NULL,
                claimed_at = NULL,
                error_message = %s
            WHERE product_id = ANY(%s)
              AND scraping_status = %s
              AND claimed_by = %s
            """,
            (PENDING_STATUS, reason, product_ids, CLAIM_STATUS, resolved_worker_id),
        )
        released = cursor.rowcount
        conn.commit()
        if released:
            print(f"✓ Released {released} unprocessed claimed products back to pending ({reason}).")
        return released
    except Exception as e:
        print(f"Error releasing unprocessed claimed products: {e}")
        return 0
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def reset_error_products_to_pending():
    """Reset all products with 'error' scraping_status to 'pending' to retry them."""
    conn = None
    cursor = None
    try:
        pg_host = os.environ.get("PG_HOST")
        pg_port = os.environ.get("PG_PORT", "5432")
        pg_user = os.environ.get("PG_USER")
        pg_pass = os.environ.get("PG_PASS")
        pg_db = os.environ.get("PG_DB")

        conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE osb_products SET scraping_status = 'pending', claimed_at = NULL, claimed_by = NULL WHERE scraping_status = 'error'"
        )
        cursor.execute(
            "UPDATE google_shopping_results SET google_seller_page_url = NULL WHERE status IN ('selection_error', 'product_not_clickable', 'no_products', 'captcha_failed')"
        )
        conn.commit()
        affected_rows = cursor.rowcount
        if affected_rows > 0:
            print(f"✓ Reset {affected_rows} failed products from 'error' to 'pending' for retry.")
        return affected_rows
    except Exception as e:
        print(f"Error resetting error products to pending: {e}")
        return 0
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def reset_invalid_url_products_for_retry():
    """Reset all products that completed but had invalid URLs back to pending and delete their scraped results."""
    conn = None
    cursor = None
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
            FROM google_shopping_results
            WHERE google_seller_page_url NOT LIKE 'https://www.google.com/search?ibp=oshop%%'
              AND google_seller_page_url NOT LIKE 'https://share.google/%%'
              AND status NOT IN ('no_products', 'no_match')
        """
        cursor.execute(select_query)
        target_ids = [row[0] for row in cursor.fetchall()]
        
        if target_ids:
            # 1. Delete seller records for these products
            cursor.execute("DELETE FROM google_shopping_sellers WHERE product_id = ANY(%s)", (target_ids,))
            
            # 2. Delete the scraping results for these products
            cursor.execute("DELETE FROM google_shopping_results WHERE product_id = ANY(%s)", (target_ids,))
            
            # 3. Update osb_products status to pending and clear claims/errors
            cursor.execute(
                """
                UPDATE osb_products
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
            
        return len(target_ids)
    except Exception as e:
        print(f"Error resetting invalid URL products for retry: {e}")
        return 0
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def generate_reconciliation_report(output_path):
    """Query the database and compile the detailed flat reconciliation CSV report."""
    conn = None
    try:
        try:
            pg_host = os.environ.get("PG_HOST")
            pg_port = os.environ.get("PG_PORT", "5432")
            pg_user = os.environ.get("PG_USER")
            pg_pass = os.environ.get("PG_PASS")
            pg_db = os.environ.get("PG_DB")

            conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_pass, dbname=pg_db)
            
            # Fetch only products that have been scraped (non-pending status) and are active (status = 1)
            products_df = pd.read_sql("SELECT product_id, name, gtin, brand, product_type AS category, keyword, url, scraping_status FROM osb_products WHERE scraping_status != 'pending' AND status = 1", conn)
            results_df = pd.read_sql(
                """
                SELECT 
                    r.product_id, 
                    r.google_title AS product_name, 
                    r.seller_count, 
                    r.osb_position, 
                    r.updated_at, 
                    r.google_seller_page_url AS url, 
                    r.osb_url_match 
                FROM google_shopping_results r
                JOIN osb_products p ON r.product_id = p.product_id
                WHERE p.status = 1
                """,
                conn
            )
            sellers_df = pd.read_sql(
                """
                SELECT 
                    s.product_id AS product_code, 
                    s.seller_name, 
                    s.price AS seller_price, 
                    s.seller_url, 
                    s.stock_status,
                    s.original_price,
                    s.discount_amount,
                    s.coupon_code,
                    s.coupon_remark,
                    s.seller_rating,
                    s.delivery_tagline,
                    s.google_position
                FROM google_shopping_sellers s
                JOIN osb_products p ON s.product_id = p.product_id
                WHERE p.status = 1
                """,
                conn
            )
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

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
                    'Scrapped url(google url)': scrapped_url,
                    'Original Price': 0.00,
                    'Discount Amount': 0.00,
                    'Coupon Code': '',
                    'Coupon Remark': '',
                    'Seller Rating': '',
                    'Delivery Tagline': ''
                })
            else:
                # Sort sellers by google_position, placing Null/None/NaN values at the end
                sorted_sellers = sorted(
                    p_sellers, 
                    key=lambda x: (
                        x.get('google_position') is None or pd.isna(x.get('google_position')), 
                        float(x.get('google_position')) if (x.get('google_position') is not None and not pd.isna(x.get('google_position'))) else 999.0
                    )
                )
                # Add a row for EACH competitor offer
                for idx, s in enumerate(sorted_sellers):
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
                            
                    s_pos = s.get('google_position')
                    if s_pos is not None and not pd.isna(s_pos):
                        site_index = int(float(s_pos))
                    else:
                        site_index = idx + 1

                    orig_price = s.get('original_price')
                    orig_price = float(orig_price) if (orig_price is not None and not pd.isna(orig_price)) else 0.00
                    
                    disc_amount = s.get('discount_amount')
                    disc_amount = float(disc_amount) if (disc_amount is not None and not pd.isna(disc_amount)) else 0.00

                    coupon_code = s.get('coupon_code')
                    coupon_code = str(coupon_code) if (coupon_code is not None and not pd.isna(coupon_code)) else ''

                    coupon_remark = s.get('coupon_remark')
                    coupon_remark = str(coupon_remark) if (coupon_remark is not None and not pd.isna(coupon_remark)) else ''

                    sel_rating = s.get('seller_rating')
                    sel_rating = float(sel_rating) if (sel_rating is not None and not pd.isna(sel_rating)) else None

                    del_tagline = s.get('delivery_tagline')
                    del_tagline = str(del_tagline) if (del_tagline is not None and not pd.isna(del_tagline)) else ''

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
                        'Site Index': site_index,
                        'Total Price': s_price,
                        'Change direction': change_dir,
                        'Stock': s_stock,
                        'URL': s_url,
                        'OSB URL match': s_osb_match,
                        'Scrapped url(google url)': scrapped_url,
                        'Original Price': orig_price,
                        'Discount Amount': disc_amount,
                        'Coupon Code': coupon_code,
                        'Coupon Remark': coupon_remark,
                        'Seller Rating': sel_rating if sel_rating is not None else '',
                        'Delivery Tagline': del_tagline
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
            'Change direction', 'Stock', 'URL', 'OSB URL match', 'Scrapped url(google url)',
            'Original Price', 'Discount Amount', 'Coupon Code', 'Coupon Remark',
            'Seller Rating', 'Delivery Tagline'
        ]
        report_df = report_df[COLUMNS_ORDER]
        report_df.to_csv(output_path, index=False)
        print(f"✓ Reconciliation report saved successfully to: {output_path} ({len(report_rows)} rows)")
        
        # FTP upload has been disabled per user request
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
    "attributes",
    "gs_images",
    "color",
    "width",
    "height",
    "depth",
    "style",
    "material",
    "shape",
    "assembly_required",
    "weight",
    "rating_star",
    "rating_count",
    "typical_price_low",
    "typical_price_high",
    "best_price_url",
    "popular_url"
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

def setup_driver(max_attempts=3, base_delay=4, headless=False):
    last_err = None
    
    def build_chrome_options():
        options = uc.ChromeOptions()
        # if headless or os.environ.get("HEADLESS", "").lower() == "true":
        #     options.add_argument("--headless")
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
            
            if headless or os.environ.get("HEADLESS", "").lower() == "true":
                uc_kwargs["headless"] = True
            
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
            # Verify driver is healthy/responsive before returning
            _ = driver.current_url
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
        'main_image': '',
        'gs_images': [],
        'rating_star': None,
        'rating_count': None,
        'typical_price_low': None,
        'typical_price_high': None,
        'popular_url': ''
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

        # Extract all product images (gallery/thumbnails)
        try:
            print("Attempting to extract all product images...")
            gallery_images = []
            # 1. Standard img elements from typical containers
            img_elements = driver.find_elements(By.XPATH, "//div[@jsname='HhYL2b']//img | //div[@jsname='SAt90e']//img | //div[contains(@class, 'm8U2Z')]//img | //div[@class='DqsAAd']//img | //div[contains(@class, 'FLY67')]//img | //div[contains(@class, 'sh-div__image-container')]//img | //img[@class='KfAt4d'] | //img[contains(@class, 'r429ob')]")
            for img in img_elements:
                try:
                    src = img.get_attribute('srcset')
                    if src:
                        src = src.split(',')[0].split(' ')[0]
                    if not src:
                        src = img.get_attribute('src')
                    if src and not src.startswith('data:') and src not in gallery_images:
                        w = int(img.get_attribute('width') or 0)
                        h = int(img.get_attribute('height') or 0)
                        if (w > 0 and h > 0 and (w < 40 or h < 40)):
                            continue
                        if any(pattern in src for pattern in ['gstatic.com', 'googleusercontent.com', 'google.com']):
                            gallery_images.append(src)
                except:
                    continue
            
            # 2. Carousel thumbnail div containers (class Asw3Oe) and any elements with data-src
            data_src_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'Asw3Oe')] | //*[@data-src]")
            for elem in data_src_elements:
                try:
                    src = elem.get_attribute('data-src')
                    if src and not src.startswith('data:') and src not in gallery_images:
                        if any(pattern in src for pattern in ['gstatic.com', 'googleusercontent.com', 'google.com']):
                            gallery_images.append(src)
                except:
                    continue

            product_info['gs_images'] = gallery_images
            print(f"✓ Found {len(gallery_images)} product images")
        except Exception as e:
            print(f"Error extracting product gallery images: {str(e)}")
            product_info['gs_images'] = []

        # Extract product ratings
        try:
            print("Attempting to extract rating...")
            rating_star = None
            rating_count = None
            rating_elements = driver.find_elements(By.XPATH, "//*[contains(@aria-label, 'out of 5')]")
            for elem in rating_elements:
                try:
                    label = elem.get_attribute("aria-label")
                    if label:
                        match = re.search(r"([0-9.]+)\s*out of 5", label)
                        if match:
                            rating_star = float(match.group(1))
                            # Check if the label also contains review/rating count (e.g. "Rated 4.5 out of 5, 180 user reviews")
                            match_count = re.search(r"([\d,]+)\s*(?:user\s*)?reviews", label, re.IGNORECASE)
                            if match_count:
                                rating_count = int(match_count.group(1).replace(",", ""))
                            break
                except:
                    pass
            review_elements = driver.find_elements(By.XPATH, "//a[contains(text(), 'reviews') or contains(text(), 'ratings')] | //span[contains(text(), 'reviews') or contains(text(), 'ratings')]")
            for elem in review_elements:
                try:
                    text = elem.text
                    if text:
                        match = re.search(r"(\d{1,3}(?:,\d{3})*)\s*(?:product\s*)?(?:reviews|ratings)", text, re.IGNORECASE)
                        if match and rating_count is None:
                            rating_count = int(match.group(1).replace(",", ""))
                            break
                except:
                    pass
            if not rating_count:
                for elem in rating_elements:
                    try:
                        parent = elem.find_element(By.XPATH, "./..")
                        parent_text = parent.text
                        match = re.search(r"\(\s*(\d{1,3}(?:,\d{3})*)\s*\)", parent_text)
                        if match:
                            rating_count = int(match.group(1).replace(",", ""))
                            break
                    except:
                        pass
            product_info['rating_star'] = rating_star
            product_info['rating_count'] = rating_count
            print(f"✓ Rating: {rating_star} ({rating_count} reviews)")
        except Exception as e:
            print(f"Error extracting ratings: {str(e)}")
            product_info['rating_star'] = None
            product_info['rating_count'] = None

        # Extract typical price range
        try:
            print("Attempting to extract typical price range...")
            typical_price_low = None
            typical_price_high = None
            typical_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Typical price') or contains(text(), 'Typical range') or contains(text(), 'typical price') or contains(text(), 'typical range')]")
            for elem in typical_elements:
                try:
                    text = elem.text
                    if text:
                        match = re.search(r"\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*[-–—]\s*\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)", text)
                        if match:
                            typical_price_low = float(match.group(1).replace(",", ""))
                            typical_price_high = float(match.group(2).replace(",", ""))
                            break
                except:
                    pass
            if not typical_price_low:
                try:
                    body_text = driver.find_element(By.TAG_NAME, "body").text
                    for line in body_text.split('\n'):
                        if "typical" in line.lower() and "$" in line:
                            match = re.search(r"\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*[-–—]\s*\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)", line)
                            if match:
                                typical_price_low = float(match.group(1).replace(",", ""))
                                typical_price_high = float(match.group(2).replace(",", ""))
                                break
                except:
                    pass
            product_info['typical_price_low'] = typical_price_low
            product_info['typical_price_high'] = typical_price_high
            print(f"✓ Typical price range: {typical_price_low} - {typical_price_high}")
        except Exception as e:
            print(f"Error extracting typical price range: {str(e)}")
            product_info['typical_price_low'] = None
            product_info['typical_price_high'] = None

        # Extract popular_url
        try:
            popular_url = ""
            pop_elem = driver.find_elements(By.XPATH, "//a[contains(@href, 'popular') or contains(text(), 'Popular') or contains(text(), 'popular')]")
            if pop_elem:
                popular_url = pop_elem[0].get_attribute('href') or ""
            product_info['popular_url'] = popular_url
        except:
            product_info['popular_url'] = ""
        
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
    max_clicks = 50
    last_offer_count = 0
    
    try:
        last_offer_count = len(driver.find_elements(By.CLASS_NAME, 'R5K7Cb'))
    except:
        pass

    while clicks < max_clicks:
        try:
            more_stores = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'duf-h')]//div[@role='button']"))
            )
            if not more_stores.is_displayed() or not more_stores.is_enabled():
                break
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", more_stores)
            time.sleep(0.5)
            more_stores.click()
            time.sleep(random.uniform(1.5, 2.5))
            clicks += 1
            
            try:
                current_offer_count = len(driver.find_elements(By.CLASS_NAME, 'R5K7Cb'))
                if current_offer_count <= last_offer_count:
                    break
                last_offer_count = current_offer_count
            except:
                pass
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
        result['gs_images'] = json.dumps(about_data.get('gs_images', []))
        result['rating_star'] = about_data.get('rating_star')
        result['rating_count'] = about_data.get('rating_count')
        result['typical_price_low'] = about_data.get('typical_price_low')
        result['typical_price_high'] = about_data.get('typical_price_high')
        result['popular_url'] = about_data.get('popular_url', '')

        # Extract mapped attributes and merge them into result
        attr_dict = about_data.get('attributes', {})
        mapped_attrs = extract_mapped_attributes(attr_dict)
        result.update(mapped_attrs)

        print("✓ Product about info, description, attributes, image, ratings, typical prices, and gallery extracted")
    except Exception as e:
        print(f"Error extracting product about info: {str(e)}")
        result['product_about_info'] = json.dumps({'description': '', 'attributes': {}, 'main_image': '', 'gs_images': []})
        result['description'] = ''
        result['attributes'] = json.dumps({})
        result['main_image'] = ''
        result['gs_images'] = json.dumps([])
        result['rating_star'] = None
        result['rating_count'] = None
        result['typical_price_low'] = None
        result['typical_price_high'] = None
        result['popular_url'] = ''
        
        # Merge empty mapped attributes
        mapped_attrs = extract_mapped_attributes({})
        result.update(mapped_attrs)

    offer_elements = offers_grid.find_elements(By.CLASS_NAME, 'R5K7Cb')
    print(f"Found {len(offer_elements)} offers")

    competitors = []
    for idx, seller_html in enumerate(offer_elements):
        google_position = idx + 1
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

        # Extract original price
        original_price = None
        try:
            old_price_el = seller_html.find_element(By.CSS_SELECTOR, "div.AoPnCe span[aria-hidden='true']")
            original_price = parse_price(old_price_el.text)
        except Exception:
            try:
                old_price_el = seller_html.find_element(By.CSS_SELECTOR, "div.AoPnCe")
                original_price = parse_price(old_price_el.text)
            except Exception:
                try:
                    old_price_el = seller_html.find_element(By.XPATH, ".//span[contains(@aria-label, 'Old price')]")
                    original_price = parse_price(old_price_el.text)
                except Exception:
                    pass

        # Compute discount amount
        discount_amount = None
        if original_price is not None:
            parsed_price = parse_price(seller_price)
            if parsed_price is not None:
                discount_amount = original_price - parsed_price

        # Extract seller rating
        seller_rating = None
        try:
            rating_el = seller_html.find_element(By.CSS_SELECTOR, "span.NFq8Ad")
            rating_text = rating_el.text.strip()
            if "/" in rating_text:
                seller_rating = parse_price(rating_text.split("/")[0])
            else:
                seller_rating = parse_price(rating_text)
        except Exception:
            try:
                rating_el = seller_html.find_element(By.XPATH, ".//span[contains(@aria-label, 'Rated')]")
                aria_label = rating_el.get_attribute("aria-label")
                match = re.search(r"Rated\s+([\d.]+)", aria_label)
                if match:
                    seller_rating = float(match.group(1))
            except Exception:
                pass

        # Extract delivery tagline
        delivery_tagline = ""
        try:
            delivery_els = seller_html.find_elements(By.XPATH, ".//span[contains(@aria-label, 'delivery') or contains(@aria-label, 'Delivery') or contains(text(), 'delivery') or contains(text(), 'Delivery') or contains(text(), 'shipping') or contains(text(), 'Shipping')]")
            for el in delivery_els:
                text_val = el.get_attribute("aria-label") or el.text
                text_val = text_val.strip()
                if text_val:
                    text_val = text_val.replace("·", "").strip()
                    if text_val:
                        delivery_tagline = text_val
                        break
        except Exception:
            pass

        # Extract coupon details
        coupon_code = ""
        coupon_remark = ""
        try:
            row_full_text = seller_html.text
            code_match = re.search(r'(?:use\s+|with\s+)?code[:\s]+([A-Z0-9_-]{3,})', row_full_text, re.IGNORECASE)
            if code_match:
                coupon_code = code_match.group(1)
            
            remark_match = re.search(r'([\d%]+\s+off\s+with\s+code\s+[A-Z0-9_-]+|save\s+[\$\d.]+\s+with\s+code\s+[A-Z0-9_-]+|[\d%]+\s+coupon)', row_full_text, re.IGNORECASE)
            if remark_match:
                coupon_remark = remark_match.group(1)
        except Exception:
            pass

        competitor_data = {
            'product_id': product_id,
            'seller': store_name,
            'seller_product_name': seller_product_name,
            'seller_url': seller_url,
            'seller_price': seller_price,
            'original_price': original_price,
            'discount_amount': discount_amount,
            'coupon_code': coupon_code,
            'coupon_remark': coupon_remark,
            'stock_status': stock_status,
            'seller_rating': seller_rating,
            'delivery_tagline': delivery_tagline,
            'google_position': google_position,
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

    # Calculate best_price_url from competitors
    best_price_url = ""
    min_price = float('inf')
    for competitor in competitors:
        price_str = competitor.get('seller_price', '')
        if price_str and price_str != 'N/A':
            parsed_p = parse_price(price_str)
            if parsed_p is not None and parsed_p < min_price:
                min_price = parsed_p
                best_price_url = competitor.get('seller_url', '')

    result.update({
        'osb_position': osb_position,
        'seller_count': seller_count,
        'osb_id': osb_id,
        'status': 'completed',
        'osb_url_match': f'{"Yes" if osb_url_match else "No"}',
        'best_price_url': best_price_url or "",
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

def get_existing_product_url_from_db(product_id):
    """Retrieve existing valid product_url from google_shopping_results if available."""
    conn = None
    cursor = None
    try:
        conn = _get_pg_conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT google_seller_page_url FROM google_shopping_results WHERE product_id = %s",
            (str(product_id),)
        )
        row = cursor.fetchone()
        if row and row[0]:
            p_url = row[0].strip()
            is_valid_url = p_url.startswith("https://www.google.com/search?ibp=oshop") or p_url.startswith("https://share.google/")
            if is_valid_url:
                return p_url
        return None
    except Exception as e:
        print(f"Error fetching existing product_url from DB for {product_id}: {e}")
        return None
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def extract_product_title_from_page(driver):
    """Attempt to extract product title from the direct Google Shopping page."""
    selectors = [
        "//h2",  # Frequently used for product headers on Google Shopping pages
        "//h1",
        "//div[contains(@class, 'sh-t__title')]",
        "//span[contains(@class, 'sh-t__title')]",
        "//div[@class='sh-pr__title']",
        "//div[contains(@class, 'E5oc2')]//h2",
        "//div[@jsname='HhYL2b']//h2",
        "//div[@jsname='Ql2bfc']//h2"
    ]
    for selector in selectors:
        try:
            element = driver.find_element(By.XPATH, selector)
            text = element.text.strip()
            if text:
                return text
        except:
            continue
    try:
        title = driver.title
        if title and "Google Shopping" in title:
            # Clean up title if it contains suffix
            title = title.split(" - Google Shopping")[0].strip()
            if title:
                return title
    except:
        pass
    return ""

def scrape_product_directly(driver, product_id, keyword, product_url, osb_url=""):
    """Scrape product directly using the product_url from previous scraper results"""
    try:
        print(f"\nScraping Product ID (Direct): {product_id}")
        print(f"Product URL: {product_url}")
        
        driver.get(product_url)
        
        # Handle captcha before proceeding
        captcha_result = handle_captcha(driver, product_url)
        if captcha_result == "failed":
            result = initialize_product_result(product_id, keyword, product_url)
            result.update({
                'last_response': 'Captcha solving failed',
                'status': 'captcha_failed'
            })
            return result
        
        time.sleep(random.uniform(4, 8))
        
        # Initialize result structure
        result = initialize_product_result(product_id, keyword, product_url)
        result['status'] = 'completed'
        
        # Extract title from page
        page_title = extract_product_title_from_page(driver)
        result['product_name'] = page_title if page_title else keyword
        
        try:
            # Populate offers from the current page
            result = populate_offers_for_selected_product(driver, result, product_id, osb_url)
            return result
        except Exception as e:
            result['last_response'] = f"Direct offers extraction failed: {str(e)}"
            result['status'] = "selection_error"
            return result
            
    except TimeoutException as e:
        print(f"Timeout error scraping product {product_id} (Direct): {str(e)}")
        traceback.print_exc()
        result = initialize_product_result(product_id, keyword, product_url)
        result.update({
            'last_response': f'Timeout Error: {str(e)}',
            'status': 'timeout_error',
            'product_name': keyword
        })
        return result
    except Exception as e:
        print(f"Error scraping product {product_id} (Direct): {str(e)}")
        traceback.print_exc()
        result = initialize_product_result(product_id, keyword, product_url)
        result.update({
            'last_response': f'Error: {str(e)}',
            'status': 'error',
            'product_name': keyword
        })
        return result

def reset_cached_product_url(product_id):
    """Reset the google_seller_page_url in google_shopping_results to NULL for this product."""
    conn = None
    cursor = None
    try:
        conn = _get_pg_conn()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE google_shopping_results SET google_seller_page_url = NULL WHERE product_id = %s",
            (str(product_id),)
        )
        conn.commit()
        print(f"✓ Reset google_seller_page_url for product {product_id} to NULL.")
    except Exception as e:
        print(f"Error resetting google_seller_page_url for {product_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def build_fallback_search_url(name, bed_size_measure=None, mattress_size=None):
    """
    Build a broad fallback search URL:
    - Removes '1stopbedrooms'
    - Removes 'color' and 'mpn' (part)
    Only searches: {name} "{bed_size_measure}" "{mattress_size}"
    """
    parts = [_decode_html_entities(name)]
    if bed_size_measure and str(bed_size_measure).strip():
        parts.append(f'"{str(bed_size_measure).strip()}"')
    if mattress_size and str(mattress_size).strip():
        parts.append(f'"{str(mattress_size).strip()}"')
    query = ' '.join(parts).replace(' ', '+').replace('#', '')
    return f'https://www.google.com/search?q={query}&udm=28&gl=US&hl=en&pws=0'

def scrape_product(driver, product_id, keyword, url, osb_url="", name="", mpn_sku="", color="", bed_size_measure="", mattress_size=""):
    """Scrape individual product from Google Shopping with multi-step search retries."""
    # 1. Check if we already have a valid product_url in product_scraping_results
    existing_product_url = get_existing_product_url_from_db(product_id)
    if existing_product_url:
        print(f"Attempting to scrape directly using cached URL: {existing_product_url}")
        result = scrape_product_directly(driver, product_id, keyword, existing_product_url, osb_url)
        status_lower = str(result.get('status', '')).strip().lower()
        if status_lower in {'completed', 'product_found'}:
            return result
        
        if status_lower in {'selection_error', 'product_not_clickable', 'no_products', 'captcha_failed', 'timeout_error', 'error'}:
            print(f"Cached URL failed with status '{status_lower}'. Resetting cached URL and falling back to search flow.")
            reset_cached_product_url(product_id)

    # 2. Search Flow (Retry sequence)
    last_result = None
    
    # Define retry attempts
    attempts = [
        ("Original search", url),
    ]
    
    retry_url = build_retry_search_url(url)
    if retry_url != url:
        attempts.append(("Without 1stopbedrooms prefix", retry_url))
        
    fallback_url = build_fallback_search_url(name, bed_size_measure, mattress_size)
    if fallback_url:
        attempts.append(("Without 1stopbedrooms/color/part", fallback_url))

    for phase_name, search_url in attempts:
        try:
            print(f"\n[PID {os.getpid()}] Scraper Phase: {phase_name}")
            print(f"Search URL: {search_url}")
            
            driver.get(search_url)
            
            # Handle captcha before proceeding
            captcha_result = handle_captcha(driver, search_url)
            if captcha_result == "failed":
                result = initialize_product_result(product_id, keyword, search_url)
                result.update({
                    'last_response': 'Captcha solving failed',
                    'status': 'captcha_failed'
                })
                last_result = result
                continue
            
            time.sleep(random.uniform(4, 8))
            
            # Initialize result structure
            result = initialize_product_result(product_id, keyword, search_url)
            
            phase_result, matched = run_product_selection_phase(
                driver, product_id, phase_name, search_url, result, osb_url
            )
            last_result = phase_result
            if matched:
                return phase_result
            
            # Fallback to first matching product on page if not fully matched in standard mode
            fallback_result, _ = run_product_selection_phase(
                driver, product_id, f"{phase_name} fallback", search_url, result, osb_url, fallback_first=True
            )
            if fallback_result.get('status') in {'completed', 'product_found', 'product_not_clickable', 'no_offers_found'}:
                return fallback_result
            
            last_result = fallback_result
            
        except TimeoutException as e:
            print(f"Timeout error scraping product {product_id} in phase '{phase_name}': {str(e)}")
            result = initialize_product_result(product_id, keyword, search_url)
            result.update({
                'last_response': f'Timeout Error: {str(e)}',
                'status': 'timeout_error'
            })
            last_result = result
        except Exception as e:
            print(f"Error scraping product {product_id} in phase '{phase_name}': {str(e)}")
            traceback.print_exc()
            result = initialize_product_result(product_id, keyword, search_url)
            result.update({
                'last_response': f'Error: {str(e)}',
                'status': 'error'
            })
            last_result = result
            
    return last_result or initialize_product_result(product_id, keyword, url)

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


def process_chunk(df, chunk_id, total_chunks, round_id=1, output_dir='output', worker_id=None, ttl_minutes=60, max_runtime_seconds=None, max_workers=1):
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
        resolved_worker_id = _get_worker_id(worker_id)
        started_at = time.monotonic()

        if max_workers > 1:
            print(f"Processing {len(df)} products from chunk {chunk_id} in parallel with {max_workers} threads")
        else:
            print(f"Processing {len(df)} products from chunk {chunk_id} sequentially")
        
        # Initialize results (thread-safe operations)
        product_results = []
        seller_results = []
        remaining_results = []

        results_lock = threading.Lock()
        stop_event = threading.Event()
        product_queue = queue.Queue()
        
        for idx, row in df.iterrows():
            product_queue.put((idx, row))
            
        consecutive_timeouts_map = {} # thread_id -> count

        # Database batch writer queue and configuration
        db_queue = queue.Queue()
        db_batch_size = _env_int("DB_BATCH_SIZE", DEFAULT_DB_BATCH_SIZE)

        def db_writer_worker():
            batch_products = []
            batch_sellers = []
            last_write_time = time.monotonic()
            
            while True:
                # If we have items in our current batch, we don't want to wait too long to write them
                timeout = 1.0 if (batch_products or batch_sellers) else 5.0
                try:
                    scraped_data = db_queue.get(timeout=timeout)
                    if scraped_data is None:
                        # Sentinel received! Flush any remaining items and exit
                        if batch_products:
                            try:
                                insert_to_postgres(batch_products, batch_sellers)
                            except Exception as db_err:
                                print(f"[DB Writer] Final batch Postgres insert failed: {db_err}")
                                traceback.print_exc()
                        db_queue.task_done()
                        break
                    else:
                        batch_products.append(scraped_data)
                        batch_sellers.extend(scraped_data.get('competitors', []))
                        db_queue.task_done()
                except queue.Empty:
                    pass
                
                # Check if we should write the current batch to Postgres
                time_since_last_write = time.monotonic() - last_write_time
                should_write = False
                
                if len(batch_products) >= db_batch_size:
                    should_write = True
                elif time_since_last_write >= 5.0 and batch_products:
                    should_write = True
                
                if should_write and batch_products:
                    try:
                        insert_to_postgres(batch_products, batch_sellers)
                    except Exception as db_err:
                        print(f"[DB Writer] Batch Postgres insert failed: {db_err}")
                        traceback.print_exc()
                    finally:
                        batch_products = []
                        batch_sellers = []
                        last_write_time = time.monotonic()

        db_writer_thread = threading.Thread(target=db_writer_worker, name="PostgresBatchWriter", daemon=True)
        db_writer_thread.start()

        def worker_thread():
            thread_id = threading.get_ident()
            driver = None
            try:
                driver = setup_driver(max_attempts=3, base_delay=5)
            except Exception as e:
                print(f"[Thread {thread_id}] Driver setup failed for chunk {chunk_id}: {str(e)}")
                traceback.print_exc()
                return
            
            try:
                while not product_queue.empty() and not stop_event.is_set():
                    if max_runtime_seconds and (time.monotonic() - started_at) >= max_runtime_seconds:
                        print(f"[Thread {thread_id}] !!! MAX RUNTIME REACHED. Stopping worker thread.")
                        stop_event.set()
                        break
                        
                    try:
                        index, row = product_queue.get_nowait()
                    except queue.Empty:
                        break

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

                    # Build/regenerate the search URL if it's missing or blank
                    if not url or not str(url).strip():
                        url = build_search_url(
                            name=name,
                            mpn=mpnsku,
                            color=row.get('color'),
                            bed_size_measure=row.get('bed_size_measure'),
                            mattress_size=row.get('mattress_size')
                        )
                        print(f"[Thread {thread_id} - URL regenerated] {url}")

                    # Also rebuild keyword if blank
                    if not keyword or not str(keyword).strip():
                        keyword = build_keyword(
                            name=name,
                            mpn=mpnsku,
                            color=row.get('color'),
                            bed_size_measure=row.get('bed_size_measure'),
                            mattress_size=row.get('mattress_size')
                        )
                    
                    print(f"\n[Thread {thread_id}] Processing {index+1}/{len(df)}: Product ID {product_id}")
                    
                    # Check database status and claim the product atomically before scraping
                    if not verify_and_claim_product(product_id, resolved_worker_id, ttl_minutes):
                        print(f"[Thread {thread_id}] Skipping product {product_id} - already claimed/completed by another worker.")
                        continue
                    
                    try:
                        scraped_data = scrape_product(
                            driver, product_id, keyword, url, osb_url,
                            name=name,
                            mpn_sku=mpnsku,
                            color=row.get('color'),
                            bed_size_measure=row.get('bed_size_measure'),
                            mattress_size=row.get('mattress_size')
                        )
                    except Exception as e:
                        print(f"[Thread {thread_id}] Error scraping product {product_id}: {str(e)}")
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
                    
                    # Queue the scraped data for batch database writing
                    db_queue.put(scraped_data)

                    # Add to thread-safe results for local CSV files
                    with results_lock:
                        product_results.append(scraped_data)
                        seller_results.extend(scraped_data.get('competitors', []))
                    
                    status_lower = str(scraped_data.get('status', '')).strip().lower()
                    if status_lower == 'timeout_error':
                        consecutive_timeouts_map[thread_id] = consecutive_timeouts_map.get(thread_id, 0) + 1
                    else:
                        consecutive_timeouts_map[thread_id] = 0

                    if status_lower == 'captcha_failed':
                        print(f"[Thread {thread_id}] !!! CAPTCHA DETECTED on Product {product_id}. Stopping all threads in this chunk.")
                        stop_event.set()
                        break
                    elif consecutive_timeouts_map.get(thread_id, 0) >= 2:
                        print(f"[Thread {thread_id}] !!! TIMEOUT PERSISTS on Product {product_id}. Stopping all threads in this chunk.")
                        stop_event.set()
                        break
                    
                    # Sleep between products
                    time.sleep(random.uniform(1, 3))
            finally:
                try:
                    if driver:
                        driver.quit()
                except Exception:
                    pass

        # Execute workers: parallel if max_workers > 1, else sequential in the main thread
        try:
            if max_workers > 1:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(worker_thread) for _ in range(max_workers)]
                    # Wait for all workers to finish
                    for future in futures:
                        try:
                            future.result()
                        except Exception as e:
                            print(f"Worker thread execution error: {e}")
                            traceback.print_exc()
            else:
                worker_thread()
        finally:
            # Signal DB writer thread to stop by sending the sentinel and join it
            print("[DB Writer] Signalling database writer thread to stop and flush remaining records...")
            db_queue.put(None)
            db_writer_thread.join()
            print("[DB Writer] Database writer thread has shut down successfully.")

        # Identify processed product IDs
        processed_pids = {str(r.get('product_id', '')).strip(): r for r in product_results}
        
        # Populate remaining_results and release unprocessed products
        unprocessed_pids = []
        for _, row in df.iterrows():
            p_id = str(row['product_id']).strip()
            if p_id not in processed_pids:
                unprocessed_pids.append(p_id)
                remaining_row = {
                    col: ('' if pd.isna(row[col]) else row[col])
                    for col in df.columns
                }
                remaining_results.append(remaining_row)
            else:
                status_lower = str(processed_pids[p_id].get('status', '')).strip().lower()
                if status_lower in ['error', 'timeout_error', 'captcha_failed']:
                    remaining_row = {
                        col: ('' if pd.isna(row[col]) else row[col])
                        for col in df.columns
                    }
                    remaining_results.append(remaining_row)

        # Release any claimed products that were not processed due to early shutdown/errors
        if unprocessed_pids:
            release_reason = "captcha_failed" if stop_event.is_set() else "unprocessed_due_to_shutdown"
            release_claimed_products(unprocessed_pids, resolved_worker_id, reason=release_reason)
        
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
                'attributes': result.get('attributes', json.dumps({})),
                'gs_images': result.get('gs_images', json.dumps([])),
                'color': result.get('color', None),
                'width': result.get('width', None),
                'height': result.get('height', None),
                'depth': result.get('depth', None),
                'style': result.get('style', None),
                'material': result.get('material', None),
                'shape': result.get('shape', None),
                'assembly_required': result.get('assembly_required', None),
                'weight': result.get('weight', None),
                'rating_star': result.get('rating_star', None),
                'rating_count': result.get('rating_count', None),
                'typical_price_low': result.get('typical_price_low', None),
                'typical_price_high': result.get('typical_price_high', None),
                'best_price_url': result.get('best_price_url', ''),
                'popular_url': result.get('popular_url', '')
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
                'original_price': seller.get('original_price', None),
                'discount_amount': seller.get('discount_amount', None),
                'coupon_code': seller.get('coupon_code', ''),
                'coupon_remark': seller.get('coupon_remark', ''),
                'stock_status': seller.get('stock_status', 'In Stock'),
                'seller_rating': seller.get('seller_rating', None),
                'delivery_tagline': seller.get('delivery_tagline', ''),
                'google_position': seller.get('google_position', None),
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
        
        # Already inserted to Postgres immediately after scraping each product.
        # insert_to_postgres(csv1_data, csv2_data)
        
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
            release_claimed_products(
                df['product_id'].tolist(),
                worker_id,
                reason="driver_connectivity_error",
            )
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
    parser.add_argument('--claim-mode', action='store_true', help='Deprecated; DB claiming is the default unless --offset-mode is used')
    parser.add_argument('--offset-mode', action='store_true', help='Use legacy LIMIT/OFFSET chunking instead of DB claiming')
    parser.add_argument('--claim-limit', type=int, default=_env_int("CLAIM_LIMIT", None), help='How many products to claim and scrape; defaults to products/hour * runtime hours')
    parser.add_argument('--products-per-hour', type=int, default=_env_int("PRODUCTS_PER_HOUR", DEFAULT_PRODUCTS_PER_HOUR), help='Expected scrape rate used to size each worker claim')
    parser.add_argument('--max-runtime-hours', type=float, default=_env_float("MAX_RUNTIME_HOURS", DEFAULT_MAX_RUNTIME_HOURS), help='Maximum hours this worker should process')
    parser.add_argument('--claim-ttl-minutes', type=int, default=_env_int("CLAIM_TTL_MINUTES", None), help='Release claims older than this TTL')
    parser.add_argument('--worker-id', type=str, default=os.environ.get("SCRAPER_WORKER_ID", None), help='Worker identifier stored in DB claims')
    parser.add_argument('--max-workers', type=int, default=_env_int("MAX_WORKERS", 2), help='Number of parallel worker threads inside this chunk (default: 1, sequential)')
    parser.add_argument('--start-sales', type=str, default=None, help='Start sales boundary for this account partition')
    parser.add_argument('--start-id', type=str, default=None, help='Start product ID boundary for this account partition')
    parser.add_argument('--end-sales', type=str, default=None, help='End sales boundary for this account partition')
    parser.add_argument('--end-id', type=str, default=None, help='End product ID boundary for this account partition')
    
    args = parser.parse_args()
    args.claim_limit = int(args.claim_limit) if args.claim_limit is not None else None
    if args.claim_ttl_minutes is None:
        args.claim_ttl_minutes = max(DEFAULT_CLAIM_TTL_MINUTES, int(math.ceil(args.max_runtime_hours * 60)) + 120)
    else:
        args.claim_ttl_minutes = int(args.claim_ttl_minutes)
    effective_claim_limit = calculate_parallel_claim_limit(
        claim_limit=args.claim_limit,
        products_per_hour=args.products_per_hour,
        max_runtime_hours=args.max_runtime_hours,
    )
    max_runtime_seconds = int(args.max_runtime_hours * 60 * 60)
    
    # Handlers for dedicated utility commands
    if args.reset_errors:
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
    # REMOVED: chunk 1 automatic reset to avoid race conditions and infinite loops in concurrent/multi-account environments.
    # Error resets are now triggered once at the start of the workflow run via the workflow dispatch input.
    # if args.chunk_id == 1:
    #     reset_error_products_to_pending()
    #     reset_invalid_url_products_for_retry()
    
    # Release expired claims before checking queue size to ensure stale rows are recycled
    release_expired_claims(ttl_minutes=args.claim_ttl_minutes)
    
    # Fetch total pending products count first (extremely fast)
    total_pending = get_pending_count_from_db()
    if total_pending == 0:
        print("No pending products found in DB. All done!")
        sys.exit(0)

    print(f"Total pending products in DB: {total_pending}")

    if not args.offset_mode:
        if args.start_id or args.end_id:
            print(f"Boundary partition mode: start=({args.start_sales}, {args.start_id}) end=({args.end_sales}, {args.end_id})")
            product_ids = get_product_ids_in_boundary(args.start_sales, args.start_id, args.end_sales, args.end_id)
            M = len(product_ids)
            print(f"Total products in account boundary: {M}")
            if M == 0:
                print("No products in boundary. Exiting.")
                sys.exit(0)
                
            base_rows = M // args.total_chunks
            remainder = M % args.total_chunks
            limit = base_rows + (1 if args.chunk_id <= remainder else 0)
            offset = (args.chunk_id - 1) * base_rows + min(args.chunk_id - 1, remainder)
            
            worker_product_ids = product_ids[offset : offset + limit]
            print(f"Worker chunk {args.chunk_id} of {args.total_chunks}: Slice size={len(worker_product_ids)} (Offset={offset}, Limit={limit})")
            
            chunk_df = claim_specific_products_from_db(
                worker_product_ids, 
                worker_id=args.worker_id, 
                limit=effective_claim_limit, 
                ttl_minutes=args.claim_ttl_minutes
            )
        else:
            print(
                f"DB claim queue enabled: worker={_get_worker_id(args.worker_id)} "
                f"limit={effective_claim_limit} ttl={args.claim_ttl_minutes}m "
                f"runtime={args.max_runtime_hours}h rate={args.products_per_hour}/h"
            )
            chunk_df = claim_pending_products_from_db(limit=effective_claim_limit, worker_id=args.worker_id, ttl_minutes=args.claim_ttl_minutes)
            
        if chunk_df.empty:
            print("No claimable pending products found (or claim columns missing).")
            sys.exit(0)
        chunk_result = process_chunk(
            chunk_df,
            args.chunk_id,
            args.total_chunks,
            worker_id=args.worker_id,
            ttl_minutes=args.claim_ttl_minutes,
            max_runtime_seconds=max_runtime_seconds,
            max_workers=args.max_workers,
        )
        success = chunk_result.get("success", False)
        sys.exit(0 if success else 1)

    # Legacy mode: calculate balanced limit and offset for this chunk.
    # Prefer the DB claim queue above for parallel scraping; LIMIT/OFFSET is unstable
    # when many workers update the pending set concurrently.
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

    chunk_result = process_chunk(
        chunk_df,
        args.chunk_id,
        args.total_chunks,
        worker_id=args.worker_id,
        ttl_minutes=args.claim_ttl_minutes,
        max_runtime_seconds=max_runtime_seconds,
        max_workers=args.max_workers,
    )
    success = chunk_result.get("success", False)
    
    reset_error_products_to_pending()
    
    if success:
        print("\n✓ Chunk processing completed successfully")
        sys.exit(0)
    else:
        print("\n✗ Chunk processing failed")
        sys.exit(1)

if __name__ == "__main__":
    main()

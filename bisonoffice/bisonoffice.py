import os
import csv
import time
import sys
import gc
import gzip
import io
import threading
import requests
import re
import json
import html
import ast
from typing import Optional, List, Dict, Any, Set
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, urlencode, parse_qs, urljoin
from pathlib import Path
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add repository root for shared imports like db_queue
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

try:
    from db_queue import filter_pending_urls, save_scraped_product
    HAS_DB_QUEUE = True
except Exception:
    HAS_DB_QUEUE = False


class BisonofficeScraper:

    # ============================================================
    #  Init — all config + shared state
    # ============================================================

    def __init__(self):
        self.curr_url             = os.getenv("CURR_URL", "https://www.bisonoffice.com").rstrip("/")
        self.api_base_url         = os.getenv("API_BASE_URL", "").rstrip("/")
        self.sitemap_offset       = int(os.getenv("SITEMAP_OFFSET", "0"))
        self.max_sitemaps         = int(os.getenv("MAX_SITEMAPS", "0"))
        self.max_urls_per_sitemap = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
        self.max_workers          = int(os.getenv("MAX_WORKERS", "4"))
        self.request_delay        = float(os.getenv("REQUEST_DELAY", "1.0"))

        self.output_dir   = "media/output/scrapping/bisonoffice"
        self.failure_dir  = "media/output/scrapping/failure_csv"
        self.output_csv   = f"bisonoffice_products_chunk_{self.sitemap_offset}.csv"
        self.scraped_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        self.csv_header = [
            "Ref Product URL",
            "Ref Product ID",
            "Ref Variant ID",
            "Ref Category",
            "Ref Category URL",
            "Ref Brand Name",
            "Ref Product Name",
            "Ref SKU",
            "Ref MPN",
            "Ref GTIN",
            "Ref Price",
            "Ref Main Image",
            "Ref Quantity",
            "Ref Group Attr 1",
            "Ref Group Attr 2",
            "Ref Status",
            "Date Scrapped",
        ]

        self.csv_lock  = threading.Lock()
        self.fail_lock = threading.Lock()
        self.seen: Set[str] = set()
        self.stats = {
            "sitemaps_processed": 0,
            "urls_processed":     0,
            "products_fetched":   0,
            "errors":             0,
        }

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language":           "en-US,en;q=0.9",
            "Connection":                "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest":            "document",
            "Sec-Fetch-Mode":            "navigate",
            "Sec-Fetch-Site":            "none",
            "Sec-Fetch-User":            "?1",
        })

    # ============================================================
    #  URL helpers
    # ============================================================

    def append_bo_param(self, url: str) -> str:
        """Append ?bo=0 (or &bo=0) to any URL, skipping empty strings."""
        if not url:
            return url
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        qs["bo"] = ["0"]
        new_query = urlencode({k: v[0] for k, v in qs.items()}, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    def clean_url(self, url: str) -> str:
        """Strip query params and trailing slashes, then append ?bo=0."""
        if not url:
            return url
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
        return self.append_bo_param(base)

    def normalize_image(self, url: str) -> str:
        """Make image URL absolute and append ?bo=0."""
        if not url:
            return ""
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("/"):
            url = f"{self.curr_url}{url}"
        elif not url.startswith("http"):
            url = f"https://{url}"
        return self.append_bo_param(url)

    # ============================================================
    #  Logger
    # ============================================================

    def log(self, msg: str, level: str = "INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
        sys.stderr.flush()

    # ============================================================
    #  HTTP
    # ============================================================

    def http_get(self, url: str, is_json: bool = False) -> Optional[str]:
        """GET with up to 3 retries. Always appends ?bo=0. Switches headers for JSON vs HTML."""
        url = self.append_bo_param(url)
        for attempt in range(3):
            try:
                if is_json:
                    headers = {
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/120.0.0.0 Safari/537.36"
                        ),
                        "Accept":           "application/json, text/javascript, */*; q=0.01",
                        "Accept-Language":  "en-US,en;q=0.9",
                        "Referer":          f"{self.curr_url}/",
                        "X-Requested-With": "XMLHttpRequest",
                        "Sec-Fetch-Dest":   "empty",
                        "Sec-Fetch-Mode":   "cors",
                        "Sec-Fetch-Site":   "same-origin",
                    }
                    r = self.session.get(url, headers=headers, timeout=15, verify=True)
                else:
                    r = self.session.get(url, timeout=15, verify=True)

                if r.status_code == 200:
                    self.log(f"Success: {url}", "DEBUG")
                    return r.text

                self.log(f"Status {r.status_code} for {url}", "WARNING")
                if r.status_code == 429:
                    time.sleep(5)

            except requests.exceptions.Timeout:
                self.log(f"Timeout attempt {attempt + 1} for {url}", "WARNING")
                time.sleep(2)
            except Exception as e:
                self.log(f"Attempt {attempt + 1} failed for {url}: {type(e).__name__}", "WARNING")
                time.sleep(1)

        return None

    def http_get_bytes(self, url: str) -> Optional[bytes]:
        """GET raw bytes with up to 3 retries. Always appends ?bo=0.
        Used for sitemap fetching so .gz files can be decompressed before decoding."""
        url = self.append_bo_param(url)
        for attempt in range(3):
            try:
                r = self.session.get(url, timeout=30, verify=True)
                if r.status_code == 200:
                    self.log(f"Success (bytes): {url}", "DEBUG")
                    return r.content
                self.log(f"Status {r.status_code} for {url}", "WARNING")
                if r.status_code == 429:
                    time.sleep(5)
            except requests.exceptions.Timeout:
                self.log(f"Timeout attempt {attempt + 1} for {url}", "WARNING")
                time.sleep(2)
            except Exception as e:
                self.log(f"Attempt {attempt + 1} failed for {url}: {type(e).__name__}", "WARNING")
                time.sleep(1)
        return None

    # ============================================================
    #  Sitemap
    # ============================================================

    def load_xml(self, url: str) -> Optional[ET.Element]:
        """Load XML from a URL, transparently decompressing .gz content."""
        raw: Optional[bytes] = None
        for attempt in range(3):
            try:
                raw = self.http_get_bytes(url)
                if raw:
                    break
            except Exception as e:
                self.log(f"Attempt {attempt + 1} for sitemap failed: {e}", "WARNING")
                time.sleep(2)

        if not raw:
            self.log(f"Failed to load XML from {url}", "ERROR")
            return None

        # ---- Decompress if gzip ----
        if raw[:2] == b'\x1f\x8b':
            self.log(f"Detected gzip content, decompressing: {url}", "DEBUG")
            try:
                raw = gzip.decompress(raw)
            except Exception as e:
                self.log(f"gzip decompression failed for {url}: {e}", "ERROR")
                return None

        # ---- Decode bytes → str ----
        try:
            data = raw.decode("utf-8")
        except UnicodeDecodeError:
            data = raw.decode("latin-1")

        # ---- Strip CDATA wrappers so ET can parse cleanly ----
        data = re.sub(r'<!\[CDATA\[(.*?)]]>', lambda m: m.group(1), data, flags=re.DOTALL)

        # ---- Ensure XML declaration ----
        if "<?xml" not in data[:100]:
            data = '<?xml version="1.0" encoding="UTF-8"?>\n' + data

        try:
            return ET.fromstring(data)
        except ET.ParseError as e:
            self.log(f"XML parsing failed for {url}: {e}", "ERROR")
            # Regex fallback — also handles any residual CDATA
            try:
                root = ET.Element("urlset")
                pattern = r'<loc>(?:<!\[CDATA\[)?(https?://[^<\]]+?)(?:]]>)?</loc>'
                for url_text in re.findall(pattern, data):
                    url_elem = ET.SubElement(root, "url")
                    loc_elem = ET.SubElement(url_elem, "loc")
                    loc_elem.text = url_text.strip()
                self.log(f"Regex fallback found {len(root)} URLs in {url}", "WARNING")
                return root
            except Exception as e2:
                self.log(f"Regex extraction also failed for {url}: {e2}", "ERROR")
                return None

    def get_child_sitemaps(self, index_url: str) -> List[str]:
        """Parse sitemap index and return list of child sitemap URLs (each with ?bo=0)."""
        ns    = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        index = self.load_xml(index_url)
        if index is None:
            return []

        for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
            elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
            if elements:
                sitemaps = [
                    self.append_bo_param(e.text.strip())
                    for e in elements if e.text
                ]
                if sitemaps:
                    self.log(f"Found {len(sitemaps)} child sitemaps", "INFO")
                    return sitemaps

        self.log("No child sitemaps found", "WARNING")
        return []

    def get_product_urls(self, sitemap_url: str) -> List[str]:
        """Parse a product sitemap and return only bisonoffice /ip/ URLs (each with ?bo=0)."""
        ns  = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        xml = self.load_xml(sitemap_url)
        if not xml:
            return []

        for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
            elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
            if elements:
                urls = [
                    self.append_bo_param(e.text.strip())
                    for e in elements if e.text and "/p/" in e.text
                ]
                if urls:
                    return urls

        self.log(f"No /ip/ product URLs found in: {sitemap_url}", "WARNING")
        return []

    # ============================================================
    #  Helpers
    # ============================================================

    def extract_product_id(self, url: str) -> Optional[str]:
        """Pull bisonoffice item ID from /ip/product-name/123456789 style URLs."""
        if not url:
            return None
        match = re.search(r'/ip/(?:[^/]+/)?(\d+)', url)
        if match:
            return match.group(1)
        last = url.rstrip("/").split("/")[-1].split("?")[0]
        if last.isdigit():
            return last
        self.log(f"No product ID found in URL: {url}", "WARNING")
        return None

    # ============================================================
    #  HTML Extraction - Primary Data Source
    # ============================================================
    
    def extract_from_html(self, html_text, product_url):
        """Extract product data directly from HTML structure"""
        soup = BeautifulSoup(html_text, 'html.parser')
        product_data = {}
        
        # Extract Product ID from URL first (fallback)
        product_data['product_id'] = self.extract_product_id(product_url) or ''
        
        # Extract from meta tags and schema.org markup
        # Product ID from visible element
        product_id_elem = soup.find('div', {'itemprop': 'productId'})
        if product_id_elem:
            # Extract just the number from "Product ID: 99064"
            id_text = product_id_elem.get_text(strip=True)
            import re
            id_match = re.search(r'(\d+)', id_text)
            if id_match:
                product_data['product_id'] = id_match.group(1)
        
        # GTIN from meta tag
        gtin_elem = soup.find('meta', {'itemprop': 'gtin12'})
        if gtin_elem and gtin_elem.get('content'):
            product_data['gtin'] = gtin_elem['content']
        
        # Product Name
        name_elem = soup.find('div', {'itemprop': 'name'})
        if name_elem:
            product_data['name'] = html.unescape(name_elem.get_text(strip=True))
        
        # Brand
        brand_elem = soup.find('div', {'itemprop': 'brand'})
        if brand_elem:
            brand_link = brand_elem.find('a')
            if brand_link:
                product_data['brand'] = html.unescape(brand_link.get_text(strip=True))
                # Extract brand URL
                brand_url = brand_link.get('href', '')
                if brand_url:
                    product_data['brand_url'] = self.append_bo_param(urljoin(self.curr_url, brand_url))
        
        # Price from schema.org markup
        price_elem = soup.find('div', {'itemprop': 'price'})
        if price_elem and price_elem.get('content'):
            product_data['price'] = price_elem['content']
        else:
            # Fallback to visible price element
            price_elem = soup.find('div', {'itemprop': 'offers'})
            if price_elem:
                price_span = price_elem.find('div', {'itemprop': 'price'})
                if price_span and price_span.get('content'):
                    product_data['price'] = price_span['content']
        
        # Main Image from meta tag
        image_elem = soup.find('meta', {'itemprop': 'image'})
        if image_elem and image_elem.get('content'):
            product_data['main_image'] = self.normalize_image(image_elem['content'])
        else:
            # Fallback to main image element
            main_image = soup.find('img', {'id': 'myimage'})
            if main_image and main_image.get('src'):
                product_data['main_image'] = self.normalize_image(main_image['src'])
        
        # Extract quantity from stock information
        max_qty_elem = soup.find('div', class_='product-main__info-prices-item-maxqty')
        if max_qty_elem:
            qty_text = max_qty_elem.get_text()
            import re
            qty_match = re.search(r'Only\s+(\d+)', qty_text)
            if qty_match:
                product_data['quantity'] = int(qty_match.group(1))
            else:
                product_data['quantity'] = 1
        else:
            product_data['quantity'] = 1
        
        # Extract category from breadcrumb navigation
        breadcrumb = soup.find('div', class_='menu__container')
        if breadcrumb:
            category_links = breadcrumb.find_all('a')
            if len(category_links) >= 2:
                # Get the second-to-last category (usually the most specific)
                if len(category_links) >= 2:
                    category_elem = category_links[-2]  # Second last is usually the current category
                    product_data['category'] = html.unescape(category_elem.get_text(strip=True))
                    
                    # Get category URL
                    if category_elem.get('href'):
                        product_data['category_url'] = category_elem['href']
        
        # Extract MPN from specifications if available
        specs_section = soup.find('div', class_='products-main__filter')
        if specs_section:
            spec_items = specs_section.find_all('li', class_='products-main__filter-item-content-list-item')
            for item in spec_items:
                name_divs = item.find_all('div', class_=lambda x: x and ('item-name' in x))
                if len(name_divs) >= 2:
                    spec_name = name_divs[0].get_text(strip=True).lower()
                    spec_value = name_divs[1].get_text(strip=True)
                    
                    if 'manufacturer' in spec_name and 'part' in spec_name:
                        product_data['mpn'] = spec_value
                    elif 'model' in spec_name:
                        if 'mpn' not in product_data:
                            product_data['mpn'] = spec_value
        
        # Set status based on availability
        out_of_stock = soup.find('div', class_='out-of-stock-wrapper')
        if out_of_stock and 'display: none' not in str(out_of_stock):
            product_data['status'] = 'Out of Stock'
        else:
            product_data['status'] = 'In Stock'
        
        return product_data

    # ============================================================
    #  DataLayer Extraction - Secondary/Supplemental
    # ============================================================
    
    def _clean_strings(self, obj):
        """Clean strings by replacing escaped slashes."""
        if isinstance(obj, dict):
            return {k: self._clean_strings(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._clean_strings(v) for v in obj]
        if isinstance(obj, str):
            return obj.replace('\\/', '/')
        return obj

    def _extract_datalayer_pushes(self, html_text):
        matches = []

        for m in re.finditer(r'dataLayer\.push\s*\(', html_text):
            start = m.end()
            paren_count = 1
            i = start

            while i < len(html_text) and paren_count > 0:
                char = html_text[i]

                # Skip string literals
                if char in ('"', "'"):
                    quote = char
                    i += 1
                    while i < len(html_text) and html_text[i] != quote:
                        if html_text[i] == '\\':
                            i += 2
                        else:
                            i += 1

                elif char == '(':
                    paren_count += 1

                elif char == ')':
                    paren_count -= 1

                i += 1

            if paren_count == 0:
                content = html_text[start:i-1].strip()
                matches.append(content)

        return matches


    def _extract_products_recursive(self, obj):
        results = []

        if isinstance(obj, dict):
            for key, value in obj.items():

                if key in ("products", "items") and isinstance(value, list):
                    results.extend(value)
                else:
                    results.extend(self._extract_products_recursive(value))

        elif isinstance(obj, list):
            for item in obj:
                results.extend(self._extract_products_recursive(item))

        return results


    def extract_datalayer(self, html_text):

        merged_items = []
        matches = self._extract_datalayer_pushes(html_text)

        for match in matches:
            try:
                cleaned = html.unescape(match)

                # Remove JS comments
                cleaned = re.sub(r'//.*', '', cleaned)
                cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)

                # Normalize JS literals
                cleaned = cleaned.replace(':true', ':True') \
                                .replace(':false', ':False') \
                                .replace(':null', ':None')

                # Quote unquoted keys
                cleaned = re.sub(
                    r'([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:',
                    r'\1"\2":',
                    cleaned
                )

                # Remove trailing commas
                cleaned = re.sub(r',\s*([}\]])', r'\1', cleaned)

                try:
                    data = json.loads(cleaned)
                except json.JSONDecodeError:
                    data = ast.literal_eval(cleaned)

                data = self._clean_strings(data)

                extracted = self._extract_products_recursive(data)
                merged_items.extend(extracted)

            except Exception as e:
                self.log(f"Failed parsing dataLayer entry: {e}", "DEBUG")
                continue

        # Optional deduplication by id/item_id
        seen = set()
        unique = []

        for item in merged_items:
            key = item.get("id") or item.get("item_id")
            if key:
                if key not in seen:
                    seen.add(key)
                    unique.append(item)
            else:
                unique.append(item)

        self.log(f"Merged {len(unique)} products/items", "DEBUG")

        return unique

    def extract_product_data(self, datalayer_entries: list) -> dict:
        """
        Extract product information from all dataLayer entries.
        This finds the add_to_cart and PageView events and extracts product data.
        """
        product_info = {
            'product_id': '',
            'variation_id': '',
            'category': '',
            'category_url': '',
            'brand': '',
            'name': '',
            'sku': '',
            'mpn': '',
            'gtin': '',
            'price': '',
            'main_image': '',
            'quantity': 1,
            'group_attr_1': '',
            'group_attr_2': '',
            'status': 'In Stock',
            'additional_data': ''
        }
        
        if not datalayer_entries:
            return product_info
        
        # Process each dataLayer entry
        for entry in datalayer_entries:
            if not isinstance(entry, dict):
                continue
            
            event = entry.get('event', '')
            
            # Look for add_to_cart events (contains most complete product data)
            if event == 'add_to_cart':
                ecommerce = entry.get('ecommerce', {})
                items = ecommerce.get('items', [])
                
                if items and isinstance(items, list) and len(items) > 0:
                    item = items[0]
                    
                    product_info.update({
                        'product_id': item.get('item_id', ''),
                        'name': html.unescape(item.get('item_name', '')),
                        'sku': item.get('item_id', ''),
                        'brand': item.get('item_brand', ''),
                        'category': item.get('item_category', ''),
                        'price': str(item.get('price', '')),
                        'quantity': item.get('quantity', 1),
                        'variation_id': item.get('item_variant', 'Regular'),
                    })
                    
                    # Add currency info to group_attr_1
                    currency = ecommerce.get('currency', 'USD')
                    product_info['group_attr_1'] = f"Currency: {currency}"
                    
                    # Add value/total to group_attr_2
                    value = ecommerce.get('value', '')
                    if value:
                        product_info['group_attr_2'] = f"Total Value: {value}"
            
            # Look for PageView events (contains detail product data)
            elif event == 'PageView':
                ecommerce = entry.get('ecommerce', {})
                detail = ecommerce.get('detail', {})
                products = detail.get('products', [])
                
                if products and isinstance(products, list) and len(products) > 0:
                    product = products[0]
                    
                    # Only update if fields are empty (prioritize add_to_cart data)
                    if not product_info['product_id']:
                        product_info['product_id'] = product.get('id', '')
                    if not product_info['name']:
                        product_info['name'] = html.unescape(product.get('name', ''))
                    if not product_info['sku']:
                        product_info['sku'] = product.get('id', '')
                    if not product_info['brand']:
                        product_info['brand'] = product.get('brand', '')
                    if not product_info['category']:
                        product_info['category'] = product.get('category', '')
                    if not product_info['price']:
                        product_info['price'] = str(product.get('price', ''))
            
            # Look for any ecommerce data in other formats
            elif 'ecommerce' in entry and not product_info['product_id']:
                ecommerce = entry.get('ecommerce', {})
                
                # Try to find items directly
                items = ecommerce.get('items', [])
                if items and isinstance(items, list) and len(items) > 0:
                    item = items[0]
                    product_info.update({
                        'product_id': item.get('item_id', item.get('id', '')),
                        'name': html.unescape(item.get('item_name', item.get('name', ''))),
                        'sku': item.get('item_id', item.get('id', '')),
                        'brand': item.get('item_brand', item.get('brand', '')),
                        'category': item.get('item_category', item.get('category', '')),
                        'price': str(item.get('price', '')),
                        'quantity': item.get('quantity', 1),
                    })
        
        return product_info

    # ============================================================
    #  CSV Write
    # ============================================================

    def write_row(self, writer: csv.writer, product: Dict):
        """Thread-safe write of one product row to CSV."""
        row = [
            product.get("competitor_url", ""),
            product.get("competitor_product_id", ""),
            product.get("variation_id", ""),
            product.get("category", ""),
            product.get("category_url", ""),
            product.get("brand", ""),
            product.get("comp_received_name", ""),
            product.get("comp_received_sku", ""),
            product.get("mpn", ""),
            product.get("gtin", ""),
            product.get("competitor_price", ""),
            product.get("main_image", ""),
            product.get("quantity", 0),
            product.get("group_attr_1", ""),
            product.get("group_attr_2", ""),
            product.get("status", ""),
            product.get("scraped_date", self.scraped_date),
        ]
        with self.csv_lock:
            writer.writerow(row)
        if HAS_DB_QUEUE:
            try:
                save_scraped_product("BisonOffice.com", 17927, {
                    "url": row[0], "product_id": row[1], "variant_id": row[2],
                    "category": row[3], "category_url": row[4], "brand": row[5],
                    "name": row[6], "sku": row[7], "mpn": row[8], "gtin": row[9],
                    "price": row[10], "image": row[11], "quantity": row[12],
                    "group_attr_1": row[13], "group_attr_2": row[14],
                    "status": row[15], "date_scraped": row[16]
                })
            except Exception:
                pass

    def log_failure(self, url: str, reason: str):
        """Append a failed URL to the failure CSV."""
        os.makedirs(self.failure_dir, exist_ok=True)
        failure_path = os.path.join(self.failure_dir, "bisonoffice_failures.csv")
        file_exists  = os.path.isfile(failure_path)
        with self.fail_lock:
            with open(failure_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if not file_exists:
                    w.writerow(["URL", "Reason", "Timestamp"])
                w.writerow([url, reason, self.scraped_date])

    # ============================================================
    #  Process Single Product - MODIFIED to use HTML extraction
    # ============================================================

    def process_product_data(self, product_url: str, writer: csv.writer, seen: Set[str], 
                            seen_lock: threading.Lock, stats: dict, stats_lock: threading.Lock,
                            crawl_delay: Optional[float] = None):
        """Process product data using HTML extraction as primary source, dataLayer as supplement"""
        with seen_lock:
            if product_url in seen:
                return
            seen.add(product_url)
        
        self.log(f"Processing product URL: {product_url}", "DEBUG")
        
        # Clean URL
        clean_product_url = self.clean_url(product_url)
        
        # Fetch HTML
        html_content = self.http_get(clean_product_url, is_json=False)
        
        if not html_content:
            with stats_lock:
                stats['errors'] += 1
            self.log(f"No HTML content for product {product_url}", "ERROR")
            self.log_failure(product_url, "No HTML content")
            return
        
        # Extract data from HTML (primary source)
        product_data = self.extract_from_html(html_content, clean_product_url)
        
        # Extract from dataLayer as supplement for any missing fields
        datalayer_entries = self.extract_datalayer(html_content)
        datalayer_product = self.extract_product_data(datalayer_entries)
        
        # Supplement missing data from dataLayer
        if not product_data.get('name') and datalayer_product.get('name'):
            product_data['name'] = datalayer_product['name']
        if not product_data.get('brand') and datalayer_product.get('brand'):
            product_data['brand'] = datalayer_product['brand']
        if not product_data.get('price') and datalayer_product.get('price'):
            product_data['price'] = datalayer_product['price']
        if not product_data.get('category') and datalayer_product.get('category'):
            product_data['category'] = datalayer_product['category']
        if not product_data.get('variation_id') and datalayer_product.get('variation_id'):
            product_data['variation_id'] = datalayer_product['variation_id']
        
        # Validate we have at least basic data
        if not product_data.get('name') and not product_data.get('product_id'):
            with stats_lock:
                stats['errors'] += 1
            self.log(f"Insufficient data for product {product_url}", "ERROR")
            self.log_failure(product_url, "Insufficient product data")
            return
        
        try:
            # Prepare row data matching your CSV header exactly
            row = [
                clean_product_url,                          # Ref Product URL
                product_data.get('product_id', ''),         # Ref Product ID
                product_data.get('variation_id', ''),       # Ref Variant ID
                product_data.get('category', ''),           # Ref Category
                product_data.get('category_url', ''),       # Ref Category URL
                product_data.get('brand', ''),              # Ref Brand Name
                product_data.get('name', ''),               # Ref Product Name
                product_data.get('sku', product_data.get('product_id', '')),  # Ref SKU (use product_id if not found)
                product_data.get('mpn', ''),                 # Ref MPN
                product_data.get('gtin', ''),                # Ref GTIN
                product_data.get('price', ''),               # Ref Price
                product_data.get('main_image', ''),          # Ref Main Image
                str(product_data.get('quantity', 1)),        # Ref Quantity
                '',  # Ref Group Attr 1 (not used)
                '',  # Ref Group Attr 2 (not used)
                product_data.get('status', 'In Stock'),      # Ref Status
                self.scraped_date                            # Date Scrapped
            ]
            
            with self.csv_lock:
                writer.writerow(row)
            if HAS_DB_QUEUE:
                try:
                    save_scraped_product("BisonOffice.com", 17927, {
                        "url": row[0], "product_id": row[1], "variant_id": row[2],
                        "category": row[3], "category_url": row[4], "brand": row[5],
                        "name": row[6], "sku": row[7], "mpn": row[8], "gtin": row[9],
                        "price": row[10], "image": row[11], "quantity": row[12],
                        "group_attr_1": row[13], "group_attr_2": row[14],
                        "status": row[15], "date_scraped": row[16]
                    })
                except Exception:
                    pass
            
            with stats_lock:
                stats['products_fetched'] += 1
            
            self.log(f"Fetched product {product_data.get('product_id', 'unknown')}: {product_data.get('name', '')[:50]}...", "INFO")
            
        except Exception as e:
            self.log(f"Error creating row for product {product_url}: {e}", "ERROR")
            with stats_lock:
                stats['errors'] += 1
            self.log_failure(product_url, f"Row creation error: {str(e)}")
        
        with stats_lock:
            stats['urls_processed'] += 1
        
        # Delay between requests
        if crawl_delay:
            time.sleep(crawl_delay)

    def fetch_json(self, url: str) -> Optional[dict]:
        """This method is kept for compatibility but not used"""
        return None

    # ============================================================
    #  Robots.txt — sitemap discovery
    # ============================================================

    def get_sitemap_from_robots_txt(self) -> Optional[str]:
        """Fetch robots.txt and extract the Sitemap directive URL (with ?bo=0)."""
        robots_url = self.append_bo_param(f"{self.curr_url}/robots.txt")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        try:
            response = requests.get(robots_url, headers=headers, timeout=10, verify=True)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.log(f"Error fetching robots.txt: {e}", "ERROR")
            return None

        for line in response.text.splitlines():
            if line.lower().startswith("sitemap:"):
                sitemap_url = line.split(":", 1)[1].strip()
                sitemap_url = self.append_bo_param(sitemap_url)
                self.log(f"Extracted Sitemap URL: {sitemap_url}", "INFO")
                return sitemap_url

        self.log("No Sitemap directive found in robots.txt", "WARNING")
        return None

    # ============================================================
    #  Run — main orchestrator (UNCHANGED)
    # ============================================================
    
    def run(self):
        sitemap_url = self.get_sitemap_from_robots_txt()

        self.log("=" * 60)
        self.log("BisonOffice Parallel Scraper")
        self.log(f"Timestamp:            {self.scraped_date}")
        self.log(f"Base URL:             {self.curr_url}")
        self.log(f"Sitemap Index:        {sitemap_url}")
        self.log(f"Sitemap Offset:       {self.sitemap_offset}")
        self.log(f"Max Sitemaps:         {self.max_sitemaps if self.max_sitemaps > 0 else 'All'}")
        self.log(f"Max URLs per Sitemap: {self.max_urls_per_sitemap if self.max_urls_per_sitemap > 0 else 'All'}")
        self.log(f"Max Workers:          {self.max_workers}")
        self.log(f"Request Delay:        {self.request_delay}s")
        self.log("=" * 60)

        if not sitemap_url:
            # Fall back to the known sitemap index URL
            sitemap_url = self.append_bo_param(f"{self.curr_url}/sitemap_index.xml")
            self.log(f"Falling back to default sitemap: {sitemap_url}", "WARNING")

        all_sitemaps = self.get_child_sitemaps(sitemap_url)

        if not all_sitemaps:
            self.log("No sitemaps found. Exiting.", "ERROR")
            sys.exit(1)

        if self.sitemap_offset >= len(all_sitemaps):
            self.log(
                f"Offset {self.sitemap_offset} >= total ({len(all_sitemaps)}), nothing to do.",
                "WARNING",
            )
            sys.exit(0)

        end_idx = self.sitemap_offset + self.max_sitemaps if self.max_sitemaps > 0 else len(all_sitemaps)
        sitemaps_to_run = all_sitemaps[self.sitemap_offset:end_idx]

        self.log(f"Total sitemaps found:  {len(all_sitemaps)}")
        self.log(f"Sitemaps to process:   {len(sitemaps_to_run)}")

        os.makedirs(self.output_dir, exist_ok=True)
        output_path = os.path.join(self.output_dir, self.output_csv)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(self.csv_header)

            self.all_product_urls: list = []  # track URLs across all sitemaps

            for sm_url in sitemaps_to_run:
                self.stats["sitemaps_processed"] += 1
                self.log(
                    f"Sitemap {self.stats['sitemaps_processed']}/{len(sitemaps_to_run)}: {sm_url}"
                )

                urls = self.get_product_urls(sm_url)
                if not urls:
                    continue
                self.all_product_urls.extend(urls)  # collect for URL tracking

                if self.max_urls_per_sitemap > 0:
                    self.log(f"Limiting to {self.max_urls_per_sitemap} of {len(urls)} URLs")
                    urls = urls[:self.max_urls_per_sitemap]
                else:
                    self.log(f"Found {len(urls)} product URLs")

                if HAS_DB_QUEUE:
                    init_c = len(urls)
                    urls = filter_pending_urls("BisonOffice.com", urls, competitor_id=17927)
                    self.log(f"MySQL DB Queue check: {init_c - len(urls):,} URLs already completed in DB. Remaining pending: {len(urls):,}")

                # Create locks for thread-safe operations
                seen_lock = threading.Lock()
                stats_lock = threading.Lock()
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(
                            self.process_product_data,
                            url,
                            writer,
                            self.seen,
                            seen_lock,
                            self.stats,
                            stats_lock,
                            self.request_delay
                        )
                        for url in urls
                    ]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            self.log(f"Thread error: {e}", "ERROR")
                            self.stats["errors"] += 1

                gc.collect()

        self.log("=" * 60)
        self.log("SCRAPING COMPLETE")
        self.log(f"  Sitemaps processed:  {self.stats['sitemaps_processed']}")
        self.log(f"  URLs processed:      {self.stats['urls_processed']}")
        self.log(f"  Products saved:      {self.stats['products_fetched']}")
        self.log(f"  Errors:              {self.stats['errors']}")
        if self.stats["urls_processed"] > 0:
            rate = self.stats["products_fetched"] / self.stats["urls_processed"] * 100
            self.log(f"  Success rate:        {rate:.1f}%")
        self.log(f"  Output:              {output_path}")
        self.log("=" * 60)


# ============================================================
#  Entry point
# ============================================================

if __name__ == "__main__":
    if not os.getenv("CURR_URL"):
        sys.stderr.write("[ERROR] CURR_URL environment variable is required\n")
        sys.exit(1)

    _bison_scraper = BisonofficeScraper()
    _bison_scraper.run()

    # Save per-chunk URL list for artifact upload (merged in YML merge job)
    if hasattr(_bison_scraper, 'all_product_urls') and _bison_scraper.all_product_urls:
        try:
            _url_f = f"url_list_chunk_{_bison_scraper.sitemap_offset}.txt"
            with open(_url_f, "w", encoding="utf-8") as _uf:
                _uf.write("\n".join(_bison_scraper.all_product_urls))
            _bison_scraper.log(f"[URL-LIST] Saved {len(_bison_scraper.all_product_urls)} URLs to {_url_f}")
        except Exception as _exc:
            import sys as _sys; _sys.stderr.write(f"[URL-LIST] Warning: {_exc}\n")

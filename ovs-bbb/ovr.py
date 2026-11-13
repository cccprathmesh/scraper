import os
import csv
import time
import sys
import gc
import threading
import requests
import re
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add repository root for shared imports like db_queue
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

try:
    from db_queue import filter_pending_urls, save_scraped_product
    HAS_DB_QUEUE = True
except Exception:
    HAS_DB_QUEUE = False

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "").rstrip("/")
API_BASE_URL = os.getenv("API_BASE_URL", "").rstrip("/")
BBB_API_BASE_URL = os.getenv("BBB_API_BASE_URL", "").rstrip("/")
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.0"))

OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

# ================= HTTP SESSION =================

session = requests.Session()
# Add default headers to session for all requests
session.headers.update({
    # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    # "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
})

def http_get(url: str, is_json: bool = False) -> Optional[str]:
    """HTTP GET request with different headers for sitemap vs API requests"""
    for attempt in range(3):
        try:
            if is_json:
                # For API/JSON requests, override with JSON-specific headers
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": f"{CURR_URL}/",
                    "X-Requested-With": "XMLHttpRequest",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                }
                r = session.get(url, headers=headers, timeout=15, verify=True)
            else:
                # For sitemap/XML requests, use default session headers (already set)
                r = session.get(url, timeout=15, verify=True)
                
            if r.status_code == 200:
                log(f"Success fetching {url}", "DEBUG")
                return r.text
            else:
                log(f"Status {r.status_code} for {url}", "WARNING")
                if r.status_code == 429:  # Rate limited
                    time.sleep(5)
        except requests.exceptions.Timeout:
            log(f"Timeout on attempt {attempt+1} for {url}", "WARNING")
            time.sleep(2)
        except Exception as e:
            log(f"Attempt {attempt+1} failed for {url}: {type(e).__name__}", "WARNING")
            time.sleep(1)
    return None

def fetch_json(url: str) -> Optional[dict]:
    """Fetch JSON data with proper headers"""
    try:
        # Headers specifically for JSON/API requests
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": f"{CURR_URL}/",
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        
        r = session.get(url, headers=headers, timeout=15, verify=True)
        if r.status_code == 200:
            return r.json()
        else:
            log(f"JSON fetch failed: {r.status_code} for {url}", "WARNING")
            return None
    except json.JSONDecodeError as e:
        log(f"JSON decode error for {url}: {e}", "ERROR")
        return None
    except Exception as e:
        log(f"Error fetching JSON from {url}: {e}", "ERROR")
        return None

# ================= SITEMAP PROCESSING =================

def load_xml(url: str) -> Optional[ET.Element]:
    """Load XML with appropriate headers"""
    # For GitHub Actions, we might need longer timeout for sitemap
    data = None
    for attempt in range(3):
        try:
            # Use http_get with is_json=False for sitemap requests
            data = http_get(url, is_json=False)
            if data:
                break
        except Exception as e:
            log(f"Attempt {attempt+1} for sitemap failed: {e}", "WARNING")
            time.sleep(2)
    
    if not data:
        log(f"Failed to load XML from {url}", "ERROR")
        return None
        
    try:
        # Clean XML if needed
        if "<?xml" not in data[:100]:
            data = '<?xml version="1.0" encoding="UTF-8"?>\n' + data
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parsing failed for {url}: {e}", "ERROR")
        # Try to extract URLs with regex
        try:
            # Create a dummy element
            root = ET.Element("urlset")
            urls = re.findall(r'<loc>(https?://[^<]+)</loc>', data)
            for url_text in urls:
                url_elem = ET.SubElement(root, "url")
                loc_elem = ET.SubElement(url_elem, "loc")
                loc_elem.text = url_text
            return root
        except Exception as e2:
            log(f"Regex extraction also failed: {e2}", "ERROR")
            return None

def extract_product_id(product_url: str) -> Optional[str]:
    """Extract product ID from Overstock URL"""
    patterns = [
        r'/(\d+)/product\.html',      
        r'/product/(\d+)/',
        r'/catalog/(\d+)/',
        r'/[\w-]+/(\d+)\.html',
        r'/(\d+)\.html',
        r'[?&]IID=(\d+)',
    ]

    for pattern in patterns:
        match = re.search(pattern, product_url)
        if match:
            product_id = match.group(1)
            log(f"Extracted product ID {product_id} from {product_url}", "DEBUG")
            return product_id

    log(f"No product ID found in URL: {product_url}", "WARNING")
    return None

# ================= PRODUCT PROCESSING =================

csv_lock = threading.Lock()

def normalize_image_url(url: str) -> str:
    """Normalize image URL for Overstock"""
    if not url:
        return ""
    
    if url.startswith("//"):
        return "https:" + url
    elif url.startswith("/"):
        return f"{CURR_URL}{url}"
    elif not url.startswith("http"):
        return f"https://ak1.ostkcdn.com{url}" if 'ostkcdn.com' not in url else f"https://{url}"
    
    return url

from typing import Dict


def fetch_json_bbb(api_url: str) -> Optional[dict]:
    response = requests.get(api_url, timeout=10)
    response.raise_for_status()
    return response.json()

def extract_bbb_data(variant_data: dict) -> Dict[str, Any]:
    """
    Extract data from BBB API response based on actual data structure
    """
    try:
        if not variant_data:
            return {}
        
        # Basic extraction
        result = {
            'BBB_SKU': variant_data.get('modelNumber'),
            'BBB_ModelNumber': variant_data.get('modelNumber'),
            'BBB_OptionId': variant_data.get('optionId'),
            'BBB_Description': variant_data.get('description'),
            'BBB_Dimensions': None,
            'BBB_Attributes': None,
            'BBB_Attributes_Count': 0,
            'BBB_AttributeIcons_Count': 0,
            'BBB_AttributeIcons_URLs': None,
            'BBB_AttributeIcons_Names': None
        }
        
        # Extract dimensions with units
        dims = variant_data.get('assembledDimensions', {})
        if dims:
            length = dims.get('length', '')
            width = dims.get('width', '')
            height = dims.get('height', '')
            length_units = dims.get('lengthUnits', '')
            width_units = dims.get('widthUnits', '')
            height_units = dims.get('heightUnits', '')
            
            # Format: LxWxH with units
            if length and width:
                if height and height != 0:
                    result['BBB_Dimensions'] = f"{length}{length_units} x {width}{width_units} x {height}{height_units}"
                else:
                    result['BBB_Dimensions'] = f"{length}{length_units} x {width}{width_units}"
        
        # Extract attributes as string
        attributes = variant_data.get('attributes', [])
        if attributes:
            attr_list = []
            for attr in attributes:
                name = attr.get('name', '').strip()
                value = attr.get('value', '').strip()
                if name and value:
                    attr_list.append(f"{name}: {value}")
            
            if attr_list:
                result['BBB_Attributes'] = " | ".join(attr_list)
            result['BBB_Attributes_Count'] = len(attributes)
        
        # Extract attribute icons
        icons = variant_data.get('attributeIcons', [])
        result['BBB_AttributeIcons_Count'] = len(icons)
        
        # Extract icon URLs and names
        if icons:
            icon_urls = []
            icon_names = []
            for icon in icons:
                url = icon.get('url', '')
                attribute_name = icon.get('attributeName', '')
                attribute_value = icon.get('attributeValue', '')
                
                if url:
                    icon_urls.append(url)
                
                if attribute_name:
                    icon_names.append(f"{attribute_name}: {attribute_value}")
            
            if icon_urls:
                result['BBB_AttributeIcons_URLs'] = " | ".join(icon_urls)
            
            if icon_names:
                result['BBB_AttributeIcons_Names'] = " | ".join(icon_names)
        
        return result
        
    except Exception as e:
        logger.error(f"Error extracting BBB data: {e}")
        return {}

def extract_overstock_data(product_data: dict, product_url: str) -> List[Dict]:
    try:
        if not product_data or not isinstance(product_data, dict):
            log(f"Invalid product_data: {type(product_data)}", "ERROR")
            return []

        product = product_data  # API already returns product root
        
        # Safe getter function with fallback
        def safe_get(data, keys, default=''):
            if not data:
                return default
            
            for key in keys if isinstance(keys, list) else [keys]:
                if isinstance(data, dict):
                    data = data.get(key)
                elif isinstance(data, list) and isinstance(key, int) and 0 <= key < len(data):
                    data = data[key]
                else:
                    return default
                
                if data is None:
                    return default
            
            return data or default
        
        # ---------- Common data ----------
        product_id = str(safe_get(product, 'productId', ''))
        name = str(safe_get(product, 'name', '')).strip()

        brand_obj = safe_get(product, 'brand', {})
        brand = safe_get(brand_obj, 'name', '') if isinstance(brand_obj, dict) else str(brand_obj)

        # ---------- SKU / MPN ----------
        details = safe_get(product, 'details', {})
        sku = safe_get(details, 'sku', '')

        specs = safe_get(product, 'specifications', {})
        group_attr_1 = safe_get(specs, ['Color', 0], '')
        mpn = safe_get(specs, ['Model Number', 0], '') 

        # ---------- Category ----------
        breadcrumbs = safe_get(product, 'breadcrumbs', [])
        category = ''
        category_url = ''

        bbb_url = ''
        bbb_sku = ''
        bbb_modelnumber = ''
        bbb_optionid = ''
        bbb_description = ''
        bbb_dimensions = ''
        bbb_attributes = ''

        if breadcrumbs and isinstance(breadcrumbs, list) and len(breadcrumbs) > 0:
            last = breadcrumbs[-1]
            if isinstance(last, dict):
                category = safe_get(last, 'label', '')
                url = safe_get(last, 'url', '')
                last_url = url.lstrip('/') if url else ''
                category_url = f"{CURR_URL}/{last_url}" if last_url else ''

        # ---------- Images ----------
        images = safe_get(product, 'images', [])
        image_data = safe_get(product, 'imageData', {})
        
        main_image = ''
        if images and isinstance(images, list) and len(images) > 0:
            first_image = images[0]
            if isinstance(first_image, dict):
                main_image = safe_get(first_image, 'url', '')
        if not main_image:
            main_image = safe_get(image_data, 'productImageUrl', '')
        
        all_products = []
        
        # ---------- Check for multiple variations ----------
        multiple_variations = safe_get(product, 'multipleInStockVariations', False)
        variations = safe_get(product, 'variations', [])
        
        if multiple_variations and isinstance(variations, list) and len(variations) > 1:
            # Process each variation
            for variation in variations:
                if not isinstance(variation, dict):
                    continue
                    
                variation_id = safe_get(variation, 'variationId', '')
                full_sku = safe_get(variation, 'fullSku', '')
                
                # Create variation-specific URL
                if variation_id:
                    variation_url = f"{product_url}?option={variation_id}"
                    try:
                        bbb_api_url = f"{BBB_API_BASE_URL}/{variation_id}"
                        bbb_data = fetch_json_bbb(bbb_api_url)
                        variant_info = extract_bbb_data(bbb_data)
                        bbb_sku = variant_info.get('BBB_SKU', '')
                        bbb_modelnumber = variant_info.get('BBB_ModelNumber', '')
                        bbb_optionid = variant_info.get('BBB_OptionId', '')
                        bbb_description = variant_info.get('BBB_Description', '')
                        bbb_dimensions = variant_info.get('BBB_Dimensions', '')
                        bbb_attributes = variant_info.get('BBB_Attributes', '')
                    except Exception as e:
                        log(f"fetch bbb data failed : {bbb_api_url}", "ERROR")

                else:
                    variation_url = product_url
                
                # ---------- Price ----------
                prices = safe_get(variation, 'prices', {})
                price = ''
                
                # Try salePrice first, then basePrice, then product selectedPrice
                if isinstance(prices, dict):
                    sale_price = safe_get(prices, ['salePrice', 'amount'], '')
                    if sale_price:
                        price = sale_price
                    else:
                        base_price = safe_get(prices, ['basePrice', 'amount'], '')
                        if base_price:
                            price = base_price
                
                if not price:
                    selected_price = safe_get(product, ['selectedPrice', 'amount'], '')
                    if selected_price:
                        price = selected_price
                
                variant_name = safe_get(variation, 'name', '')
                if variant_name:
                    name = variant_name

                variant_image = safe_get(variation, 'imageUrl', '')
                if variant_image:
                    main_image = variant_image
                    
                # ---------- Quantity & Status ----------
                quantity = safe_get(variation, 'quantityAvailable', '')
                
                # Check both possible status fields
                status_value = safe_get(variation, 'status', '')
                sellable_status = safe_get(variation, 'sellableStatus', '')
                if status_value == 'SELLABLE' or sellable_status == 'SELLABLE':
                    status = 'In Stock'
                else:
                    status = 'Out of Stock'
                
                # ---------- Group Attributes ----------
                variation_desc = safe_get(variation, 'description', '')
                if variation_desc:
                    group_attr_1 = variation_desc
                elif isinstance(specs.get('Color'), list) and len(specs['Color']) > 0:
                    group_attr_1 = specs['Color'][0]
                else:
                    group_attr_1 = ''
                    
                group_attr_2 = ''
                if isinstance(specs.get('Material'), list) and len(specs['Material']) > 0:
                    group_attr_2 = specs['Material'][0]
                elif isinstance(specs.get('Top Material'), list) and len(specs['Top Material']) > 0:
                    group_attr_2 = specs['Top Material'][0]

                bbb_url = variation_url.replace("www.overstock.com","www.bedbathandbeyond.com")
                product_info = {
                    'product_id': product_id,
                    'name': name,
                    'brand': brand,
                    'price': price,
                    'main_image': main_image,
                    'sku': full_sku if full_sku else sku,
                    'mpn': mpn,
                    'category': category,
                    'category_url': category_url,
                    'quantity': quantity,
                    'status': status,
                    'variation_id': variation_id,
                    'group_attr_1': group_attr_1,
                    'group_attr_2': group_attr_2,
                    'product_url': variation_url,
                    'bbb_url' : bbb_url,
                    'bbb_sku' : bbb_sku,
                    'bbb_modelnumber' : bbb_modelnumber,
                    'bbb_optionid' : bbb_optionid,
                    'bbb_description' : bbb_description,
                    'bbb_dimensions' : bbb_dimensions,
                    'bbb_attributes' : bbb_attributes
                }
                
                all_products.append(product_info)
            
            return all_products if all_products else []
            
        else:
            # Single product or no variations
            variation_id = ''
            quantity = ''
            price = ''
            status = 'Out of Stock'
            group_attr_1_current = group_attr_1
            
            # Get data from first variation if exists
            if isinstance(variations, list) and len(variations) > 0:
                first_variation = variations[0]
                if isinstance(first_variation, dict):
                    variation_id = safe_get(first_variation, 'variationId', '')
                    quantity = safe_get(first_variation, 'quantityAvailable', '')
                    variation_desc = safe_get(first_variation, 'description', '')
                    if variation_desc:
                        group_attr_1_current = variation_desc
                    
                    # Get price from variation if available
                    prices = safe_get(first_variation, 'prices', {})
                    price = ''
                    if isinstance(prices, dict):
                        sale_price = safe_get(prices, ['salePrice', 'amount'], '')
                        if sale_price:
                            price = sale_price
                        else:
                            base_price = safe_get(prices, ['basePrice', 'amount'], '')
                            if base_price:
                                price = base_price
                    
                    # Check status
                    status_value = safe_get(first_variation, 'status', '')
                    sellable_status = safe_get(first_variation, 'sellableStatus', '')
                    if status_value == 'SELLABLE' or sellable_status == 'SELLABLE':
                        status = 'In Stock'
                    else:
                        status = 'Out of Stock'

                    if variation_id:
                        product_url = f"{product_url}?option={variation_id}"
                        try:
                            bbb_api_url = f"{BBB_API_BASE_URL}/{variation_id}"
                            bbb_data = fetch_json_bbb(bbb_api_url)
                            variant_info = extract_bbb_data(bbb_data)
                            bbb_sku = variant_info.get('BBB_SKU', '')
                            bbb_modelnumber = variant_info.get('BBB_ModelNumber', '')
                            bbb_optionid = variant_info.get('BBB_OptionId', '')
                            bbb_description = variant_info.get('BBB_Description', '')
                            bbb_dimensions = variant_info.get('BBB_Dimensions', '')
                            bbb_attributes = variant_info.get('BBB_Attributes', '')
                        except Exception as e:
                            log(f"fetch bbb data failed : {bbb_api_url}", "ERROR")
            
            # If price not found in variation, try product level
            if not price:
                selected_price = safe_get(product, ['selectedPrice', 'amount'], '')
                if selected_price:
                    price = selected_price
                else:
                    # Try lowestVariationPrice as fallback
                    lowest_price = safe_get(product, ['lowestVariationPrice', 'amount'], '')
                    if lowest_price:
                        price = lowest_price
            
            # If no variations, check product-level stock
            if not variation_id:
                in_stock = safe_get(product, 'inStock', False)
                is_sellable = safe_get(product, 'isSellable', False)
                sellable_status = safe_get(product, 'sellableStatus', '')
                
                if in_stock or is_sellable or sellable_status == 'SELLABLE':
                    status = 'In Stock'
                else:
                    status = 'Out of Stock'
            
            # ---------- Group Attributes ----------
            group_attr_2 = ''
            if isinstance(specs.get('Material'), list) and len(specs['Material']) > 0:
                group_attr_2 = specs['Material'][0]
            elif isinstance(specs.get('Top Material'), list) and len(specs['Top Material']) > 0:
                group_attr_2 = specs['Top Material'][0]
            bbb_url = product_url.replace("www.overstock.com","www.bedbathandbeyond.com")
            product_info = {
                'product_id': product_id,
                'name': name,
                'brand': brand,
                'price': price,
                'main_image': main_image,
                'sku': sku,
                'mpn': mpn,
                'category': category,
                'category_url': category_url,
                'quantity': quantity,
                'status': status,
                'variation_id': variation_id,
                'group_attr_1': group_attr_1_current,
                'group_attr_2': group_attr_2,
                'product_url': product_url,
                'bbb_url' : bbb_url,
                'bbb_sku' : bbb_sku,
                'bbb_modelnumber' : bbb_modelnumber,
                'bbb_optionid' : bbb_optionid,
                'bbb_description' : bbb_description,
                'bbb_dimensions' : bbb_dimensions,
                'bbb_attributes' : bbb_attributes
            }
            
            return [product_info]

    except Exception as e:
        log(f"Error extracting Overstock data: {str(e)} - Product data: {product_data.get('productId', 'Unknown') if isinstance(product_data, dict) else 'Invalid'}", "ERROR")
        return []


def process_product_data(product_url: str, writer, seen: set, stats: dict):
    """Process a single Overstock product URL - handles multiple variations"""
    if product_url in seen:
        return
    seen.add(product_url)
    
    log(f"Processing product URL: {product_url}", "DEBUG")
    
    # Extract product ID
    product_id = extract_product_id(product_url)
    if not product_id:
        stats['errors'] += 1
        log(f"No product ID found for URL: {product_url}", "ERROR")
        return
    
    api_endpoints = [
        f"{API_BASE_URL}/{product_id}",
    ]
    
    data = None
    for api_url in api_endpoints:
        log(f"Trying API endpoint: {api_url}", "DEBUG")
        data = fetch_json(api_url)  # This uses JSON-specific headers
        if data:
            break
        time.sleep(0.5)
    
    if not data:
        # Try direct product page scraping as fallback
        log(f"API failed, trying direct page for {product_id}", "WARNING")
        page_content = http_get(product_url, is_json=False)
        if page_content:
            # Look for JSON-LD or product data in page
            json_ld_pattern = r'<script type="application/ld\+json">(.*?)</script>'
            matches = re.findall(json_ld_pattern, page_content, re.DOTALL)
            if matches:
                try:
                    data = json.loads(matches[0])
                except:
                    pass
    
    if not data:
        stats['errors'] += 1
        log(f"No data found for product {product_id}", "ERROR")
        return
    
    # Extract data from response - now returns a list of variations
    products_list = extract_overstock_data(data, product_url)
    
    if not products_list:
        stats['errors'] += 1
        log(f"No variations found for product {product_id}", "ERROR")
        return
    
    
    
    for product_info in products_list:
        if not product_info.get('product_id'):
            continue
            
        try:
            # Prepare row data for each variation
            row = [
                product_info['product_url'],  # Variation-specific URL
                product_info['product_id'],  # Ref Product ID
                product_info['variation_id'],  # Ref Varient ID
                product_info['category'],  # Ref Category
                product_info['category_url'],  # Ref Category URL
                product_info['brand'],  # Ref Brand Name
                product_info['name'],  # Ref Product Name
                product_info['sku'],  # Ref SKU
                product_info['mpn'],  # Ref MPN
                '',  # Ref GTIN (empty for now)
                product_info['price'],  # Ref Price
                normalize_image_url(product_info['main_image']),  # Ref Main Image
                product_info['quantity'],  # Ref Quantity
                product_info['group_attr_1'],  # Ref Group Attr 1
                product_info['group_attr_2'],  # Ref Group Attr 2
                product_info['status'],  # Ref Status
                product_info['bbb_url'],
                product_info['bbb_sku'],
                product_info['bbb_modelnumber'],
                product_info['bbb_optionid'],
                product_info['bbb_description'],
                product_info['bbb_dimensions'],
                product_info['bbb_attributes'],
                SCRAPED_DATE  # Date Scrapped
            ]
            
            with csv_lock:
                writer.writerow(row)

            if HAS_DB_QUEUE:
                try:
                    save_scraped_product("Overstock", 16167, {
                        "url": row[0], "product_id": row[1], "variant_id": row[2],
                        "category": row[3], "category_url": row[4], "brand": row[5],
                        "name": row[6], "sku": row[7], "mpn": row[8], "gtin": row[9],
                        "price": row[10], "image": row[11], "quantity": row[12],
                        "group_attr_1": row[13], "group_attr_2": row[14],
                        "status": row[15], "date_scraped": row[23]
                    })
                except Exception:
                    pass

            stats['products_fetched'] += 1
            
            log(f"Fetched product {product_info['product_id']}: {product_info['name'][:50]}...", "INFO")
            
        except Exception as e:
            log(f"Error creating row for product {product_id}: {e}", "ERROR")
            stats['errors'] += 1
        
    
    # Respect request delay
    time.sleep(REQUEST_DELAY)
    stats['urls_processed'] += 1

# ================= MAIN =================
def get_sitemap_from_robots_txt():
    try:
        # Construct robots.txt URL
        robots_url = f"{CURR_URL}/robots.txt"

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        response = requests.get(robots_url, headers=headers, timeout=10, verify=True)
        
        # Fetch the robots.txt content
        # response = session.get(robots_url, timeout=15, verify=True)
        # response.raise_for_status()
     
        # Extract Sitemap URL
        sitemap_url = None
        for line in response.text.split('\n'):
            if line.lower().startswith('sitemap:'):
                sitemap_url = line.split(':', 1)[1].strip()
                break
        
        if sitemap_url:
            print(f"Extracted Sitemap URL: {sitemap_url}")
            return sitemap_url
        else:
            print("No Sitemap directive found in robots.txt")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching robots.txt: {e}")
        return None

def main():
    sitemap = get_sitemap_from_robots_txt()
    log("=" * 60)
    log("Overstock Parallel Scraper")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Base URL: {CURR_URL}")
    log(f"API Base URL: {API_BASE_URL}")
    log(f"Sitemap Index: {sitemap}")
    log(f"Sitemap Offset: {SITEMAP_OFFSET}")
    log(f"Max Sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Max Workers: {MAX_WORKERS}")
    log(f"Request Delay: {REQUEST_DELAY}s")
    log("=" * 60)
    
    # Load sitemap index - NO HEADERS for sitemap
    log(f"Loading sitemap index from {sitemap}")
    index = load_xml(sitemap)
    if index is None:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)
    
    # Extract sitemap URLs
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = []
    
    # Try different XML structures
    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
        if elements:
            sitemaps = [e.text.strip() for e in elements if e.text]
            break
    
    # If still no sitemaps, try regex
    if not sitemaps:
        log("No sitemaps found with XML parsing, trying regex", "WARNING")
        # Try common Overstock sitemap patterns
        sitemaps = [
            "https://www.overstock.com/sitemap_products_1.xml",
            "https://www.overstock.com/sitemap_products_2.xml",
            "https://www.overstock.com/sitemap.xml",
        ]
    
    # Apply offset and limit
    if SITEMAP_OFFSET >= len(sitemaps):
        log(f"Offset {SITEMAP_OFFSET} exceeds total sitemaps ({len(sitemaps)})", "WARNING")
        sys.exit(0)
    
    end_index = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemaps)
    sitemaps_to_process = sitemaps[SITEMAP_OFFSET:end_index]
    
    log(f"Total sitemaps found: {len(sitemaps)}")
    log(f"Sitemaps to process: {len(sitemaps_to_process)}")
    
    # Initialize CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow([
            "Ref Product URL",
            "Ref Product ID",
            "Ref Varient ID",
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
            'BBB URL',
            'BBB SKU',
            'BBB ModelNumber',
            'BBB OptionId',
            'BBB Description',
            'BBB Dimensions',
            'BBB Attributes',
            "Date Scrapped"
        ])
        
        # Initialize tracking
        seen = set()
        stats = {
            'sitemaps_processed': 0,
            'urls_processed': 0,
            'products_fetched': 0,
            'errors': 0
        }
        
        # Process each sitemap
        for sitemap_url in sitemaps_to_process:
            stats['sitemaps_processed'] += 1
            log(f"Processing sitemap {stats['sitemaps_processed']}/{len(sitemaps_to_process)}: {sitemap_url}")
            
            # Load product sitemap - NO HEADERS for sitemap
            xml = load_xml(sitemap_url)
            if not xml:
                log(f"Failed to load sitemap: {sitemap_url}", "ERROR")
                continue
            
            # Extract product URLs - Overstock product URLs typically contain /product/ or /catalog/
            urls = []
            for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
                elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
                if elements:
                    urls = [
                        e.text.strip()
                        for e in elements
                        if e.text
                        and (
                            '/product/' in e.text
                            or '/product.html' in e.text
                            or '/catalog/' in e.text
                            or 'IID=' in e.text
                        )
                    ]
                    if urls:
                        break
            
            if not urls:
                log(f"No product URLs found in sitemap: {sitemap_url}", "WARNING")
                continue

            if HAS_DB_QUEUE:
                init_c = len(urls)
                urls = filter_pending_urls("Overstock", urls, competitor_id=16167)
                log(f"MySQL DB Queue check: {init_c - len(urls):,} URLs already completed in DB. Remaining pending: {len(urls):,}")
                if not urls:
                    log("All URLs in this sitemap already scraped in database. Skipping.")
                    continue
            
            # Apply URL limit
            if MAX_URLS_PER_SITEMAP > 0:
                original_count = len(urls)
                urls = urls[:MAX_URLS_PER_SITEMAP]
                log(f"Limited to {len(urls)} out of {original_count} URLs")
            else:
                log(f"Found {len(urls)} product URLs in this sitemap")
            
            # Process URLs in parallel
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(process_product_data, url, writer, seen, stats)
                    for url in urls
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Error in thread execution: {e}", "ERROR")
                        stats['errors'] += 1
            
            # Clean up memory
            gc.collect()
    
    # Print statistics
    log("=" * 60)
    log("SCRAPING STATISTICS")
    log("=" * 60)
    log(f"Sitemaps processed: {stats['sitemaps_processed']}")
    log(f"URLs processed: {stats['urls_processed']}")
    log(f"Products successfully fetched: {stats['products_fetched']}")
    log(f"Errors encountered: {stats['errors']}")
    if stats['urls_processed'] > 0:
        success_rate = (stats['products_fetched'] / stats['urls_processed']) * 100
        log(f"Success rate: {success_rate:.1f}%")
    log("=" * 60)
    log(f"Completed: {OUTPUT_CSV}")
    log("=" * 60)

if __name__ == "__main__":
    # Suppress SSL warnings
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Validate environment variables
    if not CURR_URL:
        log("Error: CURR_URL environment variable is required", "ERROR")
        sys.exit(1)
    
    main()
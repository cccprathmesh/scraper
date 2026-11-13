import gzip
import xml.etree.ElementTree as ET
import json
import re
import csv
import sqlite3
from datetime import datetime
from urllib.parse import urlparse, urljoin
from scrapy import Spider, Request
import sys
from pathlib import Path
import time
import os
import logging

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Add repo root for db_queue
REPO_ROOT = str(Path(__file__).parent.parent.parent)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

try:
    from db_queue import filter_pending_urls, save_scraped_product
    HAS_DB_QUEUE = True
except Exception:
    HAS_DB_QUEUE = False

try:
    from utils.sitemap_processor import SitemapProcessor
except ImportError:
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class ProductFetcher(Spider):
    name = 'product'
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Get verbose flag
        self.verbose = kwargs.get('verbose', False)
        
        try:
            self.logger.setLevel(logging.INFO if self.verbose else logging.WARNING)
            self.logger.propagate = True
        except:
            self.logger = logging.getLogger('product')
            self.logger.setLevel(logging.INFO if self.verbose else logging.WARNING)
            self.logger.propagate = True

        # Ashley mode flags
        self.is_ashley = kwargs.get('is_ashley', False)
        self.ashley_urls = kwargs.get('ashley_urls', [])
        
        # CHUNK MODE parameters
        self.chunk_mode = kwargs.get('chunk_mode', False)
        self.chunk_id = int(kwargs.get('chunk_id', 0))
        self.total_chunks = int(kwargs.get('total_chunks', 1))
        self.chunk_size = int(kwargs.get('chunk_size', 0))

        self.website_url = kwargs.get('website_url')
        if not self.website_url:
            raise ValueError("website_url parameter is required")
        
        self.sitemap_offset = int(kwargs.get('sitemap_offset', 0))
        self.max_sitemaps = int(kwargs.get('max_sitemaps', 0))
        self.max_urls_per_sitemap = int(kwargs.get('max_urls_per_sitemap', 0))
        self.job_id = kwargs.get('job_id', datetime.now().strftime('%Y%m%d_%H%M%S'))
        self.output_dir = kwargs.get('output_dir', 'output')
        
        parsed_url = urlparse(self.website_url)
        self.domain = parsed_url.netloc
        self.base_domain = '.'.join(self.domain.split('.')[-2:]).replace('.', '_')
        
        # URL state tracking:
        # - queued_or_processing_urls: already scheduled and not yet finished
        # - processed_successfully_urls: completed without request failure
        self.queued_or_processing_urls = set()
        self.processed_successfully_urls = set()
        self.success_db_conn = None
        self.success_db_write_counter = 0
        
        # PROGRESS TRACKING
        self.start_time = time.time()
        self.total_urls_found = 0
        self.processed_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.failed_requests = {}
        self.unscraped_requests = {}
        self.last_log_time = self.start_time
        self.log_interval = 30  # Log progress every 30 seconds
        self.sitemap_urls_count = {}  # Track URLs per sitemap
        self._hypernova_cache = {}
        
        self.logger.info(f"📁 Starting job {self.job_id} - chunk {self.chunk_id}")

        # Persistent cross-job dedup store:
        # once URL is scraped successfully, skip it in future jobs.
        self._init_success_store()
        
        # Only process sitemaps if not in Ashley mode
        if not self.is_ashley:
            try:
                sitemap_processor = SitemapProcessor()
                
                self.sitemap_index_url = sitemap_processor.get_sitemap_from_robots(self.website_url)
                self.logger.info(f"📍 Found sitemap index: {self.sitemap_index_url}")
                
                self.all_sitemaps = sitemap_processor.extract_all_sitemaps(self.sitemap_index_url)
                self.logger.info(f"📚 Total sitemaps discovered: {len(self.all_sitemaps)}")
                
                self.sitemap_chunk = sitemap_processor.get_sitemap_chunks(
                    self.all_sitemaps, 
                    self.sitemap_offset, 
                    self.max_sitemaps
                )
                
                self.logger.info(f"🎯 This job will process {len(self.sitemap_chunk)} sitemaps")
                
            except Exception as e:
                self.logger.error(f"❌ Failed to discover sitemap: {e}")
                raise
    
    def get_headers(self):
        """Get headers for Ashley requests"""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        }
    
    def normalize_url(self, url):
        """Normalize URL for consistent deduplication"""
        if not url:
            return url
        # Normalize scheme/netloc, remove trailing slash and fragment, sort query params
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        query = parsed.query
        if query:
            query_parts = [p for p in query.split('&') if p]
            query_parts.sort()
            query = '&'.join(query_parts)
        normalized = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"
        if query:
            normalized = f"{normalized}?{query}"
        return normalized

    def _should_schedule_url(self, url: str) -> bool:
        normalized_url = self.normalize_url(url)
        if not normalized_url:
            return False

        # Skip if already completed successfully
        if normalized_url in self.processed_successfully_urls:
            self.skipped_count += 1
            if self.verbose:
                self.logger.info(f"⏭️ URL already scraped successfully: {normalized_url}")
            return False

        # Skip if already queued/in-progress
        if normalized_url in self.queued_or_processing_urls:
            self.skipped_count += 1
            if self.verbose:
                self.logger.info(f"⏭️ URL already queued/in-progress: {normalized_url}")
            return False

        self.queued_or_processing_urls.add(normalized_url)
        return True

    def _get_success_store_path(self):
        override_path = os.getenv('SUCCESS_URL_DB_PATH', '').strip()
        if override_path:
            return override_path
        os.makedirs(self.output_dir, exist_ok=True)
        return os.path.join(self.output_dir, f"success_urls_{self.base_domain}.sqlite3")

    def _init_success_store(self):
        try:
            success_store_path = self._get_success_store_path()
            self.success_db_conn = sqlite3.connect(success_store_path, timeout=30)
            cursor = self.success_db_conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS successful_urls (
                    domain TEXT NOT NULL,
                    normalized_url TEXT NOT NULL,
                    first_success_at TEXT NOT NULL,
                    job_id TEXT,
                    PRIMARY KEY (domain, normalized_url)
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_successful_urls_domain
                ON successful_urls(domain)
            """)
            self.success_db_conn.commit()

            cursor.execute(
                "SELECT normalized_url FROM successful_urls WHERE domain = ?",
                (self.domain.lower(),)
            )
            rows = cursor.fetchall()
            if rows:
                self.processed_successfully_urls.update(row[0] for row in rows if row and row[0])
            self.logger.info(
                f"🗂️ Loaded {len(rows)} previously successful URLs for {self.domain} from {success_store_path}"
            )
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize persistent dedup store: {e}")
            self.success_db_conn = None

    def _persist_success_url(self, normalized_url: str):
        if not self.success_db_conn or not normalized_url:
            return
        try:
            self.success_db_conn.execute(
                """
                INSERT OR IGNORE INTO successful_urls (domain, normalized_url, first_success_at, job_id)
                VALUES (?, ?, ?, ?)
                """,
                (
                    self.domain.lower(),
                    normalized_url,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    self.job_id
                )
            )
            self.success_db_write_counter += 1
            if self.success_db_write_counter >= 50:
                self.success_db_conn.commit()
                self.success_db_write_counter = 0
        except Exception as e:
            self.logger.error(f"❌ Failed to persist successful URL {normalized_url}: {e}")
    async def start(self):
        for req in self.start_requests():
            yield req

    def start_requests(self):
        if self.is_ashley:
            # Filter URLs for this chunk if in chunk mode
            urls_to_process = self.ashley_urls
            self.total_urls_found = len(urls_to_process)
            
            if self.chunk_mode and self.total_chunks > 1:
                # Calculate chunk size
                if self.chunk_size > 0:
                    chunk_size = self.chunk_size
                else:
                    chunk_size = len(self.ashley_urls) // self.total_chunks
                    if len(self.ashley_urls) % self.total_chunks != 0:
                        chunk_size += 1
                
                start_idx = self.chunk_id * chunk_size
                end_idx = start_idx + chunk_size if self.chunk_id < self.total_chunks - 1 else len(self.ashley_urls)
                urls_to_process = self.ashley_urls[start_idx:end_idx]
                self.total_urls_found = len(urls_to_process)
                
                self.logger.info(f"📊 Ashley Chunk {self.chunk_id + 1}/{self.total_chunks}: Processing {len(urls_to_process)} URLs (indices {start_idx}-{end_idx-1})")
            else:
                self.logger.info(f"📊 Ashley mode: Processing {len(self.ashley_urls)} direct product URLs")
            
            self.logger.info(f"🎯 Total URLs to process in this job: {self.total_urls_found}")
            
            # Create requests for each URL with Scrapy's built-in dupefilter
            for i, url in enumerate(urls_to_process):
                if not self._should_schedule_url(url):
                    continue
                
                # Add referer for subsequent requests
                headers = self.get_headers()
                if i > 0:
                    headers['Referer'] = urls_to_process[0]
                
                req_meta = {
                    'url': url,
                    'is_ashley': True,
                    'chunk_id': self.chunk_id,
                    'chunk_mode': self.chunk_mode
                }
                proxy = os.getenv('PROXY') or os.getenv('HTTPS_PROXY') or ''
                if proxy:
                    req_meta['proxy'] = proxy

                yield Request(
                    url,
                    callback=self.parse_product_page_with_check,
                    meta=req_meta,
                    errback=self.handle_product_error,
                    priority=10,
                    dont_filter=True,  # Bypass Scrapy's dupefilter since we handle it
                    headers=headers
                )
            return
        
        # SITEMAP MODE for non-Ashley websites
        if not hasattr(self, 'sitemap_chunk') or not self.sitemap_chunk:
            self.logger.error("❌ No sitemaps to process")
            return
        
        self.logger.info(f"🚀 Starting to process {len(self.sitemap_chunk)} sitemaps")

        for sitemap_url in self.sitemap_chunk:
            yield Request(
                sitemap_url,
                callback=self.parse_product_sitemap,
                meta={'sitemap_level': 1, 'sitemap_url': sitemap_url},
                errback=self.handle_sitemap_error
            )
    
    def parse_product_sitemap(self, response):
        if response.url.endswith('.gz'):
            content = gzip.decompress(response.body)
            root = ET.fromstring(content)
        else:
            root = ET.fromstring(response.body)
        
        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        all_urls = [url.text for url in root.findall('ns:url/ns:loc', ns) if url.text]
        
        if self.max_urls_per_sitemap > 0:
            all_urls = all_urls[:self.max_urls_per_sitemap]
        
        sitemap_url = response.meta.get('sitemap_url', response.url)
        self.sitemap_urls_count[sitemap_url] = len(all_urls)
        self.total_urls_found += len(all_urls)
        
        self.logger.info(f"📄 Sitemap {sitemap_url}: Found {len(all_urls)} URLs")
        self.logger.info(f"📊 Cumulative URLs found so far: {self.total_urls_found}")
        
        plp_count = 0
        pdp_count = 0

        for url in all_urls:
            if self._is_plp_url(url):
                plp_count += 1
                continue
            pdp_count += 1

            if not self._should_schedule_url(url):
                continue
            
            pdp_meta = {'url': url, 'sitemap': sitemap_url}
            proxy = os.getenv('PROXY') or os.getenv('HTTPS_PROXY') or ''
            if proxy:
                pdp_meta['proxy'] = proxy
            
            yield Request(
                url,
                callback=self.parse_product_page_with_check,
                meta=pdp_meta,
                errback=self.handle_product_error
            )
        
        self.logger.info(f"📊 Sitemap summary: {plp_count} PLP pages filtered out, {pdp_count} PDP pages to scrape")
    
    def _is_plp_url(self, url: str) -> bool:
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/')
        
        if not path:
            return True
        return '/' in path

    def parse_product_page_with_check(self, response):
        # Update progress counters
        self.processed_count += 1

        requested_url = response.meta.get('url', response.url)
        requested_normalized = self.normalize_url(requested_url)
        response_normalized = self.normalize_url(response.url)
        
        # Log progress periodically
        current_time = time.time()
        if current_time - self.last_log_time > self.log_interval:
            self.log_progress()
            self.last_log_time = current_time
        
        # Log every 100 items as well
        if self.processed_count % 100 == 0:
            success_rate = ((self.processed_count - self.failed_count) / self.processed_count * 100) if self.processed_count > 0 else 0
            self.logger.info(f"📊 Progress: {self.processed_count}/{self.total_urls_found} URLs processed | ✅ Success: {self.processed_count - self.failed_count} | ⏭️ Skipped: {self.skipped_count} | ❌ Failed: {self.failed_count} | 📈 Rate: {success_rate:.1f}%")
        
        json_scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        has_product_json = False
        for script in json_scripts:
            try:
                data = json.loads(script.strip())
                if isinstance(data, dict):
                    data_type = data.get('@type')
                    if data_type:
                        if isinstance(data_type, str):
                            if 'Product' in data_type:
                                has_product_json = True
                                break
                        elif isinstance(data_type, list):
                            if any('Product' in str(t) for t in data_type):
                                has_product_json = True
                                break
                    elif data.get('name') and (data.get('offers') or data.get('sku')):
                        has_product_json = True
                        break
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            item_type = item.get('@type')
                            if item_type:
                                if isinstance(item_type, str) and 'Product' in item_type:
                                    has_product_json = True
                                    break
                                elif isinstance(item_type, list) and any('Product' in str(t) for t in item_type):
                                    has_product_json = True
                                    break
                    if has_product_json:
                        break
            except Exception as e:
                self.logger.debug(f"Error parsing JSON-LD: {e}")
                continue
        
        if has_product_json:
            if self.verbose:
                self.logger.info(f"✅ Found Product JSON-LD for {response.url}")
            # Mark URL as successfully scraped only when Product JSON-LD is present.
            if requested_normalized in self.queued_or_processing_urls:
                self.queued_or_processing_urls.discard(requested_normalized)
            if requested_normalized:
                self.processed_successfully_urls.add(requested_normalized)
                self._persist_success_url(requested_normalized)
            if response_normalized:
                self.processed_successfully_urls.add(response_normalized)
                self._persist_success_url(response_normalized)
            yield from self.parse_product_page(response)
            yield from self.extract_bundle_products(response)
        else:
            if requested_normalized in self.queued_or_processing_urls:
                self.queued_or_processing_urls.discard(requested_normalized)
            if response_normalized in self.queued_or_processing_urls:
                self.queued_or_processing_urls.discard(response_normalized)
            self.failed_count += 1
            if self.verbose:
                self.logger.warning(f"⚠️ No Product JSON-LD found for {response.url}")
            return
    
    # Keys whose subtrees should never be walked for product URLs
    _EXCLUDED_URL_KEYS = frozenset({
        'crossSell', 'breadcrumb', 'categoryTree',
        'accordion', 'header', 'footer', 'splitTest', 'meta',
    })

    @staticmethod
    def _collect_htm_urls(obj, found=None, _skip_keys=None):
        """Recursively walk any JSON structure and collect all .htm product URLs,
        skipping subtrees under excluded keys (crossSell, breadcrumb, etc.)."""
        if found is None:
            found = set()
        if _skip_keys is None:
            _skip_keys = frozenset()
        if isinstance(obj, dict):
            for key, val in obj.items():
                if key in _skip_keys:
                    continue  # skip this entire subtree
                if key == 'url' and isinstance(val, str) and val.endswith('.htm'):
                    found.add(val)
                else:
                    ProductFetcher._collect_htm_urls(val, found, _skip_keys)
        elif isinstance(obj, list):
            for item in obj:
                ProductFetcher._collect_htm_urls(item, found, _skip_keys)
        return found

    def get_hypernova_content(self, response):
        """Extract and parse Hypernova App JSON from response."""
        cache_key = id(response)
        if hasattr(self, '_hypernova_cache') and cache_key in self._hypernova_cache:
            return self._hypernova_cache[cache_key]
        
        content = {}
        try:
            script_full = response.xpath('//script[@data-hypernova-key="App"]').get()
            if script_full and '{' in script_full and '}' in script_full:
                start = script_full.find('{')
                end = script_full.rfind('}')
                raw = script_full[start:end+1]
                hypernova_data = json.loads(raw)
                content = hypernova_data.get('data', {}).get('content', {})
                if not content and 'content' in hypernova_data:
                    content = hypernova_data.get('content', {})
        except Exception as e:
            self.logger.debug(f"Error parsing Hypernova script: {e}")

        if not hasattr(self, '_hypernova_cache'):
            self._hypernova_cache = {}
        self._hypernova_cache[cache_key] = content
        return content

    def _find_selected_simple_item(self, response, content=None):
        if content is None:
            content = self.get_hypernova_content(response)
        if not content:
            return None
        product_layouts = content.get('productLayouts', {})
        simple_items = product_layouts.get('simpleItems', [])
        if not simple_items:
            return None

        current_url = response.url.rstrip('/')
        try:
            current_path = urlparse(response.url).path.rstrip('/')
        except Exception:
            current_path = ''
        selected_layout_id = str(product_layouts.get('selectedLayoutId') or '')

        # 1. Match by URL or path
        for item in simple_items:
            if isinstance(item, dict):
                item_url = item.get('url', '').rstrip('/')
                try:
                    item_path = urlparse(item_url).path.rstrip('/') if item_url else ''
                except Exception:
                    item_path = ''
                if item_url and (item_url == current_url or (item_path and item_path == current_path)):
                    return item

        # 2. Match by selectedLayoutId == productId
        if selected_layout_id:
            for item in simple_items:
                if isinstance(item, dict) and str(item.get('productId') or '') == selected_layout_id:
                    return item

        # 3. Fallback to single item if only 1 exists
        if len(simple_items) == 1 and isinstance(simple_items[0], dict):
            return simple_items[0]

        return None

    def extract_bundle_products(self, response):
        content = self.get_hypernova_content(response)
        if not content:
            return
        try:
            # Deep-scan the entire content blob for any .htm URL
            # (skipping crossSell, breadcrumb, categoryTree, accordion, etc.)
            all_urls = self._collect_htm_urls(content, _skip_keys=self._EXCLUDED_URL_KEYS)
            # Exclude the current page itself
            all_urls.discard(response.url)
            all_urls.discard(response.url.rstrip('/'))

            bundle_count = 0
            for sub_product_url in all_urls:
                if not self._should_schedule_url(sub_product_url):
                    if self.verbose:
                        self.logger.info(f"⏭️ Bundle product already tracked: {sub_product_url}")
                    continue
                bundle_count += 1
                if self.verbose:
                    self.logger.info(f"📦 Found bundle product #{bundle_count}: {sub_product_url}")
                yield Request(
                    sub_product_url,
                    callback=self.parse_product_page_with_check,
                    meta={'url': sub_product_url, 'is_bundle': True},
                    errback=self.handle_product_error
                )

            if bundle_count > 0:
                self.logger.info(f"📦 Added {bundle_count} bundle products from {response.url}")

        except Exception as e:
            self.logger.error(f"Error extracting bundle products: {e}")


    def parse_product_page(self, response):
        item = {}

        sku = self.extract_sku(response)
        item['Ref Product URL'] = response.url
        item['Ref SKU'] = sku
        item['Date Scrapped'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        item['Ref Product Name'] = self.extract_product_name(response)
        item['Ref Price'] = self.extract_price(response)
        item['Ref MPN'] = self.extract_mpn(response)
        item['Ref GTIN'] = self.extract_gtin(response)
        brand_name = self.extract_brand(response)
        item['Ref Brand Name'] = brand_name
        item['Ref Main Image'] = self.extract_main_image(response)
        item['Ref Category'] = self.extract_category(response)
        item['Ref Category URL'] = self.extract_category_url(response)
        item['Ref Quantity'] = self.extract_quantity(response)
        item['Ref Status'] = self.extract_status(response)
        item['Ref Product ID'] = self.extract_product_id(response)
        item['Ref Variant ID'] = self.extract_variant_id(response)
        item['Ref Group Attr 1'] = self.extract_group_attr1(response, 1)
        item['Ref Group Attr 2'] = self.extract_group_attr2(response, 2)
        item['Ref Images'] = self.extract_main_images(response)
        item['Ref Highlights'] = self.extract_highlights(response)
        item['Ref Dimensions'] = self.extract_dimensions(response)

        # Parse Hypernova content for fields
        content = self.get_hypernova_content(response)

        # 1. Product Type: read directly from productInfo.type (bundle/simple/configurable etc.)
        product_type = content.get('productInfo', {}).get('type', 'simple')
        item['Product Type'] = product_type

        # 2. Row JSON — content sub-object only (csv viewer shows "" due to CSV spec, data is correct)
        item['Row JSON'] = json.dumps(content) if content else ''

        # 3. Collection Name
        item['Collection Name'] = self.extract_collection_name(content, brand_name)
        
        if self.verbose and sku:
            self.logger.info(f"💾 Extracted product: {sku} - {item.get('Ref Product Name', '')[:50]}...")
            
        if HAS_DB_QUEUE:
            try:
                domain_val = getattr(self, "base_domain", "") or getattr(self, "website_url", "")
                comp_name = "Home Gallery Stores" if "homegallery" in str(domain_val).lower() else "Coleman Furniture"
                cid = 15177 if comp_name == "Home Gallery Stores" else 17028
                save_scraped_product(comp_name, cid, dict(item))
            except Exception:
                pass

        yield item

    def extract_collection_name(self, content, brand_name):
        try:
            if not content or not brand_name:
                return ''
            
            accordion_data = content.get('accordion', {})
            product_detail = accordion_data.get('productDetail', {})
            
            # Try features -> items first
            items = product_detail.get('features', {}).get('items', [])
            if not items or not isinstance(items, list):
                # Fallback to items directly under productDetail
                items = product_detail.get('items', [])
                
            if not items or not isinstance(items, list):
                return ''
            
            # Find the item that contains "part of" (case-insensitive)
            target_str = ''
            for item_val in items:
                val = ''
                if isinstance(item_val, dict):
                    val = item_val.get('content', '') or item_val.get('value', '') or ''
                elif isinstance(item_val, str):
                    val = item_val
                
                if val and "part of" in val.lower():
                    # If it's a comma-separated string, use the first part
                    parts = [p.strip() for p in val.split(',')]
                    if parts and "part of" in parts[0].lower():
                        target_str = parts[0]
                        break
            
            # Fallback to the first item if no item contains "part of"
            if not target_str:
                item0 = items[0]
                val0 = ''
                if isinstance(item0, dict):
                    val0 = item0.get('content', '') or item0.get('value', '') or ''
                elif isinstance(item0, str):
                    val0 = item0
                parts = [p.strip() for p in val0.split(',')]
                target_str = parts[0] if parts else ''

            if not target_str:
                return ''
                
            target_str = target_str.strip()
            target_lower = target_str.lower()
            
            # Find "part of"
            part_of_idx = target_lower.find("part of")
            if part_of_idx == -1:
                return ''
            
            # Find "collection from [brand_name]"
            brand_clean = brand_name.strip().lower()
            collection_from_str = f"collection from {brand_clean}"
            coll_from_idx = target_lower.find(collection_from_str)
            
            if coll_from_idx != -1 and coll_from_idx > part_of_idx:
                # The text between "part of" and "collection from [brand_name]"
                start_pos = part_of_idx + len("part of")
                extracted = target_str[start_pos:coll_from_idx].strip()
                # Clean up any colon or extra spaces
                if extracted.startswith(':'):
                    extracted = extracted[1:].strip()
                return f"{extracted} Collection"
                
            # If not "collection from [brand_name]", look for "from [brand_name]"
            from_str = f"from {brand_clean}"
            from_idx = target_lower.find(from_str)
            if from_idx != -1 and from_idx > part_of_idx:
                start_pos = part_of_idx + len("part of")
                extracted = target_str[start_pos:from_idx].strip()
                if extracted.startswith(':'):
                    extracted = extracted[1:].strip()
                # If the extracted part ends with "collection", just return it
                if extracted.lower().endswith("collection"):
                    return extracted
                else:
                    return f"{extracted} Collection"
            
            return ''
        except Exception as e:
            self.logger.error(f"Error in extract_collection_name: {e}")
            return ''
    
    def log_progress(self):
        """Log detailed progress information"""
        elapsed = time.time() - self.start_time
        rate = self.processed_count / elapsed if elapsed > 0 else 0
        success_rate = ((self.processed_count - self.failed_count) / self.processed_count * 100) if self.processed_count > 0 else 0
        
        self.logger.info("=" * 70)
        self.logger.info(f"📈 PROGRESS REPORT - Job: {self.job_id}")
        self.logger.info(f"   Processed: {self.processed_count}/{self.total_urls_found} URLs ({self.processed_count/self.total_urls_found*100:.1f}% complete)" if self.total_urls_found > 0 else f"   Processed: {self.processed_count} URLs")
        self.logger.info(f"   ✅ Successful: {self.processed_count - self.failed_count}")
        self.logger.info(f"   ⏭️ Skipped (duplicates): {self.skipped_count}")
        self.logger.info(f"   ❌ Failed: {self.failed_count}")
        self.logger.info(f"   📊 Success rate: {success_rate:.1f}%")
        self.logger.info(f"   ⚡ Speed: {rate:.2f} URLs/sec")
        self.logger.info(f"   ⏱️ Elapsed: {self.format_time(elapsed)}")
        
        # Show sitemap breakdown if available
        if self.sitemap_urls_count:
            self.logger.info(f"   📚 Sitemap breakdown:")
            for sitemap, count in list(self.sitemap_urls_count.items())[:5]:  # Show first 5 only
                short_name = sitemap.split('/')[-1][:30]
                self.logger.info(f"      - {short_name}: {count} URLs")
            if len(self.sitemap_urls_count) > 5:
                self.logger.info(f"      ... and {len(self.sitemap_urls_count) - 5} more sitemaps")
        
        self.logger.info("=" * 70)
    
    def format_time(self, seconds):
        """Format time in seconds to HH:MM:SS"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"
       
    def extract_product_name(self, response):
        selected_item = self._find_selected_simple_item(response)
        if selected_item:
            name = selected_item.get('name', '').strip()
            if name:
                return name

        content = self.get_hypernova_content(response)
        if content:
            title = content.get('productInfo', {}).get('title') or content.get('productInfo', {}).get('name')
            if title:
                return str(title).strip()

        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') in ('Product', 'ProductGroup') and data.get('name'):
                    return str(data['name']).strip()
            except Exception:
                pass

        selectors = [
            '//*[@id="contentId"]/div/div[1]/div[2]/div[2]/h1/text()',
            '//h1/text()',
        ]
        return self.extract_using_selectors(response, selectors)
    
    def extract_price(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') in ('Product', 'ProductGroup'):
                    offers = data.get('offers', {})
                    if isinstance(offers, dict) and 'price' in offers and offers['price']:
                        return str(offers['price'])
            except:
                continue

        selected_item = self._find_selected_simple_item(response)
        if selected_item:
            price_obj = selected_item.get('price', {})
            if isinstance(price_obj, dict):
                price_val = price_obj.get('sale') or price_obj.get('final') or price_obj.get('regular')
                if price_val:
                    return str(price_val)
            elif price_obj:
                return str(price_obj)

        return ''
    
    def extract_sku(self, response):
        content = self.get_hypernova_content(response)
        selected_item = self._find_selected_simple_item(response, content)
        if selected_item:
            sku = selected_item.get('sku', '').strip()
            if sku and 'ROOM' not in sku.upper() and 'HOST' not in sku.upper():
                return sku

        # Fallback to JSON-LD
        sku = ''
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') in ('Product', 'ProductGroup'):
                    candidate = (data.get('sku', '') or '').strip()
                    if candidate:
                        sku = candidate
                        break
            except:
                continue

        # If SKU has ROOM or HOST, we MUST get the specific selected product's SKU
        if not sku or 'ROOM' in sku.upper() or 'HOST' in sku.upper():
            if selected_item:
                selected_sku = selected_item.get('sku', '').strip()
                if selected_sku:
                    return selected_sku

            # Check all simple items for a non-ROOM/non-HOST SKU
            if content:
                simple_items = content.get('productLayouts', {}).get('simpleItems', [])
                for item in simple_items:
                    if isinstance(item, dict):
                        cand = item.get('sku', '').strip()
                        if cand and 'ROOM' not in cand.upper() and 'HOST' not in cand.upper():
                            return cand

        return sku
    
    def extract_mpn(self, response):
        content = self.get_hypernova_content(response)
        selected_item = self._find_selected_simple_item(response, content)
        if selected_item:
            mpn = (selected_item.get('mpn') or selected_item.get('sku') or '').strip()
            if mpn and 'ROOM' not in mpn.upper() and 'HOST' not in mpn.upper():
                return mpn

        mpn = ''
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') in ('Product', 'ProductGroup'):
                    candidate = (data.get('mpn', '') or data.get('sku', '') or '').strip()
                    if candidate:
                        mpn = candidate
                        break
            except:
                continue

        if not mpn or 'ROOM' in mpn.upper() or 'HOST' in mpn.upper():
            if selected_item:
                selected_mpn = (selected_item.get('mpn') or selected_item.get('sku') or '').strip()
                if selected_mpn:
                    return selected_mpn
            if content:
                simple_items = content.get('productLayouts', {}).get('simpleItems', [])
                for item in simple_items:
                    if isinstance(item, dict):
                        cand = (item.get('mpn') or item.get('sku') or '').strip()
                        if cand and 'ROOM' not in cand.upper() and 'HOST' not in cand.upper():
                            return cand

        return mpn

    def extract_gtin(self, response):
        return ''
    
    def extract_brand(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    brand = data.get('brand', {})
                    if isinstance(brand, dict):
                        return brand.get('name', '')
                    else:
                        return str(brand)
            except:
                continue
        return ''
    
    def extract_main_image(self, response):
        selected_item = self._find_selected_simple_item(response)
        if selected_item:
            gallery = selected_item.get('gallery', [])
            if isinstance(gallery, list) and gallery:
                for img in gallery:
                    if isinstance(img, dict):
                        original_url = img.get('original', '')
                        if original_url:
                            return original_url
            for img_key in ['defaultImage', 'imageUrl']:
                img_url = selected_item.get(img_key, '')
                if img_url:
                    return img_url

        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') in ('Product', 'ProductGroup'):
                    images = data.get('image', [])
                    if isinstance(images, list) and images:
                        return images[0]
                    elif isinstance(images, str):
                        return images
            except:
                continue
        return ''
        return ''
    
    def extract_category(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'BreadcrumbList':
                    categories = []
                    for item in data.get('itemListElement', []):
                        item_data = item.get('item', {})
                        name = item_data.get('name', '')
                        if name and name.lower() not in ['home', 'shop', 'all']:
                            categories.append(name)
                    if len(categories) > 1:
                        categories = categories[:-1]
                    if categories:
                        return ' > '.join(categories)
            except:
                continue
        return ''

    def extract_category_url(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'BreadcrumbList':
                    urls = []
                    for item in data.get('itemListElement', []):
                        item_data = item.get('item', {})
                        url = item_data.get('@id', '')
                        if url:
                            urls.append(url)
                    if len(urls) >= 2:
                        return urls[-2]
            except:
                continue
        return ''
    
    def extract_quantity(self, response):
        return ''

    def extract_status(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    offers = data.get('offers', {})
                    if isinstance(offers, dict):
                        availability = str(offers.get('availability', '')).lower()
                        if 'instock' in availability:
                            return 'Active'
                        elif 'outofstock' in availability or 'soldout' in availability:
                            return 'Out of Stock'
                        elif 'preorder' in availability:
                            return 'Active'
            except:
                continue
        return ''
    
    def extract_product_id(self, response):
        selected_item = self._find_selected_simple_item(response)
        if selected_item:
            productId = selected_item.get('productId', '')
            if productId:
                return str(productId)

        content = self.get_hypernova_content(response)
        if content:
            pid = content.get('productInfo', {}).get('ProductId') or content.get('productInfo', {}).get('id')
            if pid:
                return str(pid)

        product_id = response.xpath('//div[@data-id]/@data-id').get()        
        if product_id:
            return product_id
        return ''
    
    def extract_variant_id(self, response):
        return ''
    
    def extract_group_attr1(self, response, attr_num):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    return data.get('color', '')
            except:
                continue
        return ''

    def extract_group_attr2(self, response, attr_num):
        return ''
       
    def extract_using_selectors(self, response, selectors):
        for selector in selectors:
            if selector.startswith('//'):
                result = response.xpath(selector).get()
            else:
                result = response.css(selector).get()
            
            if result:
                cleaned = result.strip()
                if cleaned:
                    return cleaned
        return ''

    def extract_highlights(self, response):
        highlights = []
        highlight_items = response.xpath('//div[contains(@class, "product-hightlights-items-item")]')       
        for item in highlight_items:
            title = item.xpath('.//span[contains(@class, "product-hightlights-items-item-title")]/text()').get()
            desc = item.xpath('.//p[contains(@class, "product-hightlights-items-item-desc")]/text()').get()
            if title:
                highlights.append({
                    'title': title.strip() if title else '',
                    'desc': desc.strip() if desc else ''
                })
        json_output = json.dumps(highlights, indent=2)
        return json_output
    
    def extract_main_images(self, response):
        image_urls = []
        content = self.get_hypernova_content(response)
        if content:
            try:
                matching_item = self._find_selected_simple_item(response, content)
                if matching_item:
                    gallery = matching_item.get('gallery', [])
                    if isinstance(gallery, list):
                        for img in gallery:
                            if isinstance(img, dict):
                                original_url = img.get('original')
                                if original_url:
                                    image_urls.append(original_url)
                        
                        if image_urls:
                            return '\n'.join(image_urls)
                
                if not image_urls:
                    gallery = content.get('gallery', [])
                    if isinstance(gallery, list):
                        for img in gallery:
                            if isinstance(img, dict):
                                original_url = img.get('original')
                                if original_url:
                                    image_urls.append(original_url)
                        
                        if image_urls:
                            return '\n'.join(image_urls)
                            
            except Exception as e:
                self.logger.debug(f"Error extracting images: {e}")
        
        return '\n'.join(image_urls) if image_urls else ''

    def extract_dimensions(self, response):
        content = self.get_hypernova_content(response)
        if not content:
            return ''
        
        try:
            setIncludes = content.get('setIncludes', {})
            
            result = {}
            
            items = setIncludes.get('items', [])
            for item in items:
                if not isinstance(item, dict):
                    continue
                
                item_short_name = item.get('itemShortName', '')
                dimension = item.get('dimension', {})
                image_url = dimension.get('image', {}).get('url', '') if isinstance(dimension.get('image'), dict) else ''
                if image_url and not self.is_valid_image_url(image_url):
                    image_url = ''
                dimensions_list = dimension.get('list', [])
                
                dimension_data = []
                for dim in dimensions_list:
                    if dim and isinstance(dim, str):
                        dimension_data.append(dim)
                
                if item_short_name:
                    result[item_short_name.lower()] = {
                        "url": image_url,
                        "data": dimension_data if dimension_data else []
                    }
            
            for item in items:
                if not isinstance(item, dict):
                    continue
                
                configurables = item.get('configurables', [])
                for config in configurables:
                    if not isinstance(config, dict):
                        continue
                    
                    options = config.get('options', [])
                    for option in options:
                        if not isinstance(option, dict):
                            continue
                        
                        item_short_name = option.get('itemShortName', '')
                        dimension = option.get('dimension', {})
                        image_url = dimension.get('image', {}).get('url', '') if isinstance(dimension.get('image'), dict) else ''
                        if image_url and not self.is_valid_image_url(image_url):
                            image_url = ''
                        dimensions_list = dimension.get('list', [])
                        
                        dimension_data = []
                        for dim in dimensions_list:
                            if dim and isinstance(dim, str):
                                dimension_data.append(dim)
                        
                        if item_short_name:
                            result[item_short_name.lower()] = {
                                "url": image_url,
                                "data": dimension_data if dimension_data else []
                            }
            
            additional_items_data = content.get('additionalItems', {})
            if isinstance(additional_items_data, dict):
                additional_items = additional_items_data.get('items', [])
                for item in additional_items:
                    if not isinstance(item, dict):
                        continue
                    
                    item_short_name = item.get('itemShortName', '')
                    dimension = item.get('dimension', {})
                    image_url = dimension.get('image', {}).get('url', '') if isinstance(dimension.get('image'), dict) else ''
                    if image_url and not self.is_valid_image_url(image_url):
                        image_url = ''
                    dimensions_list = dimension.get('list', [])
                    
                    dimension_data = []
                    for dim in dimensions_list:
                        if dim and isinstance(dim, str):
                            dimension_data.append(dim)
                    
                    if item_short_name:
                        result[item_short_name.lower()] = {
                            "url": image_url,
                            "data": dimension_data if dimension_data else []
                        }

            simpleItems = content.get('productLayouts', {}).get('simpleItems', [])
            if isinstance(simpleItems, list):
                for item in simpleItems:
                    if not isinstance(item, dict):
                        continue
                    item_short_name = item.get('itemShortName', '')
                    dimension = item.get('dimension', {})
                    image_url = dimension.get('image', {}).get('url', '') if isinstance(dimension.get('image'), dict) else ''
                    if image_url and not self.is_valid_image_url(image_url):
                        image_url = ''
                    dimensions_list = dimension.get('list', [])
                    
                    dimension_data = []
                    for dim in dimensions_list:
                        if dim and isinstance(dim, str):
                            dimension_data.append(dim)
                    
                    if item_short_name:
                        result[item_short_name.lower()] = {
                            "url": image_url,
                            "data": dimension_data if dimension_data else []
                        }

            if not result:
                accordion_data = content.get('accordion', {})
                dimensions_data = accordion_data.get('dimensions', {})
                
                if dimensions_data and isinstance(dimensions_data, dict):
                    dimension_list = dimensions_data.get('dimensionList', [])
                    
                    image_url = dimensions_data.get('image', {}).get('url', '') if isinstance(dimensions_data.get('image'), dict) else ''
                    if image_url and not self.is_valid_image_url(image_url):
                        image_url = ''
                    
                    dimension_data = []
                    for dim in dimension_list:
                        if dim and isinstance(dim, str):
                            dimension_data.append(dim)
                    
                    if dimension_data:
                        result["dimensions"] = {
                            "url": image_url,
                            "data": dimension_data
                        }
            if result:
                return json.dumps(result, indent=2)
            return ''
            
        except Exception as e:
            self.logger.error(f"Error extracting dimensions: {e}")
            return ''

    def is_valid_image_url(self, url):
        if not url or not isinstance(url, str):
            return False
        
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.bmp']
        url_lower = url.lower()
        
        if not (url_lower.startswith('http://') or url_lower.startswith('https://')):
            return False
        
        if not any(url_lower.endswith(ext) for ext in image_extensions):
            if not any(ext in url_lower for ext in image_extensions):
                return False
        
        return True
        
    def clean_price(self, price_text):
        if not price_text:
            return ''
        
        cleaned = re.sub(r'[^\d.,]', '', price_text)
        
        if ',' in cleaned and '.' in cleaned:
            if cleaned.rfind(',') > cleaned.rfind('.'):
                cleaned = cleaned.replace('.', '').replace(',', '.')
            else:
                cleaned = cleaned.replace(',', '')
        
        try:
            price_float = float(cleaned)
            return f"{price_float:.2f}"
        except ValueError:
            return cleaned
    
    def handle_sitemap_error(self, failure):
        self.logger.error(f"❌ Sitemap request failed: {failure.value}")

    def _get_remaining_file_path(self):
        feed_uri = ""
        if hasattr(self, "crawler") and getattr(self, "crawler", None):
            feed_uri = self.crawler.settings.get("FEED_URI", "") or ""

        if feed_uri:
            feed_dir = os.path.dirname(feed_uri) or "."
            feed_name = os.path.basename(feed_uri)
            feed_stem, _ = os.path.splitext(feed_name)
            if feed_stem.startswith("output_"):
                remaining_name = feed_stem.replace("output_", "remaining_", 1) + ".csv"
            else:
                remaining_name = f"{feed_stem}_remaining.csv"
            return os.path.join(feed_dir, remaining_name)

        fallback_dir = "output"
        os.makedirs(fallback_dir, exist_ok=True)
        return os.path.join(
            fallback_dir,
            f"remaining_{self.base_domain}_{self.job_id}.csv"
        )

    def _get_unscraped_file_path(self):
        feed_uri = ""
        if hasattr(self, "crawler") and getattr(self, "crawler", None):
            feed_uri = self.crawler.settings.get("FEED_URI", "") or ""

        if feed_uri:
            feed_dir = os.path.dirname(feed_uri) or "."
            feed_name = os.path.basename(feed_uri)
            feed_stem, _ = os.path.splitext(feed_name)
            if feed_stem.startswith("output_"):
                unscraped_name = feed_stem.replace("output_", "unscraped_", 1) + ".csv"
            else:
                unscraped_name = f"{feed_stem}_unscraped.csv"
            return os.path.join(feed_dir, unscraped_name)

        fallback_dir = "output"
        os.makedirs(fallback_dir, exist_ok=True)
        return os.path.join(
            fallback_dir,
            f"unscraped_{self.base_domain}_{self.job_id}.csv"
        )

    def _record_unscraped(
        self,
        url: str,
        reason: str,
        status: str = "N/A",
        error_type: str = "",
        error_message: str = ""
    ):
        if not url:
            return
        key = self.normalize_url(url) or url
        self.unscraped_requests[key] = {
            "url": url,
            "reason": reason,
            "status": str(status),
            "error_type": error_type,
            "error_message": error_message,
            "failed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
    
    def handle_product_error(self, failure):
        self.failed_count += 1
        
        # Initialize variables
        failed_url = "Unknown URL"
        status_code = "N/A"
        error_type = "Unknown"
        error_msg = "Unknown"
        
        # Try to extract URL and status code from the failure
        if hasattr(failure, 'request') and failure.request:
            request_url = failure.request.url
            failed_url = request_url
            
            # Check meta for original URL if available
            if hasattr(failure.request, 'meta') and 'url' in failure.request.meta:
                failed_url = failure.request.meta['url']
            # Unlock failed URL(s) so they can be retried if seen again
            failed_normalized = self.normalize_url(failed_url)
            request_normalized = self.normalize_url(request_url)
            if failed_normalized:
                self.queued_or_processing_urls.discard(failed_normalized)
            if request_normalized:
                self.queued_or_processing_urls.discard(request_normalized)
        
        # Extract status code from response if available
        if hasattr(failure, 'value') and hasattr(failure.value, 'response') and failure.value.response:
            response = failure.value.response
            status_code = response.status
            if not failed_url or failed_url == "Unknown URL":
                failed_url = response.url
        elif hasattr(failure, 'value') and hasattr(failure.value, 'status'):
            # Some errors have status directly
            status_code = failure.value.status
        
        # Get error type and message
        if hasattr(failure, 'value'):
            error_type = type(failure.value).__name__
            error_msg = str(failure.value)

        failed_url = failed_url or "Unknown URL"
        failed_url_key = failed_url if failed_url != "Unknown URL" else f"unknown_{self.failed_count}"
        self.failed_requests[failed_url_key] = {
            "url": failed_url,
            "status": str(status_code),
            "error_type": error_type,
            "error_message": error_msg,
            "failed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

        # Final "unscraped" CSV includes only 404 and 301.
        if str(status_code) in {"404", "301"}:
            self._record_unscraped(
                failed_url,
                reason="REQUEST_FAILED",
                status=str(status_code),
                error_type=error_type,
                error_message=error_msg
            )
               
        # Log the detailed error with status code prominently displayed
        self.logger.error("=" * 70)
        self.logger.error(f"❌ ERROR #{self.failed_count} - Status Code: {status_code}")
        self.logger.error("=" * 70)
        self.logger.error(f"📍 URL: {failed_url}")
        self.logger.error(f"📊 Status: {status_code}")
        self.logger.error(f"📋 Error Type: {error_type}")
        self.logger.error(f"📝 Message: {error_msg}")
        
        # Additional details for specific status codes
        if status_code == 404:
            self.logger.error(f"💡 Page not found - The product might be discontinued")
        elif status_code == 403:
            self.logger.error(f"💡 Access forbidden - The server is blocking our request")
        elif status_code == 429:
            self.logger.error(f"💡 Rate limited - Too many requests, consider increasing DOWNLOAD_DELAY")
        elif status_code == 500 or status_code == 502 or status_code == 503:
            self.logger.error(f"💡 Server error - The website might be experiencing issues")
        elif status_code == 301 or status_code == 302:
            self.logger.error(f"💡 Redirect - The page has moved")
        
        # Log response headers for debugging (optional)
        if hasattr(failure, 'value') and hasattr(failure.value, 'response') and failure.value.response:
            headers = dict(failure.value.response.headers)
            self.logger.debug(f"📋 Response Headers: {headers}")
        
        self.logger.error("=" * 70)

    def closed(self, reason):
        """Log final stats when spider closes"""
        elapsed = time.time() - self.start_time
        success_rate = ((self.processed_count - self.failed_count) / self.processed_count * 100) if self.processed_count > 0 else 0
        rate = self.processed_count / elapsed if elapsed > 0 else 0

        remaining_file = None
        if self.failed_requests:
            remaining_file = self._get_remaining_file_path()
            os.makedirs(os.path.dirname(remaining_file) or ".", exist_ok=True)
            with open(remaining_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "url",
                        "status",
                        "error_type",
                        "error_message",
                        "failed_at",
                        "job_id",
                        "chunk_id",
                    ]
                )
                writer.writeheader()
                for row in self.failed_requests.values():
                    writer.writerow({
                        "url": row.get("url", ""),
                        "status": row.get("status", ""),
                        "error_type": row.get("error_type", ""),
                        "error_message": row.get("error_message", ""),
                        "failed_at": row.get("failed_at", ""),
                        "job_id": self.job_id,
                        "chunk_id": self.chunk_id,
                    })

        unscraped_file = None
        if self.unscraped_requests:
            unscraped_file = self._get_unscraped_file_path()
            os.makedirs(os.path.dirname(unscraped_file) or ".", exist_ok=True)
            with open(unscraped_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "url",
                        "reason",
                        "status",
                        "error_type",
                        "error_message",
                        "failed_at",
                        "job_id",
                        "chunk_id",
                    ]
                )
                writer.writeheader()
                for row in self.unscraped_requests.values():
                    writer.writerow({
                        "url": row.get("url", ""),
                        "reason": row.get("reason", ""),
                        "status": row.get("status", ""),
                        "error_type": row.get("error_type", ""),
                        "error_message": row.get("error_message", ""),
                        "failed_at": row.get("failed_at", ""),
                        "job_id": self.job_id,
                        "chunk_id": self.chunk_id,
                    })
        
        self.logger.info("=" * 70)
        self.logger.info(f"🏁 FINAL SCRAPING REPORT - Job: {self.job_id}")
        self.logger.info(f"   📊 Summary:")
        self.logger.info(f"      - Total URLs found: {self.total_urls_found}")
        self.logger.info(f"      - URLs processed: {self.processed_count}")
        self.logger.info(f"      - ✅ Successful: {self.processed_count - self.failed_count}")
        self.logger.info(f"      - ⏭️ Skipped (duplicates): {self.skipped_count}")
        self.logger.info(f"      - ❌ Failed: {self.failed_count}")
        if remaining_file:
            self.logger.info(f"      - 🔁 Remaining file: {remaining_file}")
            print(f"REMAINING_FILE={remaining_file}")
        if unscraped_file:
            self.logger.info(f"      - 📄 Unscraped file (only 404/301): {unscraped_file}")
            print(f"UNSCRAPED_FILE={unscraped_file}")
        self.logger.info(f"   📈 Performance:")
        self.logger.info(f"      - Success rate: {success_rate:.1f}%")
        self.logger.info(f"      - Total time: {self.format_time(elapsed)}")
        self.logger.info(f"      - Average speed: {rate:.2f} URLs/sec")
        
        if self.sitemap_urls_count:
            self.logger.info(f"   📚 Sitemaps processed: {len(self.sitemap_urls_count)}")
        
        self.logger.info("=" * 70)

        if self.success_db_conn:
            try:
                self.success_db_conn.commit()
                self.success_db_conn.close()
            except Exception as e:
                self.logger.error(f"❌ Error closing persistent dedup store: {e}")

import os
import csv
import time
import sys
import gc
import threading
import requests
import random
import re
import json
import html
import ast
import shutil
import signal
import urllib3
from typing import Optional, List, Dict, Any, Tuple
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

# Add repository root for shared imports like ftp_manager
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

try:
    from ftp_manager import FTPManager, normalize_competitor_name
    HAS_FTP_MANAGER = True
except Exception:
    HAS_FTP_MANAGER = False

try:
    from db_queue import filter_pending_urls, save_scraped_product
    HAS_DB_QUEUE = True
except Exception:
    HAS_DB_QUEUE = False

# ================= BYPASS MODULES =================

try:
    from curl_cffi import requests as cc_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False

try:
    from colemanfurniture_brand_file_scraper.utils.proxy_manager import ProxyManager
    HAS_PROXY_MANAGER = True
except Exception:
    HAS_PROXY_MANAGER = False

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "https://emmamason.com").rstrip("/")
CANONICAL_URL = CURR_URL.replace("https://www.", "https://").replace("http://www.", "http://")
SITEMAP_INDEX = f"{CANONICAL_URL}/sitemap.xml"
SITEMAP_URL = os.getenv("SITEMAP_URL", "").strip()
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
CHUNK_INDEX = int(os.getenv("CHUNK_INDEX", str(SITEMAP_OFFSET)))
TOTAL_CHUNKS = int(os.getenv("TOTAL_CHUNKS", "0"))
URLS_FILE = os.getenv("URLS_FILE", "").strip()
COLLECT_URLS_ONLY = os.getenv("COLLECT_URLS_ONLY", "").strip().lower() in ("1", "true", "yes")
MASTER_URLS_FILE = os.getenv("MASTER_URLS_FILE", "emmamason_all_urls.txt").strip()
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "1.0"))
SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "5"))
PROXY_ENV = os.getenv("PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or os.getenv("SITEMAP_PROXY") or ""

# FlareSolverr configuration
FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "http://localhost:8191/v1").rstrip("/")
FLARESOLVERR_TIMEOUT = int(os.getenv("FLARESOLVERR_TIMEOUT", "120"))

# FTP Configuration for periodic data streaming
FTP_HOST = os.getenv("FTP_HOST", "").strip()
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_USER = os.getenv("FTP_USER", "").strip()
FTP_PASS = os.getenv("FTP_PASS", "").strip()
FTP_PATH = os.getenv("FTP_PATH", "/scrap/").strip()
FTP_SYNC_ITEMS = int(os.getenv("FTP_SYNC_ITEMS", "100"))      # Upload to FTP strictly per 100 items

COMPETITOR_NAME = "EmmaMason"
DATE_DIR = datetime.now(timezone.utc).strftime("%Y%m%d")
REMOTE_TEMP_DIR = f"{FTP_PATH.rstrip('/')}/{COMPETITOR_NAME}/{DATE_DIR}/output/temp"

OUTPUT_CSV = f"products_chunk_{CHUNK_INDEX}.csv"
URL_LIST_FILE = f"url_list_chunk_{CHUNK_INDEX}.txt"
REMOTE_TEMP_CSV = f"{REMOTE_TEMP_DIR}/{OUTPUT_CSV}"
REMOTE_TEMP_URLS = f"{REMOTE_TEMP_DIR}/{URL_LIST_FILE}"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

# ================= PERIODIC FTP UPLOADER =================

csv_file_handle = None
uploader: Optional['PeriodicFTPUploader'] = None

class PeriodicFTPUploader:
    """Streams data to FTP every N records and flushes on interruption/failure."""
    def __init__(self, local_csv: str, remote_csv: str, local_urls: str = "", remote_urls: str = "", sync_items: int = 100):
        self.local_csv = local_csv
        self.remote_csv = remote_csv
        self.local_urls = local_urls
        self.remote_urls = remote_urls
        self.sync_items = sync_items
        self.last_sync_count = 0
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ftp_sync")
        self.is_uploading = False
        self.ftp_enabled = bool(HAS_FTP_MANAGER and FTP_HOST and FTP_USER and FTP_PASS)
        if self.ftp_enabled:
            log(f"[FTP-STREAM] Streaming enabled -> {self.remote_csv} (every {self.sync_items} records)")
        else:
            log("[FTP-STREAM] FTP credentials not supplied; periodic streaming disabled.")

    def on_item_scraped(self, current_count: int, file_handle=None, force: bool = False):
        if file_handle:
            try:
                file_handle.flush()
                os.fsync(file_handle.fileno())
            except Exception:
                pass

        if not self.ftp_enabled:
            return

        items_diff = current_count - self.last_sync_count
        if force or (items_diff >= self.sync_items):
            self.trigger_sync(current_count)

    def trigger_sync(self, current_count: int = 0):
        if not self.ftp_enabled:
            return
        with self.lock:
            if self.is_uploading:
                return
            self.is_uploading = True
            if current_count:
                self.last_sync_count = current_count

        self.executor.submit(self._run_upload)

    def _run_upload(self):
        try:
            temp_snapshot = f"{self.local_csv}.upload_snap"
            if os.path.exists(self.local_csv) and os.path.getsize(self.local_csv) > 0:
                shutil.copyfile(self.local_csv, temp_snapshot)
                ftp = FTPManager(FTP_HOST, FTP_USER, FTP_PASS, FTP_PORT, FTP_PATH)
                try:
                    ftp.connect()
                    ftp.upload_file(temp_snapshot, self.remote_csv)
                    if self.local_urls and os.path.exists(self.local_urls) and self.remote_urls:
                        ftp.upload_file(self.local_urls, self.remote_urls)
                    log(f"[FTP-STREAM] Periodic sync uploaded to {self.remote_csv}", "INFO")
                finally:
                    ftp.disconnect()
                if os.path.exists(temp_snapshot):
                    os.remove(temp_snapshot)
        except Exception as e:
            log(f"[FTP-STREAM] Warning during periodic upload: {e}", "WARNING")
        finally:
            with self.lock:
                self.is_uploading = False

    def shutdown(self, current_count: int = 0, file_handle=None):
        if file_handle:
            try:
                file_handle.flush()
                os.fsync(file_handle.fileno())
            except Exception:
                pass
        self.executor.shutdown(wait=True)
        if self.ftp_enabled and os.path.exists(self.local_csv) and os.path.getsize(self.local_csv) > 0:
            log(f"[FTP-STREAM] Performing final flush of remaining data to {self.remote_csv}...", "INFO")
            try:
                ftp = FTPManager(FTP_HOST, FTP_USER, FTP_PASS, FTP_PORT, FTP_PATH)
                try:
                    ftp.connect()
                    ftp.upload_file(self.local_csv, self.remote_csv)
                    if self.local_urls and os.path.exists(self.local_urls) and self.remote_urls:
                        ftp.upload_file(self.local_urls, self.remote_urls)
                    log(f"[FTP-STREAM] Final flush successfully uploaded to {self.remote_csv}", "INFO")
                finally:
                    ftp.disconnect()
            except Exception as e:
                log(f"[FTP-STREAM] Error during final flush: {e}", "ERROR")

def handle_interrupt(signum, frame):
    log(f"Process received signal {signum}. Flushing remaining data to FTP...", "WARNING")
    if uploader:
        uploader.shutdown(file_handle=csv_file_handle)
    sys.exit(128 + signum)

try:
    signal.signal(signal.SIGTERM, handle_interrupt)
    signal.signal(signal.SIGINT, handle_interrupt)
except Exception:
    pass

# ================= BYPASS & HTTP SESSION =================

class BypassSession:
    """Multi-tiered HTTP client designed to handle Cloudflare and WAF protections."""
    def __init__(self):
        self.proxy = PROXY_ENV
        self.flaresolverr_lock = threading.Lock()
        self.flaresolverr_session_id = None
        self.clearance_cookies = {}
        self.proxy_manager = None
        if not self.proxy and HAS_PROXY_MANAGER:
            try:
                self.proxy_manager = ProxyManager()
            except Exception:
                self.proxy_manager = None

        self.scraper = None
        if HAS_CLOUDSCRAPER:
            try:
                self.scraper = cloudscraper.create_scraper(
                    browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
                    delay=10
                )
            except Exception as e:
                log(f"Failed to initialize cloudscraper: {e}", "WARNING")

        self.session = requests.Session()
        self.browser_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Ch-Ua": '"Google Chrome";v="124", "Chromium";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "Connection": "keep-alive",
            "Referer": CANONICAL_URL + "/",
        }
        self.session.headers.update(self.browser_headers)

    def _get_proxies_dict(self):
        p = self.proxy
        if not p and self.proxy_manager:
            try:
                if hasattr(self.proxy_manager, 'get_proxy'):
                    p = self.proxy_manager.get_proxy(CANONICAL_URL)
            except Exception:
                p = None
        if p:
            return {"http": p, "https": p}
        return None

    def _rotate_proxy(self):
        if self.proxy_manager:
            if hasattr(self.proxy_manager, 'mark_proxy_failed') and self.proxy:
                self.proxy_manager.mark_proxy_failed(self.proxy)
            try:
                if hasattr(self.proxy_manager, 'get_proxy'):
                    self.proxy = self.proxy_manager.get_proxy(CANONICAL_URL)
                log(f"Rotated proxy to: {self.proxy}", "DEBUG")
            except Exception:
                pass

    def _is_challenge(self, text: str) -> bool:
        if not text:
            return False
        sample = text[:3000].lower()
        return ("<title>just a moment...</title>" in sample or 
                "challenge-platform" in sample or 
                "cf-turnstile" in sample or
                "human verification" in sample or
                "attention required" in sample or
                "access denied" in sample or
                "error 1020" in sample or
                "error 1015" in sample or
                "checking your browser" in sample or
                "please wait..." in sample)

    def _init_flaresolverr_session(self):
        if not FLARESOLVERR_URL:
            return
        endpoint = f"{FLARESOLVERR_URL}/v1" if not FLARESOLVERR_URL.endswith("/v1") else FLARESOLVERR_URL
        try:
            payload = {"cmd": "sessions.create"}
            if self.proxy:
                payload["proxy"] = {"url": self.proxy}
            res = requests.post(endpoint, json=payload, timeout=20)
            if res.status_code == 200:
                data = res.json()
                if data.get("status") == "ok":
                    self.flaresolverr_session_id = data.get("session")
                    log(f"Created FlareSolverr session: {self.flaresolverr_session_id}", "DEBUG")
        except Exception as e:
            log(f"Could not create persistent FlareSolverr session: {e}", "DEBUG")

    def _fetch_flaresolverr(self, url: str, timeout: int = 120) -> Optional[str]:
        if not FLARESOLVERR_URL:
            return None
        with self.flaresolverr_lock:
            # Check if clearance cookies were refreshed by another thread while waiting for lock
            if self.clearance_cookies.get("cf_clearance") and HAS_CURL_CFFI:
                try:
                    resp = cc_requests.get(
                        url,
                        headers=dict(self.browser_headers),
                        cookies=self.clearance_cookies,
                        impersonate="chrome124",
                        proxies=self._get_proxies_dict(),
                        timeout=20,
                        verify=False
                    )
                    if resp.status_code == 200 and not self._is_challenge(resp.text):
                        return resp.text
                except Exception:
                    pass

            endpoint = f"{FLARESOLVERR_URL}/v1" if not FLARESOLVERR_URL.endswith("/v1") else FLARESOLVERR_URL

            # Ensure session exists
            if not self.flaresolverr_session_id:
                self._init_flaresolverr_session()

            for attempt in range(2):
                try:
                    log(f"Requesting {url} via FlareSolverr (timeout={timeout}s, attempt={attempt+1})...", "DEBUG")
                    payload = {
                        "cmd": "request.get",
                        "url": url,
                        "maxTimeout": timeout * 1000,
                    }
                    if self.flaresolverr_session_id:
                        payload["session"] = self.flaresolverr_session_id
                    if self.proxy:
                        payload["proxy"] = {"url": self.proxy}

                    res = requests.post(endpoint, json=payload, timeout=timeout + 20)
                    if res.status_code == 200:
                        data = res.json()
                        if data.get("status") == "ok":
                            solution = data.get("solution", {})
                            resp_text = solution.get("response", "")

                            # Save cookies and userAgent for direct fast requests
                            cookies = solution.get("cookies", [])
                            for cookie in cookies:
                                c_name = cookie.get("name")
                                c_val = cookie.get("value")
                                if c_name and c_val:
                                    self.clearance_cookies[c_name] = c_val
                                    self.session.cookies.set(c_name, c_val, domain=cookie.get("domain", ""))

                            user_agent = solution.get("userAgent")
                            if user_agent:
                                self.browser_headers["User-Agent"] = user_agent
                                self.session.headers["User-Agent"] = user_agent

                            if solution.get("status") == 200 and not self._is_challenge(resp_text):
                                log(f"Success fetching {url} via FlareSolverr", "DEBUG")
                                return resp_text
                            else:
                                log(f"FlareSolverr returned status {solution.get('status')} or challenge page, resetting session", "WARNING")
                                self.flaresolverr_session_id = None
                        else:
                            log(f"FlareSolverr message: {data.get('message')}", "WARNING")
                            self.flaresolverr_session_id = None
                except Exception as e:
                    log(f"FlareSolverr attempt {attempt+1} error for {url}: {e}", "WARNING")
                    self.flaresolverr_session_id = None

                time.sleep(2)

            return None

    def fetch(self, url: str, is_json: bool = False, max_retries: int = 3, timeout: int = 25) -> Optional[str]:
        if not url or not isinstance(url, str) or not url.startswith("http"):
            return None

        proxies = self._get_proxies_dict()
        headers = dict(self.browser_headers)
        if is_json:
            headers.update({
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"{CANONICAL_URL}/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            })

        # Fast Strategy 1: Direct curl_cffi with Chrome TLS impersonation (with or without cf_clearance)
        if HAS_CURL_CFFI:
            for imp in ["chrome124", "chrome120"]:
                try:
                    resp = cc_requests.get(
                        url,
                        headers=headers,
                        cookies=self.clearance_cookies,
                        impersonate=imp,
                        proxies=proxies,
                        timeout=timeout,
                        verify=False,
                        allow_redirects=True
                    )
                    if resp.status_code == 200 and not self._is_challenge(resp.text):
                        log(f"Fast direct curl_cffi ({imp}) request succeeded for {url}", "DEBUG")
                        return resp.text
                    elif resp.status_code in [403, 503] or self._is_challenge(resp.text):
                        log(f"curl_cffi ({imp}) challenged/blocked on {url}", "DEBUG")
                        if "cf_clearance" in self.clearance_cookies:
                            self.clearance_cookies.pop("cf_clearance", None)
                        break
                except Exception as e:
                    log(f"curl_cffi ({imp}) error for {url}: {e}", "DEBUG")

        # Fast Strategy 2: If we have clearance cookies, try requests.Session
        if self.clearance_cookies.get("cf_clearance"):
            try:
                resp = self.session.get(url, headers=headers, proxies=proxies, timeout=15, verify=False, allow_redirects=True)
                if resp.status_code == 200 and not self._is_challenge(resp.text):
                    log(f"Fast direct session request succeeded for {url} using cf_clearance", "DEBUG")
                    return resp.text
                elif resp.status_code in [403, 503] or self._is_challenge(resp.text):
                    self.clearance_cookies.pop("cf_clearance", None)
            except Exception as e:
                log(f"Fast direct session request failed: {e}", "DEBUG")

        # Fallback Strategy: If FlareSolverr is configured and direct request was blocked or failed
        if FLARESOLVERR_URL:
            fs_timeout = max(timeout, FLARESOLVERR_TIMEOUT)
            content = self._fetch_flaresolverr(url, timeout=fs_timeout)
            if content:
                return content

        for attempt in range(max_retries):
            proxies = self._get_proxies_dict()

            # Strategy 1: curl_cffi with browser impersonation
            if HAS_CURL_CFFI:
                for imp in ["chrome124", "chrome120", "chrome110"]:
                    try:
                        resp = cc_requests.get(
                            url,
                            headers=headers,
                            impersonate=imp,
                            proxies=proxies,
                            timeout=timeout,
                            verify=False,
                            allow_redirects=True
                        )
                        if resp.status_code == 200 and not self._is_challenge(resp.text):
                            log(f"Success fetching {url} via curl_cffi ({imp})", "DEBUG")
                            return resp.text
                        elif resp.status_code in [403, 429, 503] or self._is_challenge(resp.text):
                            log(f"curl_cffi ({imp}) blocked (status {resp.status_code}) on {url}", "DEBUG")
                    except Exception as e:
                        log(f"curl_cffi ({imp}) error for {url}: {type(e).__name__}", "DEBUG")

            # Strategy 2: cloudscraper
            if self.scraper:
                try:
                    resp = self.scraper.get(url, headers=headers, proxies=proxies, timeout=timeout, allow_redirects=True)
                    if resp.status_code == 200 and not self._is_challenge(resp.text):
                        log(f"Success fetching {url} via cloudscraper", "DEBUG")
                        return resp.text
                    elif resp.status_code in [403, 429, 503] or self._is_challenge(resp.text):
                        log(f"cloudscraper blocked (status {resp.status_code}) for {url}", "DEBUG")
                except Exception as e:
                    log(f"cloudscraper error for {url}: {type(e).__name__}", "DEBUG")

            # Strategy 3: Standard requests with browser headers & proxy
            try:
                resp = self.session.get(url, headers=headers, proxies=proxies, timeout=timeout, verify=False, allow_redirects=True)
                if resp.status_code == 200 and not self._is_challenge(resp.text):
                    log(f"Success fetching {url} via requests", "DEBUG")
                    return resp.text
                elif resp.status_code in [403, 429, 503] or self._is_challenge(resp.text):
                    log(f"requests status {resp.status_code} for {url}", "WARNING")
                    self._rotate_proxy()
            except Exception as e:
                log(f"requests error for {url}: {type(e).__name__}", "WARNING")
                self._rotate_proxy()

            time.sleep(1.5 * (attempt + 1))

        return None

bypass_session = BypassSession()

def check_robots_txt() -> Tuple[Optional[float], Optional[str]]:
    """Check robots.txt once for crawl-delay and sitemap directive"""
    robots_url = f"{CANONICAL_URL}/robots.txt"
    log(f"Checking robots.txt: {robots_url}")
    content = bypass_session.fetch(robots_url, timeout=25)
    crawl_delay = None
    sitemap_url = None
    if content:
        s_match = re.search(r'Sitemap:\s*(https?://[^\s\r\n<]+)', content, re.IGNORECASE)
        if s_match:
            sitemap_url = s_match.group(1).strip()
            log(f"Found valid sitemap in robots.txt: {sitemap_url}")
        cd_match = re.search(r'crawl-delay:\s*(\d+(?:\.\d+)?)', content, re.IGNORECASE)
        if cd_match:
            try:
                crawl_delay = float(cd_match.group(1))
                log(f"Found Crawl-delay: {crawl_delay} seconds")
            except ValueError:
                pass
    return crawl_delay, sitemap_url

# ================= XML & DATA LAYER PARSING =================

def _is_likely_sitemap_xml(text: Optional[str]) -> bool:
    """Check if the response text contains recognizable sitemap or XML indicators."""
    if not text or not isinstance(text, str):
        return False
    lower = text.lower()
    return any(marker in lower for marker in [
        "<loc", "&lt;loc",
        "<sitemap", "&lt;sitemap",
        "<urlset", "&lt;urlset",
        "<sitemapindex", "&lt;sitemapindex",
        "webkit-xml-viewer",
        ".xml",
        "<?xml", "&lt;?xml",
        "<url", "&lt;url",
    ])

def load_xml(url: str, crawl_delay=None, max_retries: int = 3) -> Optional[ET.Element]:
    """
    Load XML sitemap with fast direct curl_cffi Chrome impersonation first,
    FlareSolverr fallback for Cloudflare Turnstile/CAPTCHA challenges,
    unwrap handling for Chrome XML viewer HTML wrapper, and robust regex fallback.
    """
    if not url or not isinstance(url, str):
        log(f"Invalid sitemap URL passed to load_xml: {url}", "ERROR")
        return None

    # Consider canonical URL alternatives if needed
    candidate_urls = [url]
    if "://www." in url:
        candidate_urls.append(url.replace("://www.", "://"))
    elif "://" in url:
        candidate_urls.append(url.replace("://", "://www."))

    data = None
    for target_url in candidate_urls:
        for attempt in range(max_retries):
            log(f"Loading XML from {target_url} (attempt {attempt + 1})...", "DEBUG")

            # Step 1: Fast direct curl_cffi attempt (bypasses Cloudflare TLS fingerprinting directly)
            if HAS_CURL_CFFI:
                try:
                    resp = cc_requests.get(
                        target_url,
                        headers=dict(bypass_session.browser_headers),
                        cookies=bypass_session.clearance_cookies,
                        impersonate="chrome124",
                        proxies=bypass_session._get_proxies_dict(),
                        timeout=30,
                        verify=False,
                        allow_redirects=True
                    )
                    if resp.status_code == 200 and not bypass_session._is_challenge(resp.text) and _is_likely_sitemap_xml(resp.text):
                        log(f"Direct curl_cffi successfully loaded XML from {target_url} ({len(resp.text):,} bytes)", "DEBUG")
                        data = resp.text
                        url = target_url
                        break
                    elif resp.status_code in [403, 503] or bypass_session._is_challenge(resp.text):
                        log(f"Direct curl_cffi challenged on {target_url} (status {resp.status_code})", "DEBUG")
                except Exception as e:
                    log(f"Direct curl_cffi error for {target_url}: {e}", "DEBUG")

            # Step 2: Use bypass_session.fetch (which invokes FlareSolverr if direct request was challenged)
            if not data:
                data = bypass_session.fetch(target_url, is_json=False, timeout=FLARESOLVERR_TIMEOUT)

            # Step 3: If FlareSolverr solved challenge (obtained cf_clearance) but returned HTML-wrapped or empty XML,
            # re-request the raw XML via curl_cffi using the freshly acquired clearance cookies!
            if (not data or not _is_likely_sitemap_xml(data)) and bypass_session.clearance_cookies.get("cf_clearance") and HAS_CURL_CFFI:
                try:
                    resp = cc_requests.get(
                        target_url,
                        headers=dict(bypass_session.browser_headers),
                        cookies=bypass_session.clearance_cookies,
                        impersonate="chrome124",
                        proxies=bypass_session._get_proxies_dict(),
                        timeout=30,
                        verify=False,
                        allow_redirects=True
                    )
                    if resp.status_code == 200 and not bypass_session._is_challenge(resp.text) and _is_likely_sitemap_xml(resp.text):
                        log(f"curl_cffi with fresh cf_clearance loaded XML from {target_url} ({len(resp.text):,} bytes)", "DEBUG")
                        data = resp.text
                        url = target_url
                        break
                except Exception:
                    pass

            if _is_likely_sitemap_xml(data):
                url = target_url
                break

            log(f"Sitemap data from {target_url} missing XML content, retrying...", "WARNING")
            bypass_session.flaresolverr_session_id = None
            time.sleep(2 * (attempt + 1))

        if _is_likely_sitemap_xml(data):
            break

    if not data:
        log(f"Failed to load XML from {url}", "ERROR")
        return None

    raw_data = data
    try:
        # Check if Chrome wrapped XML in an HTML viewer (common with FlareSolverr)
        if '<div id="webkit-xml-viewer-source-xml"' in data:
            try:
                soup = BeautifulSoup(data, 'html.parser')
                div = soup.find('div', id='webkit-xml-viewer-source-xml')
                if div:
                    inner_html = div.decode_contents().strip()
                    if inner_html:
                        data = inner_html
            except Exception as e_bs:
                log(f"BeautifulSoup webkit viewer extraction note: {e_bs}", "DEBUG")
                match = re.search(r'<div id="webkit-xml-viewer-source-xml"[^>]*>([\s\S]*?)(?:</div>\s*</body>|</div>\s*</html>|\Z)', data, re.IGNORECASE)
                if match:
                    data = match.group(1).strip()
        elif '<pre' in data and '</pre>' in data:
            try:
                soup = BeautifulSoup(data, 'html.parser')
                pre = soup.find('pre')
                if pre:
                    data = pre.get_text().strip()
            except Exception:
                match = re.search(r'<pre[^>]*>([\s\S]*?)</pre>', data, re.IGNORECASE)
                if match:
                    data = match.group(1).strip()

        # Unescape HTML entities if wrapped
        if "&lt;" in data and "&gt;" in data:
            data = html.unescape(data)

        # Remove default XML namespace to make queries simple and direct
        data_clean = re.sub(r'\sxmlns="[^"]+"', '', data, count=1)

        # Trim any HTML envelope before XML root
        start_idx = data_clean.find("<?xml")
        if start_idx == -1:
            start_idx = data_clean.find("<sitemapindex")
        if start_idx == -1:
            start_idx = data_clean.find("<urlset")
        if start_idx != -1:
            data_clean = data_clean[start_idx:]

        # Trim any trailing content after root closing tag
        for close_tag in ["</sitemapindex>", "</urlset>"]:
            end_idx = data_clean.rfind(close_tag)
            if end_idx != -1:
                data_clean = data_clean[:end_idx + len(close_tag)]
                break

        # Clean XML declaration if needed
        if "<?xml" not in data_clean[:100] and not data_clean.strip().startswith("<"):
            data_clean = '<?xml version="1.0" encoding="UTF-8"?>\n' + data_clean

        # Prefer lxml if available (faster and more forgiving)
        try:
            import lxml.etree as letree
            return letree.fromstring(data_clean.encode('utf-8'))
        except Exception:
            return ET.fromstring(data_clean)

    except Exception as e:
        log(f"XML parsing failed for {url}: {e}, attempting regex extraction", "WARNING")
        # Try to extract URLs with regex fallback on raw_data
        try:
            clean_text = html.unescape(raw_data)
            # Find <loc>...</loc>
            urls = re.findall(r'<loc[^>]*>\s*(https?://[^<\s]+)\s*</loc>', clean_text, re.IGNORECASE)
            if not urls:
                # Find href="..." with .xml (Chromium XML viewer converts loc to <a href="...xml">)
                urls = re.findall(r'href=["\'](https?://[^"\'\s]+\.xml)["\']', clean_text, re.IGNORECASE)
            if not urls:
                # Find any sitemap URLs
                urls = re.findall(r'https?://[^\s<"\'\\]+sitemap[^\s<"\'\\]*\.xml', clean_text, re.IGNORECASE)
            if not urls:
                # Fallback: find any valid http URLs ending with .xml or .html
                urls = re.findall(r'https?://[^\s<"\'\\]+\.(?:xml|html)', clean_text, re.IGNORECASE)
            if not urls:
                log("Regex extraction found no valid URLs in sitemap data", "ERROR")
                return None

            urls = list(dict.fromkeys(u.strip() for u in urls if u and u.strip()))
            is_index = any(".xml" in u.lower() for u in urls)
            root = ET.Element("sitemapindex" if is_index else "urlset")
            for url_text in urls:
                sub_elem = ET.SubElement(root, "sitemap" if is_index else "url")
                loc_elem = ET.SubElement(sub_elem, "loc")
                loc_elem.text = url_text.strip()
            log(f"Regex extraction successfully found {len(urls)} URLs in {url}")
            return root
        except Exception as e2:
            log(f"Regex extraction also failed: {e2}", "ERROR")
            return None

def _clean_strings(obj):
    """Recursively clean JSON-escaped strings like \\/"""
    if isinstance(obj, dict):
        return {k: _clean_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_strings(v) for v in obj]
    if isinstance(obj, str):
        return obj.replace('\\/', '/')
    return obj

def extract_datalayer(html_text):
    patterns = [
        r'dataLayer\.push\s*\(\s*(\{[\s\S]*?\})\s*\);',
    ]
    raw = None
    for pattern in patterns:
        match = re.search(pattern, html_text)
        if match:
            raw = match.group(1)
            break
    if not raw:
        return None

    raw = html.unescape(raw)
    raw = raw.replace(':true', ':True') \
             .replace(':false', ':False') \
             .replace(':null', ':None') \
             .replace('true,', 'True,') \
             .replace('false,', 'False,') \
             .replace('null,', 'None,')

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        try:
            if raw.strip().startswith('{'):
                raw = f'[{raw}]'
            data = ast.literal_eval(raw)
        except (SyntaxError, ValueError):
            raw = re.sub(r',\s*}', '}', raw)
            raw = re.sub(r',\s*]', ']', raw)
            try:
                data = json.loads(raw)
            except Exception:
                return None

    return _clean_strings(data)

def extract_additional_product_info(html_text):
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        table = soup.find('table', id='product-attribute-specs-table')
        if not table:
            table = soup.find('table', class_='additional-attributes')
            if not table:
                return json.dumps({})

        additional_info = {}
        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else table.find_all('tr')
        for row in rows:
            th = row.find('th')
            td = row.find('td')
            if th and td:
                label_text = th.get_text(strip=True)
                data_text = td.get_text(strip=True)
                if label_text and data_text:
                    json_key = re.sub(r'[^a-zA-Z0-9_]', '_', label_text.lower().replace(' ', '_'))
                    json_key = json_key.strip('_')
                    additional_info[json_key] = data_text

        return json.dumps(additional_info, ensure_ascii=False)
    except Exception as e:
        log(f"Error while processing additional Data: {e}", "DEBUG")
        return json.dumps({})

def fetch_json(url: str, crawl_delay=None, check_is_pdp_only: bool = False) -> Optional[dict]:
    """Fetch product page HTML using bypass engine and extract dataLayer + additional product info"""
    data = bypass_session.fetch(url, is_json=False, timeout=25)
    if not data:
        return None
    try:
        data_layer = extract_datalayer(data)
        if not data_layer:
            return None

        product_data = data_layer[0] if isinstance(data_layer, list) else data_layer
        is_pdp = product_data.get("ecommerce", {}).get("isPDP", None)

        if check_is_pdp_only:
            return {"isPDP": is_pdp}

        if is_pdp == 0:
            return None

        additional_info = extract_additional_product_info(data)
        product_data["additional_product_info_html"] = additional_info
        product_data["_raw_html"] = data
        return product_data
    except json.JSONDecodeError as e:
        log(f"JSON decode error for {url}: {e}", "DEBUG")
        return None
    except Exception as e:
        log(f"Error processing data for {url}: {e}", "DEBUG")
        return None

# ================= PRODUCT PROCESSING =================

csv_lock = threading.Lock()

def normalize_image_url(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    elif url.startswith("/"):
        return f"{CANONICAL_URL}{url}"
    elif not url.startswith("http"):
        return f"https://{url}"
    return url

def extract_product_data(product_data: dict) -> dict:
    try:
        # Product ID
        product_id = ''
        if isinstance(product_data.get('ecomm_prodid'), list) and product_data.get('ecomm_prodid'):
            product_id = str(product_data.get('ecomm_prodid')[0])
        elif product_data.get('ecomm_prodid'):
            product_id = str(product_data.get('ecomm_prodid'))

        if not product_id:
            product_id = str(product_data.get('product', {}).get('id', '')).strip()
        if not product_id:
            product_id = str(product_data.get('magentoProductId', '')).strip()

        # Product Name
        ecommerce_items = product_data.get('ecommerce', {}).get('items', [])
        name = ''
        if ecommerce_items:
            name = ecommerce_items[0].get('item_name', '').strip()
        if not name:
            name = product_data.get('product', {}).get('name', '').strip()
        if not name:
            name = product_data.get('magentoProductName', '').strip()

        # SKU
        sku = product_data.get('ecomm_prodsku', '')
        if not sku:
            sku = product_data.get('product', {}).get('sku', '')
        if not sku:
            sku = product_data.get('magentoProductSku', '')

        # Resolve ROOM / HOST if present in SKU
        if 'ROOM' in sku.upper() or 'HOST' in sku.upper():
            for ec_item in ecommerce_items:
                cand = str(ec_item.get('item_sku') or ec_item.get('sku') or '').strip()
                if cand and 'ROOM' not in cand.upper() and 'HOST' not in cand.upper():
                    sku = cand
                    break

        # Brand & Quantity
        brand = ''
        quantity = 0
        if ecommerce_items:
            brand = ecommerce_items[0].get('item_brand', '')
            quantity = ecommerce_items[0].get('quantity', 0)
        if not brand:
            brand = product_data.get('product', {}).get('brand', '')

        # Price
        price = ''
        if ecommerce_items:
            price_item = ecommerce_items[0].get('price', '')
            if price_item:
                price = str(price_item)
        if not price:
            ecomm_value = product_data.get('ecommerce', {}).get('value', '')
            if ecomm_value:
                price = str(ecomm_value)
        if not price:
            price = str(product_data.get('magentoProductPrice', ''))

        # Main Image
        main_image = product_data.get('magentoProductImage1', '')
        if not main_image:
            main_image = product_data.get('product', {}).get('image', '')

        # Additional data & MPN
        additional_data = product_data.get('additional_product_info_html', '')
        raw_html = product_data.get('_raw_html', '')
        mpn = sku
        category = ''

        try:
            additional_info_dict = json.loads(additional_data) if additional_data else {}
            item_number = additional_info_dict.get('item_number', '').strip()
            if item_number:
                mpn = item_number
            category = additional_info_dict.get('product_type', '').strip()

            if not item_number and raw_html:
                soup = BeautifulSoup(raw_html, "html.parser")
                for label in soup.select("div.label"):
                    if label.get_text(strip=True).lower() == "item number":
                        data_div = label.find_next_sibling("div", class_="data")
                        if data_div:
                            html_item_number = data_div.get_text(strip=True)
                            if html_item_number:
                                mpn = html_item_number
                                break
        except Exception as e:
            log(f"Error setting mpn or category : {e}", "DEBUG")

        if ('ROOM' in sku.upper() or 'HOST' in sku.upper()) and mpn and 'ROOM' not in mpn.upper() and 'HOST' not in mpn.upper():
            sku = mpn

        # Category
        category_url = ''
        if ecommerce_items and not category:
            category_fields = [
                'item_category', 'item_category2', 'item_category3',
                'item_category4', 'item_category5', 'item_category6',
                'item_category7', 'item_category8', 'item_category9'
            ]
            categories = []
            for field in category_fields:
                cat_value = ecommerce_items[0].get(field, '')
                if cat_value:
                    categories.append(cat_value)
            if categories:
                category = ' | '.join(categories)

        # Stock Status
        availability = product_data.get('ecommerce', {}).get('magentoProductAvailability', '')
        if not availability:
            availability = product_data.get('magentoProductAvailability', '')
        status = 'OUT_OF_STOCK'
        if availability == 'InStock':
            status = 'SELLABLE'

        variation_id = ''
        group_attr_1 = ''
        group_attr_2 = ''

        return {
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
            'group_attr_1': group_attr_1,
            'group_attr_2': group_attr_2,
            'additional_data': additional_data,
        }

    except Exception as e:
        log(f"Error extracting product data: {e}", "ERROR")
        return {}

def process_product_data(product_url: str, writer, seen: set, stats: dict, crawl_delay=None):
    if product_url in seen:
        return
    seen.add(product_url)

    log(f"Processing product URL: {product_url}", "DEBUG")
    data = fetch_json(product_url, crawl_delay)

    if not data:
        stats['errors'] += 1
        log(f"No data found for product {product_url}", "WARNING")
        return

    product_info = extract_product_data(data)
    if not product_info.get('product_id'):
        stats['errors'] += 1
        log(f"Invalid data for product {product_url}", "WARNING")
        return

    try:
        row = [
            product_url,
            product_info['product_id'],
            product_info['variation_id'],
            product_info['category'],
            product_info['category_url'],
            product_info['brand'],
            product_info['name'],
            product_info['sku'],
            product_info['mpn'],
            '',
            product_info['price'],
            normalize_image_url(product_info['main_image']),
            product_info['quantity'],
            product_info['group_attr_1'],
            product_info['group_attr_2'],
            product_info['status'],
            product_info['additional_data'],
            SCRAPED_DATE
        ]

        with csv_lock:
            writer.writerow(row)
            stats['products_fetched'] += 1
            if uploader:
                uploader.on_item_scraped(stats['products_fetched'], file_handle=csv_file_handle)

        if HAS_DB_QUEUE:
            try:
                save_scraped_product("Emma Mason", 16060, {
                    "url": row[0], "product_id": row[1], "variant_id": row[2],
                    "category": row[3], "category_url": row[4], "brand": row[5],
                    "name": row[6], "sku": row[7], "mpn": row[8], "gtin": row[9],
                    "price": row[10], "image": row[11], "quantity": row[12],
                    "group_attr_1": row[13], "group_attr_2": row[14],
                    "status": row[15], "additional_data": row[16], "date_scraped": row[17]
                })
            except Exception:
                pass

        log(f"Fetched product {product_info['product_id']}: {product_info['name'][:50]}...", "INFO")

    except Exception as e:
        log(f"Error creating row for product {product_info.get('product_id', 'unknown')}: {e}", "ERROR")
        stats['errors'] += 1

    time.sleep(REQUEST_DELAY_BASE)
    stats['urls_processed'] += 1

def collect_all_sitemap_urls(sitemap: Optional[str] = None, output_file: Optional[str] = None, crawl_delay: Optional[float] = None) -> List[str]:
    """
    Check all sitemaps, extract product counts per sitemap, log a breakdown,
    and persist the complete list of unique product URLs to output_file.
    """
    if not sitemap:
        _, robots_sitemap = check_robots_txt()
        sitemap = SITEMAP_URL or robots_sitemap or f"{CANONICAL_URL}/sitemap.xml"

    log("=" * 60)
    log("EMMA MASON SITEMAP PRODUCT DISCOVERY & INVENTORY")
    log(f"Sitemap Index: {sitemap}")
    log(f"FlareSolverr URL: {FLARESOLVERR_URL or 'None'}")
    log("=" * 60)

    index = load_xml(sitemap, crawl_delay)
    if index is None:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)

    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    child_sitemaps = []

    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
        if elements:
            child_sitemaps = [e.text.strip() for e in elements if e.text and ".xml" in e.text.lower() and "blog" not in e.text.lower()]
            if child_sitemaps:
                break

    if not child_sitemaps:
        all_locs = [e.text.strip() for e in index.findall(".//loc") if e.text]
        xml_locs = [u for u in all_locs if ".xml" in u.lower()]
        if xml_locs:
            child_sitemaps = xml_locs
        elif all_locs:
            child_sitemaps = [sitemap]
        else:
            child_sitemaps = [sitemap]

    child_sitemaps = list(dict.fromkeys(child_sitemaps))
    log(f"Discovered {len(child_sitemaps)} child sitemaps in index.")

    sitemap_counts: Dict[str, int] = {}
    all_urls: List[str] = []
    lock = threading.Lock()

    def process_child_sitemap(sm_url: str):
        xml = load_xml(sm_url, crawl_delay)
        if not xml:
            log(f"Failed to load child sitemap: {sm_url}", "WARNING")
            with lock:
                sitemap_counts[sm_url] = 0
            return

        urls = []
        for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
            elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
            if elements:
                urls = [
                    e.text.strip()
                    for e in elements
                    if e.text
                    and not any(ext in e.text.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg', '.pdf', '.css', '.js'])
                    and ".xml" not in e.text.lower()
                    and '~' not in e.text
                    and not any(x in e.text.lower() for x in ['/blog', '/category', '/catalogsearch', '/checkout', '/customer', '/brands'])
                ]
                html_urls = [u for u in urls if '.html' in u.lower()]
                if html_urls:
                    urls = html_urls
                urls = list(dict.fromkeys(urls))
                break

        with lock:
            sitemap_counts[sm_url] = len(urls)
            all_urls.extend(urls)

    fetch_workers = min(12, max(2, len(child_sitemaps)))
    log(f"Checking {len(child_sitemaps)} child sitemaps with {fetch_workers} workers...")
    with ThreadPoolExecutor(max_workers=fetch_workers) as executor:
        futures = [executor.submit(process_child_sitemap, sm) for sm in child_sitemaps]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as ex:
                log(f"Error processing child sitemap: {ex}", "ERROR")

    unique_urls = list(dict.fromkeys(all_urls))

    log("=" * 60)
    log("SITEMAP INVENTORY & PRODUCT COUNT SUMMARY")
    log("=" * 60)
    for idx, sm_url in enumerate(child_sitemaps, start=1):
        count = sitemap_counts.get(sm_url, 0)
        log(f"[{idx:02d}/{len(child_sitemaps):02d}] {sm_url} -> {count:,} product URLs")
    log("-" * 60)
    log(f"Total Sitemaps Checked: {len(child_sitemaps)}")
    log(f"Total Discovered URLs: {len(all_urls):,}")
    log(f"Total Unique Product URLs: {len(unique_urls):,}")
    log("=" * 60)

    target_file = output_file or MASTER_URLS_FILE
    with open(target_file, "w", encoding="utf-8") as f:
        f.write("\n".join(unique_urls))
    log(f"Saved {len(unique_urls):,} unique product URLs to {target_file}")

    return unique_urls

# ================= MAIN =================

def main():
    crawl_delay, robots_sitemap = check_robots_txt()
    sitemap = SITEMAP_URL or robots_sitemap or f"{CANONICAL_URL}/sitemap.xml"
    log(f"Target sitemap index: {sitemap}")

    if COLLECT_URLS_ONLY or "--collect-urls" in sys.argv:
        collect_all_sitemap_urls(sitemap=sitemap, output_file=MASTER_URLS_FILE, crawl_delay=crawl_delay)
        sys.exit(0)

    if crawl_delay:
        if crawl_delay > 30:
            log(f"Crawl-delay {crawl_delay}s is too high, capping at 30s")
            crawl_delay = 30
        log(f"Respecting crawl-delay: {crawl_delay} seconds between requests")
    else:
        log(f"Using default request delay: {REQUEST_DELAY_BASE} seconds")

    log("=" * 60)
    log("Emma Mason Parallel Scraper")
    log(f"FlareSolverr URL: {FLARESOLVERR_URL or 'None'}")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Base URL: {CANONICAL_URL}")
    log(f"Chunk Index: {CHUNK_INDEX}")
    log(f"Total Chunks: {TOTAL_CHUNKS if TOTAL_CHUNKS > 0 else 'N/A'}")
    log(f"URLs File: {URLS_FILE if URLS_FILE else 'None (Sitemap-driven)'}")
    log(f"Max Workers: {MAX_WORKERS}")
    log(f"Request Delay: {REQUEST_DELAY_BASE}s")
    log(f"curl_cffi Available: {HAS_CURL_CFFI}")
    log(f"cloudscraper Available: {HAS_CLOUDSCRAPER}")
    log(f"Proxy Configured: {'Yes' if PROXY_ENV else 'No'}")
    log("=" * 60)

    global csv_file_handle, uploader

    # Initialize periodic FTP streaming
    uploader = PeriodicFTPUploader(
        local_csv=OUTPUT_CSV,
        remote_csv=REMOTE_TEMP_CSV,
        local_urls=URL_LIST_FILE,
        remote_urls=REMOTE_TEMP_URLS,
        sync_items=FTP_SYNC_ITEMS
    )

    stats = {
        'sitemaps_processed': 0,
        'urls_processed': 0,
        'products_fetched': 0,
        'errors': 0
    }
    all_product_urls: list = []

    try:
        # Initialize CSV
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            csv_file_handle = f
            writer = csv.writer(f)

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
                "Additional Product Data",
                "Date Scrapped"
            ])
            f.flush()

            seen = set()

            # Mode 1: Pre-partitioned URL list (evenly balanced across runners)
            if URLS_FILE and os.path.exists(URLS_FILE):
                log(f"Loading pre-partitioned URLs from {URLS_FILE}")
                with open(URLS_FILE, "r", encoding="utf-8") as uf:
                    assigned_urls = [line.strip() for line in uf if line.strip() and line.strip().startswith("http")]

                if MAX_URLS_PER_SITEMAP > 0:
                    assigned_urls = assigned_urls[:MAX_URLS_PER_SITEMAP]

                all_product_urls = list(assigned_urls)
                if HAS_DB_QUEUE:
                    init_c = len(all_product_urls)
                    all_product_urls = filter_pending_urls("Emma Mason", all_product_urls, competitor_id=16060)
                    log(f"MySQL DB Queue check: {init_c - len(all_product_urls):,} URLs already completed in DB. Remaining pending: {len(all_product_urls):,}")

                log(f"Product URLs assigned to this chunk: {len(all_product_urls):,}")

                try:
                    with open(URL_LIST_FILE, "w", encoding="utf-8") as _uf:
                        _uf.write("\n".join(all_product_urls))
                    if uploader:
                        uploader.trigger_sync()
                except Exception as _uex:
                    log(f"[URL-LIST] Early write note: {_uex}", "DEBUG")

                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [
                        executor.submit(process_product_data, url, writer, seen, stats, crawl_delay)
                        for url in all_product_urls
                    ]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            log(f"Error in thread execution: {e}", "ERROR")
                            stats['errors'] += 1

                gc.collect()

            # Mode 2: Legacy Sitemap-offset processing (fallback)
            else:
                log(f"Loading sitemap index from {sitemap}")
                index = load_xml(sitemap, crawl_delay)
                if index is None:
                    log("Failed to load sitemap index", "ERROR")
                    sys.exit(1)

                ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
                sitemaps = []

                for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
                    elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
                    if elements:
                        sitemaps = [e.text.strip() for e in elements if e.text and ".xml" in e.text.lower() and "blog" not in e.text.lower()]
                        if sitemaps:
                            break

                if not sitemaps:
                    all_locs = [e.text.strip() for e in index.findall(".//loc") if e.text]
                    xml_locs = [u for u in all_locs if ".xml" in u.lower()]
                    if xml_locs:
                        sitemaps = xml_locs
                    elif all_locs:
                        log("Sitemap appears to be a direct URL set rather than an index of sitemaps")
                        sitemaps = [sitemap]
                    else:
                        sitemaps = [sitemap]

                sitemaps = list(dict.fromkeys(sitemaps))

                if SITEMAP_OFFSET >= len(sitemaps):
                    log(f"Offset {SITEMAP_OFFSET} exceeds total sitemaps ({len(sitemaps)})", "WARNING")
                    sys.exit(0)

                end_index = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemaps)
                sitemaps_to_process = sitemaps[SITEMAP_OFFSET:end_index]

                log(f"Total sitemaps found: {len(sitemaps)}")
                log(f"Sitemaps to process: {len(sitemaps_to_process)}")

                for sitemap_url in sitemaps_to_process:
                    stats['sitemaps_processed'] += 1
                    log(f"Processing sitemap {stats['sitemaps_processed']}/{len(sitemaps_to_process)}: {sitemap_url}")

                    xml = load_xml(sitemap_url, crawl_delay)
                    if not xml:
                        log(f"Failed to load sitemap: {sitemap_url}", "ERROR")
                        continue

                    urls = []
                    for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
                        elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
                        if elements:
                            urls = [
                                e.text.strip()
                                for e in elements
                                if e.text
                                and not any(ext in e.text.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg', '.pdf', '.css', '.js'])
                                and not ".xml" in e.text.lower()
                                and '~' not in e.text
                                and not any(x in e.text.lower() for x in ['/blog', '/category', '/catalogsearch', '/checkout', '/customer', '/brands'])
                            ]
                            html_urls = [u for u in urls if '.html' in u.lower()]
                            if html_urls:
                                urls = html_urls
                            urls = list(dict.fromkeys(urls))
                            if urls:
                                all_product_urls.extend(urls)
                                try:
                                    with open(URL_LIST_FILE, "w", encoding="utf-8") as _uf:
                                        _uf.write("\n".join(all_product_urls))
                                    if uploader:
                                        uploader.trigger_sync()
                                except Exception as _uex:
                                    log(f"[URL-LIST] Early write note: {_uex}", "DEBUG")
                                break

                    if not urls:
                        log(f"No product URLs found in sitemap: {sitemap_url}", "WARNING")
                        continue

                    if MAX_URLS_PER_SITEMAP > 0:
                        original_count = len(urls)
                        urls = urls[:MAX_URLS_PER_SITEMAP]
                        log(f"Limited to {len(urls)} out of {original_count} URLs")
                    else:
                        log(f"Found {len(urls)} product URLs in this sitemap")

                    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        futures = [
                            executor.submit(process_product_data, url, writer, seen, stats, crawl_delay)
                            for url in urls
                        ]
                        for future in as_completed(futures):
                            try:
                                future.result()
                            except Exception as e:
                                log(f"Error in thread execution: {e}", "ERROR")
                                stats['errors'] += 1

                    gc.collect()
    finally:
        # Guarantee that whatever data was scraped is pushed to FTP before exiting
        if uploader:
            uploader.shutdown(current_count=stats['products_fetched'], file_handle=csv_file_handle)
        csv_file_handle = None

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

    # Save per-chunk URL list for artifact upload
    if all_product_urls:
        try:
            with open(URL_LIST_FILE, "w", encoding="utf-8") as uf:
                uf.write("\n".join(all_product_urls))
            log(f"[URL-LIST] Saved {len(all_product_urls)} URLs to {URL_LIST_FILE}")
        except Exception as exc:
            log(f"[URL-LIST] Warning: {exc}", "WARNING")

if __name__ == "__main__":
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if not CURR_URL:
        log("Error: CURR_URL environment variable is required", "ERROR")
        sys.exit(1)

    main()
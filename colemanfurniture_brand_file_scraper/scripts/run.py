import os
import sys
import argparse
import logging
import csv
import json

# Configure logging similar to run_ashley.py
logger = logging.getLogger("app")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Suppress scrapy and twisted logging only for critical errors
logging.getLogger("scrapy").setLevel(logging.WARNING)
logging.getLogger("twisted").setLevel(logging.WARNING)

logging.getLogger("twisted").propagate = False
logging.getLogger("scrapy.core.engine").setLevel(logging.WARNING)
logging.getLogger("scrapy.dupefilter").setLevel(logging.WARNING)
logging.getLogger("scrapy.downloadermiddlewares").setLevel(logging.WARNING)

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
# Add repo root for db_queue
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

try:
    from db_queue import filter_pending_urls, save_scraped_product
    HAS_DB_QUEUE = True
except Exception:
    HAS_DB_QUEUE = False

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from fetcher.product_fetcher import ProductFetcher

def load_urls_from_file(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"URLs file not found: {file_path}")

    if file_path.lower().endswith(".json"):
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            urls = data.get("urls", [])
        elif isinstance(data, list):
            urls = data
        else:
            urls = []
        return [str(u).strip() for u in urls if str(u).strip()]

    if file_path.lower().endswith(".csv"):
        urls = []
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = (
                    row.get("url")
                    or row.get("Ref Product URL")
                    or row.get("URL")
                    or ""
                ).strip()
                if url:
                    urls.append(url)
        return urls

    urls = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if url:
                urls.append(url)
    return urls

def main():
    parser = argparse.ArgumentParser(description='Run Coleman and Homegallerystores product scraper')
    
    parser.add_argument('--website-url', required=True, 
                       help='Website URL to scrape (e.g., https://colemanfurniture.com)')
    sitemap_offset = os.getenv('SITEMAP_OFFSET', '0')
    max_sitemaps = os.getenv('MAX_SITEMAPS', '0')
    max_urls_per_sitemap = os.getenv('MAX_URLS_PER_SITEMAP', '0')
    max_workers = os.getenv('MAX_WORKERS', '32')
    job_id = os.getenv('GITHUB_JOB', '')
    parser.add_argument('--sitemap-offset', type=int, default=int(sitemap_offset),
                       help='Offset for sitemap processing')
    parser.add_argument('--max-sitemaps', type=int, default=int(max_sitemaps),
                       help='Maximum sitemaps to process (0 for all)')
    parser.add_argument('--max-urls-per-sitemap', type=int, default=int(max_urls_per_sitemap),
                       help='Maximum URLs per sitemap (0 for all)')
    parser.add_argument('--job-id', default='job_id',
                       help='Job identifier for output file')
    parser.add_argument('--output-dir', default='output',
                       help='Output directory for CSV files')
    parser.add_argument('--verbose', action='store_true', default=False,
                       help='Enable verbose logging with progress updates')
    parser.add_argument('--urls-file', default='',
                       help='Optional file (csv/json/txt) with URLs for direct retry mode')
    
    args = parser.parse_args()
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    settings = get_project_settings()
    
    timestamp = os.getenv('GITHUB_RUN_ID', 'local')
    domain = args.website_url.replace('https://', '').replace('http://', '').split('/')[0].replace('.', '_')
    output_file = f'{args.output_dir}/output_{domain}_{args.job_id}_{timestamp}.csv'
    settings.set('FEED_URI', output_file)
    settings.set('FEED_FORMAT', 'csv')
    
    max_workers = int(os.getenv('MAX_WORKERS', '16'))
    settings.set('CONCURRENT_REQUESTS', max_workers)
    
    download_delay = float(os.getenv('DOWNLOAD_DELAY', '0.1'))
    settings.set('DOWNLOAD_DELAY', download_delay)
    settings.set('ROBOTSTXT_OBEY', False)
    settings.set('COOKIES_ENABLED', True)
    settings.set('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
    
    # Configure logging based on verbose flag
    if args.verbose:
        settings.set('LOG_LEVEL', 'INFO')
        settings.set('LOG_ENABLED', True)
        logger.info("✅ Verbose logging enabled - full progress will be shown")
    else:
        settings.set('LOG_LEVEL', 'WARNING')
        settings.set('LOG_ENABLED', True)
    
    settings.set('LOG_FILE', None)
    settings.set('FEED_EXPORT_FIELDS', [
        'Ref Product URL',
        'Ref Product ID', 
        'Ref Variant ID',
        'Ref Category',
        'Ref Category URL',
        'Product Type',
        'Ref Brand Name',
        'Collection Name',
        'Ref Product Name',
        'Ref SKU',
        'Ref MPN',
        'Ref GTIN',
        'Ref Price',
        'Ref Main Image',
        'Ref Quantity',
        'Ref Group Attr 1',
        'Ref Group Attr 2',
        'Ref Images',
        'Ref Dimensions',
        'Ref Status',
        'Ref Highlights',
        'Row JSON',
        'Date Scrapped'
    ])
    settings.set('DUPEFILTER_CLASS', 'scrapy.dupefilters.RFPDupeFilter')
    process = CrawlerProcess(settings)
    
    logger.info(f"🚀 Starting scraper for: {args.website_url}")
    logger.info(f"📁 Output will be saved to: {output_file}")
    logger.info(f"⚙️ Job parameters: offset={args.sitemap_offset}, max_sitemaps={args.max_sitemaps}, max_urls_per_sitemap={args.max_urls_per_sitemap}")
    logger.info(f"🔧 Concurrency: {max_workers} workers, delay={download_delay}s")

    if args.urls_file:
        input_urls = load_urls_from_file(args.urls_file)
        if not input_urls:
            logger.warning(f"No URLs found in urls-file: {args.urls_file}")
            logger.info(f"✅ Scraping completed. Output saved to: {output_file}")
            return output_file

        if HAS_DB_QUEUE:
            comp_name = "Home Gallery Stores" if "homegallery" in args.website_url.lower() else "Coleman Furniture"
            cid = 15177 if comp_name == "Home Gallery Stores" else 17028
            init_c = len(input_urls)
            input_urls = filter_pending_urls(comp_name, input_urls, competitor_id=cid)
            logger.info(f"MySQL DB Queue check: {init_c - len(input_urls):,} URLs already completed in DB. Remaining pending: {len(input_urls):,}")
            if not input_urls:
                logger.info(f"All {init_c:,} URLs already scraped in database! Exiting cleanly.")
                return output_file

        logger.info(f"🔁 Retry mode enabled with {len(input_urls)} URLs from: {args.urls_file}")
        process.crawl(
            ProductFetcher,
            website_url=args.website_url,
            ashley_urls=input_urls,
            is_ashley=True,
            chunk_mode=False,
            sitemap_offset=args.sitemap_offset,
            max_sitemaps=args.max_sitemaps,
            max_urls_per_sitemap=args.max_urls_per_sitemap,
            job_id=args.job_id,
            output_dir=args.output_dir,
            verbose=args.verbose
        )
    else:
        process.crawl(ProductFetcher,
                      website_url=args.website_url,
                      sitemap_offset=args.sitemap_offset,
                      max_sitemaps=args.max_sitemaps,
                      max_urls_per_sitemap=args.max_urls_per_sitemap,
                      job_id=args.job_id,
                      output_dir=args.output_dir,
                      verbose=args.verbose)
    process.start()
    logger.info(f"✅ Scraping completed. Output saved to: {output_file}")

    # Standardized FTP output and URL tracking generation
    try:
        root_dir = Path(__file__).resolve().parent.parent.parent
        if str(root_dir) not in sys.path:
            sys.path.insert(0, str(root_dir))
        import shutil
        from ftp_manager import FTPPathManager, generate_final_url_tracking

        pm = FTPPathManager("Coleman")
        paths = pm.prepare_local_structure("output")

        if os.path.exists(output_file):
            shutil.copy2(output_file, paths["final_scraped_data"])
            shutil.copy2(output_file, paths["temp_scraped_data"])
            logger.info(f"✓ Created FinalScrappedData: {paths['final_scraped_data']}")
            logger.info(f"✓ Created Temp ScrappedData: {paths['temp_scraped_data']}")

            # Generate FinalURLListForScrap.csv
            sitemap_input = paths["temp_url_list"] if os.path.exists(paths["temp_url_list"]) else args.urls_file
            if sitemap_input and os.path.exists(sitemap_input):
                res = generate_final_url_tracking(sitemap_input, paths["final_scraped_data"], paths["final_url_list"])
                logger.info(f"✓ Created FinalURLListForScrap: {paths['final_url_list']} ({res.get('scraped')} Scraped, {res.get('not_scraped')} Not Scraped)")
    except Exception as exc:
        logger.warning(f"Note: Standardized URL tracking post-processing error: {exc}")

    return output_file

if __name__ == '__main__':
    main()

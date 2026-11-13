#!/usr/bin/env python3
"""
Downloads completed chunk artifacts from Dhruv (Run 41969305887)
and ingests them into competitor_scraped_products & competitor_urls.
"""

import os
import glob
import json
import zipfile
import io
import requests
import pymysql
from sequential_bulk_import import DBImporter, import_scraped_file

def main():
    with open('config/accounts_config.json') as f:
        cfg = json.load(f)

    token = None
    for acc in cfg.values():
        if acc.get('owner') == 'DHRUVKUMAR-RAJYAGURU':
            token = acc.get('token')
            break

    if not token:
        print("Dhruv token not found.")
        return

    dest_dir = "output/discountlivingrooms/remaining_chunks"
    os.makedirs(dest_dir, exist_ok=True)

    # 1. Fetch artifacts list
    url = "https://api.github.com/repos/DHRUVKUMAR-RAJYAGURU/scraper/actions/runs/41969305887/artifacts?per_page=100"
    r = requests.get(url, headers={
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    })
    artifacts = r.json().get('artifacts', [])
    print(f"Found {len(artifacts)} artifacts to process...")

    for a in artifacts:
        aid = a['id']
        aname = a['name']
        print(f"Downloading {aname} ({aid})...")
        dl_url = f"https://api.github.com/repos/DHRUVKUMAR-RAJYAGURU/scraper/actions/artifacts/{aid}/zip"
        res = requests.get(dl_url, headers={
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }, allow_redirects=True)
        if res.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(res.content)) as zf:
                zf.extractall(dest_dir)
        else:
            print(f"Failed to download {aname}: {res.status_code}")

    print("All artifacts extracted. Ingesting into MySQL...")
    csv_files = sorted(glob.glob(os.path.join(dest_dir, "products_*.csv")))
    print(f"Found {len(csv_files)} CSV files to import.")

    db = DBImporter()
    try:
        for csv_f in csv_files:
            import_scraped_file(db, "Discount Living Rooms", 15311, csv_f)
    finally:
        db.close()
    print("DRL remaining chunks import complete!")

if __name__ == "__main__":
    main()

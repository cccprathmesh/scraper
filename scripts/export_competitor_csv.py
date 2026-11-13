#!/usr/bin/env python3
"""
Web Export Server & CLI Tool for Competitor Scraped Data.
- Web UI: http://localhost:5050/export
- CLI usage: python scripts/export_competitor_csv.py --competitor "TV Stands Outlet" --output output.csv
"""

import os
import sys
import csv
import io
import argparse
import pymysql
from datetime import datetime
from flask import Flask, Response, render_template_string, request, jsonify

MYSQL_HOST = os.getenv("MYSQL_HOST", "2.24.198.101")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "Root@123456")
MYSQL_DB = os.getenv("MYSQL_DB", "google_shopping")

app = Flask(__name__)

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

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Competitor Scraped Data Exporter</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background: #f8fafc; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        .card { border-radius: 12px; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1); border: none; }
        .badge-scraped { background-color: #10b981; }
        .badge-pending { background-color: #f59e0b; }
        .header-title { font-weight: 700; color: #1e293b; }
    </style>
</head>
<body class="py-5">
    <div class="container" style="max-width: 960px;">
        <div class="d-flex justify-content-between align-items-center mb-4">
            <div>
                <h2 class="header-title mb-1">📊 Competitor Data Hub</h2>
                <p class="text-muted mb-0">Direct export & status breakdown from MySQL (<code>google_shopping</code>)</p>
            </div>
            <a href="/" class="btn btn-outline-secondary btn-sm">← Back to Dashboard</a>
        </div>

        <div class="card p-4 mb-4">
            <h5 class="mb-3 text-secondary">Export by Competitor</h5>
            <div class="table-responsive">
                <table class="table table-hover align-middle">
                    <thead class="table-light">
                        <tr>
                            <th>Competitor Name</th>
                            <th>Scraped Products</th>
                            <th>Pending URLs</th>
                            <th>Total URLs</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for comp in competitors %}
                        <tr>
                            <td class="fw-bold">{{ comp.name }}</td>
                            <td>
                                <span class="badge badge-scraped px-2 py-1 fs-6">{{ "{:,}".format(comp.scraped_count) }}</span>
                            </td>
                            <td>
                                <span class="badge badge-pending px-2 py-1 fs-6">{{ "{:,}".format(comp.pending_count) }}</span>
                            </td>
                            <td>{{ "{:,}".format(comp.scraped_count + comp.pending_count) }}</td>
                            <td>
                                <a href="/export/download?competitor={{ comp.name | urlencode }}&type=products" class="btn btn-primary btn-sm me-2">
                                    📥 Download Products CSV
                                </a>
                                <a href="/export/download?competitor={{ comp.name | urlencode }}&type=urls" class="btn btn-outline-secondary btn-sm">
                                    🔗 Export URL Status
                                </a>
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>

        <div class="card p-4">
            <h5 class="mb-2 text-secondary">CLI Export Command</h5>
            <p class="text-muted small">You can also export directly via terminal using:</p>
            <pre class="bg-dark text-light p-3 rounded"><code>python3 scripts/export_competitor_csv.py --competitor "TV Stands Outlet" --output tvs.csv</code></pre>
        </div>
    </div>
</body>
</html>
"""

@app.route("/export")
def export_page():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Group scraped count
            cur.execute("""
                SELECT competitor_name, COUNT(*) 
                FROM competitor_scraped_products 
                GROUP BY competitor_name;
            """)
            scraped_dict = {row[0]: row[1] for row in cur.fetchall()}

            # Group pending count
            cur.execute("""
                SELECT competitor_name, COUNT(*) 
                FROM competitor_urls 
                WHERE status = 'pending'
                GROUP BY competitor_name;
            """)
            pending_dict = {row[0]: row[1] for row in cur.fetchall()}

            all_names = sorted(list(set(scraped_dict.keys()).union(pending_dict.keys())))
            competitors = []
            for name in all_names:
                competitors.append({
                    "name": name,
                    "scraped_count": scraped_dict.get(name, 0),
                    "pending_count": pending_dict.get(name, 0),
                })

            return render_template_string(HTML_TEMPLATE, competitors=competitors)
    finally:
        conn.close()

@app.route("/export/download")
def download_csv():
    competitor = request.args.get("competitor", "")
    export_type = request.args.get("type", "products")
    if not competitor:
        return "Competitor name required", 400

    conn = get_connection()
    try:
        def generate():
            with conn.cursor() as cur:
                if export_type == "products":
                    cur.execute("""
                        SELECT 
                            product_url, product_id, variant_id, category, category_url,
                            brand_name, product_name, sku, mpn, gtin, price, main_image,
                            quantity, group_attr_1, group_attr_2, status, additional_data, date_scraped
                        FROM competitor_scraped_products
                        WHERE competitor_name = %s
                        ORDER BY id ASC;
                    """, (competitor,))

                    header = [
                        "Ref Product URL", "Ref Product ID", "Ref Varient ID", "Ref Category", "Ref Category URL",
                        "Ref Brand Name", "Ref Product Name", "Ref SKU", "Ref MPN", "Ref GTIN", "Ref Price",
                        "Ref Main Image", "Ref Quantity", "Ref Group Attr 1", "Ref Group Attr 2", "Ref Status",
                        "Additional Product Data", "Date Scrapped"
                    ]

                    output = io.StringIO()
                    writer = csv.writer(output)
                    writer.writerow(header)
                    yield output.getvalue()
                    output.seek(0)
                    output.truncate(0)

                    while True:
                        rows = cur.fetchmany(1000)
                        if not rows:
                            break
                        for row in rows:
                            writer.writerow(row)
                        yield output.getvalue()
                        output.seek(0)
                        output.truncate(0)

                else:
                    cur.execute("""
                        SELECT url, status, last_scraped_at, error_message
                        FROM competitor_urls
                        WHERE competitor_name = %s
                        ORDER BY id ASC;
                    """, (competitor,))

                    header = ["URL", "Status", "Last Scraped At", "Error Message"]
                    output = io.StringIO()
                    writer = csv.writer(output)
                    writer.writerow(header)
                    yield output.getvalue()
                    output.seek(0)
                    output.truncate(0)

                    while True:
                        rows = cur.fetchmany(2000)
                        if not rows:
                            break
                        for row in rows:
                            writer.writerow(row)
                        yield output.getvalue()
                        output.seek(0)
                        output.truncate(0)

        filename = f"{competitor.replace(' ', '_').lower()}_{export_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return Response(
            generate(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    finally:
        # Note: generator will close connection after stream
        pass

def cli_export(competitor, output_path, export_type="products"):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if export_type == "products":
                print(f"Exporting products for '{competitor}' to {output_path}...")
                cur.execute("""
                    SELECT 
                        product_url, product_id, variant_id, category, category_url,
                        brand_name, product_name, sku, mpn, gtin, price, main_image,
                        quantity, group_attr_1, group_attr_2, status, additional_data, date_scraped
                    FROM competitor_scraped_products
                    WHERE competitor_name = %s
                    ORDER BY id ASC;
                """, (competitor,))
                header = [
                    "Ref Product URL", "Ref Product ID", "Ref Varient ID", "Ref Category", "Ref Category URL",
                    "Ref Brand Name", "Ref Product Name", "Ref SKU", "Ref MPN", "Ref GTIN", "Ref Price",
                    "Ref Main Image", "Ref Quantity", "Ref Group Attr 1", "Ref Group Attr 2", "Ref Status",
                    "Additional Product Data", "Date Scrapped"
                ]
            else:
                print(f"Exporting URLs for '{competitor}' to {output_path}...")
                cur.execute("""
                    SELECT url, status, last_scraped_at, error_message
                    FROM competitor_urls
                    WHERE competitor_name = %s
                    ORDER BY id ASC;
                """, (competitor,))
                header = ["URL", "Status", "Last Scraped At", "Error Message"]

            rows_written = 0
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(header)
                while True:
                    rows = cur.fetchmany(2000)
                    if not rows:
                        break
                    writer.writerows(rows)
                    rows_written += len(rows)
            print(f"✓ Export complete: {rows_written:,} rows written to {output_path} ({os.path.getsize(output_path):,} bytes)")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export Competitor Data from MySQL")
    parser.add_argument("--competitor", type=str, help="Competitor name to export (e.g. 'TV Stands Outlet')")
    parser.add_argument("--output", type=str, help="Output CSV path")
    parser.add_argument("--type", type=str, default="products", choices=["products", "urls"], help="Export products or urls")
    parser.add_argument("--server", action="store_true", help="Run web export server on port 5055")
    parser.add_argument("--port", type=int, default=5055, help="Port for web server (default 5055)")

    args = parser.parse_args()

    if args.server:
        print(f"🚀 Export Web Server running at http://localhost:{args.port}/export")
        app.run(host="0.0.0.0", port=args.port, debug=False)
    elif args.competitor and args.output:
        cli_export(args.competitor, args.output, args.type)
    else:
        # Default run web server if no cli flags
        print(f"🚀 Export Web Server running at http://localhost:{args.port}/export")
        app.run(host="0.0.0.0", port=args.port, debug=False)


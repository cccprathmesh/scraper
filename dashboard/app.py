#!/usr/bin/env python3
"""
Scraper Workflow Dashboard
A Flask-based web UI to view and start all scraping workflows.
Run: python dashboard/app.py
Open: http://localhost:5050
"""

import os
import sys
import signal
import subprocess
import threading
import json
import uuid
import zipfile
import contextlib
from collections import deque
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, render_template, request, send_file
from werkzeug.utils import secure_filename

# ---------------------------------------------------------------------------
# Project root (one level up from dashboard/)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from ai_score import AIScoreService
except ImportError:
    class AIScoreService:
        def __init__(self, *args, **kwargs):
            pass

        def reload_data(self):
            pass

        def list_products(self):
            return []

        def list_products_page(self, query="", page=1, page_size=50):
            return {"items": [], "total": 0, "page": page, "page_size": page_size}

        def get_product_details(self, product_id, **kwargs):
            return {"product_id": product_id, "matches": []}

        def score_competitor(self, **kwargs):
            return {"status": "ok"}

        def score_all(self, **kwargs):
            return []

        def update_score(self, **kwargs):
            return {"status": "ok"}
from validate import Validate, DEFAULT_SCORE_CONFIG, DEFAULT_FILTER_CONFIG, DEFAULT_EXCLUDE_CATEGORY

# ---------------------------------------------------------------------------
# Workflow registry
# ---------------------------------------------------------------------------
WORKFLOWS = {
    "cymax": {
        "name": "Cymax Sitemap",
        "description": "Discover product .htm URLs from Cymax sitemaps via FlareSolverr",
        "script": "cymax/cymax.py",
        "category": "Sitemap",
        "config_hint": "Uses YAML config file (cymax/sitemap_config.yml)",
        "default_env": {},
        "color": "#FF6B6B",
    },
    "dlr": {
        "name": "Discount Living Rooms",
        "description": "Scrape product data from discountlivingrooms.com via dataLayer",
        "script": "drl/dlr_scraper.py",
        "category": "DataLayer",
        "config_hint": "CURR_URL, SITEMAP_OFFSET, MAX_SITEMAPS, MAX_URLS_PER_SITEMAP, MAX_WORKERS",
        "default_env": {
            "CURR_URL": "https://www.discountlivingrooms.com",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "4",
        },
        "color": "#4ECDC4",
    },
    "em_scraper": {
        "name": "Emma Mason (FlareSolverr)",
        "description": "Scrape Emma Mason product data via FlareSolverr with multi-endpoint support",
        "script": "drl/em_scraper.py",
        "category": "FlareSolverr",
        "config_hint": "CURR_URL, FLARESOLVERR_URL, SITEMAP_OFFSET, MAX_SITEMAPS, MAX_WORKERS",
        "default_env": {
            "CURR_URL": "https://www.emmamason.com",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "10",
        },
        "color": "#45B7D1",
    },
    "em_algolia": {
        "name": "Emma Mason Algolia",
        "description": "Fetch Emma Mason products from Algolia search index",
        "script": "drl/em_algolia_fetch.py",
        "category": "API",
        "config_hint": "--page, --hits-per-page, --max-workers, --output-csv",
        "default_env": {},
        "color": "#96CEB4",
    },
    "fpfc": {
        "name": "FurnitureCart / FurniturePick",
        "description": "Scrape FurnitureCart products with bundle variation support via FlareSolverr",
        "script": "fpfc/fp_fc_scraper.py",
        "category": "FlareSolverr",
        "config_hint": "CURR_URL, FLARESOLVERR_URL, SITEMAP_OFFSET, MAX_SITEMAPS, MAX_WORKERS",
        "default_env": {
            "CURR_URL": "https://www.furniturecart.com",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "4",
        },
        "color": "#FFEAA7",
    },
    "graphql": {
        "name": "Home Depot GraphQL",
        "description": "Scrape Home Depot products via GraphQL API with sitemap discovery",
        "script": "graphql/gql.py",
        "category": "GraphQL",
        "config_hint": "CURR_URL, GRAPHQL_URL, STORE_ID, ZIP_CODE, SITEMAP_OFFSET, MAX_SITEMAPS",
        "default_env": {
            "CURR_URL": "https://www.homedepot.com",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "4",
        },
        "color": "#DDA0DD",
    },
    "gshopping": {
        "name": "Google Shopping",
        "description": "Scrape Google Shopping competitor data using Selenium + CAPTCHA solver",
        "script": "gshopping/gscrapper.py",
        "category": "Selenium",
        "config_hint": "Reads product_urls.json for input URLs",
        "default_env": {},
        "color": "#F39C12",
    },
    "ovs": {
        "name": "Overstock + BBB",
        "description": "Scrape Overstock products with BBB API cross-reference",
        "script": "ovs-bbb/ovr.py",
        "category": "API",
        "config_hint": "CURR_URL, API_BASE_URL, BBB_API_BASE_URL, SITEMAP_OFFSET, MAX_SITEMAPS",
        "default_env": {
            "CURR_URL": "",
            "API_BASE_URL": "",
            "BBB_API_BASE_URL": "",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "4",
        },
        "color": "#E74C3C",
    },
    "bbb": {
        "name": "BBB SKU Extractor",
        "description": "Extract modelNumber/SKU from BBB API for variant IDs",
        "script": "ovs-bbb/bbb.py",
        "category": "API",
        "config_hint": "--chunk-id, --total-chunks, --input-file (required CLI args)",
        "default_env": {},
        "color": "#8E44AD",
    },
    "shopify_cf": {
        "name": "Shopify (Cloudflare)",
        "description": "Scrape Shopify stores protected by Cloudflare using cloudscraper + curl_cffi",
        "script": "shopify-scrapper/shopifyscrap-cloudflare.py",
        "category": "Cloudflare",
        "config_hint": "CURR_URL, SITEMAP_OFFSET, MAX_SITEMAPS, MAX_URLS_PER_SITEMAP",
        "default_env": {
            "CURR_URL": "",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "4",
        },
        "color": "#1ABC9C",
    },
    "shopify_normal": {
        "name": "Shopify (Normal)",
        "description": "Scrape standard Shopify stores via .js product endpoint",
        "script": "shopify-scrapper/shopifyscrap-normal.py",
        "category": "HTTP",
        "config_hint": "CURR_URL, SITEMAP_OFFSET, MAX_SITEMAPS, MAX_URLS_PER_SITEMAP",
        "default_env": {
            "CURR_URL": "",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "8",
        },
        "color": "#2ECC71",
    },
}

# ---------------------------------------------------------------------------
# Process manager
# ---------------------------------------------------------------------------
class ProcessManager:
    """Track running scraper sub-processes."""

    def __init__(self):
        self._procs: dict[str, dict] = {}
        self._logs: dict[str, deque] = {}
        self._lock = threading.Lock()

    def start(self, key: str, env_overrides: dict | None = None) -> dict:
        with self._lock:
            if key in self._procs and self._procs[key]["proc"].poll() is None:
                return {"error": "already running"}

            wf = WORKFLOWS[key]
            script = str(PROJECT_ROOT / wf["script"])

            env = os.environ.copy()
            # Apply default env from workflow config
            env.update(wf.get("default_env", {}))
            # Apply user overrides
            if env_overrides:
                env.update(env_overrides)

            proc = subprocess.Popen(
                [sys.executable, script],
                cwd=str(PROJECT_ROOT),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            log_buf = deque(maxlen=200)
            self._logs[key] = log_buf
            self._procs[key] = {
                "proc": proc,
                "started": datetime.now().isoformat(),
                "pid": proc.pid,
            }

            # Background thread to read output
            t = threading.Thread(target=self._reader, args=(key, proc, log_buf), daemon=True)
            t.start()

            return {"status": "started", "pid": proc.pid}

    def _reader(self, key: str, proc: subprocess.Popen, buf: deque):
        try:
            for line in proc.stdout:
                buf.append(line.rstrip("\n"))
        except Exception:
            pass

    def stop(self, key: str) -> dict:
        with self._lock:
            info = self._procs.get(key)
            if not info or info["proc"].poll() is not None:
                return {"status": "not_running"}
            try:
                os.killpg(os.getpgid(info["proc"].pid), signal.SIGTERM)
            except Exception:
                info["proc"].terminate()
            info["proc"].wait(timeout=5)
            return {"status": "stopped"}

    def status(self, key: str) -> dict:
        info = self._procs.get(key)
        if not info:
            return {"state": "idle", "logs": []}

        running = info["proc"].poll() is None
        return_code = info["proc"].returncode

        state = "running" if running else ("completed" if return_code == 0 else "error")

        logs = list(self._logs.get(key, []))
        return {
            "state": state,
            "pid": info["pid"],
            "started": info["started"],
            "return_code": return_code,
            "logs": logs[-80:],
        }

    def all_statuses(self) -> dict:
        result = {}
        for key in WORKFLOWS:
            result[key] = self.status(key)
        return result


pm = ProcessManager()


class DequeWriter:
    def __init__(self, buf: deque):
        self._buf = buf

    def write(self, text: str) -> None:
        if not text:
            return
        text = text.replace("\r", "\n")
        for line in text.splitlines():
            self._buf.append(line)

    def flush(self) -> None:
        return


class ValidationJobManager:
    def __init__(self):
        self._jobs: dict[str, dict] = {}
        self._lock = threading.Lock()

    def start(self, payload: dict, input_files: dict) -> dict:
        run_id = payload.get("run_id") or datetime.now().strftime("%Y%m%d_%H%M%S")
        run_id = f"{run_id}_{uuid.uuid4().hex[:6]}"
        job = {
            "run_id": run_id,
            "state": "running",
            "started": datetime.now().isoformat(),
            "finished": None,
            "logs": deque(maxlen=400),
            "error": None,
            "output_dir": payload.get("output_dir"),
            "zip_path": payload.get("zip_path"),
            "mode": payload.get("mode"),
            "output_type": payload.get("output_type"),
        }
        with self._lock:
            self._jobs[run_id] = job

        thread = threading.Thread(
            target=self._run_job,
            args=(run_id, payload, input_files),
            daemon=True,
        )
        thread.start()
        return {"run_id": run_id, "status": "started"}

    def _run_job(self, run_id: str, payload: dict, input_files: dict) -> None:
        job = self._jobs.get(run_id)
        if not job:
            return

        log_writer = DequeWriter(job["logs"])
        mode = payload.get("mode", "cm")
        output_type = payload.get("output_type", "valid_invalid")
        score_config = payload.get("score_config") or {}
        filter_config = payload.get("filter_config") or {}
        exclude_category = payload.get("exclude_category")
        output_dir = payload.get("output_dir")
        zip_path = payload.get("zip_path")

        try:
            with contextlib.redirect_stdout(log_writer), contextlib.redirect_stderr(log_writer):
                validator = Validate(
                    mode=mode,
                    output_type=output_type,
                    input_files=input_files,
                    output_dir=output_dir,
                    filter_config=filter_config,
                    exclude_category=exclude_category,
                )
                if score_config:
                    validator.update_score_config(score_config)
                log_writer.write("Starting validation run...\n")
                validator.prepare_details_csv()

                config_path = os.path.join(output_dir, "run_config.json")
                with open(config_path, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "mode": mode,
                            "output_type": output_type,
                            "score_config": score_config,
                            "filter_config": filter_config,
                            "exclude_category": exclude_category,
                            "input_files": input_files,
                        },
                        f,
                        indent=2,
                    )

                if zip_path:
                    self._zip_dir(output_dir, zip_path)

            job["state"] = "completed"
        except Exception as exc:
            job["state"] = "error"
            job["error"] = str(exc)
        finally:
            job["finished"] = datetime.now().isoformat()

    def _zip_dir(self, folder: str, zip_path: str) -> None:
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(folder):
                for name in files:
                    full_path = os.path.join(root, name)
                    rel_path = os.path.relpath(full_path, folder)
                    zf.write(full_path, rel_path)

    def status(self, run_id: str) -> dict:
        job = self._jobs.get(run_id)
        if not job:
            return {"error": "not_found"}
        return {
            "run_id": run_id,
            "state": job.get("state"),
            "started": job.get("started"),
            "finished": job.get("finished"),
            "error": job.get("error"),
            "logs": list(job.get("logs", []))[-120:],
            "zip_ready": bool(job.get("zip_path") and os.path.exists(job.get("zip_path"))),
            "output_dir": job.get("output_dir"),
        }

    def get_zip_path(self, run_id: str) -> str | None:
        job = self._jobs.get(run_id)
        if not job:
            return None
        zip_path = job.get("zip_path")
        if zip_path and os.path.exists(zip_path):
            return zip_path
        return None


validation_jobs = ValidationJobManager()

VALIDATION_INPUT_DIR = PROJECT_ROOT / "inputValidateFiles"
VALIDATION_RUN_DIR = PROJECT_ROOT / "validationScore" / "ui_runs"
VALIDATION_RUN_DIR.mkdir(parents=True, exist_ok=True)


AI_SYSTEM_FILE = Path(os.getenv("AI_SYSTEM_FILE", str(PROJECT_ROOT / "system.csv")))
AI_COMP_FILE = Path(os.getenv("AI_COMP_FILE", str(PROJECT_ROOT / "competitor.csv")))
AI_SCORE_CACHE_FILE = Path(
    os.getenv(
        "AI_SCORE_CACHE_FILE",
        os.getenv("AI_SCORE_CACHE_DB", str(PROJECT_ROOT / "ai_score_cache.csv")),
    )
)
AI_MODEL = os.getenv("AI_MODEL", "deepseek-ai/deepseek-v3.1-terminus")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


AI_THRESHOLD = _env_int("AI_THRESHOLD", 60)

_ai_service: AIScoreService | None = None
_ai_service_error: str | None = None
_ai_service_lock = threading.Lock()


def get_ai_service() -> tuple[AIScoreService | None, str | None]:
    global _ai_service, _ai_service_error
    with _ai_service_lock:
        if _ai_service is not None:
            return _ai_service, None
        if _ai_service_error is not None:
            return None, _ai_service_error

        try:
            _ai_service = AIScoreService(
                system_file=AI_SYSTEM_FILE,
                competitor_file=AI_COMP_FILE,
                cache_file=AI_SCORE_CACHE_FILE,
                model=AI_MODEL,
                ai_threshold=AI_THRESHOLD,
            )
            return _ai_service, None
        except Exception as exc:
            _ai_service_error = str(exc)
            return None, _ai_service_error


def invalidate_ai_service_error() -> None:
    global _ai_service_error
    with _ai_service_lock:
        _ai_service_error = None

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/ai-score")
def ai_score_page():
    return render_template("ai_score.html")


@app.route("/validate")
def validate_page():
    return render_template("validate.html")


@app.route("/api/validate/defaults")
def api_validate_defaults():
    return jsonify(
        {
            "score_config": DEFAULT_SCORE_CONFIG,
            "filter_config": DEFAULT_FILTER_CONFIG,
            "exclude_category": DEFAULT_EXCLUDE_CATEGORY,
            "input_files": {
                "comp": str(VALIDATION_INPUT_DIR / "competitor-full.csv"),
                "sys": str(VALIDATION_INPUT_DIR / "system.csv"),
                "scraped": str(VALIDATION_INPUT_DIR / "scraped.csv"),
            },
        }
    )


def _normalize_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        raw = value.replace("\r", "\n")
        parts = []
        for chunk in raw.split("\n"):
            parts.extend(chunk.split(","))
        return [p.strip() for p in parts if p.strip()]
    return [str(value).strip()]


def _normalize_score_config(overrides: dict) -> dict:
    result = {}
    if not isinstance(overrides, dict):
        return result
    for key, value in overrides.items():
        if value is None or value == "":
            continue
        try:
            result[key] = float(value)
        except Exception:
            continue
    return result


@app.route("/api/validate/run", methods=["POST"])
def api_validate_run():
    payload_raw = request.form.get("payload", "{}")
    try:
        payload = json.loads(payload_raw) if payload_raw else {}
    except Exception:
        return jsonify({"error": "bad_request", "details": "payload must be valid JSON"}), 400

    mode = payload.get("mode", "cm")
    output_type = payload.get("output_type", "valid_invalid")
    use_existing = bool(payload.get("use_existing", False))

    score_config = _normalize_score_config(payload.get("score_config") or {})
    filter_in = payload.get("filter_config") or {}
    filter_config = dict(DEFAULT_FILTER_CONFIG)
    if "allowed_reasons" in filter_in:
        filter_config["allowed_reasons"] = _normalize_list(filter_in.get("allowed_reasons"))
    if "exclude_competitors" in filter_in:
        filter_config["exclude_competitors"] = _normalize_list(filter_in.get("exclude_competitors"))
    if "include_competitors" in filter_in:
        filter_config["include_competitors"] = _normalize_list(filter_in.get("include_competitors"))
    if "disallowed_visibility" in filter_in:
        filter_config["disallowed_visibility"] = _normalize_list(filter_in.get("disallowed_visibility"))
    if "required_type" in filter_in:
        filter_config["required_type"] = filter_in.get("required_type") or None
    if "require_competitor_sku" in filter_in:
        filter_config["require_competitor_sku"] = bool(filter_in.get("require_competitor_sku", True))
    if "apply_row_filters" in filter_in:
        filter_config["apply_row_filters"] = bool(filter_in.get("apply_row_filters", False))
    exclude_category = _normalize_list(payload.get("exclude_category"))

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    run_dir = VALIDATION_RUN_DIR / run_id
    inputs_dir = run_dir / "inputs"
    outputs_dir = run_dir / "outputs" / mode
    inputs_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    defaults = {
        "comp": VALIDATION_INPUT_DIR / "competitor-full.csv",
        "sys": VALIDATION_INPUT_DIR / "system.csv",
        "scraped": VALIDATION_INPUT_DIR / "scraped.csv",
    }

    def _save_upload(field_name: str, target_name: str) -> str | None:
        file = request.files.get(field_name)
        if not file or not file.filename:
            return None
        safe_name = secure_filename(target_name)
        path = inputs_dir / safe_name
        file.save(path)
        return str(path)

    comp_path = _save_upload("comp_file", "competitor-full.csv")
    sys_path = _save_upload("sys_file", "system.csv")
    scraped_path = _save_upload("scraped_file", "scraped.csv")

    if use_existing:
        comp_path = comp_path or str(defaults["comp"])
        sys_path = sys_path or str(defaults["sys"])
        scraped_path = scraped_path or str(defaults["scraped"])

    if not comp_path or not sys_path or not scraped_path:
        return jsonify({"error": "missing_files", "details": "comp, sys, scraped files are required"}), 400

    input_files = {
        "comp": comp_path,
        "sys": sys_path,
        "scraped": scraped_path,
    }

    job_payload = {
        "mode": mode,
        "output_type": output_type,
        "score_config": score_config,
        "filter_config": filter_config,
        "exclude_category": exclude_category,
        "output_dir": str(outputs_dir),
        "zip_path": str(run_dir / "validation_outputs.zip"),
        "run_id": run_id,
    }

    result = validation_jobs.start(job_payload, input_files)
    return jsonify(result)


@app.route("/api/validate/status/<run_id>")
def api_validate_status(run_id: str):
    status = validation_jobs.status(run_id)
    if status.get("error") == "not_found":
        return jsonify({"error": "not_found"}), 404
    return jsonify(status)


@app.route("/api/validate/download/<run_id>")
def api_validate_download(run_id: str):
    zip_path = validation_jobs.get_zip_path(run_id)
    if not zip_path:
        return jsonify({"error": "not_found", "details": "zip not ready"}), 404
    return send_file(zip_path, as_attachment=True, download_name=f"validation_{run_id}.zip")


@app.route("/api/workflows")
def api_workflows():
    statuses = pm.all_statuses()
    workflows = []
    for key, wf in WORKFLOWS.items():
        st = statuses.get(key, {"state": "idle"})
        workflows.append({
            "key": key,
            "name": wf["name"],
            "description": wf["description"],
            "script": wf["script"],
            "category": wf["category"],
            "config_hint": wf["config_hint"],
            "color": wf["color"],
            "default_env": wf.get("default_env", {}),
            **st,
        })
    return jsonify(workflows)


@app.route("/api/workflows/<key>/start", methods=["POST"])
def api_start(key):
    if key not in WORKFLOWS:
        return jsonify({"error": "unknown workflow"}), 404
    body = request.get_json(silent=True) or {}
    env_overrides = body.get("env", {})
    result = pm.start(key, env_overrides)
    code = 200 if "error" not in result else 409
    return jsonify(result), code


@app.route("/api/workflows/<key>/status")
def api_status(key):
    if key not in WORKFLOWS:
        return jsonify({"error": "unknown workflow"}), 404
    return jsonify(pm.status(key))


@app.route("/api/workflows/<key>/stop", methods=["POST"])
def api_stop(key):
    if key not in WORKFLOWS:
        return jsonify({"error": "unknown workflow"}), 404
    return jsonify(pm.stop(key))


@app.route("/api/ai-score/products")
def api_ai_products():
    service, err = get_ai_service()
    if err:
        return jsonify({"error": "ai_score_init_failed", "details": err}), 500
    query = request.args.get("query", "")
    page = request.args.get("page", 1)
    page_size = request.args.get("page_size", 50)
    try:
        page = int(page)
        page_size = int(page_size)
    except Exception:
        return jsonify({"error": "bad_request", "details": "page and page_size must be integers"}), 400
    return jsonify(service.list_products_page(query=query, page=page, page_size=page_size))


@app.route("/api/ai-score/product/<product_id>")
def api_ai_product_details(product_id):
    service, err = get_ai_service()
    if err:
        return jsonify({"error": "ai_score_init_failed", "details": err}), 500
    query = request.args.get("query", "")
    source = request.args.get("source", "")
    page = request.args.get("page", 1)
    page_size = request.args.get("page_size", 50)
    try:
        page = int(page)
        page_size = int(page_size)
    except Exception:
        return jsonify({"error": "bad_request", "details": "page and page_size must be integers"}), 400
    try:
        return jsonify(
            service.get_product_details(
                product_id,
                page=page,
                page_size=page_size,
                query=query,
                source=source,
            )
        )
    except KeyError as exc:
        return jsonify({"error": "not_found", "details": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"error": "bad_request", "details": str(exc)}), 400


@app.route("/api/ai-score/score", methods=["POST"])
def api_ai_score_one():
    service, err = get_ai_service()
    if err:
        return jsonify({"error": "ai_score_init_failed", "details": err}), 500

    body = request.get_json(silent=True) or {}
    product_id = body.get("product_id", "")
    competitor_key = body.get("competitor_key", "")
    force = bool(body.get("force", False))

    try:
        result = service.score_competitor(product_id=product_id, competitor_key=competitor_key, force=force)
        return jsonify(result)
    except KeyError as exc:
        return jsonify({"error": "not_found", "details": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"error": "bad_request", "details": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": "score_failed", "details": str(exc)}), 500


@app.route("/api/ai-score/score-all", methods=["POST"])
def api_ai_score_all():
    service, err = get_ai_service()
    if err:
        return jsonify({"error": "ai_score_init_failed", "details": err}), 500

    body = request.get_json(silent=True) or {}
    product_id = body.get("product_id", "")
    force = bool(body.get("force", False))
    limit = body.get("limit")
    if limit is not None:
        try:
            limit = int(limit)
        except Exception:
            return jsonify({"error": "bad_request", "details": "limit must be an integer"}), 400

    try:
        results = service.score_all(product_id=product_id, force=force, limit=limit)
        return jsonify(
            {
                "product_id": str(product_id),
                "count": len(results),
                "scored": sum(1 for row in results if "error" not in row),
                "failed": sum(1 for row in results if "error" in row),
                "results": results,
            }
        )
    except KeyError as exc:
        return jsonify({"error": "not_found", "details": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"error": "bad_request", "details": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": "score_all_failed", "details": str(exc)}), 500


@app.route("/api/ai-score/update", methods=["POST"])
def api_ai_update_score():
    service, err = get_ai_service()
    if err:
        return jsonify({"error": "ai_score_init_failed", "details": err}), 500

    body = request.get_json(silent=True) or {}
    product_id = body.get("product_id", "")
    competitor_key = body.get("competitor_key", "")
    ai_score = body.get("ai_score")
    decision = body.get("decision")
    confidence = body.get("confidence")
    reason = body.get("reason")

    if ai_score is None:
        return jsonify({"error": "bad_request", "details": "ai_score is required"}), 400
    try:
        ai_score = float(ai_score)
    except Exception:
        return jsonify({"error": "bad_request", "details": "ai_score must be numeric"}), 400

    try:
        result = service.update_score(
            product_id=product_id,
            competitor_key=competitor_key,
            ai_score=ai_score,
            decision=decision,
            confidence=confidence,
            reason=reason,
        )
        return jsonify(result)
    except KeyError as exc:
        return jsonify({"error": "not_found", "details": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"error": "bad_request", "details": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": "update_failed", "details": str(exc)}), 500


@app.route("/api/ai-score/reload", methods=["POST"])
def api_ai_reload():
    # Allow recovering from startup read errors after CSV corrections.
    invalidate_ai_service_error()
    service, err = get_ai_service()
    if err:
        return jsonify({"error": "ai_score_init_failed", "details": err}), 500
    try:
        service.reload_data()
        return jsonify(
            {
                "status": "reloaded",
                "products": len(service.list_products()),
                "system_file": str(AI_SYSTEM_FILE),
                "competitor_file": str(AI_COMP_FILE),
                "cache_file": str(AI_SCORE_CACHE_FILE),
                "cache_db": str(AI_SCORE_CACHE_FILE),
                "ai_threshold": AI_THRESHOLD,
            }
        )
    except Exception as exc:
        return jsonify({"error": "reload_failed", "details": str(exc)}), 500


# ---------------------------------------------------------------------------
# Competitor Scraped Data Export Hub
# ---------------------------------------------------------------------------
try:
    from scripts.export_competitor_csv import export_page, download_csv
    app.add_url_rule("/export", view_func=export_page, methods=["GET"])
    app.add_url_rule("/export/download", view_func=download_csv, methods=["GET"])
except Exception as e:
    print(f"Warning: Could not mount /export routes: {e}")


if __name__ == "__main__":
    import socket
    port = int(os.environ.get("PORT", 5050))
    
    def is_port_in_use(p):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('127.0.0.1', p)) == 0

    if is_port_in_use(port) and "PORT" not in os.environ:
        port = 5051
        while is_port_in_use(port):
            port += 1

    print(f"🚀 Scraper Dashboard running at http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)

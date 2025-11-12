# Created using ChatGPT 4o and ChatGPT 5

import glob
import os
import re
import json
import time
import csv
import logging
import requests
import subprocess
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

# ========== CONFIGURATION ==========
INPUT_DIR = r"C:\...\Daily_LC_Activity_Streams"
OUTPUT_BASE = r"C:\...\LC_Activity_Downloads"
LOG_FILE = r"C:\...\LC_Activity_Logs\jsonld_download_log.txt"
CSV_FILE = rf"C:\...\LC_Activity_Logs\activity_log_{datetime.now().strftime('%Y-%m-%d')}.csv"
DELAY_SECONDS = 5
USER_AGENT = "Mozilla/5.0 (compatible; LCHarvester/1.0; +https://example.org)"

TARGET_TYPES = {"Create", "Update", "Remove"}  # exact, case-sensitive

# ========== SETUP ==========
os.makedirs(OUTPUT_BASE, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="[{asctime}] [{levelname}] {message}",
    datefmt="%Y-%m-%d %H:%M:%S",
    style="{",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def find_latest_json_files_recursive(root_folder: str) -> list[Path]:
    latest_files = []
    for subdir, _, _ in os.walk(root_folder):
        json_files = list(Path(subdir).glob("*.json")) + list(Path(subdir).glob("*.jsonld"))
        if json_files:
            latest = max(json_files, key=os.path.getmtime)
            latest_files.append(latest)
    if not latest_files:
        logging.critical(f"No JSON/JSONLD files found under {root_folder}")
    else:
        logging.info(f"Found {len(latest_files)} most-recent JSON files from subdirectories")
    return latest_files

def _collect_urls_ending_marcxml(obj: Any, acc: Set[str]) -> None:
    if isinstance(obj, dict):
        for v in obj.values():
            _collect_urls_ending_marcxml(v, acc)
    elif isinstance(obj, list):
        for item in obj:
            _collect_urls_ending_marcxml(item, acc)
    elif isinstance(obj, str):
        s = obj.strip()
        if s.lower().startswith(("http://id.loc.gov/", "https://id.loc.gov/")):
            base = s.split("?", 1)[0]
            if base.endswith(".marcxml.xml"):
                acc.add(base)

def _type_contains(node: dict, value: str) -> bool:
    t = node.get("type")
    return t == value or (isinstance(t, list) and value in t)

def _extract_remove_urls_from_node(node: Any) -> Set[str]:
    urls: Set[str] = set()
    if not (isinstance(node, dict) and _type_contains(node, "Remove")):
        return urls
    obj = node.get("object")
    if not isinstance(obj, dict):
        return urls
    for link in obj.get("url", []):
        if not isinstance(link, dict):
            continue
        media_type = link.get("mediaType", "")
        href = link.get("href", "")
        if media_type == "application/marc+xml" and isinstance(href, str):
            base = href.split("?", 1)[0]
            if base.endswith(".marcxml.xml"):
                urls.add(base)
    return urls

def parse_jsonld_structured(payload: Any) -> Dict[str, Set[str]]:
    results: Dict[str, Set[str]] = {t: set() for t in TARGET_TYPES}
    def walk(node: Any):
        if isinstance(node, dict):
            for t in ("Create", "Update"):
                if _type_contains(node, t):
                    tmp: Set[str] = set()
                    _collect_urls_ending_marcxml(node, tmp)
                    results[t].update(tmp)
            if _type_contains(node, "Remove"):
                results["Remove"].update(_extract_remove_urls_from_node(node))
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)
    walk(payload)
    return results

def download_file(url: str, dest_folder: str) -> str:
    os.makedirs(dest_folder, exist_ok=True)
    filename = os.path.basename(url.split("?", 1)[0])
    dest_path = os.path.join(dest_folder, filename)
    if os.path.exists(dest_path):
        logging.info(f"[SKIP] Already downloaded: {filename}")
        return "skipped"
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            f.write(r.content)
        logging.info(f"[OK] Downloaded: {filename} → {dest_folder}")
        time.sleep(DELAY_SECONDS)
        return "success"
    except Exception as e:
        logging.error(f"[FAIL] {filename}: {e}")
        return "failed"

def save_csv_log(rows: List[Tuple[str, str, str]]) -> None:
    os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["RecordType", "URL", "Status"])
        writer.writerows(rows)
    logging.info(f"[DONE] Results written to {CSV_FILE}")

def main():
    json_files = find_latest_json_files_recursive(INPUT_DIR)
    if not json_files:
        return

    results = []
    for json_file in json_files:
        logging.info(f"Processing: {json_file}")
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logging.error(f"Failed to parse {json_file}: {e}")
            continue

        buckets = parse_jsonld_structured(data)

        for k in TARGET_TYPES:
            logging.info(f"[COUNT] {k} in {json_file.name}: {len(buckets[k])}")
            folder = os.path.join(OUTPUT_BASE, k)
            for url in sorted(buckets[k]):
                status = download_file(url, folder)
                results.append((k, url, status))

    save_csv_log(results)

# ==================== MARCEDIT CONVERSION ====================
MARCEDIT_PATH = r"C:\...\MARCEdit\cmarcedit.exe"
CONVERTED_BASE = os.path.join(OUTPUT_BASE, "Converted_MARC")
JOINED_DIR = os.path.join(OUTPUT_BASE, "Joined_MARC")
LOG_FILE_MARCEDIT = r"C:\Scripts\LC_Activity_Logs\marcedit_conversion_log.txt"
PER_FILE_TIMEOUT = 90

today = datetime.now().strftime("%Y-%m-%d")
os.makedirs(CONVERTED_BASE, exist_ok=True)
os.makedirs(JOINED_DIR, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE_MARCEDIT), exist_ok=True)

def log_marc(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    with open(LOG_FILE_MARCEDIT, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)

def list_xml_files(folder: str) -> list:
    patterns = ["*.xml", "*.XML", "*.marcxml", "*.MARCXML", "*.marcxml.xml", "*.MARCXML.XML"]
    files = []
    for p in patterns:
        files.extend(glob.glob(os.path.join(folder, p)))
    return sorted(set(files))

def run_conversion(src: str, dest: str):
    cmd = f'"{MARCEDIT_PATH}" -s "{src}" -d "{dest}" -xmlmarc'
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=PER_FILE_TIMEOUT)
    except subprocess.TimeoutExpired:
        subprocess.run("TASKKILL /F /IM cmarcedit.exe", shell=True)
        log_marc(f"Timeout expired: {src}")
        return None

####
def convert_and_join_by_type(record_type: str):
    src_dir = os.path.join(OUTPUT_BASE, record_type)
    out_dir = os.path.join(CONVERTED_BASE, record_type)
    os.makedirs(out_dir, exist_ok=True)

    xml_files = list_xml_files(src_dir)
    if not xml_files:
        log_marc(f"[{record_type}] No MARCXML files to convert.")
        return

# === Join (only if joined file doesn’t already exist) ===

    joined_output = os.path.join(JOINED_DIR, f"LC_Authorities_{record_type}_{today}.mrc")
    if os.path.exists(joined_output) and os.path.getsize(joined_output) > 0:
        log_marc(f"[{record_type}] [SKIP] Joined MARC file already exists: {joined_output}")
        return

    converted_files = []
    for src in xml_files:
        base = os.path.splitext(os.path.basename(src))[0]
        if base.lower().endswith(".marcxml"):
            base = base[:-8]
        dest = os.path.join(out_dir, base + ".mrc")

        if os.path.exists(dest) and os.path.getsize(dest) > 0:
            log_marc(f"[{record_type}] [SKIP] Already converted: {os.path.basename(src)}")
            converted_files.append(dest)
            continue

        result = run_conversion(src, dest)
        if result and result.returncode == 0 and os.path.exists(dest) and os.path.getsize(dest) > 0:
            log_marc(f"[{record_type}] SUCCESS: {os.path.basename(src)}")
            converted_files.append(dest)
        else:
            log_marc(f"[{record_type}] FAILED: {os.path.basename(src)}")

# === Join (with versioning if joined file already exists) ===

    if len(converted_files) < 2:
        log_marc(f"[{record_type}] Skipping join: only {len(converted_files)} file(s).")
        return

# === NEW: Skip join if all converted MARC files are older than existing joined file ===
        # === NEW: Skip join if all converted MARC files are older than existing joined file ===
    base_join = os.path.join(JOINED_DIR, f"LC_Authorities_{record_type}_{today}.mrc")
    existing_join = None

    if os.path.exists(base_join):
        existing_join = base_join
    else:
        # Look for any versioned joined files (v2, v3, etc.)
        versioned_files = [
            os.path.join(JOINED_DIR, f)
            for f in os.listdir(JOINED_DIR)
            if re.match(fr"LC_Authorities_{record_type}_{today}(_v\d+)?\.mrc$", f)
        ]
        if versioned_files:
            existing_join = max(versioned_files, key=os.path.getmtime)

    # If there is an existing joined file and all converted files are older than it, skip join
    if existing_join:
        join_mtime = os.path.getmtime(existing_join)
        if all(os.path.getmtime(f) <= join_mtime for f in converted_files):
            log_marc(f"[{record_type}] [SKIP] No new converted MARC files since last join ({existing_join}).")
            return
        # Only join the newly converted files (newer than last join)
        files_to_join = [f for f in converted_files if os.path.getmtime(f) > join_mtime]
        # Allow joining even a single new file in a versioned join
        if len(files_to_join) < 1:
            log_marc(f"[{record_type}] [SKIP] Nothing new to join after filtering by mtime.")
            return
    else:
        # No prior joined file for today → join all converted files (original behavior)
        files_to_join = converted_files
        if len(files_to_join) < 2:
            log_marc(f"[{record_type}] Skipping initial join: only {len(files_to_join)} file(s).")
            return

    # Determine joined filename (add version if one already exists)
    base_join = os.path.join(JOINED_DIR, f"LC_Authorities_{record_type}_{today}.mrc")
    joined_output = base_join

    if os.path.exists(base_join):
        # File for today already exists → version it
        version = 2
        while True:
            candidate = os.path.join(JOINED_DIR, f"LC_Authorities_{record_type}_{today}_v{version}.mrc")
            if not os.path.exists(candidate):
                joined_output = candidate
                break
            version += 1
        log_marc(f"[{record_type}] Existing joined file detected. Creating new version: {joined_output}")
    else:
        log_marc(f"[{record_type}] Joining {len(files_to_join)} files → {joined_output}")

    # Perform the join using the filtered set
    try:
        with open(joined_output, "wb") as outfh:
            for name in sorted(files_to_join):
                with open(name, "rb") as infh:
                    shutil.copyfileobj(infh, outfh, length=1024 * 1024)
        if os.path.getsize(joined_output) > 0:
            log_marc(f"[{record_type}] SUCCESS: Joined MARC file created: {joined_output}")
        else:
            log_marc(f"[{record_type}] WARNING: Joined MARC file is empty.")
    except Exception as e:
        log_marc(f"[{record_type}] ERROR during join: {e}")

#######

def run_marc_conversion_pipeline():
    if not os.path.isfile(MARCEDIT_PATH):
        log_marc(f"ERROR: MARCEdit not found at {MARCEDIT_PATH}")
        return
    for typ in TARGET_TYPES:
        convert_and_join_by_type(typ)

if __name__ == "__main__":
    main()
    run_marc_conversion_pipeline()

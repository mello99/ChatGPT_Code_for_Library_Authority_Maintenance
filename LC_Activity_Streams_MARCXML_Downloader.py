# Refactored Script: LC Activity Harvester & MARC Conversion
# Author: Refactored by Codex GPT, originally written with ChatGPT 4o and 5
# Notes: Cleaned, de-duplicated, now scans ALL .json/.jsonld files, detects @type deeply, grabs href from object.url
# Designed for Windows machines

import os
import re
import glob
import csv
import json
import time
import shutil
import logging
import subprocess
import requests
from pathlib import Path
from datetime import datetime
from typing import Any, Set, List, Tuple, Dict
from sys import platform

# =================== CONFIGURATION ===================
INPUT_DIR = Path(r"C:\Scripts\Daily_LC_Activity_Streams")
OUTPUT_BASE = Path(r"C:\Scripts\LC_Activity_Downloads")
LOG_FILE = Path(r"C:\Scripts\LC_Activity_Logs\jsonld_download_log.txt")
CSV_FILE = Path(rf"C:\Scripts\LC_Activity_Logs\activity_log_{datetime.now():%Y-%m-%d}.csv")
DELAY_SECONDS = 5
USER_AGENT = "Mozilla/5.0 (compatible; LCHarvester/1.0; +https://example.org)"
TARGET_TYPES = {"Create", "Update", "Remove"}

MARCEDIT_PATH = Path(r"C:\...\cmarcedit.exe")
CONVERTED_BASE = OUTPUT_BASE / "Converted_MARC"
JOINED_DIR = OUTPUT_BASE / "Joined_MARC"
LOG_FILE_MARCEDIT = Path(r"C:\Scripts\LC_Activity_Logs\marcedit_conversion_log.txt")
archive_dir = out_dir / "Previously_Joined_MARC_Files"
archive_dir.mkdir(exist_ok=True)
PER_FILE_TIMEOUT = 90

# =================== UTILITIES ===================
def ensure_dirs(*paths):
    for path in paths:
        os.makedirs(path, exist_ok=True)

def was_modified_today(path: Path) -> bool:
    return datetime.fromtimestamp(path.stat().st_mtime).date() == datetime.now().date()

def strip_marcxml_ext(filename: str) -> str:
    return re.sub(r'\.marcxml(\.xml)?$', '', filename, flags=re.IGNORECASE)

def find_all_json_files_recursive(root_folder: Path) -> List[Path]:
    json_files = []
    for subdir, _, _ in os.walk(root_folder):
        jsons = list(Path(subdir).glob("*.json")) + list(Path(subdir).glob("*.jsonld"))
        json_files.extend(jsons)
    json_files = [f for f in json_files if was_modified_today(f)]
    if not json_files:
        logging.critical(f"No JSON/JSONLD files modified today under {root_folder}")
    else:
        logging.info(f"Found {len(json_files)} JSON files modified today")
    return json_files

# =================== JSONLD PARSING ===================
def parse_jsonld_structured(payload: Any) -> Dict[str, Set[str]]:
    results = {t: set() for t in TARGET_TYPES}

    def _type_contains(node: dict, value: str) -> bool:
        for key in ("type", "@type"):
            t = node.get(key)
            if t == value or (isinstance(t, list) and value in t):
                return True
        return False

    def _extract_href_marcxml(node: Any, acc: Set[str]) -> None:
        if isinstance(node, dict):
            url_block = node.get("url")
            if isinstance(url_block, list):
                for link in url_block:
                    if isinstance(link, dict):
                        if link.get("mediaType") == "application/marc+xml":
                            href = link.get("href", "").split("?", 1)[0]
                            if href.endswith(".marcxml.xml"):
                                acc.add(href)
            for v in node.values():
                _extract_href_marcxml(v, acc)
        elif isinstance(node, list):
            for item in node:
                _extract_href_marcxml(item, acc)

    def _walk_jsonld(node: Any):
        if isinstance(node, dict):
            for t in ("Create", "Update"):
                if _type_contains(node, t):
                    acc = set()
                    obj = node.get("object")
                    if obj:
                        _extract_href_marcxml(obj, acc)
                    results[t].update(acc)

            if _type_contains(node, "Remove"):
                acc = set()
                obj = node.get("object")
                if obj:
                    _extract_href_marcxml(obj, acc)
                results["Remove"].update(acc)

            for v in node.values():
                _walk_jsonld(v)

        elif isinstance(node, list):
            for item in node:
                _walk_jsonld(item)

    _walk_jsonld(payload)
    return results

# =================== DOWNLOAD ===================
def download_file(url: str, dest_folder: Path) -> str:
    dest_folder.mkdir(parents=True, exist_ok=True)
    filename = Path(url.split("?", 1)[0]).name
    dest_path = dest_folder / filename

    if dest_path.exists():
        logging.info(f"[SKIP] Already downloaded: {filename}")
        return "skipped"

    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        r.raise_for_status()
        dest_path.write_bytes(r.content)
        logging.info(f"[OK] Downloaded: {filename} → {dest_folder}")
        time.sleep(DELAY_SECONDS)
        return "success"
    except Exception as e:
        logging.error(f"[FAIL] {filename}: {e}")
        return "failed"

def save_csv_log(rows: List[Tuple[str, str, str]]) -> None:
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["RecordType", "URL", "Status"])
        writer.writerows(rows)
    logging.info(f"[DONE] Results written to {CSV_FILE}")

# =================== MARC CONVERSION ===================
def log_marc(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    with open(LOG_FILE_MARCEDIT, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)

def list_xml_files(folder: Path) -> List[Path]:
    exts = ["*.xml", "*.marcxml", "*.marcxml.xml"]
    files = []
    for ext in exts:
        files.extend(folder.glob(ext))
        files.extend(folder.glob(ext.upper()))
    return sorted(set(files))

def run_conversion(src: Path, dest: Path):
    if platform != "win32":
        raise RuntimeError("MARCEdit is only supported on Windows in this script.")
    try:
        return subprocess.run(
            [str(MARCEDIT_PATH), "-s", str(src), "-d", str(dest), "-xmlmarc"],
            capture_output=True,
            text=True,
            timeout=PER_FILE_TIMEOUT
        )
    except subprocess.TimeoutExpired:
        subprocess.run("TASKKILL /F /IM cmarcedit.exe", shell=True)
        log_marc(f"Timeout expired: {src}")
        return None

def convert_and_join_by_type(record_type: str):
    src_dir = OUTPUT_BASE / record_type
    out_dir = CONVERTED_BASE / record_type
    out_dir.mkdir(parents=True, exist_ok=True)

    xml_files = [
        f for f in list_xml_files(src_dir)
        if was_modified_today(f) and "Previously_Joined_MARC_Files" not in str(f)
    ]
    if not xml_files:
        log_marc(f"[{record_type}] No MARCXML files modified today; skipping.")
        return

    joined_output = JOINED_DIR / f"LC_Authorities_{record_type}_{datetime.now():%Y-%m-%d}.mrc"
    if joined_output.exists():
        version = 2
        while True:
            candidate = JOINED_DIR / f"LC_Authorities_{record_type}_{datetime.now():%Y-%m-%d}_v{version}.mrc"
            if not candidate.exists():
                joined_output = candidate
                break
            version += 1

    converted_files = []
    for src in xml_files:
        base = strip_marcxml_ext(src.stem)
        dest = out_dir / f"{base}.mrc"
        marker = dest.with_suffix(".mrc.joined")

        if marker.exists():
            log_marc(f"[{record_type}] [SKIP] Already joined previously: {dest.name}")
            continue

        if dest.exists() and was_modified_today(dest):
            log_marc(f"[{record_type}] [SKIP] Already converted today: {src.name}")
            converted_files.append(dest)
            continue

        result = run_conversion(src, dest)
        if result and result.returncode == 0 and dest.exists():
            log_marc(f"[{record_type}] SUCCESS: {src.name}")
            converted_files.append(dest)
        else:
            log_marc(f"[{record_type}] FAILED: {src.name}")

    if len(converted_files) < 2:
        log_marc(f"[{record_type}] Skipping join: only {len(converted_files)} file(s).")
        return

    try:
    with open(joined_output, "wb") as outfh:
        for name in sorted(converted_files):
            with open(name, "rb") as infh:
                shutil.copyfileobj(infh, outfh, length=1024 * 1024)

    
    if joined_output.stat().st_size > 0:
        log_marc(f"[{record_type}] SUCCESS: Joined MARC file created: {joined_output}")

        #  Move joined files into archive folder
        archive_dir = src_dir / "Previously_Joined_MARC_Files"
        archive_dir.mkdir(exist_ok=True)

        for name in converted_files:
            try:
                target = archive_dir / name.name
                shutil.move(str(name), str(target))
                log_marc(f"[{record_type}] Moved to archive: {name.name}")
            except Exception as e:
                log_marc(f"[{record_type}] WARNING: Couldn't move {name.name} to archive: {e}")
    else:
        log_marc(f"[{record_type}] WARNING: Joined MARC file is empty.")
except Exception as e:
    log_marc(f"[{record_type}] ERROR during join: {e}")



        if joined_output.stat().st_size > 0:
            log_marc(f"[{record_type}] SUCCESS: Joined MARC file created: {joined_output}")
        else:
            log_marc(f"[{record_type}] WARNING: Joined MARC file is empty.")
    except Exception as e:
        log_marc(f"[{record_type}] ERROR during join: {e}")

def run_marc_conversion_pipeline():
    if not MARCEDIT_PATH.exists():
        log_marc(f"ERROR: MARCEdit not found at {MARCEDIT_PATH}")
        return
    for typ in TARGET_TYPES:
        convert_and_join_by_type(typ)

# =================== MAIN ===================
def main():
    ensure_dirs(OUTPUT_BASE, LOG_FILE.parent, CSV_FILE.parent, CONVERTED_BASE, JOINED_DIR, LOG_FILE_MARCEDIT.parent)
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

    json_files = find_all_json_files_recursive(INPUT_DIR)
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
            folder = OUTPUT_BASE / k
            for url in sorted(buckets[k]):
                status = download_file(url, folder)
                results.append((k, url, status))

    save_csv_log(results)
    run_marc_conversion_pipeline()

if __name__ == "__main__":
    main()

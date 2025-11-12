# Created with ChatGPT 5; Skip join if all converted MARC files are older than existing joined file

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

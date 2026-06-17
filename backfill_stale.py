#!/usr/bin/env python3
"""
backfill_stale.py
=================
One-off script to correct pre-existing Firebase records where a journey was
marked COMPLETED by the old bug-prone logic (either via the partial provider
feed or the time-window fallback) but the final stop was never actually
confirmed reached.

Criteria for a "corrupted" record
----------------------------------
  metadata.status == "COMPLETED"
  AND  metadata.finalStopReached is absent, None, or False

These records predate the code fix that introduced STALE as a distinct status
and that reliably writes finalStopReached on every journey finalization.

What the script does
--------------------
  1. Reads all journeys/{busId}/{journeyKey}/metadata nodes from Firebase.
  2. Reads all busHistory/{busId}/{journeyKey}/metadata nodes from Firebase.
  3. For every matching record, updates:
       metadata.status             -> "STALE"
       metadata.staledAt           -> current Unix timestamp
       metadata.backfillNote       -> "Corrected by backfill_stale.py"
       (top-level) status          -> "STALE"   (busHistory archive only)
  4. Prints a summary table.

Usage
-----
  Dry run (default — no writes):
      python backfill_stale.py

  Apply changes to Firebase:
      python backfill_stale.py --apply

Requirements
------------
  pip install firebase-admin
  FIREBASE_CREDENTIALS env-var must be set (JSON string), OR place
  serviceAccountKey.json in the same directory (auto-detected).
"""

import sys
import os
import json
import time
import datetime

# ---------------------------------------------------------------------------
# Firebase init — mirrors the approach in main.py
# ---------------------------------------------------------------------------
import firebase_admin
from firebase_admin import credentials, db as firebase_db

DRY_RUN = "--apply" not in sys.argv

def init_firebase():
    cred_obj = None

    # 1. Try environment variable first (production / Heroku / Render)
    raw_env = os.environ.get("FIREBASE_CREDENTIALS")
    if raw_env:
        try:
            cred_obj = credentials.Certificate(json.loads(raw_env))
            print("[Firebase] Using credentials from FIREBASE_CREDENTIALS env-var.")
        except Exception as e:
            print(f"[Firebase] Failed to parse FIREBASE_CREDENTIALS: {e}")

    # 2. Fall back to local service-account file
    if cred_obj is None:
        key_path = os.path.join(os.path.dirname(__file__), "serviceAccountKey.json")
        if os.path.exists(key_path):
            cred_obj = credentials.Certificate(key_path)
            print(f"[Firebase] Using credentials from {key_path}")
        else:
            print("[Firebase] ERROR: No credentials found. Set FIREBASE_CREDENTIALS or provide serviceAccountKey.json.")
            sys.exit(1)

    firebase_admin.initialize_app(
        cred_obj,
        {"databaseURL": "https://realtime-tracking-3809e-default-rtdb.firebaseio.com"},
    )


# ---------------------------------------------------------------------------
# Backfill logic
# ---------------------------------------------------------------------------

NEW_STATUS = "STALE"
BACKFILL_NOTE = "Corrected by backfill_stale.py — was COMPLETED without finalStopReached"


def is_corrupted(metadata: dict) -> bool:
    """Return True if this record should be corrected."""
    if not isinstance(metadata, dict):
        return False
    if metadata.get("status") != "COMPLETED":
        return False
    fsr = metadata.get("finalStopReached")
    # Corrupted: field absent (None from Firebase), or explicitly False
    return fsr is None or fsr is False


def scan_tree(tree_name: str, data: dict) -> list[dict]:
    """
    Walk  {tree_name}/{busId}/{journeyKey}  and return a list of dicts:
        { path, bus_id, journey_key, metadata }
    for every record that matches is_corrupted().
    """
    hits = []
    if not isinstance(data, dict):
        return hits
    for bus_id, journey_map in data.items():
        if not isinstance(journey_map, dict):
            continue
        for journey_key, journey_data in journey_map.items():
            if not isinstance(journey_data, dict):
                continue
            meta = journey_data.get("metadata") or {}
            if is_corrupted(meta):
                hits.append({
                    "path": f"{tree_name}/{bus_id}/{journey_key}",
                    "bus_id": bus_id,
                    "journey_key": journey_key,
                    "tree": tree_name,
                    "metadata": meta,
                })
    return hits


def apply_correction(hit: dict, staled_at: int) -> None:
    """Write the corrected status to Firebase for a single record."""
    path = hit["path"]
    tree = hit["tree"]
    bus_id = hit["bus_id"]
    journey_key = hit["journey_key"]

    meta_update = {
        "status": NEW_STATUS,
        "staledAt": staled_at,
        "backfillNote": BACKFILL_NOTE,
    }

    updates = {
        f"{path}/metadata/status": NEW_STATUS,
        f"{path}/metadata/staledAt": staled_at,
        f"{path}/metadata/backfillNote": BACKFILL_NOTE,
    }

    # For busHistory, also update the top-level "status" field on the archive object
    if tree == "busHistory":
        updates[f"{path}/status"] = NEW_STATUS

    firebase_db.reference().update(updates)
    print(f"  [APPLIED] {path}  ->  status=STALE")


def main():
    init_firebase()

    mode = "DRY RUN" if DRY_RUN else "APPLY"
    print(f"\n{'='*60}")
    print(f"  backfill_stale.py  [{mode}]")
    print(f"  New status:  {NEW_STATUS}")
    if DRY_RUN:
        print("  Pass --apply to write changes to Firebase.")
    print(f"{'='*60}\n")

    # Read entire database (two trees only to minimise bandwidth)
    print("[1/3] Fetching journeys/ tree ...")
    journeys_data = firebase_db.reference("journeys").get() or {}
    print(f"      Found {len(journeys_data)} bus entries under journeys/")

    print("[2/3] Fetching busHistory/ tree ...")
    history_data = firebase_db.reference("busHistory").get() or {}
    print(f"      Found {len(history_data)} bus entries under busHistory/\n")

    # Scan both trees
    hits_j = scan_tree("journeys", journeys_data)
    hits_h = scan_tree("busHistory", history_data)
    all_hits = hits_j + hits_h

    # ---- Summary table ----
    print(f"[3/3] Corrupted records found: {len(all_hits)}")
    print(f"      journeys/    : {len(hits_j)}")
    print(f"      busHistory/  : {len(hits_h)}\n")

    if not all_hits:
        print("Nothing to correct. Exiting.")
        return

    # Print table
    col_w = max(len(h["path"]) for h in all_hits) + 2
    header = f"  {'PATH':<{col_w}}  CURRENT_STATUS  finalStopReached"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for h in all_hits:
        fsr = h["metadata"].get("finalStopReached")
        fsr_str = repr(fsr)
        print(f"  {h['path']:<{col_w}}  COMPLETED       {fsr_str}")

    print()

    if DRY_RUN:
        print("[DRY RUN] No changes written. Re-run with --apply to commit.")
        return

    # Apply corrections
    staled_at = int(time.time())
    print(f"Applying corrections (staledAt={staled_at}) ...")
    errors = 0
    for hit in all_hits:
        try:
            apply_correction(hit, staled_at)
        except Exception as e:
            print(f"  [ERROR] {hit['path']}: {e}")
            errors += 1

    print(f"\nDone. {len(all_hits) - errors} records updated, {errors} errors.")
    if errors == 0:
        print("All corrupted records corrected successfully.")


if __name__ == "__main__":
    main()

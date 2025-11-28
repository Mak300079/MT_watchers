import os
import requests
import time
import json
import pandas as pd
from datetime import datetime, UTC
from pathlib import Path
from dotenv import load_dotenv

# ==== NEW: DB imports ====
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging

# === CONFIG ===
API_URL   = "https://api-v2.pendle.finance/core/v1/assets/all"
PARAMS    = {"chainId": 1}  # Ethereum mainnet

load_dotenv()
BOT_TOKEN     = os.getenv("BOT_TOKEN")
CHAT_ID       = os.getenv("CHAT_ID")
DATABASE_URL  = os.getenv("DATABASE_URL")  # <-- required for DB

# Local state (still useful for diffing + snapshots; safe to keep)
BASE_DIR = Path(__file__).resolve().parent
STATE_FP = BASE_DIR / "pendle_assets_latest.json"   # canonical last snapshot
LOG_FP   = BASE_DIR / "pendle_new_assets_log.csv"   # append-only log (optional)
SNAP_DIR = BASE_DIR / "pendle_snapshots"            # dated snapshots

POLL_SECS = 15 * 60  # 15 minutes between checks

SNAP_DIR.mkdir(parents=True, exist_ok=True)
BASE_DIR.mkdir(parents=True, exist_ok=True)

# Logging
log = logging.getLogger("pendle_watcher")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

# ==== DB engine reused across cycles ====
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set; please set it in .env or Render Env Vars")
_engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def notify(msg: str):
    if not (BOT_TOKEN and CHAT_ID):
        log.info(msg)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=10,
        )
    except Exception as e:
        log.warning("[ERR][TELEGRAM] %s", e)

def fetch_assets():
    r = requests.get(API_URL, params=PARAMS, timeout=30)
    r.raise_for_status()
    return r.json()  # expects {"assets": [...]}

def load_state():
    if STATE_FP.exists():
        with open(STATE_FP, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"assets": []}

def atomic_save(path: Path, payload: dict):
    """Write safely so a crash never leaves a corrupt file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def save_state(payload):
    atomic_save(STATE_FP, payload)

def snapshot_payload(payload):
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    snap_fp = SNAP_DIR / f"assets_{ts}.json"
    atomic_save(snap_fp, payload)
    return snap_fp

def normalize_key(a: dict) -> str:
    """Stable identity per asset: chain address + symbol."""
    addr = (a.get("address") or "").lower()
    sym  = a.get("symbol") or ""
    return f"{addr}::{sym}"

def detect_new_assets(prev_assets, curr_assets):
    prev_set = {normalize_key(a) for a in prev_assets}
    return [a for a in curr_assets if normalize_key(a) not in prev_set]

# ==== DB helpers ====
UPSERT_SQL = text("""
    INSERT INTO pendle_assets(address, name, symbol, chain_id)
    VALUES (:address, :name, :symbol, :chain_id)
    ON CONFLICT (address) DO UPDATE
    SET name = EXCLUDED.name,
        symbol = EXCLUDED.symbol,
        chain_id = EXCLUDED.chain_id,
        last_seen_ts = now()
""")

def upsert_asset_batch(assets: list[dict]):
    """Upsert all current assets so last_seen_ts stays fresh."""
    rows = []
    for a in assets:
        rows.append({
            "address": (a.get("address") or "").lower(),
            "name": a.get("name") or a.get("symbol") or "",
            "symbol": a.get("symbol") or "",
            "chain_id": a.get("chainId") or a.get("chain_id") or PARAMS["chainId"],
        })
    if not rows:
        return
    try:
        with _engine.begin() as con:
            con.execute(UPSERT_SQL, rows)
    except SQLAlchemyError as e:
        log.error("[ERR][DB] Upsert failed: %s", e)

def append_log_csv(new_assets: list[dict]):
    """Optional: append-only CSV of just the NEW assets discovered in this cycle."""
    if not new_assets:
        return
    try:
        df = pd.DataFrame([{
            "ts": datetime.now(UTC).isoformat(timespec="seconds"),
            "address": (a.get("address") or "").lower(),
            "name": a.get("name") or a.get("symbol") or "",
            "symbol": a.get("symbol") or "",
            "chain_id": a.get("chainId") or a.get("chain_id") or PARAMS["chainId"],
        } for a in new_assets])
        header = not LOG_FP.exists()
        df.to_csv(LOG_FP, mode="a", header=header, index=False, encoding="utf-8")
    except Exception as e:
        log.warning("[WARN] Could not append CSV log: %s", e)

def one_cycle(cycle_idx: int):
    # 1) fetch current
    payload = fetch_assets()
    curr_assets = payload.get("assets", [])

    # 2) load previous state
    prev_payload = load_state()
    prev_assets  = prev_payload.get("assets", [])

    # 3) diff
    new_assets = detect_new_assets(prev_assets, curr_assets)

    # 4) DB persist (ALL current assets → keeps last_seen_ts fresh)
    upsert_asset_batch(curr_assets)

    # 5) notify/report
    ts = datetime.now(UTC).isoformat(timespec="seconds")
    if new_assets:
        asset_names  = [a.get("name") or a.get("symbol") or "?" for a in new_assets]
        msg = "Pendle watcher: " + str(len(new_assets)) + " new assets detected:\n" + "\n".join(asset_names)
        notify(msg)
        append_log_csv(new_assets)
        log.info("[%s] Found %s new assets.", ts, len(new_assets))
    else:
        log.info("[%s] No new assets.", ts)

    # 6) persist current as the new baseline (for next diffs)
    save_state(payload)

    # 7) occasional snapshot
    if cycle_idx % 96 == 0:
        snap_fp = snapshot_payload(payload)
        log.info("[%s] Snapshot saved → %s", ts, snap_fp)

# ---- stop-aware sleep helper ----
def stop_aware_sleep(stop_event, secs: int) -> bool:
    """Sleep up to secs, but exit early if stop_event is set. Returns True if stopped."""
    for _ in range(secs):
        if stop_event.is_set():
            return True
        time.sleep(1)
    return False

# ---- exported loop for Render ----
def run_forever(stop_event):
    """Long-running loop for Render Web Service (background thread)."""
    log.info("Starting Pendle asset watcher…")

    # DB startup sanity check
    try:
        with _engine.connect() as con:
            con.execute(text("SELECT 1"))
        log.info("DB connected.")
    except SQLAlchemyError as e:
        log.error("DB connection failed at startup: %s", e)

    i = 0
    while not stop_event.is_set():
        try:
            one_cycle(i)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            log.warning("[WARN] HTTP error %s: %s. Backing off 5 minutes.", status, e)
            if stop_aware_sleep(stop_event, 5 * 60):  # 5 minutes
                break
        except Exception as e:
            log.error("[ERROR] %s. Backing off 5 minutes.", e, exc_info=True)
            if stop_aware_sleep(stop_event, 5 * 60):
                break
        finally:
            i += 1
            if stop_aware_sleep(stop_event, POLL_SECS):
                break

    log.info("Pendle watcher stopping gracefully.")

# ---- local dev entrypoint ----
if __name__ == "__main__":
    from threading import Event
    run_forever(Event())

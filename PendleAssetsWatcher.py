import os
import requests
import time
import json
import pandas as pd
from datetime import datetime, UTC
from pathlib import Path
from dotenv import load_dotenv

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging

# === CONFIG ===
API_URL   = "https://api-v2.pendle.finance/core/v1/assets/all"
PARAMS    = {"chainId": 1}  # Ethereum mainnet

load_dotenv()
BOT_TOKEN     = os.getenv("BOT_TOKEN")
CHAT_ID       = os.getenv("CHAT_ID")
DATABASE_URL  = os.getenv("DATABASE_URL")  # required for DB

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set; please set it in Render env vars")

# Local folder mainly for snapshots (nice to keep)
BASE_DIR = Path(__file__).resolve().parent
SNAP_DIR = BASE_DIR / "pendle_snapshots"
SNAP_DIR.mkdir(parents=True, exist_ok=True)

POLL_SECS = 15 * 60  # 15 minutes between checks

# Logging
log = logging.getLogger("pendle_watcher")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")

# Single shared engine
_engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def notify(msg: str):
    """Send Telegram message if BOT_TOKEN + CHAT_ID available, else just log."""
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

def atomic_save(path: Path, payload: dict):
    """Write safely so a crash never leaves a corrupt file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def snapshot_payload(payload):
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    snap_fp = SNAP_DIR / f"assets_{ts}.json"
    atomic_save(snap_fp, payload)
    return snap_fp

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

def get_known_addresses() -> set[str]:
    """
    Return the set of lowercased addresses already present in pendle_assets.
    If table is empty, returns empty set.
    """
    try:
        with _engine.begin() as con:
            rows = con.execute(text("SELECT address FROM pendle_assets"))
            return { (row[0] or "").lower() for row in rows }
    except SQLAlchemyError as e:
        log.warning("[WARN][DB] Could not fetch known addresses: %s", e)
        return set()

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

def append_log_csv(new_assets: list[dict], log_fp: Path):
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
        header = not log_fp.exists()
        df.to_csv(log_fp, mode="a", header=header, index=False, encoding="utf-8")
    except Exception as e:
        log.warning("[WARN] Could not append CSV log: %s", e)

LOG_FP = BASE_DIR / "pendle_new_assets_log.csv"

def one_cycle(cycle_idx: int):
    # 0) ensure DB reachable
    try:
        with _engine.connect() as con:
            con.execute(text("SELECT 1"))
    except SQLAlchemyError as e:
        log.error("DB connection check failed: %s", e)
        return

    # 1) fetch current payload
    payload = fetch_assets()
    curr_assets = payload.get("assets", [])

    # 2) DB-based "already known" addresses
    known_addrs = get_known_addresses()
    bootstrap = len(known_addrs) == 0

    # 3) detect "new" vs DB
    new_assets = []
    for a in curr_assets:
        addr = (a.get("address") or "").lower()
        if addr and addr not in known_addrs:
            new_assets.append(a)

    # 4) upsert everything into DB
    upsert_asset_batch(curr_assets)

    # 5) notify/report
    ts = datetime.now(UTC).isoformat(timespec="seconds")
    if new_assets and not bootstrap:
        names = [a.get("name") or a.get("symbol") or "?" for a in new_assets]
        msg = "Pendle watcher: " + str(len(new_assets)) + " new assets detected:\n" + "\n".join(names)
        notify(msg)
        append_log_csv(new_assets, LOG_FP)
        log.info("[%s] Found %s new assets (DB-based).", ts, len(new_assets))
    elif bootstrap:
        log.info("[%s] Bootstrap run: %s assets loaded into DB (no Telegram).", ts, len(curr_assets))
    else:
        log.info("[%s] No new assets (DB-based).", ts)

    # 6) occasional snapshot
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

def run_forever(stop_event):
    """Long-running loop for Render Web Service / Worker."""
    log.info("Starting Pendle asset watcher…")

    # Initial DB sanity check
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
            if stop_aware_sleep(stop_event, 5 * 60):
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

if __name__ == "__main__":
    from threading import Event
    run_forever(Event())

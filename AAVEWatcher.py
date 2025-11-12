# -*- coding: utf-8 -*-
from __future__ import annotations

from web3 import Web3
from datetime import datetime, UTC
from web3.exceptions import ContractLogicError
from eth_abi import decode
from functools import lru_cache
import requests
from dotenv import load_dotenv
import os, time, math
import traceback

# -------------------------
# Config & setup
# -------------------------
load_dotenv()
ALCHEMY_API = os.getenv("ALCHEMY_API")
BOT_TOKEN   = os.getenv("BOT_TOKEN")
CHAT_ID     = os.getenv("CHAT_ID")

ALCHEMY_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API}"
w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL, request_kwargs={"timeout": 30}))

# PoolConfigurator (proxy) on ETH mainnet
CONFIGURATOR = Web3.to_checksum_address("0x64b761D848206f447Fe2dd461b0c635Ec39EbB27")

# Event topic0 hashes
TOPIC_SUPPLY_CAP_CHANGED = Web3.keccak(text="SupplyCapChanged(address,uint256,uint256)").hex()
TOPIC_BORROW_CAP_CHANGED = Web3.keccak(text="BorrowCapChanged(address,uint256,uint256)").hex()
#TOPIC_DEBT_CEIL_CHANGED = Web3.keccak(text="DebtCeilingChanged(address,uint256,uint256)").hex()

# Watcher parameters
MAX_BLOCK_SPAN   = 10    # Respect Alchemy free plan window (<= 10 blocks)
CONFIRMATIONS    = 3     # Safety against reorgs
POLL_SECONDS     = 5     # Sleep between polls when caught up
START_BLOCK_ENV  = os.getenv("AAVE_START_BLOCK")  # optional manual start

# -------------------------
# Alerting (Telegram)
# -------------------------
def notify(msg: str):
    if not (BOT_TOKEN and CHAT_ID):
        print("[WARN][NO-TELEGRAM]", msg)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=10,
        )
    except Exception as e:
        print("[ERR][TELEGRAM]", e)

# -------------------------
# Helpers to get asset label
# -------------------------
SEL_SYMBOL = Web3.keccak(text="symbol()")[:4]  # 0x95d89b41 (first 4 bytes)
SEL_NAME   = Web3.keccak(text="name()")[:4]    # 0x06fdde03

def _call_fn(to_addr: str, selector: bytes) -> bytes | None:
    """Raw eth_call helper. Returns return-data bytes or None on error/empty."""
    try:
        data = "0x" + selector.hex()
        out  = w3.eth.call({"to": Web3.to_checksum_address(to_addr), "data": data})
        return bytes(out) if out is not None else None
    except Exception:
        return None

def _decode_string_or_bytes32(ret: bytes) -> str | None:
    """Try decode dynamic string; if not, try bytes32 -> str."""
    if not ret:
        return None
    try:
        # dynamic string ABI: offset(32) | length(32) | bytes
        if len(ret) >= 96:
            length = int.from_bytes(ret[64:96], "big")
            raw    = ret[96:96+length]
            s = raw.decode(errors="ignore").strip("\x00").strip()
            if s:
                return s
        # bytes32 fallback
        s = ret.strip(b"\x00").decode(errors="ignore").strip()
        return s or None
    except Exception:
        return None

@lru_cache(maxsize=4096)
def resolve_token_label(addr: str) -> str:
    """Best-effort token label: symbol -> name -> address."""
    if not addr:
        return ""
    ca = Web3.to_checksum_address(addr)

    # Ensure it's a contract
    try:
        if not w3.eth.get_code(ca):
            return ca
    except Exception:
        return ca

    # symbol()
    ret = _call_fn(ca, SEL_SYMBOL)
    sym = _decode_string_or_bytes32(ret) if ret is not None else None
    if sym:
        return sym

    # name()
    ret = _call_fn(ca, SEL_NAME)
    nm  = _decode_string_or_bytes32(ret) if ret is not None else None
    if nm:
        return nm

    return ca

# -------------------------
# Log decoding
# -------------------------
def decode_cap_change_log(log) -> dict:
    """
    Decodes either SupplyCapChanged or BorrowCapChanged.
    Returns a dict with fields: event, block, tx, asset_addr, asset_label, old_cap, new_cap, ts.
    """
    topic0 = log["topics"][0].hex() if hasattr(log["topics"][0], "hex") else str(log["topics"][0])
    if topic0 == TOPIC_SUPPLY_CAP_CHANGED:
        event = "SupplyCapChanged"
    elif topic0 == TOPIC_BORROW_CAP_CHANGED:
        event = "BorrowCapChanged"
#    elif topic0 == TOPIC_DEBT_CEIL_CHANGED:
#        event = "DebtCeilingChanged"
    else:
        event = "UnknownEvent"

    # indexed address (topic[1]) = asset
    asset_addr = "0x" + log["topics"][1].hex()[-40:]
    asset_label = resolve_token_label(asset_addr)

    # data: oldCap, newCap
    data_hex = log["data"].hex() if isinstance(log["data"], (bytes, bytearray)) else str(log["data"])
    if data_hex.startswith("0x"):
        data_hex = data_hex[2:]
    old_cap, new_cap = decode(["uint256", "uint256"], bytes.fromhex(data_hex))

    # timestamp
    bn = log["blockNumber"]
    blk = w3.eth.get_block(bn)
    ts  = datetime.fromtimestamp(blk.timestamp, UTC)

    return {
        "event": event,
        "block": bn,
        "tx": log["transactionHash"].hex(),
        "asset_addr": asset_addr,
        "asset_label": asset_label,
        "old_cap": int(old_cap),
        "new_cap": int(new_cap),
        "ts": ts,
    }

def emit_event_msg(ev: dict):
    msg = (
        f"🔥 {ev['ts']:%Y-%m-%d %H:%M:%S %Z} | Block {ev['block']} | "
        f"{ev['event']} | Asset {ev['asset_label']} | "
        f"{ev['old_cap']} → {ev['new_cap']}"
    )
    print(msg)
    notify(msg)

# -------------------------
# Fetch & process
# -------------------------
def fetch_and_process(from_block: int, to_block: int) -> int:
    """
    Fetch logs in [from_block, to_block] for both cap-change events and process them.
    Returns number of logs processed.
    """
    if to_block < from_block:
        return 0

    # topics filter: OR on topic0 → pass as list in the first element
    # topics_filter = [[TOPIC_SUPPLY_CAP_CHANGED, TOPIC_BORROW_CAP_CHANGED,TOPIC_DEBT_CEIL_CHANGED]]
    topics_filter = [[TOPIC_SUPPLY_CAP_CHANGED, TOPIC_BORROW_CAP_CHANGED]]

    logs = w3.eth.get_logs({
        "address": CONFIGURATOR,
        "fromBlock": from_block,
        "toBlock": to_block,
        "topics": topics_filter,
    })

    count = 0
    for lg in logs:
        try:
            ev = decode_cap_change_log(lg)
            emit_event_msg(ev)
            count += 1
        except Exception:
            print("[ERR] failed to decode/process a log:")
            traceback.print_exc()
    if count:
        print(f"[INFO] Processed {count} cap-change logs in blocks {from_block}..{to_block}")
    return count

# -------------------------
# Main watcher loop
# -------------------------
def main():
    print("chainId:", w3.eth.chain_id)
    print("head:", w3.eth.block_number)
    print("CONFIGURATOR:", CONFIGURATOR)
    print("TOPIC_SUPPLY_CAP_CHANGED:", TOPIC_SUPPLY_CAP_CHANGED)
    print("TOPIC_BORROW_CAP_CHANGED:", TOPIC_BORROW_CAP_CHANGED)
    #print("TOPIC_DEBT_CEIL_CHANGED",TOPIC_DEBT_CEIL_CHANGED)

    head = w3.eth.block_number
    if START_BLOCK_ENV:
        try:
            last_processed = int(START_BLOCK_ENV)
        except ValueError:
            last_processed = head - max(CONFIRMATIONS, 1)
    else:
        # Start at the most recent safe block (so we don't miss the next block)
        last_processed = max(0, head - CONFIRMATIONS)

    print(f"[START] last_processed set to {last_processed}")

    backoff = 1
    while True:
        try:
            head = w3.eth.block_number
            safe_head = max(0, head - CONFIRMATIONS)

            # Nothing to do? sleep & continue
            if safe_head < last_processed:
                time.sleep(POLL_SECONDS)
                continue

            # Process in ≤10-block windows (Alchemy free)
            while last_processed <= safe_head:
                window_end = min(last_processed + MAX_BLOCK_SPAN - 1, safe_head)
                fetch_and_process(last_processed, window_end)
                last_processed = window_end + 1

            # reset backoff on success
            backoff = 1
            time.sleep(POLL_SECONDS)

        except Exception as e:
            print("[LOOP-ERR]", repr(e))
            traceback.print_exc()
            # Exponential backoff (capped)
            time.sleep(min(60, backoff))
            backoff = min(60, backoff * 2)

if __name__ == "__main__":
    main()


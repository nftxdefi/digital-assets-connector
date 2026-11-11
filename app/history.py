"""Wallet transaction history for the dashboard's tax review queue.

Sources (all read-only):
  Zerion      EVM chains + Solana: decoded transactions with the USD value of each transfer at the time.
              Needs ZERION_API_KEY. Covers every chain Zerion supports, not only the ones the balance tools check.
  Hyperliquid perps and spot fills (closed P&L, fees), funding, deposits, withdrawals and transfers. No key.
  Bitcoin     mempool.space. No key.
  Blockscout  Robinhood Chain fallback when Zerion does not cover it (BLOCKSCOUT_API_KEY).

Every source is normalised to one event per wallet per transaction:
  {id, src, chain, wallet, address, time, ms, hash, op, app, status, legs[], fee, ...}
  leg: {dir: in|out|self, sym, fam, contract, amt, usd, px, cp, own, peg, nft, fee}
Past history never changes, so normalised events are kept on disk (DATA_DIR, default /data) and only the
newest part is fetched again. classify() then labels every event and says whether it needs review.
"""
from __future__ import annotations

import asyncio
import base64
import contextvars
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx

import sources as S

log = logging.getLogger("digital-assets.history")

DATA = Path(S.env("DATA_DIR") or "/data")
HIST = DATA / "history"
# A fetcher that stops at one of its safety limits says so here; synced() keeps the note in the store and the
# coverage row shows it, so a cut-off history is never silent (Nov 10, 2026).
_cut: contextvars.ContextVar[list | None] = contextvars.ContextVar("history_cut", default=None)


def note_cut(msg: str) -> None:
    lst = _cut.get()
    if lst is not None and msg not in lst:
        lst.append(msg)
    log.warning("history cut off: %s", msg)


SYNC_TTL = int(os.getenv("HISTORY_SYNC_SECONDS", "600"))  # re-fetch the newest part at most this often
OVERLAP_MS = 2 * 86400 * 1000  # re-read the last two days on each sync (late indexing, pending txs)
ZERION = "https://api.zerion.io/v1"
HL_BRIDGE = "0x2df1c51e09aecf9cacb7bc98cb1742757f163df7"  # Hyperliquid's USDC bridge on Arbitrum

S._MIN_INTERVAL.setdefault("api.zerion.io", 0.4)
S._MIN_INTERVAL.setdefault("mempool.space", 0.2)

STABLE_USD = {"USDC", "USDT", "DAI", "USDS", "USDG", "PYUSD", "USDE", "GHO", "LUSD", "FRAX", "USDBC", "USDC.E",
              "USDT0", "RLUSD", "FDUSD", "TUSD", "USDH", "USDT.E", "USD₮0"}
STABLE_CAD = {"CADC", "QCAD", "CADT"}
# Same asset under another wrapper: a swap between them is a wrap, not a disposition.
FAMILY_ALIAS = {"WETH": "ETH", "WSOL": "SOL", "SOL": "SOL", "ETH": "ETH", "WHYPE": "HYPE", "HYPE": "HYPE",
                "WPOL": "POL", "WMATIC": "POL", "MATIC": "POL", "POL": "POL", "USDBC": "USDC", "USDC.E": "USDC",
                "USDC": "USDC"}
DEFI_OPS = {"deposit", "withdraw", "stake", "unstake", "borrow", "repay", "mint", "burn", "vault_deposit",
            "vault_withdraw"}
BRIDGE_RE = re.compile(r"relay|across|stargate|li\.?fi|jumper|bungee|socket|debridge|squid|hop|orbiter|"
                       r"rhino|layerzero|wormhole|mayan|synapse|circle|cctp|bridge|rango|gas\.zip", re.I)


# ---------------------------------------------------------------- small helpers
def _f(x: Any) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(Decimal(str(x)))
    except Exception:  # noqa: BLE001
        return None


def _r(x: float | None, nd: int = 10) -> float | None:
    return None if x is None else float(f"{x:.{nd}g}")


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def day_ms(day: str, end: bool = False) -> int:
    """'YYYY-MM-DD' (UTC) to milliseconds; end=True gives the last millisecond of that day."""
    d = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(d.timestamp() * 1000) + (86400 * 1000 - 1 if end else 0)


def now_ms() -> int:
    return int(time.time() * 1000)


def peg(sym: str | None) -> str | None:
    s = (sym or "").upper()
    return "USD" if s in STABLE_USD else ("CAD" if s in STABLE_CAD else None)


def family(leg: dict) -> str:
    s = (leg.get("sym") or "").upper()
    if s in FAMILY_ALIAS:
        return FAMILY_ALIAS[s]
    return leg.get("fam") or s


def leg(dir_: str, sym: str | None, amt: float | None, usd: float | None = None, px: float | None = None,
        cp: str | None = None, contract: str | None = None, fam: str | None = None, name: str | None = None,
        nft: bool = False, fee: bool = False) -> dict:
    out = {"dir": dir_, "sym": sym or "?", "amt": _r(amt), "usd": _r(usd, 8), "px": _r(px, 8), "cp": cp,
           "contract": contract, "fam": fam, "peg": peg(sym)}
    if name and name != sym:
        out["name"] = name
    if nft:
        out["nft"] = True
    if fee:
        out["fee"] = True
    return out


# ---------------------------------------------------------------- disk store
def _store_path(src: str, addr: str) -> Path:
    return HIST / f"{src}-{addr.lower() if addr.startswith('0x') else addr}.json"


def _load(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except Exception:  # noqa: BLE001
        return {}


def _save(path: Path, obj: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(obj, separators=(",", ":")))
        tmp.replace(path)
    except OSError as e:  # read-only or missing volume: keep working from memory
        log.warning("history store not writable (%s): %s", path, e)


_mem_store: dict[str, dict] = {}
_locks: dict[str, asyncio.Lock] = {}

# Long fetches are split into windows (Zerion's cursor paging silently dropped events on a 9,000-event fetch,
# Nov 6, 2026). 0 = one window. Set per source in _job.
WINDOW_DAYS = int(os.getenv("HISTORY_WINDOW_DAYS", "365"))
# After a source keeps answering HTTP 429, leave it alone for a while (doubling up to COOL_MAX) and serve the
# stored events instead: retrying only spends more of the daily allowance.
COOL_FIRST = int(os.getenv("HISTORY_COOL_SECONDS", "900"))
COOL_MAX = 4 * 3600
_cool: dict[str, dict] = {}  # src -> {"until": epoch seconds, "wait": seconds, "why": text}


class Cooling(Exception):
    """Source is in a cool-down after rate limiting; no request was made."""


def cooling(src: str) -> dict | None:
    c = _cool.get(src)
    return c if c and time.time() < c["until"] else None


def _note_429(src: str, e: httpx.HTTPStatusError) -> None:
    prev = _cool.get(src)
    wait = min(COOL_MAX, (prev["wait"] * 2) if prev and time.time() - prev["until"] < 3600 else COOL_FIRST)
    try:  # honour a long Retry-After (a daily quota), capped
        wait = max(wait, min(COOL_MAX, int(float(e.response.headers.get("retry-after", "0")))))
    except ValueError:
        pass
    _cool[src] = {"until": time.time() + wait, "wait": wait, "why": f"HTTP 429 from {e.request.url.host}"}
    log.warning("%s rate limited: pausing %s for %d s", e.request.url.host, src, wait)


def _windows(a: int, b: int, days: int) -> list[tuple[int, int]]:
    """[a, b] split into consecutive windows of `days` (ascending, inclusive ends)."""
    if days <= 0 or b - a <= days * 86400 * 1000:
        return [(a, b)]
    step, out, s = days * 86400 * 1000, [], a
    while s <= b:
        out.append((s, min(b, s + step - 1)))
        s += step
    return out


async def synced(src: str, addr: str, since: int, fetch, refresh: bool, window_days: int = 0,
                 cool_key: str | None = None) -> tuple[list[dict], dict]:
    """Events for one source and address from `since`, fetching only what is new since the last sync.

    Store: {v, from, through, synced, items}. `from`..`through` is fetched completely; `synced` is the time of the
    last successful sync. New results are merged by event id, never replacing what is stored, so a short page
    from the API can't lose events. If a window comes back with fewer events than are stored for it, it is
    fetched once more. If a fetch fails, the stored events are returned with `error` set (nothing is lost and the
    next sync starts where the last good one ended)."""
    path = _store_path(src, addr)
    lock = _locks.setdefault(str(path), asyncio.Lock())
    async with lock:
        st = _mem_store.get(str(path)) or _load(path)
        if not st or st.get("v") != STORE_VERSION:
            st = {"v": STORE_VERSION, "from": since, "through": 0, "synced": 0, "items": {}}
        st.setdefault("through", st.get("synced", 0))  # stores written before Nov 7, 2026
        if src == "hyperliquid" and _rekey_hashless(st):
            _mem_store[str(path)] = st
            _save(path, st)
        if not st["items"] and not st["through"]:
            st["from"] = since
        now = now_ms()
        fresh = now - st.get("synced", 0) < SYNC_TTL * 1000
        info = {"fetched": 0, "cached": len(st["items"]), "synced_at": _iso(st["synced"]) if st["synced"] else None,
                "error": None, "rechecked": 0}
        plan: list[tuple[str, int, int]] = []
        if st["from"] > since:  # an earlier start than stored: fetch only the missing part, newest window first
            plan += [("back", a, b) for a, b in reversed(_windows(since, st["from"] - 1 + OVERLAP_MS, window_days))]
        if refresh or not fresh:
            # Resume from where the last sync ended whatever `since` this call asks for: starting at a later
            # `since` would mark the days in between as fetched without ever fetching them (Nov 10, 2026).
            start = max(st["from"], st["through"] - OVERLAP_MS) if st["through"] else max(since, st["from"])
            plan += [("fwd", a, b) for a, b in _windows(start, now, window_days)]
        changed = False
        try:
            if plan and cool_key and (c := cooling(cool_key)):
                raise Cooling(f"{c['why']}: paused until {_iso(int(c['until'] * 1000))[11:16]} UTC")
            for kind, a, b in plan:
                before = sum(1 for e in st["items"].values() if a <= e["ms"] <= b)
                cuts: list = []
                tok = _cut.set(cuts)
                try:
                    new = await fetch(addr, a, b)
                    if before and len(new) < before:  # paging likely dropped some: ask once more and merge both
                        new = {e["id"]: e for e in new + await fetch(addr, a, b)}.values()
                        info["rechecked"] += 1
                finally:
                    _cut.reset(tok)
                for c in cuts:
                    msg = f"{c} ({_iso(a)[:10]} to {_iso(b)[:10]})"
                    if msg not in st.setdefault("cut", []):
                        st["cut"].append(msg)
                for e in new:
                    st["items"][e["id"]] = e
                info["fetched"] += len(new)
                if kind == "back":
                    st["from"] = min(st["from"], a)
                else:
                    st["through"] = b
                changed = True
                _mem_store[str(path)] = st
                _save(path, st)  # keep progress window by window
            if any(k == "fwd" for k, _, _ in plan):
                st["synced"] = now
                info["synced_at"] = _iso(now)
                if cool_key:
                    _cool.pop(cool_key, None)
        except Cooling as e:
            info["error"] = str(e)
        except Exception as e:  # noqa: BLE001
            if cool_key and isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 429:
                _note_429(cool_key, e)
            log.warning("history %s %s: %s (serving %d stored events)", src, addr, e, len(st["items"]))
            info["error"] = S.short_err(e) if not isinstance(e, httpx.HTTPStatusError) else (
                S.short_err(e) + (": check ZERION_API_KEY" if e.response.status_code == 401 and "zerion" in src else ""))
        if changed:
            _mem_store[str(path)] = st
            _save(path, st)
        items = [e for e in st["items"].values() if e["ms"] >= since]
        if info["error"]:
            info["kept"] = len(items)
        if st.get("cut"):
            info["cut"] = "History may be incomplete: " + "; ".join(st["cut"][-3:])
        return items, info


STORE_VERSION = 1


# ---------------------------------------------------------------- Zerion (EVM + Solana)
def _zauth() -> dict:
    key = S.env("ZERION_API_KEY")
    return {"Authorization": "Basic " + base64.b64encode(f"{key}:".encode()).decode()}


# Our chain keys for Zerion's ids (others keep Zerion's id).
_Z_ALIASES = {"ethereum": "ethereum", "base": "base", "arbitrum": "arbitrum", "optimism": "optimism",
              "polygon": "polygon", "solana": "solana", "robinhood": "robinhood", "robinhood-chain": "robinhood",
              "hyperevm": "hyperevm", "hyperliquid": "hyperevm", "hyper-evm": "hyperevm", "xdai": "gnosis"}


async def zerion_chains() -> dict[str, str]:
    """Zerion chain id -> our chain key, for chains whose transactions Zerion indexes."""

    async def go():
        r = await S.get_json(f"{ZERION}/chains/", headers=_zauth())
        out = {}
        for c in r.get("data") or []:
            cid = c.get("id") or ""
            a = c.get("attributes") or {}
            if not (a.get("flags") or {}).get("supports_transactions", True):
                continue
            name = (a.get("name") or "").lower()
            key = _Z_ALIASES.get(cid)
            if not key and "robinhood" in (cid + name):
                key = "robinhood"
            if not key and ("hyperevm" in cid or "hyperevm" in name.replace(" ", "")):
                key = "hyperevm"
            out[cid] = key or cid
        return out

    return await S.cached("zerion:chains", go, False, ttl=86400)


def _impl_contract(fi: dict, zchain: str) -> str | None:
    for im in fi.get("implementations") or []:
        if im.get("chain_id") == zchain:
            return im.get("address")
    return None


def zerion_event(tx: dict, addr: str, nick: str, zmap: dict) -> dict | None:
    a = tx.get("attributes") or {}
    if a.get("status") == "pending":
        return None
    zchain = (((tx.get("relationships") or {}).get("chain") or {}).get("data") or {}).get("id") or "?"
    chain = zmap.get(zchain, _Z_ALIASES.get(zchain, zchain))
    acts = {x.get("id"): x for x in a.get("acts") or []}
    legs = []
    for t in a.get("transfers") or []:
        fi, ni = t.get("fungible_info"), t.get("nft_info")
        q = t.get("quantity") or {}
        amt = _f(q.get("numeric") if q.get("numeric") is not None else q.get("float"))
        d = t.get("direction")
        cp = t.get("recipient") if d == "out" else t.get("sender")
        is_fee = (acts.get(t.get("act_id")) or {}).get("type") == "fee"
        if fi:
            legs.append(leg(d, fi.get("symbol"), amt, _f(t.get("value")), _f(t.get("price")), cp,
                            _impl_contract(fi, zchain), fi.get("id"), fi.get("name"), fee=is_fee))
        elif ni:
            nm = ((ni.get("name") or "NFT") + "")[:40]
            legs.append(leg(d, nm, amt or 1, _f(t.get("value")), None, cp, ni.get("contract_address"),
                            None, None, nft=True))
    fee = None
    fe = a.get("fee") or {}
    fq = _f((fe.get("quantity") or {}).get("numeric") or (fe.get("quantity") or {}).get("float"))
    sent_from = (a.get("sent_from") or "").lower()
    paid = sent_from == addr.lower() or (a.get("operation_type") != "receive" and not a.get("paymaster"))
    if fq and paid:
        fi = fe.get("fungible_info") or {}
        fee = {"sym": fi.get("symbol") or "?", "amt": _r(fq), "usd": _r(_f(fe.get("value")), 6)}
    app = (a.get("application_metadata") or {})
    h = a.get("hash") or tx.get("id")
    ms = _ms(a["mined_at"]) if a.get("mined_at") else 0
    return {"id": f"{chain}:{h}:{addr.lower()[:10]}", "src": "zerion", "chain": chain, "wallet": nick,
            "address": addr, "time": _iso(ms), "ms": ms, "hash": h, "op": a.get("operation_type"),
            "app": app.get("name"), "method": (app.get("method") or {}).get("name") if isinstance(app.get("method"), dict) else None,
            "to": a.get("sent_to"), "status": a.get("status") or "confirmed", "legs": legs, "fee": fee}


ZERION_MAX_PAGES = 300


async def zerion_fetch(addr: str, nick: str, start: int, end: int, solana: bool) -> list[dict]:
    zmap = await zerion_chains()
    params: dict[str, Any] = {"currency": "usd", "page[size]": 100, "filter[min_mined_at]": start,
                              "filter[max_mined_at]": end, "filter[trash]": "only_non_trash"}
    if solana:
        params["filter[chain_ids]"] = "solana"
    url: str | None = f"{ZERION}/wallets/{addr}/transactions/"
    out: list[dict] = []
    for _ in range(ZERION_MAX_PAGES):  # 30,000 transactions at most
        r = await S.get_json(url, params, headers=_zauth())
        for tx in r.get("data") or []:
            e = zerion_event(tx, addr, nick, zmap)
            if e:
                out.append(e)
        url = (r.get("links") or {}).get("next")
        params = None
        if not url or not r.get("data"):
            break
    else:
        note_cut(f"Zerion list cut off after {ZERION_MAX_PAGES} pages")
    return out


# ---------------------------------------------------------------- Hyperliquid
_hl_lock = asyncio.Lock()
_hl_last = 0.0


async def hl_post(body: dict) -> Any:
    """History calls are heavy on Hyperliquid's rate limit: space them out (balance calls are not affected)."""
    global _hl_last
    async with _hl_lock:
        wait = 0.35 - (time.monotonic() - _hl_last)
        if wait > 0:
            await asyncio.sleep(wait)
        _hl_last = time.monotonic()
    return await S.post_json(S.HL_INFO, body)


async def hl_spot_names() -> dict[str, tuple[str, str]]:
    """Spot coin id as used in fills ('@107', 'PURR/USDC') -> (base token, quote token)."""

    async def go():
        meta = await S.post_json(S.HL_INFO, {"type": "spotMeta"})
        names = {t["index"]: t["name"] for t in meta.get("tokens", [])}
        out = {}
        for u in meta.get("universe", []):
            toks = u.get("tokens") or []
            if len(toks) == 2:
                pair = (names.get(toks[0], "?"), names.get(toks[1], "USDC"))
                out[u.get("name")] = pair
                out[f"@{u.get('index')}"] = pair
        return out

    return await S.cached("hl:spotnames", go, False, ttl=3600)


# Each request returns at most 500 to 2,000 entries; funding alone is 24 a day per open position. 400 requests
# cover a rebuild of several years (was 80, with no warning when reached).
HL_MAX_CALLS = int(os.getenv("HISTORY_HL_MAX_CALLS", "400"))


async def _hl_paged(kind: str, addr: str, start: int, end: int, key, extra: dict | None = None) -> list:
    """Time-range endpoints return a capped batch (500 to 2000 items). A capped batch is a contiguous run of
    time from one end of the window, so the rest lies before its first or after its last item: query both."""
    seen: dict[Any, Any] = {}
    windows = [(start, end)]
    calls = 0
    while windows and calls < HL_MAX_CALLS:
        lo, hi = windows.pop()
        calls += 1
        batch = await hl_post({"type": kind, "user": addr, "startTime": lo, "endTime": hi, **(extra or {})})
        if not isinstance(batch, list) or not batch:
            continue
        new = 0
        for x in batch:
            k = key(x)
            if k not in seen:
                seen[k] = x
                new += 1
        if len(batch) < 500 or new == 0:
            continue
        times = [int(x.get("time") or 0) for x in batch]
        tmin, tmax = min(times), max(times)
        if tmax < hi:
            windows.append((tmax, hi))
        if tmin > lo:
            windows.append((lo, tmin))
    if windows:
        note_cut(f"Hyperliquid {kind} cut off after {HL_MAX_CALLS} requests ({len(seen)} entries read)")
    return list(seen.values())


def _hl_ev(addr: str, nick: str, eid: str, ms: int, h: str | None, op: str, legs: list, **kw) -> dict:
    return {"id": eid, "src": "hyperliquid", "chain": "hyperliquid", "wallet": nick, "address": addr,
            "time": _iso(ms), "ms": ms, "hash": h, "op": op, "app": "Hyperliquid", "status": "confirmed",
            "legs": legs, "fee": kw.pop("fee", None), **kw}


async def hl_fetch(addr: str, nick: str, start: int, end: int) -> list[dict]:
    me = addr.lower()
    spot = await hl_spot_names()
    fills, ledger = await asyncio.gather(
        _hl_paged("userFillsByTime", addr, start, end, lambda x: x.get("tid") or (x.get("hash"), x.get("time"), x.get("oid"))),
        _hl_paged("userNonFundingLedgerUpdates", addr, start, end,
                  lambda x: (x.get("hash"), x.get("time"), json.dumps(x.get("delta"), sort_keys=True))))
    # Funding: main dex, plus each HIP-3 dex that has fills (the dex argument is not documented for funding;
    # if Hyperliquid ignores or rejects it the duplicates collapse on the event id).
    fkey = lambda x: (x.get("time"), (x.get("delta") or {}).get("coin"))  # noqa: E731
    funding = await _hl_paged("userFunding", addr, start, end, fkey)
    # The main call already returns HIP-3 funding (coins like "xyz:BE"). Ask a dex separately only when it
    # has fills but none of its coins came back, in case some dex is only reachable that way.
    have = {(x.get("delta") or {}).get("coin", "").split(":", 1)[0] for x in funding if ":" in str((x.get("delta") or {}).get("coin"))}
    dexes = sorted({(f.get("coin") or "").split(":", 1)[0] for f in fills if ":" in (f.get("coin") or "")} - have)
    for d in dexes:
        try:
            funding += await _hl_paged("userFunding", addr, start, end, fkey, {"dex": d})
        except Exception:  # noqa: BLE001
            pass

    out: list[dict] = []
    for f in fills:
        coin = f.get("coin") or "?"
        ms = int(f.get("time") or 0)
        sz, px, fee = _f(f.get("sz")) or 0, _f(f.get("px")) or 0, _f(f.get("fee")) or 0
        ftok = f.get("feeToken") or "USDC"
        fee_d = {"sym": ftok, "amt": _r(fee), "usd": _r(fee * (1 if peg(ftok) else px), 6) if fee else 0}
        eid = f"hyperliquid:fill:{f.get('tid') or f.get('hash')}"
        if coin in spot or coin.startswith("@"):
            base, quote = spot.get(coin, (coin, "USDC"))
            buy = f.get("side") == "B"
            val = sz * px
            legs = [leg("in" if buy else "out", base, sz, val if peg(quote) else None, px if peg(quote) else None,
                        "hyperliquid:spot"),
                    leg("out" if buy else "in", quote, val, val if peg(quote) else None, 1 if peg(quote) else None,
                        "hyperliquid:spot")]
            out.append(_hl_ev(addr, nick, eid, ms, f.get("hash"), "trade", legs, fee=fee_d,
                              spot={"coin": coin, "base": base, "quote": quote, "side": "buy" if buy else "sell",
                                    "sz": sz, "px": px}))
        else:
            dex, _, sym = coin.rpartition(":")
            out.append(_hl_ev(addr, nick, eid, ms, f.get("hash"), "perp", [], fee=fee_d,
                              perp={"coin": coin, "sym": sym, "dex": dex, "dir": f.get("dir"), "side": f.get("side"),
                                    "sz": sz, "px": px, "start": _f(f.get("startPosition")),
                                    "pnl": _f(f.get("closedPnl")) or 0, "fee": fee, "ntl": _r(sz * px, 10),
                                    "liq": bool(f.get("liquidation"))}))
    for x in funding:
        d = x.get("delta") or {}
        ms = int(x.get("time") or 0)
        usdc = _f(d.get("usdc")) or 0
        coin = d.get("coin") or "?"
        out.append(_hl_ev(addr, nick, f"hyperliquid:funding:{ms}:{coin}", ms, x.get("hash"), "funding", [],
                          funding={"coin": coin, "sym": coin.rpartition(":")[2], "usdc": usdc,
                                   "szi": _f(d.get("szi")), "rate": _f(d.get("fundingRate"))}))
    for x in ledger:
        out.append(hl_ledger_event(addr, nick, me, x))
    return out


def _hashless(h: Any) -> bool:
    return not h or set(str(h)[2:]) <= {"0"}


def _ledger_key(e: dict) -> str:
    """Id for a ledger entry that has no transaction hash, from what the entry says (type, time, amounts). Before
    Nov 10, 2026 the id used the entry's position in the fetched list, which changes from one sync to the next, so
    the same entry was stored again and again."""
    legs = [[l.get("dir"), l.get("sym"), l.get("amt")] for l in e.get("legs") or []]
    body = json.dumps([e.get("kind"), e.get("ms"), legs, e.get("raw")], sort_keys=True, default=str)
    return f"hyperliquid:ledger:nohash:{e.get('kind')}:{e.get('ms')}:{hashlib.sha1(body.encode()).hexdigest()[:12]}"


_OLD_HASHLESS = re.compile(r"^hyperliquid:ledger:(None|0x0*):.+:\d+:\d+$")


def _rekey_hashless(st: dict) -> bool:
    """Give entries stored under the old position-based ids their new id; copies of one entry collapse into one."""
    old = [k for k in st.get("items", {}) if _OLD_HASHLESS.match(k)]
    for k in old:
        e = st["items"].pop(k)
        e["id"] = _ledger_key(e)
        st["items"][e["id"]] = e
    return bool(old)


def hl_ledger_event(addr: str, nick: str, me: str, x: dict, i: int = 0) -> dict:
    e = _hl_ledger_event(addr, nick, me, x)
    if _hashless(x.get("hash")):
        e["id"] = _ledger_key(e)
    return e


def _hl_ledger_event(addr: str, nick: str, me: str, x: dict) -> dict:
    d = x.get("delta") or {}
    typ = d.get("type") or "?"
    ms = int(x.get("time") or 0)
    h = x.get("hash")
    eid = f"hyperliquid:ledger:{h}:{typ}"
    usdc = _f(d.get("usdc"))
    fee = _f(d.get("fee"))
    fee_d = {"sym": "USDC", "amt": fee, "usd": fee} if fee else None

    def mk(op, legs, **kw):
        return _hl_ev(addr, nick, eid, ms, h, op, legs, fee=fee_d, kind=typ, **kw)

    if typ == "deposit":
        return mk("deposit_bridge", [leg("in", "USDC", usdc, usdc, 1, HL_BRIDGE)])
    if typ == "withdraw":
        return mk("withdraw_bridge", [leg("out", "USDC", usdc, usdc, 1, HL_BRIDGE)])
    if typ in ("internalTransfer", "subAccountTransfer"):
        out = (d.get("user") or "").lower() == me
        cp = d.get("destination") if out else d.get("user")
        return mk("send" if out else "receive", [leg("out" if out else "in", "USDC", usdc, usdc, 1, cp)])
    if typ in ("spotTransfer", "send"):
        src, dst = (d.get("user") or "").lower(), (d.get("destination") or "").lower()
        tok = d.get("token") or "?"
        amt, val = _f(d.get("amount")), _f(d.get("usdcValue"))
        if src == dst == me:  # between this account's own dexes or spot and perps
            return mk("internal", [leg("self", tok, amt, val, None, addr)])
        out = src == me
        return mk("send" if out else "receive",
                  [leg("out" if out else "in", tok, amt, val, (val / amt) if val and amt else None,
                       d.get("destination") if out else d.get("user"))])
    if typ in ("accountClassTransfer", "cStakingTransfer"):
        tok = d.get("token") or "USDC"
        return mk("internal", [leg("self", tok, usdc if usdc is not None else _f(d.get("amount")), None, None, addr)])
    if typ == "vaultDeposit":
        return mk("vault_deposit", [leg("out", "USDC", usdc, usdc, 1, d.get("vault"))])
    if typ == "vaultWithdraw":
        net = _f(d.get("netWithdrawnUsd")) or _f(d.get("requestedUsd"))
        return mk("vault_withdraw", [leg("in", "USDC", net, net, 1, d.get("vault"))],
                  vault={"basis": _f(d.get("basis")), "commission": _f(d.get("commission")),
                         "closing": _f(d.get("closingCost"))})
    if typ == "vaultDistribution":
        return mk("reward", [leg("in", "USDC", usdc, usdc, 1, d.get("vault"))])
    if typ == "rewardsClaim":
        amt = _f(d.get("amount"))
        tok = d.get("token") or "USDC"
        return mk("reward", [leg("in", tok, amt, amt if peg(tok) else None, 1 if peg(tok) else None, "hyperliquid")])
    if typ == "spotGenesis":
        return mk("reward", [leg("in", d.get("token") or "?", _f(d.get("amount")), None, None, "hyperliquid")])
    if typ == "liquidation":
        return mk("liquidation", [], raw=d)
    return mk(typ, [], raw=d)


# ---------------------------------------------------------------- Bitcoin (mempool.space)
async def btc_price(day: str) -> float | None:
    async def go():
        ts = day_ms(day) // 1000 + 43200
        r = await S.get_json(f"{S.MEMPOOL}/api/v1/historical-price", {"currency": "USD", "timestamp": ts})
        p = (r.get("prices") or [{}])[0]
        return p.get("USD")

    return await price_cached(f"btc:{day}", go)


async def btc_fetch(addr: str, nick: str, start: int, end: int) -> list[dict]:
    out, last = [], None
    for _ in range(200):
        url = f"{S.MEMPOOL}/api/address/{addr}/txs/chain" + (f"/{last}" if last else "")
        txs = await S.get_json(url)
        if not txs:
            break
        stop = False
        for tx in txs:
            st = tx.get("status") or {}
            if not st.get("confirmed"):
                continue
            ms = int(st.get("block_time", 0)) * 1000
            if ms < start:
                stop = True
                break
            if ms > end:
                continue
            got = sum(o.get("value", 0) for o in tx.get("vout", []) if o.get("scriptpubkey_address") == addr)
            spent = sum((i.get("prevout") or {}).get("value", 0) for i in tx.get("vin", [])
                        if (i.get("prevout") or {}).get("scriptpubkey_address") == addr)
            others_in = [(i.get("prevout") or {}).get("scriptpubkey_address") for i in tx.get("vin", [])]
            others_out = [o.get("scriptpubkey_address") for o in tx.get("vout", [])]
            day = _iso(ms)[:10]
            px = await btc_price(day)
            legs, fee = [], None
            if spent:
                fee_sat = tx.get("fee", 0)
                sent = spent - got - fee_sat
                fee = {"sym": "BTC", "amt": fee_sat / 1e8, "usd": _r(fee_sat / 1e8 * px, 6) if px else None}
                if sent > 0:
                    cp = next((a for a in others_out if a and a != addr), None)
                    legs.append(leg("out", "BTC", sent / 1e8, sent / 1e8 * px if px else None, px, cp, fam="btc"))
            elif got:
                cp = next((a for a in others_in if a and a != addr), None)
                legs.append(leg("in", "BTC", got / 1e8, got / 1e8 * px if px else None, px, cp, fam="btc"))
            out.append({"id": f"bitcoin:{tx['txid']}:{addr[:10]}", "src": "mempool", "chain": "bitcoin",
                        "wallet": nick, "address": addr, "time": _iso(ms), "ms": ms, "hash": tx["txid"],
                        "op": "send" if spent else "receive", "app": None, "status": "confirmed", "legs": legs,
                        "fee": fee})
        last = txs[-1]["txid"]
        if stop or len(txs) < 25:
            break
    return out


# ---------------------------------------------------------------- Blockscout (Robinhood Chain fallback)
async def cg_day_price(cg_id: str, day: str) -> float | None:
    """Daily USD price from CoinGecko (demo keys reach back 365 days)."""

    async def go():
        base, h = S._cg_base()
        d = datetime.strptime(day, "%Y-%m-%d").strftime("%d-%m-%Y")
        r = await S.get_json(f"{base}/coins/{cg_id}/history", {"date": d, "localization": "false"}, headers=h)
        return ((r.get("market_data") or {}).get("current_price") or {}).get("usd")

    return await price_cached(f"cg:{cg_id}:{day}", go)


async def _bs_pages(chain: str, path: str, start: int, params: dict | None = None) -> list[dict]:
    items, q = [], dict(params or {})
    for _ in range(100):
        r = await S.bs_get(chain, path + ("?" + urlencode(q) if q else ""))
        batch = r.get("items") or []
        items += batch
        nxt = r.get("next_page_params")
        if not nxt or not batch:
            break
        if batch[-1].get("timestamp") and _ms(batch[-1]["timestamp"]) < start:
            break
        q = {**(params or {}), **{k: v for k, v in nxt.items() if v is not None}}
    else:
        note_cut(f"Blockscout {path.rsplit('/', 1)[-1]} cut off after 100 pages")
    return items


async def blockscout_fetch(chain: str, addr: str, nick: str, start: int, end: int) -> list[dict]:
    me = addr.lower()
    cfg = S.EVM_CHAINS[chain]
    txs, toks, ints = await asyncio.gather(
        _bs_pages(chain, f"/addresses/{addr}/transactions", start),
        _bs_pages(chain, f"/addresses/{addr}/token-transfers", start, {"type": "ERC-20"}),
        _bs_pages(chain, f"/addresses/{addr}/internal-transactions", start))
    by: dict[str, dict] = {}

    def ev(h: str, ts: str) -> dict:
        ms = _ms(ts)
        return by.setdefault(h, {"id": f"{chain}:{h}:{me[:10]}", "src": "blockscout", "chain": chain,
                                 "wallet": nick, "address": addr, "time": _iso(ms), "ms": ms, "hash": h,
                                 "op": None, "app": None, "status": "confirmed", "legs": [], "fee": None,
                                 "_native": []})

    for t in txs:
        if not t.get("timestamp"):
            continue
        e = ev(t["hash"], t["timestamp"])
        if t.get("status") not in (None, "ok"):
            e["status"] = "failed"
        e["op"] = t.get("method") or e["op"]
        frm, to = ((t.get("from") or {}).get("hash") or "").lower(), ((t.get("to") or {}).get("hash") or "").lower()
        val = int(t.get("value") or 0)
        if frm == me:
            fee = int((t.get("fee") or {}).get("value") or 0)
            if fee:
                e["fee"] = {"sym": cfg["native"], "amt": fee / 1e18, "usd": None}
        if val and e["status"] == "confirmed" and frm != to:
            e["_native"].append(("out" if frm == me else "in", val, to if frm == me else frm))
    for t in ints:
        if not t.get("timestamp") or t.get("success") is False:
            continue
        e = ev(t.get("transaction_hash"), t["timestamp"])
        frm, to = ((t.get("from") or {}).get("hash") or "").lower(), ((t.get("to") or {}).get("hash") or "").lower()
        val = int(t.get("value") or 0)
        if val and frm != to and me in (frm, to):
            e["_native"].append(("out" if frm == me else "in", val, to if frm == me else frm))
    for t in toks:
        tk = t.get("token") or {}
        if tk.get("reputation") == "scam" or tk.get("is_scam"):
            continue
        h = t.get("transaction_hash") or t.get("tx_hash")
        if not h or not t.get("timestamp"):
            continue
        e = ev(h, t["timestamp"])
        frm, to = ((t.get("from") or {}).get("hash") or "").lower(), ((t.get("to") or {}).get("hash") or "").lower()
        if frm == to:
            continue
        tot = t.get("total") or {}
        amt = float(S.scale(tot.get("value"), tot.get("decimals") or tk.get("decimals")))
        sym = tk.get("symbol")
        stable = peg(sym) == "USD"
        e["legs"].append(leg("out" if frm == me else "in", sym, amt, amt if stable else None, 1 if stable else None,
                             to if frm == me else frm, tk.get("address_hash") or tk.get("address"), None, tk.get("name")))
    out = []
    for e in by.values():
        if not (start <= e["ms"] <= end):
            continue
        day = e["time"][:10]
        px = None
        if e["_native"] or e["fee"]:
            try:
                px = await cg_day_price(cfg["cg"], day)
            except Exception:  # noqa: BLE001
                px = None
        for d, val, cp in e.pop("_native"):
            amt = val / 1e18
            e["legs"].append(leg(d, cfg["native"], amt, amt * px if px else None, px, cp, fam=cfg["cg"]))
        if e["fee"] and px:
            e["fee"]["usd"] = _r(e["fee"]["amt"] * px, 6)
        out.append(e)
    return out


# ---------------------------------------------------------------- price store (historical prices never change)
_prices: dict[str, Any] | None = None


async def price_cached(key: str, fn) -> float | None:
    global _prices
    path = HIST / "prices.json"
    if _prices is None:
        _prices = _load(path)
    if key in _prices:
        return _prices[key]
    try:
        v = await fn()
    except Exception:  # noqa: BLE001
        return None
    if v is not None:
        _prices[key] = v
        _save(path, _prices)
    return v


# ---------------------------------------------------------------- classification
def _total_usd(legs: list[dict]) -> float:
    return sum(abs(l["usd"]) for l in legs if l.get("usd") is not None)


def _priced(l: dict) -> bool:
    """A market value exists (a price of exactly 0 means worthless, the same as no price)."""
    return bool(l.get("usd")) or l.get("peg") is not None


def _set(e: dict, cls: str, auto: bool, why: str) -> None:
    e["class"], e["auto"], e["why"] = cls, auto, why


def classify(events: list[dict], own: set[str], dust_usd: float = 1.0) -> None:
    """Label every event in place: class, auto (True = certain, no review needed), why (plain words)."""
    own = {a.lower() for a in own}
    groups: dict[str, list[dict]] = {}
    for e in events:
        if e.get("hash") and e["src"] != "hyperliquid":
            groups.setdefault(f"{e['chain']}:{e['hash'].lower()}", []).append(e)
    for g, es in groups.items():
        if len({x["address"].lower() for x in es}) > 1:
            for x in es:
                x["group"] = g

    for e in events:
        for l in e["legs"]:
            l["own"] = bool(l.get("cp")) and l["cp"].lower() in own
        op = (e.get("op") or "").lower()

        if e["src"] == "hyperliquid":
            if op == "perp":
                p = e["perp"]
                _set(e, "perp", True, f"{p['dir'] or 'Fill'} {p['sym']}" + (f", closed P&L {p['pnl']:+.2f} USDC" if p["pnl"] else ""))
                continue
            if op == "funding":
                _set(e, "funding", True, f"Funding on {e['funding']['sym']}")
                continue
            if op == "trade":
                _set(e, "swap", True, "Hyperliquid spot trade")
                continue
            if op == "internal":
                _set(e, "self", True, "Moved inside this Hyperliquid account (spot, perps or staking)")
                continue
            if op in ("deposit_bridge", "withdraw_bridge"):
                _set(e, "bridge", True, "Hyperliquid bridge deposit" if op == "deposit_bridge" else "Hyperliquid bridge withdrawal")
                continue
            if op in ("vault_deposit", "vault_withdraw"):
                _set(e, "defi", False, "Hyperliquid vault " + ("deposit" if op == "vault_deposit" else "withdrawal"))
                continue
            if op == "reward":
                _set(e, "reward", False, "Hyperliquid reward or vault distribution")
                continue
            if op == "liquidation":
                _set(e, "perp", False, "Liquidation: check the closed P&L")
                continue
            if not e["legs"]:
                _set(e, "other", False, f"Hyperliquid {e.get('kind') or op}: not recognised, please check")
                continue

        if e.get("status") == "failed":
            _set(e, "failed", True, "Failed transaction (only the fee was spent)")
            continue
        legs = [l for l in e["legs"] if l["dir"] in ("in", "out") and not l.get("fee")]
        if any(l.get("fee") for l in e["legs"]):
            e["fee_tokens"] = [{"sym": l["sym"], "amt": l["amt"], "usd": l["usd"]} for l in e["legs"] if l.get("fee")]
        if not legs:
            _set(e, "approve" if op == "approve" else "gas", True,
                 "Approval only" if op == "approve" else "No tokens moved (contract call or fee only)")
            continue
        ext = [l for l in legs if not l["own"]]
        if not ext:
            _set(e, "self", True, "Between your own wallets")
            continue
        outs = [l for l in ext if l["dir"] == "out"]
        ins = [l for l in ext if l["dir"] == "in"]
        usd = _total_usd(ext)
        if not any(_priced(l) for l in ext):
            _set(e, "unpriced", True, "Only tokens with no market price moved (spam, test or receipt tokens)")
            continue
        if all((l.get("cp") or "").lower() == HL_BRIDGE for l in ext):
            _set(e, "bridge", True, "Deposit to Hyperliquid" if outs else "Withdrawal from Hyperliquid")
            continue
        if outs and ins:
            fo, fi = {family(l) for l in outs}, {family(l) for l in ins}
            ao, ai = sum(l["amt"] or 0 for l in outs), sum(l["amt"] or 0 for l in ins)
            if fo == fi and len(fo) == 1 and ao and abs(ai - ao) / ao < 0.01:
                _set(e, "wrap", True, f"Wrap or unwrap of {next(iter(fo))}")
            elif op in DEFI_OPS:
                _set(e, "defi", False, f"DeFi {op}" + (f" on {e['app']}" if e.get("app") else ""))
            elif all(_priced(l) for l in outs) or all(_priced(l) for l in ins):
                _set(e, "swap", True, "Swap: " + ", ".join(l["sym"] for l in outs) + " for " + ", ".join(l["sym"] for l in ins))
            else:
                _set(e, "swap", False, "Swap with no price on either side")
        elif ins:
            if not any(_priced(l) for l in ins):
                _set(e, "unpriced", True, "Received a token with no market price (airdrop or spam)")
            elif op in ("claim",) or (e.get("app") and re.search(r"stak|reward|airdrop|claim", e["app"], re.I)):
                _set(e, "reward", False, "Claim or reward: income at its value when received")
            elif op in DEFI_OPS:
                _set(e, "defi", False, f"DeFi {op}" + (f" on {e['app']}" if e.get("app") else ""))
            elif usd < dust_usd:
                _set(e, "dust", True, f"Under US${dust_usd:g}")
            else:
                _set(e, "receive", False, "Received from an address that isn't one of your wallets")
        else:
            if not any(_priced(l) for l in outs):
                _set(e, "unpriced", True, "Sent a token with no market price")
            elif op in DEFI_OPS:
                _set(e, "defi", False, f"DeFi {op}" + (f" on {e['app']}" if e.get("app") else ""))
            elif usd < dust_usd:
                _set(e, "dust", True, f"Under US${dust_usd:g}")
            else:
                _set(e, "send", False, "Sent to an address that isn't one of your wallets")

    _match_bridges(events)
    _flag_sold_airdrops(events)
    for e in events:
        ext = [l for l in e["legs"] if l["dir"] in ("in", "out") and not l.get("own") and not l.get("fee")]
        if ext and all(l.get("peg") for l in ext):
            e["stable_only"] = "CAD" if all(l["peg"] == "CAD" for l in ext) else "USD"


def _one_sided(e: dict, d: str) -> list[dict]:
    return [l for l in e["legs"] if l["dir"] == d and not l["own"] and not l.get("fee")]


def _match_bridges(events: list[dict]) -> None:
    """Pair a send on one chain with a receive on another chain (any of your wallets) a little later.

    Same asset (USDC to USDC, ETH to ETH) within 5% of the amount and 3 hours: a bridge, your own move.
    Different assets through a bridge app within 30 minutes and 15% of the value: a cross-chain swap.
    """
    sends = [e for e in events if e.get("class") in ("send", "defi", "bridge") and e["src"] != "hyperliquid"
             and _one_sided(e, "out") and not _one_sided(e, "in")]
    recvs = [e for e in events if e.get("class") in ("receive", "reward", "bridge", "dust")
             and _one_sided(e, "in") and not _one_sided(e, "out")]
    # Hyperliquid deposits are receipts on the Hyperliquid side; the matching send is on Arbitrum (or another
    # chain via a deposit service). Hyperliquid withdrawals are the send side.
    sends += [e for e in events if e["src"] == "hyperliquid" and e.get("op") in ("withdraw_bridge", "send")]
    recvs += [e for e in events if e["src"] == "hyperliquid" and e.get("op") in ("deposit_bridge", "receive")]
    recvs.sort(key=lambda e: e["ms"])
    used: set[str] = set()
    for s in sorted(sends, key=lambda e: e["ms"]):
        if s.get("pair"):
            continue
        so = _one_sided(s, "out")
        if len(so) != 1:
            continue
        o = so[0]
        best = None
        for r in recvs:
            if r["id"] in used or r.get("pair"):
                continue
            dt = r["ms"] - s["ms"]
            if dt < -300_000:
                continue
            if dt > 3 * 3600_000:
                break
            ri = _one_sided(r, "in")
            if len(ri) != 1:
                continue
            i = ri[0]
            if r["chain"] == s["chain"] and r["src"] == s["src"]:
                # Same chain: only a swap split over two transactions by the same wallet with the same
                # contract (one that pays out separately), never a bridge. Different contracts on each side
                # is usually a lending loop (deposit to one, borrow from another): left for review.
                ov, iv = o.get("usd"), i.get("usd")
                if (r["address"] == s["address"] and abs(dt) <= 180_000 and family(i) != family(o)
                        and (o.get("cp") or "").lower() == (i.get("cp") or "x").lower()
                        and ov and iv and abs(iv - ov) / ov <= 0.2):
                    best = (r, "split")
                    break
                continue
            if family(i) == family(o) and o["amt"] and 0.95 <= (i["amt"] or 0) / o["amt"] <= 1.001:
                best = (r, "bridge")
                break
            ov, iv = o.get("usd"), i.get("usd")
            via_bridge = any(BRIDGE_RE.search(x or "") for x in (s.get("app"), r.get("app"), s.get("op"), r.get("op")))
            if via_bridge and dt <= 1800_000 and ov and iv and abs(iv - ov) / ov <= 0.15:
                best = (r, "xswap")
                break
        if not best:
            continue
        r, kind = best
        used.add(r["id"])
        s["pair"], r["pair"] = r["id"], s["id"]
        if kind == "split":
            i = _one_sided(r, "in")[0]
            why = f"Swap in two steps: {o['sym']} for {i['sym']}" + (f" ({s['app'] or r['app']})" if s.get("app") or r.get("app") else "")
            _set(s, "swap", True, why)
            _set(r, "swap", True, why)
            s["xchain"], r["xchain"] = "out", "in"
        elif kind == "bridge":
            _set(s, "bridge", True, f"Bridged {o['sym']} to {r['chain']} ({r['wallet']})")
            _set(r, "bridge", True, f"Bridged {o['sym']} from {s['chain']} ({s['wallet']})")
        else:
            i = _one_sided(r, "in")[0]
            _set(s, "swap", True, f"Cross-chain swap: {o['sym']} on {s['chain']} for {i['sym']} on {r['chain']}")
            _set(r, "swap", True, f"Cross-chain swap: {o['sym']} on {s['chain']} for {i['sym']} on {r['chain']}")
            s["xchain"], r["xchain"] = "out", "in"


def _flag_sold_airdrops(events: list[dict]) -> None:
    """An unpriced token that was received and later sent or swapped away had value after all: review it."""
    out_tokens = {(e["chain"], (l.get("contract") or l["sym"]).lower())
                  for e in events if e.get("class") in ("swap", "defi") and e["src"] != "hyperliquid"
                  and any(x["dir"] == "in" and _priced(x) for x in e["legs"] if not x.get("own"))
                  for l in e["legs"] if l["dir"] == "out" and not _priced(l)}
    for e in events:
        if e.get("class") == "unpriced" and any(l["dir"] == "in" and (e["chain"], (l.get("contract") or l["sym"]).lower()) in out_tokens
                                                for l in e["legs"]):
            _set(e, "receive", False, "Received with no price, but later swapped for a priced token: it had a value")


# ---------------------------------------------------------------- build
def sources_for(addr: str) -> list[str]:
    t = S.address_type(addr)
    zerion = bool(S.env("ZERION_API_KEY"))
    if t == "evm":
        return (["zerion"] if zerion else []) + ["hyperliquid", "robinhood"]
    if t == "solana":
        return ["zerion"] if zerion else []
    if t == "bitcoin":
        return ["bitcoin"]
    return []


async def _job(src: str, w: dict, since: int, refresh: bool) -> tuple[list[dict], dict]:
    addr, nick = w["address"].strip(), w.get("nickname") or w["address"][:8]
    cov = {"wallet": nick, "address": addr, "source": src, "events": 0, "error": None, "note": None}
    try:
        if src == "zerion":
            sol = S.address_type(addr) == "solana"
            items, info = await synced("zerion", addr, since,
                                       lambda a, s, e: zerion_fetch(a, nick, s, e, sol), refresh,
                                       window_days=WINDOW_DAYS, cool_key="zerion")
            try:
                zchains = set((await zerion_chains()).values()) if not cooling("zerion") else None
            except Exception:  # noqa: BLE001  (the note is optional)
                zchains = None
            if not sol and zchains:
                missing = [c for c in S.EVM_ADDRESS_CHAINS if c not in zchains and c not in ("hyperliquid", "robinhood")]
                if missing:
                    cov["note"] = "Not covered by Zerion: " + ", ".join(missing)
        elif src == "hyperliquid":
            items, info = await synced("hyperliquid", addr, since, lambda a, s, e: hl_fetch(a, nick, s, e), refresh)
        elif src == "bitcoin":
            items, info = await synced("bitcoin", addr, since, lambda a, s, e: btc_fetch(a, nick, s, e), refresh)
        elif src == "robinhood":
            try:
                zc = set((await zerion_chains()).values()) if S.env("ZERION_API_KEY") and not cooling("zerion") else set()
            except Exception:  # noqa: BLE001
                zc = set()
            if "robinhood" in zc or (S.env("ZERION_API_KEY") and not zc and not _store_path(
                    "blockscout-robinhood", addr).exists()):
                return [], {**cov, "note": "covered by Zerion"}
            items, info = await synced("blockscout-robinhood", addr, since,
                                       lambda a, s, e: blockscout_fetch("robinhood", a, nick, s, e), refresh)
        else:
            return [], cov
        for e in items:  # nickname may have changed since the events were stored
            e["wallet"] = nick
        cov.update(events=len(items), fetched=info["fetched"], synced_at=info["synced_at"])
        if info.get("rechecked"):
            cov["rechecked"] = info["rechecked"]
        if info.get("error"):  # fetch failed: these are the stored events up to synced_at
            cov.update(error=info["error"], kept=True)
        if info.get("cut"):
            cov["note"] = "; ".join(x for x in (cov.get("note"), info["cut"]) if x)
        return items, cov
    except Exception as e:  # noqa: BLE001
        log.warning("history %s %s: %s", src, addr, e)
        cov["error"] = S.short_err(e) if not isinstance(e, httpx.HTTPStatusError) else (
            S.short_err(e) + (": check ZERION_API_KEY" if src == "zerion" and e.response.status_code == 401 else ""))
        return [], cov


_builds: dict[str, dict] = {}
BUILD_TTL = int(os.getenv("HISTORY_BUILD_SECONDS", "300"))
BUILD_KEEP = 3


async def _build(wallets: list[dict], own: set[str], since: int, dust: float, refresh: bool, state: dict) -> dict:
    jobs = [(src, w) for w in wallets for src in sources_for(w["address"])]
    state["progress"] = {"done": 0, "total": len(jobs)}
    sem = asyncio.Semaphore(4)

    async def run(src, w):
        async with sem:
            r = await _job(src, w, since, refresh)
        state["progress"]["done"] += 1
        return r

    results = await asyncio.gather(*(run(s, w) for s, w in jobs))
    events = [e for items, _ in results for e in items]
    # Deep copy so classification never alters the stored events.
    events = json.loads(json.dumps(events))
    classify(events, own, dust)
    events.sort(key=lambda e: (e["ms"], e["id"]))
    cov = [c for _, c in results]
    for w in wallets:
        if not sources_for(w["address"]):
            cov.append({"wallet": w.get("nickname"), "address": w["address"], "source": None, "events": 0,
                        "error": None, "note": "No history source for this address type"
                        + ("" if S.env("ZERION_API_KEY") else " (Solana needs ZERION_API_KEY)")})
    if not S.env("ZERION_API_KEY"):
        cov.append({"wallet": None, "address": None, "source": "zerion", "events": 0,
                    "error": "ZERION_API_KEY is not set: no history for EVM chains or Solana", "note": None})
    return {"built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "events": events, "coverage": cov}


async def history(wallets: list[dict], own_extra: list[str] | None, since: str, until: str | None,
                  only: list[str] | None, status: str, dust_usd: float, refresh: bool, offset: int, limit: int,
                  wait_seconds: float, funding: str = "events", tz: str = "UTC") -> dict:
    since_ms = day_ms(since)
    until_ms = day_ms(until, end=True) if until else None
    own = {w["address"].strip() for w in wallets} | {a.strip() for a in (own_extra or []) if a}
    key = hashlib.sha1(json.dumps([sorted(a.lower() for a in own), since, dust_usd,
                                   sorted(w["address"].lower() for w in wallets)]).encode()).hexdigest()[:16]
    b = _builds.get(key)
    if refresh and b and (b.get("task") or (b.get("result") and time.time() - b["at"] < 60)):
        refresh = False  # a poll repeating refresh=True must not restart a build that just ran
    stale = not b or (b.get("result") and time.time() - b["at"] > BUILD_TTL)
    if refresh or stale or (b.get("error") and not b.get("task")):
        if not (b and b.get("task") and not b["task"].done()):
            b = {"at": time.time(), "result": None, "progress": {"done": 0, "total": 0}, "error": None}
            b["task"] = asyncio.ensure_future(_build(wallets, own, since_ms, dust_usd, refresh, b))
            _builds[key] = b
            # each build holds every event: keep the newest few (a new set of arguments used to stay for good)
            idle = sorted((k for k, v in _builds.items() if k != key and not v.get("task")), key=lambda k: _builds[k]["at"])
            for k in idle[:max(0, len(_builds) - BUILD_KEEP)]:
                _builds.pop(k, None)
    task = b.get("task")
    if task and not task.done():
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=max(0.0, min(wait_seconds, 50.0)))
        except asyncio.TimeoutError:
            return {"status": "building", "progress": b["progress"],
                    "message": "Still collecting history; call again in a few seconds with the same arguments."}
    if task and task.done():
        if task.exception():
            b["error"] = S.short_err(task.exception())  # type: ignore[arg-type]
            b["task"] = None
            return {"status": "error", "error": b["error"]}
        b["result"], b["at"], b["task"] = task.result(), time.time(), None
    res = b["result"]
    evs = res["events"]
    if until_ms:
        evs = [e for e in evs if e["ms"] <= until_ms]
    if only:
        want = {a.strip().lower() for a in only}
        evs = [e for e in evs if e["address"].lower() in want]
    counts: dict[str, int] = {}
    for e in evs:
        counts[e["class"]] = counts.get(e["class"], 0) + 1
    review = sum(1 for e in evs if not e["auto"])
    daily = None
    if funding == "daily":
        # funding is one event per position per hour (more than half of all events): sum it per day, wallet and coin
        evs, daily, tz = funding_by_day(evs, tz)
    if status == "review":
        evs = [e for e in evs if not e["auto"]]
    elif status == "auto":
        evs = [e for e in evs if e["auto"]]
    page = evs[offset: offset + limit]
    out = {"status": "ready", "built_at": res["built_at"], "since": since, "until": until,
           "total": len(evs), "offset": offset, "next_offset": offset + limit if offset + limit < len(evs) else None,
           "counts": counts, "review": review, "coverage": res["coverage"],
           "perps_summary": perps_summary(res["events"], since_ms, until_ms, only),
           "events": page}
    if daily is not None:
        out["funding_tz"] = tz
        out["funding_events"] = sum(r["n"] for r in daily)
        # the whole list comes with the first page only; later pages carry just the count
        out["funding_daily"] = daily if offset == 0 else None
    return out


def funding_by_day(events: list[dict], tz: str) -> tuple[list[dict], list[dict], str]:
    """Split out the funding events and sum them per local day (in tz), wallet and coin.
    Returns (the other events, [{d, wallet, address, coin, sym, usdc, n}], the time zone used)."""
    try:
        zone: Any = ZoneInfo(tz)
    except Exception:  # noqa: BLE001  unknown name or no zone data: UTC days
        zone, tz = timezone.utc, "UTC"
    rest: list[dict] = []
    agg: dict[tuple, dict] = {}
    for e in events:
        f = e.get("funding")
        if e.get("class") != "funding" or not f:
            rest.append(e)
            continue
        d = datetime.fromtimestamp(e["ms"] / 1000, zone).strftime("%Y-%m-%d")
        sym = f.get("sym") or f.get("coin") or "?"
        a = agg.setdefault((d, e.get("wallet") or "", e["address"].lower(), sym),
                           {"d": d, "wallet": e.get("wallet") or "", "address": e["address"], "coin": f.get("coin") or sym,
                            "sym": sym, "usdc": 0.0, "n": 0})
        a["usdc"] += f.get("usdc") or 0
        a["n"] += 1
    rows = sorted(agg.values(), key=lambda r: (r["d"], r["wallet"], r["sym"]))
    for r in rows:
        r["usdc"] = round(r["usdc"], 6)
    return rest, rows, tz


def perps_summary(events: list[dict], since: int, until: int | None, only: list[str] | None) -> list[dict]:
    """Per wallet and coin: fills, closed P&L, trading fees and net funding (USDC) in the range."""
    want = {a.lower() for a in only} if only else None
    agg: dict[tuple, dict] = {}
    for e in events:
        if e["src"] != "hyperliquid" or e["ms"] < since or (until and e["ms"] > until):
            continue
        if want and e["address"].lower() not in want:
            continue
        if e["op"] == "perp":
            p = e["perp"]
            a = agg.setdefault((e["wallet"], p["coin"]), {"wallet": e["wallet"], "coin": p["coin"], "fills": 0,
                                                          "closed_pnl": 0.0, "fees": 0.0, "funding": 0.0})
            a["fills"] += 1
            a["closed_pnl"] += p["pnl"] or 0
            a["fees"] += p["fee"] or 0
        elif e["op"] == "funding":
            f = e["funding"]
            a = agg.setdefault((e["wallet"], f["coin"]), {"wallet": e["wallet"], "coin": f["coin"], "fills": 0,
                                                          "closed_pnl": 0.0, "fees": 0.0, "funding": 0.0})
            a["funding"] += f["usdc"] or 0
    out = sorted(agg.values(), key=lambda a: (a["wallet"], a["coin"]))
    for a in out:
        for k in ("closed_pnl", "fees", "funding"):
            a[k] = round(a[k], 6)
    return out


# ---------------------------------------------------------------- Bank of Canada exchange rates
BOC_VALET = "https://www.bankofcanada.ca/valet/observations"
BOC_SERIES = {"FXUSDCAD", "FXEURCAD", "FXGBPCAD", "FXJPYCAD", "FXCHFCAD", "FXAUDCAD", "FXCNYCAD", "FXHKDCAD",
              "FXMXNCAD", "FXINRCAD"}


async def boc_rates(series: str, since: str, until: str | None) -> dict:
    """Bank of Canada daily rates (one value per business day, published about 16:30 ET) from the Valet API."""
    series = (series or "FXUSDCAD").upper()
    if series not in BOC_SERIES:
        return {"error": f"Unknown series {series!r}; use one of " + ", ".join(sorted(BOC_SERIES))}
    params = {"start_date": since}
    if until:
        params["end_date"] = until

    async def go():
        return await S.get_json(f"{BOC_VALET}/{series}/json", params)

    r = await S.cached(f"boc:{series}:{since}:{until}", go, False, ttl=3600)
    rates: dict[str, float] = {}
    for o in r.get("observations") or []:
        v = _f(((o.get(series) or {}).get("v")))
        if o.get("d") and v:
            rates[o["d"]] = v
    days = sorted(rates)
    return {"series": series, "source": "Bank of Canada Valet " + series, "since": since, "until": until,
            "count": len(days), "first": days[0] if days else None, "last": days[-1] if days else None,
            "rates": rates}

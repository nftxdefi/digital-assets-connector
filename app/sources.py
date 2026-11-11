"""Balance and price sources for the Digital Assets connector.

Every fetcher returns a dict:
  {"chain": key, "name": label, "tokens": [...], "error": str | None}
Each token: symbol, name, chain, amount, price_usd (or None), value_usd (or None),
contract (or None), kind ("native" | "token" | "staked" | "perps" | "perp" | "vault").
"perp" is one open Hyperliquid position (value = its margin, so value is not amount x price);
"perps" is perps margin not tied to a position.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx

HERE = Path(__file__).resolve().parent
TIMEOUT = httpx.Timeout(25.0, connect=10.0)
HEADERS = {"User-Agent": "digital-assets-connector/1.0", "Accept": "application/json"}
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "60"))

# ---------------------------------------------------------------- chains
EVM_CHAINS: dict[str, dict[str, Any]] = {
    "ethereum": {"name": "Ethereum", "chain_id": 1, "blockscout": "https://eth.blockscout.com", "native": "ETH", "cg": "ethereum", "alchemy": "eth-mainnet"},
    "base": {"name": "Base", "chain_id": 8453, "blockscout": "https://base.blockscout.com", "native": "ETH", "cg": "ethereum", "alchemy": "base-mainnet"},
    "arbitrum": {"name": "Arbitrum", "chain_id": 42161, "blockscout": "https://arbitrum.blockscout.com", "native": "ETH", "cg": "ethereum", "alchemy": "arb-mainnet"},
    "optimism": {"name": "Optimism", "chain_id": 10, "blockscout": "https://optimism.blockscout.com", "native": "ETH", "cg": "ethereum", "alchemy": "opt-mainnet"},
    "polygon": {"name": "Polygon", "chain_id": 137, "blockscout": "https://polygon.blockscout.com", "native": "POL", "cg": "polygon-ecosystem-token", "alchemy": "matic-mainnet"},
    "robinhood": {"name": "Robinhood Chain", "chain_id": 4663, "blockscout": "https://robinhoodchain.blockscout.com", "native": "ETH", "cg": "ethereum"},
}
OTHER_CHAINS = {
    "hyperevm": {"name": "HyperEVM"},
    "hyperliquid": {"name": "Hyperliquid"},
    "solana": {"name": "Solana"},
    "bitcoin": {"name": "Bitcoin"},
}
ALL_CHAINS = {**{k: {"name": v["name"]} for k, v in EVM_CHAINS.items()}, **OTHER_CHAINS}
EVM_ADDRESS_CHAINS = list(EVM_CHAINS) + ["hyperevm", "hyperliquid"]

# Optional per-chain explorer overrides, e.g. BLOCKSCOUT_URL_ROBINHOOD=https://...
for _k, _v in EVM_CHAINS.items():
    _o = os.getenv(f"BLOCKSCOUT_URL_{_k.upper()}")
    if _o:
        _v["blockscout"] = _o.rstrip("/")

HL_INFO = os.getenv("HYPERLIQUID_INFO_URL", "https://api.hyperliquid.xyz/info")
HYPEREVM_RPC = os.getenv("HYPEREVM_RPC_URL", "https://rpc.hyperliquid.xyz/evm")
MEMPOOL = os.getenv("MEMPOOL_URL", "https://mempool.space").rstrip("/")
SOLANA_PUBLIC_RPC = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")


def env(name: str) -> str:
    return (os.getenv(name) or "").strip()


# ---------------------------------------------------------------- helpers
_cache: dict[str, tuple[float, Any]] = {}
_client: httpx.AsyncClient | None = None
_host_limits: dict[str, asyncio.Semaphore] = {}


def client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True)
    return _client


def _sem(url: str) -> asyncio.Semaphore:
    host = httpx.URL(url).host
    if host not in _host_limits:
        _host_limits[host] = asyncio.Semaphore(3)
    return _host_limits[host]


# Minimum seconds between requests per host (free tiers: Blockscout key 5 req/s).
_MIN_INTERVAL = {"api.blockscout.com": 0.25}
_DEFAULT_INTERVAL = 0.1  # be polite to keyless public explorers too
_next_slot: dict[str, float] = {}
_slot_lock = asyncio.Lock()


async def _pace(url: str) -> None:
    host = httpx.URL(url).host
    gap = _MIN_INTERVAL.get(host, _DEFAULT_INTERVAL if "blockscout" in host else 0)
    if not gap:
        return
    async with _slot_lock:
        now = time.monotonic()
        slot = max(now, _next_slot.get(host, 0))
        _next_slot[host] = slot + gap
    if slot > now:
        await asyncio.sleep(slot - now)


def _retry_after(r: httpx.Response, attempt: int) -> float:
    try:
        return min(float(r.headers.get("retry-after", "")), 10.0)
    except ValueError:
        return 1.0 * (2 ** attempt)


_inflight: dict[str, asyncio.Task] = {}


async def cached(key: str, fn, refresh: bool = False, ttl: int = CACHE_TTL):
    """TTL cache that also shares one in-flight request between concurrent callers."""
    now = time.time()
    hit = _cache.get(key)
    if hit and not refresh and now - hit[0] < ttl:
        return hit[1]
    task = _inflight.get(key)
    if task is None:
        task = asyncio.ensure_future(fn())
        _inflight[key] = task
        try:
            val = await task
        finally:
            _inflight.pop(key, None)
        _cache[key] = (time.time(), val)
        return val
    return await asyncio.shield(task)


async def get_json(url: str, params: dict | None = None, headers: dict | None = None, retries: int = 4) -> Any:
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            async with _sem(url):
                await _pace(url)
                r = await client().get(url, params=params, headers=headers)
            if r.status_code == 429 and attempt < retries:
                await asyncio.sleep(_retry_after(r, attempt))
                continue
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPError, ValueError) as e:  # ValueError: bad JSON
            last = e
            if isinstance(e, httpx.HTTPStatusError) and e.response.status_code not in (429, 500, 502, 503, 504):
                break
            if attempt < retries:
                await asyncio.sleep(1.0 * (attempt + 1))
    raise last  # type: ignore[misc]


async def post_json(url: str, body: Any, headers: dict | None = None, retries: int = 4) -> Any:
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            async with _sem(url):
                await _pace(url)
                r = await client().post(url, json=body, headers=headers)
            if r.status_code == 429 and attempt < retries:
                await asyncio.sleep(_retry_after(r, attempt))
                continue
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPError, ValueError) as e:
            last = e
            if isinstance(e, httpx.HTTPStatusError) and e.response.status_code not in (429, 500, 502, 503, 504):
                break
            if attempt < retries:
                await asyncio.sleep(1.0 * (attempt + 1))
    raise last  # type: ignore[misc]


def D(x: Any) -> Decimal:
    if x is None or x == "":
        return Decimal(0)
    try:
        return Decimal(str(x))
    except InvalidOperation:
        return Decimal(0)


def scale(raw: Any, decimals: Any) -> Decimal:
    try:
        d = int(decimals or 0)
    except (TypeError, ValueError):
        d = 0
    return D(raw) / (Decimal(10) ** d)


def token(symbol, name, chain, amount: Decimal, price: Decimal | None, contract=None, kind="token") -> dict:
    value = (amount * price) if price is not None else None
    return {
        "symbol": symbol or "?",
        "name": name or symbol or "?",
        "chain": chain,
        "amount": float(amount),
        "price_usd": float(price) if price is not None else None,
        "value_usd": float(value) if value is not None else None,
        "contract": contract,
        "kind": kind,
    }


def short_err(e: Exception) -> str:
    if isinstance(e, httpx.HTTPStatusError):
        return f"HTTP {e.response.status_code} from {e.request.url.host}"
    if isinstance(e, RuntimeError):
        return str(e)[:200]
    return f"{type(e).__name__}: {e}"[:200]


# ---------------------------------------------------------------- address type
EVM_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
B58_RE = re.compile(f"^[{B58}]+$")


def address_type(addr: str) -> str:
    a = addr.strip()
    if EVM_RE.match(a):
        return "evm"
    low = a.lower()
    if low.startswith("bc1") and 14 <= len(a) <= 74:
        return "bitcoin"
    if low[:4] in ("xpub", "ypub", "zpub"):
        return "bitcoin-xpub"
    if B58_RE.match(a):
        if a[0] in "13" and 26 <= len(a) <= 35:
            return "bitcoin"
        if 32 <= len(a) <= 44:
            return "solana"
    return "unknown"


def chains_for(addr: str) -> list[str]:
    t = address_type(addr)
    return {"evm": EVM_ADDRESS_CHAINS, "bitcoin": ["bitcoin"], "solana": ["solana"]}.get(t, [])


# ---------------------------------------------------------------- prices
CG_NATIVE_IDS = ["ethereum", "polygon-ecosystem-token", "hyperliquid", "solana", "bitcoin"]


def _cg_base() -> tuple[str, dict]:
    key = env("COINGECKO_API_KEY")
    plan = (env("COINGECKO_PLAN") or "demo").lower()
    if key and plan == "pro":
        return "https://pro-api.coingecko.com/api/v3", {"x-cg-pro-api-key": key}
    if key:
        return "https://api.coingecko.com/api/v3", {"x-cg-demo-api-key": key}
    return "https://api.coingecko.com/api/v3", {}


async def cg_simple(ids: list[str], refresh=False) -> dict[str, dict[str, float]]:
    ids = sorted(set(i for i in ids if i))
    if not ids:
        return {}

    async def go():
        base, h = _cg_base()
        return await get_json(f"{base}/simple/price", {"ids": ",".join(ids), "vs_currencies": "usd,cad"}, headers=h)

    return await cached("cg:" + ",".join(ids), go, refresh, ttl=max(CACHE_TTL, 120))


# USD/CAD: the Bank of Canada daily rate (the one the dashboard and the CRA use), so wallets and ledger USD balances
# are valued at one rate and CADC is worth exactly C$1. CoinGecko's BTC price in CAD over USD is the fallback, then
# mempool.space. The Bank of Canada publishes once a business day (about 16:30 ET): cached 1 hour; after a failure it
# isn't asked again for 10 minutes, so a slow Valet never holds up balances.
BOC_FX_URL = os.getenv("BOC_VALET_URL", "https://www.bankofcanada.ca/valet/observations").rstrip("/") + "/FXUSDCAD/json"
_boc_fail = 0.0


async def boc_usd_cad() -> tuple[float, str] | None:
    """Latest Bank of Canada USD/CAD rate and its date, or None if it can't be had right now."""
    global _boc_fail
    if time.time() - _boc_fail < 600:
        return None

    async def go():
        start = (datetime.now(timezone.utc) - timedelta(days=14)).strftime("%Y-%m-%d")
        r = await get_json(BOC_FX_URL, {"start_date": start}, retries=1)
        best = None
        for o in r.get("observations") or []:
            try:
                v = float((o.get("FXUSDCAD") or {}).get("v"))
            except (TypeError, ValueError):
                continue
            if o.get("d") and 0.5 < v < 3 and (best is None or o["d"] > best[1]):
                best = (v, o["d"])
        if best is None:
            raise ValueError("no Bank of Canada USD/CAD rate in the last 14 days")
        return best

    try:
        return await cached("boc:usdcad:latest", go, False, ttl=3600)
    except Exception:  # noqa: BLE001
        _boc_fail = time.time()
        return None


async def native_prices(refresh=False) -> dict[str, Any]:
    """USD prices for native coins plus the USD->CAD rate (Bank of Canada first, see above)."""
    out: dict[str, Any] = {"usd": {}, "fx_usd_cad": None, "fx_source": None, "price_source": None, "errors": []}
    boc_task = asyncio.ensure_future(boc_usd_cad())
    cg_fx = mp_fx = None
    try:
        data = await cg_simple(CG_NATIVE_IDS, refresh)
        for k, v in data.items():
            if "usd" in v:
                out["usd"][k] = v["usd"]
        btc = data.get("bitcoin", {})
        if btc.get("usd") and btc.get("cad"):
            cg_fx = btc["cad"] / btc["usd"]
        out["price_source"] = "coingecko"
    except Exception as e:  # noqa: BLE001
        out["errors"].append("CoinGecko: " + short_err(e))
    boc = await boc_task
    if (boc is None and cg_fx is None) or "bitcoin" not in out["usd"]:
        try:
            p = await cached("mempool:prices", lambda: get_json(f"{MEMPOOL}/api/v1/prices"), refresh)
            if p.get("USD"):
                out["usd"].setdefault("bitcoin", p["USD"])
                if p.get("CAD"):
                    mp_fx = p["CAD"] / p["USD"]
        except Exception as e:  # noqa: BLE001
            out["errors"].append("mempool prices: " + short_err(e))
    if boc:
        out["fx_usd_cad"], out["fx_source"] = boc[0], f"Bank of Canada {boc[1]}"
    elif cg_fx:
        out["fx_usd_cad"], out["fx_source"] = cg_fx, "CoinGecko (Bank of Canada unavailable)"
    elif mp_fx:
        out["fx_usd_cad"], out["fx_source"] = mp_fx, "mempool.space (Bank of Canada unavailable)"
    return out


# ---------------------------------------------------------------- EVM via Blockscout
_BS_NEEDS_KEY: set[str] = set()  # chains whose public explorer refused keyless access this run


async def bs_get(chain: str, path: str) -> Any:
    """Blockscout v2 GET: public explorer first (no credits used); on 401/403 retry
    through api.blockscout.com with BLOCKSCOUT_API_KEY and remember that choice."""
    cfg = EVM_CHAINS[chain]
    key = env("BLOCKSCOUT_API_KEY")
    pro = f"https://api.blockscout.com/{cfg['chain_id']}/api/v2{path}"
    if key and chain in _BS_NEEDS_KEY:
        return await get_json(pro, {"apikey": key})
    try:
        return await get_json(f"{cfg['blockscout']}/api/v2{path}")
    except httpx.HTTPStatusError as e:
        if e.response.status_code in (401, 403):
            if not key:
                raise RuntimeError(f"HTTP {e.response.status_code} from {e.request.url.host}: "
                                   "needs BLOCKSCOUT_API_KEY") from e
            _BS_NEEDS_KEY.add(chain)
            return await get_json(pro, {"apikey": key})
        raise


ALCHEMY_DATA = "https://api.g.alchemy.com/data/v1"


def _raw_int(v: Any) -> int:
    if v is None:
        return 0
    s = str(v)
    try:
        return int(s, 16) if s.startswith("0x") else int(Decimal(s))
    except (ValueError, InvalidOperation):
        return 0


ALCHEMY_MAX_PAGES = int(os.getenv("ALCHEMY_MAX_PAGES", "80"))


async def alchemy_tokens(addr: str, refresh=False) -> tuple[list, list, bool]:
    """All Alchemy-covered networks for one address in a single (paged) Portfolio API call.

    Returns (tokens, partial errors, truncated). Pages are read until Alchemy stops sending a pageKey; an old
    wallet with hundreds of airdropped tokens needs many pages, and stopping early silently drops real tokens."""
    key = env("ALCHEMY_API_KEY")
    nets = [c["alchemy"] for c in EVM_CHAINS.values() if c.get("alchemy")]

    async def go():
        tokens, partial, page_key = [], [], None
        for _ in range(ALCHEMY_MAX_PAGES):
            body = {"addresses": [{"address": addr, "networks": nets}], "withMetadata": True, "withPrices": True,
                    "includeNativeTokens": True, "includeErc20Tokens": True}
            if page_key:
                body["pageKey"] = page_key
            r = await post_json(f"{ALCHEMY_DATA}/{key}/assets/tokens/by-address", body)
            data = (r or {}).get("data") or {}
            tokens += data.get("tokens") or []
            partial += ((r or {}).get("error") or {}).get("partialErrors") or []
            page_key = data.get("pageKey")
            if not page_key:
                break
        return tokens, partial, bool(page_key)

    return await cached(f"alchemy:{addr.lower()}", go, refresh)


async def evm_chain_alchemy(chain: str, addr: str, prices: dict, refresh=False) -> dict:
    cfg = EVM_CHAINS[chain]
    net = cfg["alchemy"]
    res = {"chain": chain, "name": cfg["name"], "tokens": [], "error": None}
    try:
        toks, partial, truncated = await alchemy_tokens(addr, refresh)
        errs = [p for p in partial if net in json.dumps(p)]
        if errs:
            res["error"] = ("Alchemy: " + json.dumps(errs[0]))[:200]
        elif truncated:
            res["error"] = f"Alchemy: token list cut off after {ALCHEMY_MAX_PAGES} pages; some tokens may be missing"
        for t in toks:
            if t.get("network") != net:
                continue
            md = t.get("tokenMetadata") or {}
            raw = _raw_int(t.get("tokenBalance"))
            if raw <= 0:
                continue
            native = t.get("tokenAddress") is None
            dec = md.get("decimals")
            if dec is None:
                if not native:
                    continue  # unknown token without decimals: cannot size it
                dec = 18
            px = next((p.get("value") for p in (t.get("tokenPrices") or []) if p.get("currency") == "usd"), None)
            if native:
                px = px or prices["usd"].get(cfg["cg"])
                res["tokens"].append(token(md.get("symbol") or cfg["native"], md.get("name") or cfg["native"], chain,
                                           scale(raw, dec), D(px) if px else None, kind="native"))
            else:
                res["tokens"].append(token(md.get("symbol"), md.get("name"), chain, scale(raw, dec),
                                           D(px) if px else None, contract=t.get("tokenAddress")))
    except Exception as e:  # noqa: BLE001
        res["error"] = short_err(e)
    return res


async def evm_chain(chain: str, addr: str, prices: dict, refresh=False) -> dict:
    cfg = EVM_CHAINS[chain]
    if cfg.get("alchemy") and env("ALCHEMY_API_KEY"):
        return await evm_chain_alchemy(chain, addr, prices, refresh)
    res = {"chain": chain, "name": cfg["name"], "tokens": [], "error": None}

    async def go():
        info, toks = await asyncio.gather(bs_get(chain, f"/addresses/{addr}"),
                                          bs_get(chain, f"/addresses/{addr}/token-balances"),
                                          return_exceptions=True)
        return info, toks

    try:
        info, toks = await cached(f"bs:{chain}:{addr.lower()}", go, refresh)
        # A brand-new address returns 404 on /addresses; treat as empty.
        if isinstance(info, httpx.HTTPStatusError) and info.response.status_code == 404:
            info = {}
        if isinstance(toks, httpx.HTTPStatusError) and toks.response.status_code == 404:
            toks = []
        if isinstance(info, Exception) and isinstance(toks, Exception):
            raise info
        errs = []
        if isinstance(info, Exception):
            errs.append("native: " + short_err(info))
            info = {}
        if isinstance(toks, Exception):
            errs.append("tokens: " + short_err(toks))
            toks = []

        bal = scale(info.get("coin_balance"), 18)
        if bal > 0:
            px = info.get("exchange_rate") or prices["usd"].get(cfg["cg"])
            res["tokens"].append(token(cfg["native"], cfg["native"], chain, bal, D(px) if px else None, kind="native"))

        for t in toks or []:
            meta = t.get("token") or {}
            if meta.get("type") != "ERC-20":
                continue
            if meta.get("reputation") == "scam" or meta.get("is_scam"):
                continue
            amt = scale(t.get("value"), meta.get("decimals"))
            if amt <= 0:
                continue
            px = meta.get("exchange_rate")
            res["tokens"].append(
                token(meta.get("symbol"), meta.get("name"), chain, amt, D(px) if px else None,
                      contract=meta.get("address_hash") or meta.get("address"))
            )
        if errs:
            res["error"] = "; ".join(errs)
    except Exception as e:  # noqa: BLE001
        res["error"] = short_err(e)
    return res


# ---------------------------------------------------------------- DeFi vault positions (hand-kept list)
# app/defi_positions.json lists vault contracts to value. Each entry:
#   {"chain": "base", "type": "erc4626", "address": "0x…", "label": "…", "asset_symbol": "…", "price": "cg:<coingecko id>" | "usd"}
# The wallet's share balance is read on-chain and converted with the vault's own convertToAssets(), so the row shows
# the underlying asset amount at the asset's price. Works for ERC-4626 vaults and ERC-7540 vaults that are
# ERC-4626 compatible (e.g. Superform SuperVaults). Shares waiting in an async redeem request are not counted.
PUBLIC_RPC = {"ethereum": "https://ethereum-rpc.publicnode.com", "base": "https://mainnet.base.org",
              "arbitrum": "https://arb1.arbitrum.io/rpc", "optimism": "https://mainnet.optimism.io",
              "polygon": "https://polygon-rpc.com"}


def defi_position_list() -> list[dict]:
    path = Path(env("DEFI_POSITIONS_FILE") or HERE / "defi_positions.json")
    try:
        return [p for p in json.loads(path.read_text()) if p.get("address") and p.get("chain")]
    except Exception:  # noqa: BLE001
        return []


def chain_rpc(chain: str) -> str | None:
    override = env(f"RPC_URL_{chain.upper()}")
    if override:
        return override
    cfg = EVM_CHAINS.get(chain, {})
    if cfg.get("alchemy") and env("ALCHEMY_API_KEY"):
        return f"https://{cfg['alchemy']}.g.alchemy.com/v2/{env('ALCHEMY_API_KEY')}"
    return PUBLIC_RPC.get(chain)


def _word(x: int | str) -> str:
    if isinstance(x, str):
        return x.lower().removeprefix("0x").rjust(64, "0")
    return format(x, "x").rjust(64, "0")


async def _eth_call(chain: str, to: str, data: str) -> str:
    url, public = chain_rpc(chain), PUBLIC_RPC.get(chain)
    try:
        return await _rpc(url, "eth_call", [{"to": to, "data": data}, "latest"])
    except Exception:  # noqa: BLE001  (e.g. the Alchemy app has this network switched off)
        if not public or public == url:
            raise
        return await _rpc(public, "eth_call", [{"to": to, "data": data}, "latest"])


async def vault_position(chain: str, vault: str, owner: str, refresh=False) -> tuple[int, int, int]:
    """(raw shares, raw underlying assets, asset decimals) for owner in an ERC-4626 vault."""

    async def static():  # asset address and decimals never change
        asset = "0x" + (await _eth_call(chain, vault, "0x38d52e0f"))[-40:]
        return _hex_int(await _eth_call(chain, asset, "0x313ce567"))

    async def live():
        shares = _hex_int(await _eth_call(chain, vault, "0x70a08231" + _word(owner)))
        if not shares:
            return shares, 0
        return shares, _hex_int(await _eth_call(chain, vault, "0x07a2d13a" + _word(shares)))

    dec = await cached(f"vault:dec:{chain}:{vault.lower()}", static, False, ttl=86400)
    shares, assets = await cached(f"vault:{chain}:{vault.lower()}:{owner.lower()}", live, refresh)
    return shares, assets, dec


async def apply_defi_positions(owner: str, results: list[dict], prices: dict, refresh=False) -> None:
    """Replace listed vault share tokens with their underlying value (or add them if the token API missed them)."""
    by_chain = {r["chain"]: r for r in results}
    for p in defi_position_list():
        res = by_chain.get(p["chain"])
        if res is None or p["chain"] not in EVM_CHAINS or not chain_rpc(p["chain"]):
            continue
        vault = p["address"]
        try:
            shares, assets, dec = await vault_position(p["chain"], vault, owner, refresh)
        except Exception as e:  # noqa: BLE001
            msg = f"{p.get('label') or vault[:10]}: " + short_err(e)
            res["error"] = f"{res['error']}; {msg}" if res["error"] else msg
            continue
        res["tokens"] = [t for t in res["tokens"] if (t.get("contract") or "").lower() != vault.lower()]
        if not shares:
            continue
        amount = scale(assets, dec)
        price = await _resolve_price(p.get("price"), {}, prices)
        res["tokens"].append(token(p.get("asset_symbol") or "?", p.get("label") or "Vault position", p["chain"],
                                   amount, price, contract=vault, kind="vault"))


# ---------------------------------------------------------------- core tokens (read on-chain)
# app/core_tokens.json lists the tokens that matter most (stablecoins used for payments). Their balances are read
# straight from the chain with balanceOf and override whatever the token API said, so a token the API leaves out,
# cuts off or reports late (seen Nov 9, 2026: USDC on Base missing from Alchemy's list) still shows correctly.
# Each entry: {"chain": "base", "address": "0x…", "symbol": "USDC", "name": "USD Coin", "decimals": 6, "price": "usd"}
# "price": "usd" values it at 1.00 and "cad" at one Canadian dollar (CADC); leave it out to keep the token API's
# price (or none).
def core_token_list() -> list[dict]:
    path = Path(env("CORE_TOKENS_FILE") or HERE / "core_tokens.json")
    try:
        return [p for p in json.loads(path.read_text()) if p.get("address") and p.get("chain") in EVM_CHAINS]
    except Exception:  # noqa: BLE001
        return []


async def _balances_of(chain: str, owner: str, contracts: list[str]) -> list[int | None]:
    """balanceOf(owner) for each contract in one batched RPC request; None where a call failed."""
    data = "0x70a08231" + _word(owner)
    body = [{"jsonrpc": "2.0", "id": i, "method": "eth_call", "params": [{"to": c, "data": data}, "latest"]}
            for i, c in enumerate(contracts)]

    async def ask(url: str) -> list[int | None]:
        r = await post_json(url, body, retries=1)
        if not isinstance(r, list):
            raise RuntimeError(str(r)[:150])
        got = {x.get("id"): x for x in r if isinstance(x, dict)}
        out = [None if (x := got.get(i)) is None or "error" in x or not str(x.get("result") or "").startswith("0x")
               or x.get("result") == "0x" else _hex_int(x["result"]) for i in range(len(contracts))]
        if all(v is None for v in out):
            raise RuntimeError("no balances returned")
        return out

    url, public = chain_rpc(chain), PUBLIC_RPC.get(chain)
    try:
        return await ask(url)
    except Exception:  # noqa: BLE001
        if not public or public == url:
            raise
        return await ask(public)


async def apply_core_tokens(owner: str, results: list[dict], prices: dict | None = None, refresh=False) -> None:
    """Set the listed tokens to their on-chain balance: add a missing one, correct a wrong amount, drop a spent one."""
    by_chain: dict[str, list[dict]] = {}
    for p in core_token_list():
        by_chain.setdefault(p["chain"], []).append(p)

    async def one(res: dict) -> None:
        want = by_chain.get(res["chain"])
        if not want or not chain_rpc(res["chain"]):
            return
        try:
            raws = await cached(f"core:{res['chain']}:{owner.lower()}",
                                lambda: _balances_of(res["chain"], owner, [p["address"] for p in want]), refresh)
        except Exception:  # noqa: BLE001  (a backstop: if the chain can't be read, keep what the token API said)
            return
        for p, raw in zip(want, raws):
            if raw is None:
                continue
            addr = p["address"].lower()
            old = next((t for t in res["tokens"] if (t.get("contract") or "").lower() == addr), None)
            if old is not None and old.get("kind") != "token":
                continue
            res["tokens"] = [t for t in res["tokens"] if t is not old]
            if raw <= 0:
                continue
            fx = (prices or {}).get("fx_usd_cad")
            if p.get("price") == "usd":
                price = Decimal(1)
            elif p.get("price") == "cad" and fx:
                price = Decimal(1) / D(fx)
            else:
                price = D(old["price_usd"]) if old and old.get("price_usd") else None
            res["tokens"].append(token(p.get("symbol") or (old or {}).get("symbol"), p.get("name") or (old or {}).get("name"),
                                       res["chain"], scale(raw, p.get("decimals", 18)), price, contract=addr))

    await asyncio.gather(*(one(r) for r in results))


# ---------------------------------------------------------------- Hyperliquid (HyperCore)
async def hl_spot_prices(refresh=False) -> dict[str, Decimal]:
    """Token name -> USD price, from Hyperliquid spot markets quoted in USDC."""

    async def go():
        return await post_json(HL_INFO, {"type": "spotMetaAndAssetCtxs"})

    data = await cached("hl:spotmeta", go, refresh)
    meta, ctxs = data[0], data[1]
    names = {t["index"]: t["name"] for t in meta.get("tokens", [])}
    usdc_idx = next((i for i, n in names.items() if n == "USDC"), 0)
    out: dict[str, Decimal] = {"USDC": Decimal(1)}
    for pos, u in enumerate(meta.get("universe", [])):
        toks = u.get("tokens") or []
        if len(toks) != 2 or toks[1] != usdc_idx:
            continue
        idx = u.get("index", pos)
        ctx = ctxs[idx] if idx < len(ctxs) else (ctxs[pos] if pos < len(ctxs) else {})
        px = ctx.get("midPx") or ctx.get("markPx")
        name = names.get(toks[0])
        if name and px and name not in out:
            out[name] = D(px)
    return out


async def hl_perp_dexs(refresh=False) -> list[str]:
    """Perps dexes to check: "" is the main one, then the HIP-3 builder-deployed dexes (e.g. "xyz" = trade.xyz stocks)."""

    async def go():
        return await post_json(HL_INFO, {"type": "perpDexs"})

    data = await cached("hl:perpdexs", go, refresh, ttl=3600)
    return [""] + [d["name"] for d in (data or []) if isinstance(d, dict) and d.get("name")]


def _hl_row(symbol, name, amount: Decimal, price: Decimal | None, value: Decimal, kind: str, contract=None) -> dict:
    """A Hyperliquid row whose value is not simply amount x price (positions are valued at their margin)."""
    t = token(symbol, name, "hyperliquid", amount, price, contract=contract, kind=kind)
    t["value_usd"] = float(value)
    return t


def hl_positions(state: dict, dex: str) -> tuple[list[dict], Decimal]:
    """Rows for one perps dex and its account value.

    One row per open position: amount = size (negative for a short), price = mark (position value / size),
    value = margin used. Isolated margin already includes unrealised PnL; any remainder of the dex's account
    value (cross PnL, unused margin) becomes one USDC row, so the rows always add up to the account value.
    """
    eq = D((state.get("marginSummary") or {}).get("accountValue"))
    rows: list[dict] = []
    used = Decimal(0)
    for ap in state.get("assetPositions") or []:
        p = ap.get("position") or {}
        size = D(p.get("szi"))
        if not size:
            continue
        coin = p.get("coin") or "?"
        sym = coin.split(":", 1)[1] if ":" in coin else coin
        mark = D(p.get("positionValue")) / abs(size)
        margin = D(p.get("marginUsed"))
        lev = (p.get("leverage") or {}).get("value")
        desc = " · ".join(x for x in [f"{dex} perp" if dex else "perp", f"{lev}x" if lev else "",
                                       "long" if size > 0 else "short"] if x)
        rows.append(_hl_row(sym, desc, size, mark, margin, "perp", contract=coin))
        used += margin
    rest = eq - used
    if abs(rest) >= Decimal("0.005"):
        label = f"Perps margin ({dex})" if dex else ("Perps margin" if rows else "Perps account equity")
        rows.append(_hl_row("USDC", label, rest, Decimal(1), rest, "perps"))
    return rows, eq


async def hyperliquid(addr: str, prices: dict, refresh=False) -> dict:
    res = {"chain": "hyperliquid", "name": "Hyperliquid", "tokens": [], "error": None}
    errs = []

    async def q(body):
        key = f"hl:{body['type']}:{body.get('dex', '')}:{addr.lower()}"
        return await cached(key, lambda: post_json(HL_INFO, body), refresh)

    try:
        px = await hl_spot_prices(refresh)
    except Exception as e:  # noqa: BLE001
        px = {"USDC": Decimal(1)}
        errs.append("prices: " + short_err(e))
    hype_px = px.get("HYPE") or (D(prices["usd"]["hyperliquid"]) if prices["usd"].get("hyperliquid") else None)
    try:
        dexs = await hl_perp_dexs(refresh)
    except Exception as e:  # noqa: BLE001
        dexs = [""]
        errs.append("perp dex list: " + short_err(e))

    spot, stake, vaults, mode, *perps = await asyncio.gather(
        q({"type": "spotClearinghouseState", "user": addr}),
        q({"type": "delegatorSummary", "user": addr}),
        q({"type": "userVaultEquities", "user": addr}),
        q({"type": "userAbstraction", "user": addr}),
        *(q({"type": "clearinghouseState", "user": addr, **({"dex": d} if d else {})}) for d in dexs),
        return_exceptions=True,
    )

    # perps on every dex: main plus HIP-3 markets such as trade.xyz tokenized stocks
    perp_rows: list[dict] = []
    perp_eq = Decimal(0)
    for d, st in zip(dexs, perps):
        if isinstance(st, Exception):
            errs.append(f"perps {d or 'main'}: " + short_err(st))
            continue
        rows, eq = hl_positions(st or {}, d)
        perp_rows += rows
        perp_eq += eq

    spot_rows: list[dict] = []
    usdc_hold = Decimal(0)
    if isinstance(spot, Exception):
        errs.append("spot: " + short_err(spot))
    else:
        for b in spot.get("balances", []):
            amt = D(b.get("total"))
            if amt <= 0:
                continue
            coin = b.get("coin")
            if coin == "USDC":
                usdc_hold = D(b.get("hold"))
            p = hype_px if coin == "HYPE" else px.get(coin)
            spot_rows.append(token(coin, coin, "hyperliquid", amt, p, kind="token"))

    # Unified / portfolio-margin accounts keep perps collateral inside the spot USDC balance (as "hold"),
    # so the perps account value is already in spot USDC: show only the free part as USDC.
    if isinstance(mode, Exception):
        unified = perp_eq > 0 and usdc_hold >= perp_eq - Decimal("0.01")
    else:
        unified = any(k in str(mode).lower() for k in ("unified", "portfolio"))
    if unified and perp_eq:
        usdc = next((t for t in spot_rows if t["symbol"] == "USDC"), None)
        if usdc:
            free = D(usdc["amount"]) - perp_eq
            spot_rows.remove(usdc)
            if abs(free) >= Decimal("0.005"):
                spot_rows.append(_hl_row("USDC", "USDC (free)", free, Decimal(1), free, "token"))
        else:
            errs.append("unified account without spot USDC; perps value may be counted twice")
    res["tokens"] += spot_rows + perp_rows

    if isinstance(stake, Exception):
        errs.append("staking: " + short_err(stake))
    elif isinstance(stake, dict):
        st = D(stake.get("delegated")) + D(stake.get("undelegated")) + D(stake.get("totalPendingWithdrawal"))
        if st > 0:
            res["tokens"].append(token("HYPE", "Staked HYPE", "hyperliquid", st, hype_px, kind="staked"))
    if isinstance(vaults, Exception):
        errs.append("vaults: " + short_err(vaults))
    elif isinstance(vaults, list):
        for v in vaults:
            eq = D(v.get("equity"))
            if eq > 0:
                va = v.get("vaultAddress", "")
                res["tokens"].append(token("USDC", f"Vault {va[:6]}…{va[-4:]}", "hyperliquid", eq, Decimal(1),
                                           contract=va, kind="vault"))
    if errs:
        res["error"] = "; ".join(errs)
    return res


# ---------------------------------------------------------------- HyperEVM
def hyperevm_token_list() -> list[dict]:
    path = Path(env("HYPEREVM_TOKENS_FILE") or HERE / "hyperevm_tokens.json")
    try:
        return json.loads(path.read_text())
    except Exception:  # noqa: BLE001
        return []


async def _rpc(url: str, method: str, params: list) -> Any:
    r = await post_json(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    if "error" in r:
        raise RuntimeError(str(r["error"])[:150])
    return r.get("result")


def _hex_int(h: Any) -> int:
    if not h or h == "0x":
        return 0
    try:
        return int(h, 16)
    except (TypeError, ValueError):
        return 0


async def _resolve_price(spec: str | None, hl_px: dict, prices: dict) -> Decimal | None:
    if not spec:
        return None
    if spec == "usd":
        return Decimal(1)
    if spec.startswith("hl:"):
        return hl_px.get(spec[3:])
    if spec.startswith("cg:"):
        cid = spec[3:]
        if cid in prices["usd"]:
            return D(prices["usd"][cid])
        try:
            d = await cg_simple([cid])
            return D(d[cid]["usd"]) if cid in d else None
        except Exception:  # noqa: BLE001
            return None
    return None


async def hyperevm(addr: str, prices: dict, refresh=False) -> dict:
    res = {"chain": "hyperevm", "name": "HyperEVM", "tokens": [], "error": None}
    errs = []
    try:
        hl_px = await hl_spot_prices(refresh)
    except Exception:  # noqa: BLE001
        hl_px = {}
    hype_px = hl_px.get("HYPE") or (D(prices["usd"]["hyperliquid"]) if prices["usd"].get("hyperliquid") else None)
    tokens = hyperevm_token_list()
    data = "0x70a08231" + addr.lower().replace("0x", "").rjust(64, "0")
    reqs = [("eth_getBalance", [addr, "latest"])]
    reqs += [("eth_call", [{"to": t["address"], "data": data}, "latest"]) for t in tokens]

    async def go():
        # One batched request for the wallet (was one request per token). Each answer is a hex string, or the
        # exception for that call. If the RPC doesn't answer a batch as a list, ask one call at a time as before.
        try:
            r = await post_json(HYPEREVM_RPC, [{"jsonrpc": "2.0", "id": i, "method": m, "params": p}
                                               for i, (m, p) in enumerate(reqs)], retries=1)
            if not isinstance(r, list):
                raise RuntimeError("no batch answer")
            got = {x.get("id"): x for x in r if isinstance(x, dict)}
            if not any(i in got for i in range(len(reqs))):
                raise RuntimeError("no batch answer")
            return [RuntimeError("no answer") if (x := got.get(i)) is None
                    else RuntimeError(str(x["error"])[:150]) if "error" in x else x.get("result")
                    for i in range(len(reqs))]
        except Exception:  # noqa: BLE001
            return await asyncio.gather(*(_rpc(HYPEREVM_RPC, m, p) for m, p in reqs), return_exceptions=True)

    try:
        results = await cached(f"hyperevm:{addr.lower()}", go, refresh)
        native = results[0]
        if isinstance(native, Exception):
            errs.append("native: " + short_err(native))
        else:
            bal = scale(_hex_int(native), 18)
            if bal > 0:
                res["tokens"].append(token("HYPE", "HYPE", "hyperevm", bal, hype_px, kind="native"))
        failed = []
        for t, r in zip(tokens, results[1:]):
            if isinstance(r, Exception):
                # A contract that reverts is a bad entry in the list: skip it quietly. Anything else (rate limit,
                # timeout, no answer) means the balance is unknown, so say so instead of showing nothing.
                if not (isinstance(r, RuntimeError) and "revert" in str(r).lower()):
                    failed.append(str(t.get("symbol") or t["address"][:10]))
                continue
            raw = _hex_int(r)
            if raw <= 0:
                continue
            dec = t.get("decimals")
            try:  # trust the contract over the list
                dec = await cached(f"hyperevm:dec:{t['address'].lower()}", lambda t=t: _token_decimals(t["address"]),
                                   False, ttl=86400) or dec
            except Exception:  # noqa: BLE001
                pass
            amt = scale(raw, dec if dec is not None else 18)
            p = await _resolve_price(t.get("price"), hl_px, prices)
            res["tokens"].append(token(t.get("symbol"), t.get("name"), "hyperevm", amt, p, contract=t["address"]))
        if failed:
            errs.append("couldn't read " + ", ".join(failed[:6]) + (" and more" if len(failed) > 6 else ""))
    except Exception as e:  # noqa: BLE001
        errs.append(short_err(e))
    if errs:
        res["error"] = "; ".join(errs)
    return res


async def _token_decimals(address: str) -> int:
    return _hex_int(await _rpc(HYPEREVM_RPC, "eth_call", [{"to": address, "data": "0x313ce567"}, "latest"]))


# ---------------------------------------------------------------- Solana
async def solana(addr: str, prices: dict, refresh=False) -> dict:
    res = {"chain": "solana", "name": "Solana", "tokens": [], "error": None}
    key = env("HELIUS_API_KEY")
    sol_px = prices["usd"].get("solana")
    try:
        if key:
            url = f"https://mainnet.helius-rpc.com/?api-key={key}"

            async def go():
                items, native, page = [], None, 1
                while page <= 5:
                    body = {"jsonrpc": "2.0", "id": "da", "method": "getAssetsByOwner",
                            "params": {"ownerAddress": addr, "page": page, "limit": 1000,
                                       "displayOptions": {"showFungible": True, "showNativeBalance": True}}}
                    r = await post_json(url, body)
                    if "error" in r:
                        raise RuntimeError(str(r["error"])[:150])
                    result = r.get("result") or {}
                    native = native or result.get("nativeBalance")
                    batch = result.get("items") or []
                    items += batch
                    if len(batch) < 1000:
                        break
                    page += 1
                return items, native, page > 5

            items, native, cut = await cached(f"sol:{addr}", go, refresh)
            if cut:
                res["error"] = "Helius: asset list cut off after 5,000; some tokens may be missing"
            if native and native.get("lamports"):
                amt = scale(native["lamports"], 9)
                p = native.get("price_per_sol") or sol_px
                res["tokens"].append(token("SOL", "Solana", "solana", amt, D(p) if p else None, kind="native"))
            for it in items:
                ti = it.get("token_info") or {}
                if it.get("interface") not in ("FungibleToken", "FungibleAsset") or not ti:
                    continue
                amt = scale(ti.get("balance"), ti.get("decimals"))
                if amt <= 0:
                    continue
                md = (it.get("content") or {}).get("metadata") or {}
                pi = ti.get("price_info") or {}
                p = pi.get("price_per_token")
                res["tokens"].append(token(ti.get("symbol") or md.get("symbol"), md.get("name"), "solana", amt,
                                           D(p) if p else None, contract=it.get("id")))
        else:
            r = await cached(f"sol-public:{addr}", lambda: post_json(
                SOLANA_PUBLIC_RPC, {"jsonrpc": "2.0", "id": 1, "method": "getBalance", "params": [addr]}), refresh)
            lam = ((r or {}).get("result") or {}).get("value", 0)
            if lam:
                res["tokens"].append(token("SOL", "Solana", "solana", scale(lam, 9), D(sol_px) if sol_px else None,
                                           kind="native"))
            res["error"] = "No HELIUS_API_KEY set: showing SOL only, no SPL tokens"
    except Exception as e:  # noqa: BLE001
        res["error"] = short_err(e)
    return res


# ---------------------------------------------------------------- Bitcoin
async def bitcoin(addr: str, prices: dict, refresh=False) -> dict:
    res = {"chain": "bitcoin", "name": "Bitcoin", "tokens": [], "error": None}
    try:
        d = await cached(f"btc:{addr}", lambda: get_json(f"{MEMPOOL}/api/address/{addr}"), refresh)
        cs, ms = d.get("chain_stats", {}), d.get("mempool_stats", {})
        sats = (cs.get("funded_txo_sum", 0) - cs.get("spent_txo_sum", 0)
                + ms.get("funded_txo_sum", 0) - ms.get("spent_txo_sum", 0))
        if sats > 0:
            p = prices["usd"].get("bitcoin")
            res["tokens"].append(token("BTC", "Bitcoin", "bitcoin", scale(sats, 8), D(p) if p else None, kind="native"))
    except Exception as e:  # noqa: BLE001
        res["error"] = short_err(e)
    return res


# ---------------------------------------------------------------- aggregation
async def fetch_chain(chain: str, addr: str, prices: dict, refresh: bool) -> dict:
    if chain in EVM_CHAINS:
        return await evm_chain(chain, addr, prices, refresh)
    return await {"hyperliquid": hyperliquid, "hyperevm": hyperevm, "solana": solana, "bitcoin": bitcoin}[chain](
        addr, prices, refresh)


SPAM_RE = re.compile(r"(https?://|www\.|\.(com|io|org|net|xyz|app|site|top|gift|pro|vip)\b|t\.me|visit|claim|reward|airdrop|voucher)", re.I)


def _is_spam(t: dict) -> bool:
    """Unpriced tokens whose name or symbol advertises a site or a 'claim' are phishing airdrops."""
    return t["value_usd"] is None and bool(SPAM_RE.search(f"{t['name']} {t['symbol']}"))


# ---------------------------------------------------------------- Aave positions
_AAVE_CHAIN = r"(EthLido|EthEtherFi|Eth|Arb|Opt|Pol|Base|Bas|Ava|Gno|Scr|Lin|Zks|Son|Cel|Mnt|Sonic)?"
_AAVE_DEBT_RE = re.compile(rf"^(variableDebt|stableDebt){_AAVE_CHAIN}([A-Za-z0-9.]+)$")
_AAVE_SUPPLY_RE = re.compile(rf"^a{_AAVE_CHAIN}([A-Za-z0-9][A-Za-z0-9.]*)$")
_STABLES = {"USDC", "USDT", "DAI", "USDS", "GHO", "LUSD", "USDE", "PYUSD", "FRAX", "USDC.E", "USDBC", "USDT0", "RLUSD",
            "SUSDE", "CRVUSD"}
_UNDERLYING_CG = {"WETH": "ethereum", "ETH": "ethereum", "WSTETH": "wrapped-steth", "WEETH": "wrapped-eeth",
                  "RETH": "rocket-pool-eth", "CBETH": "coinbase-wrapped-staked-eth", "WBTC": "wrapped-bitcoin",
                  "CBBTC": "coinbase-wrapped-btc", "TBTC": "tbtc", "LINK": "chainlink", "AAVE": "aave",
                  "ARB": "arbitrum", "OP": "optimism", "POL": "polygon-ecosystem-token", "WPOL": "polygon-ecosystem-token",
                  "MATIC": "polygon-ecosystem-token", "WMATIC": "polygon-ecosystem-token", "UNI": "uniswap",
                  "SUSDE": "ethena-staked-usde"}


def _aave_parse(t: dict) -> tuple[str, str] | None:
    """('debt'|'supply', underlying symbol) for Aave aTokens and debt tokens, else None."""
    sym, name = t["symbol"] or "", (t["name"] or "").lower()
    if "aave" not in name:  # forks such as Sturdy reuse Aave's symbols; only genuine Aave is valued
        return None
    m = _AAVE_DEBT_RE.match(sym)
    if m:
        return "debt", m.group(3)
    m = _AAVE_SUPPLY_RE.match(sym)
    if m:
        return "supply", m.group(2)
    return None


async def apply_aave(tokens: list[dict], prices: dict) -> None:
    """Price Aave deposits at their underlying token and turn debts into negative values (in place)."""
    found = [(t, p) for t in tokens if t["value_usd"] is None and (p := _aave_parse(t))]
    if not found:
        return
    known = {t["symbol"].upper(): t["price_usd"] for t in tokens if t["price_usd"]}
    want = {_UNDERLYING_CG[u.upper()] for _, (_, u) in found if u.upper() in _UNDERLYING_CG}
    cg = {}
    if want:
        try:
            cg = await cg_simple(sorted(want))
        except Exception:  # noqa: BLE001
            cg = {}
    for t, (side, under) in found:
        u = under.upper()
        if u in _STABLES and u != "SUSDE":
            px = 1.0
        elif u in _UNDERLYING_CG and _UNDERLYING_CG[u] in cg:
            px = cg[_UNDERLYING_CG[u]].get("usd")
        elif u in _UNDERLYING_CG and _UNDERLYING_CG[u] in prices["usd"]:
            px = prices["usd"][_UNDERLYING_CG[u]]
        else:
            px = known.get(u)
        if side == "debt":
            t["amount"] = -abs(t["amount"])
            t["kind"] = "aave-debt"
            t["name"] = f"Aave debt: {under}"
        else:
            t["kind"] = "aave-supply"
            t["name"] = f"Aave deposit: {under}"
        if px:
            t["price_usd"] = float(px)
            t["value_usd"] = t["amount"] * float(px)


def _finish_tokens(tokens: list[dict], min_usd: float, include_unpriced: bool) -> tuple[list[dict], dict]:
    tokens = [t for t in tokens if not _is_spam(t)]
    priced = [t for t in tokens if t["value_usd"] is not None]
    unpriced = [t for t in tokens if t["value_usd"] is None]
    shown = [t for t in priced if abs(t["value_usd"]) >= min_usd]
    small = [t for t in priced if abs(t["value_usd"]) < min_usd]
    shown.sort(key=lambda t: t["value_usd"], reverse=True)
    if include_unpriced:
        shown += sorted(unpriced, key=lambda t: t["symbol"].upper())
    hidden = {
        "small_count": len(small),
        "small_value_usd": round(sum(t["value_usd"] for t in small), 2),
        "unpriced_count": 0 if include_unpriced else len(unpriced),
    }
    return shown, hidden


async def wallet(address: str, nickname: str | None = None, chains: list[str] | None = None,
                 min_usd: float = 0.0, include_unpriced: bool = True, refresh: bool = False,
                 prices: dict | None = None) -> dict:
    address = address.strip()
    atype = address_type(address)
    supported = chains_for(address)
    out: dict[str, Any] = {"nickname": nickname or "", "address": address, "address_type": atype}
    if not supported:
        out.update(error=("Extended public keys (xpub/zpub) are not supported yet; add individual addresses."
                          if atype == "bitcoin-xpub" else "Unrecognised address format"),
                   total_usd=0, total_cad=0, chains=[], tokens=[])
        return out
    want = [c for c in (chains or supported) if c in supported]
    prices = prices or await native_prices(refresh)
    results = await asyncio.gather(*(fetch_chain(c, address, prices, refresh) for c in want))
    fx = prices.get("fx_usd_cad")
    all_tokens: list[dict] = []
    chain_rows = []
    await apply_core_tokens(address, results, prices, refresh)
    await apply_defi_positions(address, results, prices, refresh)
    await apply_aave([t for r in results for t in r["tokens"]], prices)
    for r in results:
        sub = sum(t["value_usd"] or 0 for t in r["tokens"])
        chain_rows.append({"chain": r["chain"], "name": r["name"], "total_usd": round(sub, 2),
                           "token_count": len(r["tokens"]), "error": r["error"]})
        all_tokens += r["tokens"]
    total = sum(t["value_usd"] or 0 for t in all_tokens)
    shown, hidden = _finish_tokens(all_tokens, min_usd, include_unpriced)
    chain_rows.sort(key=lambda c: c["total_usd"], reverse=True)
    out.update(total_usd=round(total, 2), total_cad=round(total * fx, 2) if fx else None,
               chains=chain_rows, tokens=shown, hidden=hidden)
    return out


async def portfolio(wallets: list[dict], min_usd: float = 0.0, include_unpriced: bool = True,
                    refresh: bool = False) -> dict:
    prices = await native_prices(refresh)
    fx = prices.get("fx_usd_cad")
    ws = await asyncio.gather(*(
        wallet(w["address"], w.get("nickname"), w.get("chains"), min_usd, include_unpriced, refresh, prices)
        for w in wallets))
    by_chain: dict[str, float] = {}
    by_token: dict[str, dict] = {}
    for w in ws:
        for c in w.get("chains", []):
            by_chain[c["chain"]] = by_chain.get(c["chain"], 0) + c["total_usd"]
        for t in w.get("tokens", []):
            perp = t.get("kind") == "perp"
            k = t["symbol"].upper() + (" perp" if perp else "")
            agg = by_token.setdefault(k, {"symbol": t["symbol"], "amount": 0.0, "value_usd": 0.0,
                                          "chains": set(), "wallets": set(), "priced": True,
                                          **({"kind": "perp", "_ntl": 0.0} if perp else {})})
            if perp and t["price_usd"] is not None:
                agg["_ntl"] += t["amount"] * t["price_usd"]
            agg["amount"] += t["amount"]
            agg["value_usd"] += t["value_usd"] or 0
            agg["priced"] = agg["priced"] and t["value_usd"] is not None
            agg["chains"].add(t["chain"])
            agg["wallets"].add(w["nickname"] or w["address"][:8])
    total = sum(w.get("total_usd") or 0 for w in ws)
    tokens = sorted(by_token.values(), key=lambda a: a["value_usd"], reverse=True)
    for a in tokens:
        a["value_usd"] = round(a["value_usd"], 2)
        ntl = a.pop("_ntl", None)  # positions: mark price, not value / size
        a["price_usd"] = ((ntl if ntl is not None else a["value_usd"]) / a["amount"]) if a["amount"] and a["priced"] else None
        a["chains"] = sorted(a["chains"])
        a["wallets"] = sorted(a["wallets"])
    return {
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_usd": round(total, 2),
        "total_cad": round(total * fx, 2) if fx else None,
        "fx_usd_cad": round(fx, 6) if fx else None,
        "fx_source": prices.get("fx_source"),
        "by_chain": sorted(({"chain": k, "name": ALL_CHAINS[k]["name"], "total_usd": round(v, 2)}
                            for k, v in by_chain.items()), key=lambda r: r["total_usd"], reverse=True),
        "by_token": tokens,
        "wallets": ws,
        "price_errors": prices.get("errors", []),
    }


def default_wallets() -> list[dict]:
    """WALLETS env: 'Nickname:address,Other:address'."""
    out = []
    for part in env("WALLETS").split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            nick, addr = part.rsplit(":", 1)
        else:
            nick, addr = "", part
        out.append({"nickname": nick.strip(), "address": addr.strip()})
    return out

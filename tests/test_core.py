"""Offline test: core tokens read on-chain, and Alchemy paging (python tests/test_core.py)."""
import asyncio, os, sys, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
os.environ["DATA_DIR"] = tempfile.mkdtemp(); os.environ["MCP_PATH_TOKEN"] = "x" * 30
os.environ["ALCHEMY_API_KEY"] = "k"
import sources as S

PRIMARY = "0x5f62ecE59FbBA2Bae1654e9b69bbfA4e4d5b329b"
USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
USDC_ARB = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
state = {"pages": 14, "seen": 0, "rpc_fail": False, "base_usdc": 54881007, "list_usdc": False}


def tok(net, addr, sym, raw, dec=18, px=None):
    return {"network": net, "tokenAddress": addr, "tokenBalance": hex(raw),
            "tokenMetadata": {"symbol": sym, "name": sym, "decimals": dec},
            "tokenPrices": [{"currency": "usd", "value": str(px)}] if px else []}


async def fake_post(url, body, headers=None, retries=4):
    if "assets/tokens/by-address" in url:
        n = int(body.get("pageKey") or 0); state["seen"] += 1
        toks = [tok("base-mainnet", f"0xspam{n}", f"SPAM{n}", 10**18)]
        if n == 0:
            toks.append(tok("arb-mainnet", USDC_ARB, "USDC", 7_000000, 6, 0.9998))  # stale: really spent
            if state["list_usdc"]:
                toks.append(tok("base-mainnet", USDC_BASE, "USDC", 140293, 6, 0.9998))  # stale: before the deposit
        return {"data": {"tokens": toks, "pageKey": str(n + 1) if n + 1 < state["pages"] else None}}
    if isinstance(body, list):  # batched eth_call
        if state["rpc_fail"]:
            raise RuntimeError("rpc down")
        out = []
        for c in body:
            to = c["params"][0]["to"].lower()
            assert c["params"][0]["data"] == "0x70a08231" + PRIMARY.lower()[2:].rjust(64, "0")
            raw = state["base_usdc"] if (to == USDC_BASE and "base" in url) else 0
            if to == "0x5e63e9c06c3f2c779ef29a8071f94f5a724eb5e4" and "base" in url:
                raw = state.get("cadc", 0)
            out.append({"jsonrpc": "2.0", "id": c["id"], "result": "0x" + format(raw, "x").rjust(64, "0")})
        return list(reversed(out))  # order must not matter
    if isinstance(body, dict) and body.get("method") == "eth_call":
        return {"result": "0x" + "0" * 64}
    raise AssertionError(url)


async def fake_prices(refresh=False):
    return {"usd": {"ethereum": 2100.0, "polygon-ecosystem-token": 0.2}, "fx_usd_cad": 1.41}

S.post_json = fake_post; S.native_prices = fake_prices
CH = ["base", "arbitrum"]


async def main():
    # 1. Alchemy leaves USDC on Base out: the on-chain balance is added; all 14 pages are read
    w = await S.wallet(PRIMARY, "Primary", CH, refresh=True)
    usdc = [t for t in w["tokens"] if t["symbol"] == "USDC"]
    print("1:", [(t["chain"], t["amount"], t["value_usd"]) for t in usdc], "pages", state["seen"])
    assert state["seen"] == 14
    assert len(usdc) == 1 and usdc[0]["chain"] == "base" and usdc[0]["amount"] == 54.881007 and usdc[0]["value_usd"] == 54.881007
    assert not any(c["error"] for c in w["chains"]), w["chains"]
    assert sum(1 for t in w["tokens"] if t["symbol"].startswith("SPAM")) == 14
    # 2. Alchemy lists a stale amount: corrected, not doubled
    state["list_usdc"] = True
    w = await S.wallet(PRIMARY, "Primary", CH, refresh=True)
    usdc = [t for t in w["tokens"] if t["symbol"] == "USDC"]
    assert len(usdc) == 1 and usdc[0]["amount"] == 54.881007, usdc
    # 3. RPC down: keep what Alchemy said, no crash
    state["rpc_fail"] = True
    w = await S.wallet(PRIMARY, "Primary", CH, refresh=True)
    usdc = sorted((t["chain"], t["amount"]) for t in w["tokens"] if t["symbol"] == "USDC")
    print("3:", usdc)
    assert usdc == [("arbitrum", 7.0), ("base", 0.140293)]
    state["rpc_fail"] = False
    # 4. more pages than the cap: flagged on the chain row, core token still right
    state["pages"] = S.ALCHEMY_MAX_PAGES + 5
    w = await S.wallet(PRIMARY, "Primary", CH, refresh=True)
    errs = [c["error"] for c in w["chains"]]
    print("4:", errs)
    assert all(e and "cut off" in e for e in errs)
    assert [t["amount"] for t in w["tokens"] if t["symbol"] == "USDC"] == [54.881007]
    # 5. CADC on Base (not in Alchemy's list here): added, valued at one Canadian dollar
    state["pages"] = 2; state["cadc"] = 1500_000000
    w = await S.wallet(PRIMARY, "Primary", CH, refresh=True)
    cadc = [t for t in w["tokens"] if t["symbol"] == "CADC"]
    print("5:", cadc)
    assert len(cadc) == 1 and cadc[0]["amount"] == 1500.0 and abs(cadc[0]["value_usd"] - 1500 / 1.41) < 0.01
    # 6. the list file is valid: five USDC entries and CADC, 40-hex addresses, 6 decimals
    L = S.core_token_list()
    assert len(L) == 6 and all(len(p["address"]) == 42 and p["decimals"] == 6 and p["price"] in ("usd", "cad") for p in L)
    print("CORE OK")

asyncio.run(main())

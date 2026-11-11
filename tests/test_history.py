"""Offline test of history.py with mocked API responses (no network)."""
import asyncio
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
os.environ["DATA_DIR"] = tempfile.mkdtemp()
os.environ["ZERION_API_KEY"] = "zk_test"
os.environ["MCP_PATH_TOKEN"] = "x" * 30

import sources as S  # noqa: E402
import history as H  # noqa: E402

PRIMARY = "0x5f62ecE59FbBA2Bae1654e9b69bbfA4e4d5b329b"
TRIA = "0xf015E19287DADC2E88e62a95Fd2cCC9155f59b6b"
BANKR = "0xf93f154841256e0d006ef2b7b99dccbf8cac52f7"
BTC = "3H4DTz3tKF6zqAwmgLHbEmwVdUWrK1ec5X"
PHANTOM = "G8i4SziqvFFfzvDUcpkkPn32XH9a6HLbbsUCDkwpDPkq"
ROUTER = "0x1111111254eeb25477b68fb85ed929f73a960582"
TRIA_DEPOSIT = "0x9999999999999999999999999999999999999999"
VAULT = "0x02b12a394f0a98b80e53d85f6831041932f1b621"
CALLS = []


def fi(sym, fid=None, chain="base", addr=None):
    return {"symbol": sym, "name": sym, "id": fid or sym.lower(),
            "implementations": [{"chain_id": chain, "address": addr, "decimals": 18}]}


def tr(d, sym, amt, usd, sender, recipient, chain="base", fid=None, addr=None, price=None, act=None):
    return {"fungible_info": fi(sym, fid, chain, addr), "direction": d, "quantity": {"numeric": str(amt)},
            "value": usd, "price": price if price is not None else (usd / amt if usd else None),
            "sender": sender, "recipient": recipient, **({"act_id": act} if act else {})}


def ztx(h, when, op, chain, transfers, sent_from, app=None, fee=0.00001, acts=None, status="confirmed"):
    return {"type": "transactions", "id": h,
            "attributes": {"operation_type": op, "hash": h, "mined_at": when, "mined_at_block": 1,
                           "sent_from": sent_from, "sent_to": ROUTER, "status": status, "nonce": 1,
                           "fee": {"fungible_info": fi("ETH", "eth", chain), "quantity": {"numeric": str(fee)},
                                   "price": 2600, "value": fee * 2600},
                           "transfers": transfers, "approvals": [], "acts": acts or [],
                           "application_metadata": {"name": app} if app else None},
            "relationships": {"chain": {"data": {"type": "chains", "id": chain}}}}


ZERION = {
    BANKR.lower(): [
        # Sep 26: USDC -> BNKR on Base via Bankr (one trade)
        ztx("0xea9884", "2026-09-26T14:00:00Z", "trade", "base",
            [tr("out", "USDC", 584.99, 584.99, BANKR, ROUTER, fid="usdc"),
             tr("in", "BNKR", 1265621.964678, 585.10, ROUTER, BANKR, addr="0xbnkr")], BANKR, app="Bankr"),
        # Sep 27: 0.012 ETH out on Robinhood Chain via Relay ...
        ztx("0xe6724e", "2026-09-27T10:00:00Z", "send", "robinhood",
            [tr("out", "ETH", 0.012, 32.35, BANKR, "0xrelaysolver", chain="robinhood", fid="eth")], BANKR, app="Relay"),
        # ... arrives as BNKR on Base a minute later
        ztx("0x29cb66", "2026-09-27T10:01:10Z", "receive", "base",
            [tr("in", "BNKR", 69456.074688, 31.90, "0xrelaysolver2", BANKR, addr="0xbnkr")], "0xrelaysolver2", app="Relay"),
        # Oct 7: staking reward claim
        ztx("0xstake1", "2026-10-07T12:00:00Z", "claim", "base",
            [tr("in", "BNKR", 13243.654889, 5.15, "0xstaking", BANKR, addr="0xbnkr")], BANKR, app="Bankr Staking"),
        # Oct 9: Bankr sends BNKR to Primary
        ztx("0xmove1", "2026-10-09T09:00:00Z", "send", "base",
            [tr("out", "BNKR", 1348321.694255, 430.0, BANKR, PRIMARY, addr="0xbnkr")], BANKR),
        # spam: unpriced airdrop
        ztx("0xspam1", "2026-10-02T00:00:00Z", "receive", "base",
            [tr("in", "FREEMONEY", 1000, None, "0xspammer", BANKR, addr="0xspamtoken")], "0xspammer"),
    ],
    PRIMARY.lower(): [
        ztx("0xmove1", "2026-10-09T09:00:00Z", "receive", "base",
            [tr("in", "BNKR", 1348321.694255, 430.0, BANKR, PRIMARY, addr="0xbnkr")], BANKR),
        ztx("0x28ded0", "2026-10-09T09:10:00Z", "trade", "base",
            [tr("out", "BNKR", 1348321.694255, 431.0, PRIMARY, ROUTER, addr="0xbnkr"),
             tr("in", "USDC", 429.740905, 429.74, ROUTER, PRIMARY, fid="usdc")], PRIMARY, app="KyberSwap"),
        ztx("0x9473f8", "2026-10-09T09:20:00Z", "deposit", "base",
            [tr("out", "USDC", 430.401616, 430.40, PRIMARY, VAULT, fid="usdc"),
             tr("in", "superSPCX", 2.63828525, None, VAULT, PRIMARY, addr=VAULT)], PRIMARY, app="Superform"),
        # top-up: USDC sent to Tria's deposit address, arrives on Hyperliquid
        ztx("0xtopup1", "2026-10-20T15:00:00Z", "send", "base",
            [tr("out", "USDC", 1279.27, 1279.27, PRIMARY, TRIA_DEPOSIT, fid="usdc")], PRIMARY),
        # Ambire-style fee paid in USDC inside a send
        ztx("0xpay1", "2026-10-21T15:00:00Z", "send", "base",
            [tr("out", "USDC", 50, 50, PRIMARY, "0xmerchant", fid="usdc"),
             tr("out", "USDC", 0.05, 0.05, PRIMARY, "0xambirefee", fid="usdc", act="a2")], PRIMARY,
            acts=[{"id": "a1", "type": "send"}, {"id": "a2", "type": "fee"}]),
        # wrap ETH -> WETH
        ztx("0xwrap1", "2026-10-22T15:00:00Z", "trade", "base",
            [tr("out", "ETH", 0.01, 26, PRIMARY, "0x4200000000000000000000000000000000000006", fid="eth"),
             tr("in", "WETH", 0.01, 26, "0x4200000000000000000000000000000000000006", PRIMARY, fid="weth")], PRIMARY),
        ztx("0xappr", "2026-10-23T15:00:00Z", "approve", "base", [], PRIMARY),
        ztx("0xfail", "2026-10-24T15:00:00Z", "trade", "base", [], PRIMARY, status="failed"),
        ztx("0xdust", "2026-10-25T15:00:00Z", "receive", "arbitrum",
            [tr("in", "ARB", 0.5, 0.20, "0xsomeone", PRIMARY, chain="arbitrum", fid="arb")], "0xsomeone"),
        # Arbitrum USDC to the Hyperliquid bridge
        # split swap with one contract (pays out in a second tx) and a lending loop with two contracts
        ztx("0xsplitA", "2026-10-27T15:00:00Z", "send", "hyperevm",
            [tr("out", "WHYPE", 1.0, 40.0, PRIMARY, "0xstaker", chain="hyperevm", fid="whype")], PRIMARY),
        ztx("0xsplitB", "2026-10-27T15:01:30Z", "receive", "hyperevm",
            [tr("in", "kHYPE", 0.98, 39.5, "0xstaker", PRIMARY, chain="hyperevm", fid="khype")], "0xstaker"),
        ztx("0xloopA", "2026-10-28T15:00:00Z", "receive", "hyperevm",
            [tr("in", "kHYPE", 0.5, 20.0, "0xborrowmkt", PRIMARY, chain="hyperevm", fid="khype")], "0xborrowmkt"),
        ztx("0xloopB", "2026-10-28T15:02:00Z", "send", "hyperevm",
            [tr("out", "WHYPE", 0.5, 20.1, PRIMARY, "0xlendmkt", chain="hyperevm", fid="whype")], PRIMARY),
        ztx("0xmint0", "2026-10-28T16:00:00Z", "mint", "base",
            [tr("in", "SONG", 3, 0.0, "0x0000000000000000000000000000000000000000", PRIMARY, addr="0xsong")], PRIMARY),
        ztx("0xhlbr", "2026-10-26T15:00:00Z", "send", "arbitrum",
            [tr("out", "USDC", 100, 100, PRIMARY, H.HL_BRIDGE, chain="arbitrum", fid="usdc")], PRIMARY),
    ],
    TRIA.lower(): [],
    PHANTOM.lower(): [
        ztx("5solswap", "2026-08-01T15:00:00Z", "trade", "solana",
            [tr("out", "SOL", 0.1, 15.0, PHANTOM, "jup", chain="solana", fid="sol"),
             tr("in", "PUMP", 965.3, 14.9, "jup", PHANTOM, chain="solana", fid="pump")], PHANTOM, app="Jupiter"),
    ],
}

HL = {
    "spotMeta": {"tokens": [{"name": "USDC", "index": 0}, {"name": "HYPE", "index": 150}],
                 "universe": [{"name": "@107", "tokens": [150, 0], "index": 107}]},
    ("userFillsByTime", TRIA.lower()): [
        {"coin": "xyz:BE", "px": "150.0", "sz": "2.21", "side": "B", "time": 1790000000000, "startPosition": "0",
         "dir": "Open Long", "closedPnl": "0.0", "hash": "0xf1", "oid": 1, "crossed": True, "fee": "0.15", "tid": 11,
         "feeToken": "USDC"},
        {"coin": "xyz:TSLA", "px": "400.0", "sz": "1.0", "side": "A", "time": 1791000000000, "startPosition": "1.0",
         "dir": "Close Long", "closedPnl": "-12.5", "hash": "0xf2", "oid": 2, "crossed": True, "fee": "0.2", "tid": 12,
         "feeToken": "USDC"},
        {"coin": "@107", "px": "40.0", "sz": "0.5", "side": "B", "time": 1791500000000, "startPosition": "0",
         "dir": "Buy", "closedPnl": "0.0", "hash": "0xf3", "oid": 3, "crossed": True, "fee": "0.0003", "tid": 13,
         "feeToken": "HYPE"},
    ],
    ("userFunding", TRIA.lower(), ""): [
        {"time": 1790100000000, "hash": "0x0", "delta": {"type": "funding", "coin": "ETH", "usdc": "-0.10",
                                                           "szi": "0.1", "fundingRate": "0.00001"}}],
    ("userFunding", TRIA.lower(), "xyz"): [
        {"time": 1790200000000, "hash": "0x0", "delta": {"type": "funding", "coin": "xyz:BE", "usdc": "-0.42",
                                                           "szi": "2.21", "fundingRate": "0.0000125"}}],
    ("userNonFundingLedgerUpdates", TRIA.lower()): [
        # matches Primary's top-up 0xtopup1 (a little less arrives)
        {"time": 1792508400000 + 600_000, "hash": "0xd1", "delta": {"type": "deposit", "usdc": "1278.90"}},
        {"time": 1792600000000, "hash": "0xd2", "delta": {"type": "accountClassTransfer", "usdc": "100", "toPerp": True}},
        {"time": 1792700000000, "hash": "0xd3", "delta": {"type": "spotTransfer", "token": "USDC", "amount": "25",
                                                           "usdcValue": "25", "user": TRIA.lower(),
                                                           "destination": "0xcardsettle", "fee": "0"}},
    ],
    ("userNonFundingLedgerUpdates", PRIMARY.lower()): [
        {"time": 1793026800000 + 120_000, "hash": "0xd9", "delta": {"type": "deposit", "usdc": "99.0"}},
    ],
}


async def fake_get(url, params=None, headers=None, retries=4):
    CALLS.append(url)
    if url.endswith("/chains/"):
        return {"data": [{"id": c, "attributes": {"name": c, "flags": {"supports_transactions": True}}}
                         for c in ("ethereum", "base", "arbitrum", "optimism", "polygon", "solana", "robinhood")]}
    if "/wallets/" in url:
        addr = url.split("/wallets/")[1].split("/")[0].lower()
        data = ZERION.get(addr, ZERION.get(url.split("/wallets/")[1].split("/")[0], []))
        lo, hi = params["filter[min_mined_at]"], params["filter[max_mined_at]"]
        data = [t for t in data if lo <= H._ms(t["attributes"]["mined_at"]) <= hi]
        return {"links": {"self": url}, "data": data}
    if "historical-price" in url:
        return {"prices": [{"time": params["timestamp"], "USD": 60000}]}
    if "/txs/chain" in url:
        return [{"txid": "btc1", "status": {"confirmed": True, "block_time": 1785000000}, "fee": 500,
                 "vin": [{"prevout": {"scriptpubkey_address": "bc1qexchange", "value": 100000}}],
                 "vout": [{"scriptpubkey_address": BTC, "value": 46735},
                          {"scriptpubkey_address": "bc1qexchange", "value": 52765}]}]
    raise AssertionError("unexpected GET " + url)


async def fake_post(url, body, headers=None, retries=4):
    t = body.get("type")
    if t == "spotMeta":
        return HL["spotMeta"]
    if t == "perpDexs":
        return [None, {"name": "xyz"}, {"name": "flx"}]
    u = body.get("user", "").lower()
    if t == "userFunding":
        return [x for x in HL.get((t, u, body.get("dex", "")), []) if body["startTime"] <= x["time"] <= body["endTime"]]
    return [x for x in HL.get((t, u), []) if body["startTime"] <= x["time"] <= body["endTime"]]


S.get_json = fake_get
S.post_json = fake_post
H.hl_post = lambda body: fake_post(S.HL_INFO, body)


async def main():
    ws = [{"address": PRIMARY, "nickname": "Primary"}, {"address": TRIA, "nickname": "Tria"},
          {"address": BANKR, "nickname": "Bankr"}, {"address": PHANTOM, "nickname": "Phantom"},
          {"address": BTC, "nickname": "BTC Xverse"}]
    r = await H.history(ws, None, "2026-01-01", None, None, "all", 1.0, False, 0, 1000, 30)
    assert r["status"] == "ready", r
    for c in r["coverage"]:
        print("COV", c["wallet"], c["source"], c["events"], c.get("error"), c.get("note"))
    print("COUNTS", r["counts"], "review", r["review"])
    for e in r["events"]:
        legs = "; ".join(f"{l['dir']} {l['amt']} {l['sym']}{' own' if l.get('own') else ''}" for l in e["legs"])
        print(f"{e['time'][:16]} {e['wallet']:<10} {e['chain']:<11} {e['class']:<8} {'auto' if e['auto'] else 'REVIEW'}"
              f"  {legs[:70]:<70} | {e['why']}{'  pair' if e.get('pair') else ''}{'  group' if e.get('group') else ''}")
    print("PERPS", json.dumps(r["perps_summary"]))
    by = {e["id"].split(":")[1] + ":" + e["wallet"]: e for e in r["events"]}
    cls = lambda k: (by[k]["class"], by[k]["auto"])  # noqa: E731
    assert cls("0xea9884:Bankr") == ("swap", True)
    assert cls("0xe6724e:Bankr") == ("swap", True) and by["0xe6724e:Bankr"]["xchain"] == "out"
    assert cls("0x29cb66:Bankr") == ("swap", True) and by["0x29cb66:Bankr"]["pair"] == by["0xe6724e:Bankr"]["id"]
    assert cls("0xstake1:Bankr") == ("reward", False)
    assert cls("0xmove1:Bankr") == ("self", True) and cls("0xmove1:Primary") == ("self", True)
    assert by["0xmove1:Bankr"]["group"] == by["0xmove1:Primary"]["group"]
    assert cls("0x28ded0:Primary") == ("swap", True)
    assert cls("0x9473f8:Primary") == ("defi", False)
    assert cls("0xtopup1:Primary") == ("bridge", True), by["0xtopup1:Primary"]
    assert cls("0xpay1:Primary") == ("send", False) and by["0xpay1:Primary"]["fee_tokens"][0]["amt"] == 0.05
    assert cls("0xwrap1:Primary") == ("wrap", True)
    assert cls("0xappr:Primary") == ("approve", True)
    assert cls("0xfail:Primary") == ("failed", True)
    assert cls("0xdust:Primary") == ("dust", True)
    assert cls("0xhlbr:Primary") == ("bridge", True)
    assert cls("0xspam1:Bankr") == ("unpriced", True)
    assert cls("5solswap:Phantom") == ("swap", True)
    assert cls("btc1:BTC Xverse") == ("receive", False)
    assert cls("0xsplitA:Primary") == ("swap", True) and by["0xsplitA:Primary"]["pair"] == by["0xsplitB:Primary"]["id"]
    assert cls("0xloopA:Primary") == ("receive", False) and cls("0xloopB:Primary") == ("send", False)
    assert cls("0xmint0:Primary") == ("unpriced", True)
    assert by["0xtopup1:Primary"].get("stable_only") == "USD" and by["0xpay1:Primary"].get("stable_only") == "USD"
    assert not by["0x28ded0:Primary"].get("stable_only")
    hl = [e for e in r["events"] if e["src"] == "hyperliquid"]
    assert {e["class"] for e in hl} >= {"perp", "funding", "swap", "self", "bridge", "send"}, {e["class"] for e in hl}
    dep = next(e for e in hl if e.get("kind") == "deposit" and e["wallet"] == "Tria")
    assert dep.get("pair") == by["0xtopup1:Primary"]["id"]
    ps = {(p["wallet"], p["coin"]): p for p in r["perps_summary"]}
    assert ps[("Tria", "xyz:TSLA")]["closed_pnl"] == -12.5 and ps[("Tria", "xyz:BE")]["funding"] == -0.42
    # second call: served from the build cache; review filter and paging
    r2 = await H.history(ws, None, "2026-01-01", None, [PRIMARY], "review", 1.0, False, 0, 2, 30)
    assert r2["total"] == 4 and all(not e["auto"] for e in r2["events"]), r2["total"]
    # third: a later since on a fresh build reuses the stored events without re-fetching
    n = len(CALLS)
    H._builds.clear()
    r3 = await H.history(ws, None, "2026-10-01", None, None, "all", 1.0, False, 0, 1000, 30)
    assert all(e["ms"] >= H.day_ms("2026-10-01") for e in r3["events"])
    assert len(CALLS) == n, CALLS[n:]
    print("\nALL CHECKS PASSED;", len(r["events"]), "events")


asyncio.run(main())

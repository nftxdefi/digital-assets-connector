"""Offline test of the Nov 10, 2026 audit fixes (python tests/test_audit.py). No network.
A1 a later `since` no longer leaves a gap; A2 ledger entries without a hash keep one id (and old ids are rekeyed);
A3 HyperEVM reports balances it couldn't read; A4 a fetch that stops at a safety limit says so; A5 old builds go."""
import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
os.environ["DATA_DIR"] = tempfile.mkdtemp()
os.environ["MCP_PATH_TOKEN"] = "x" * 30

import history as H  # noqa: E402
import sources as S  # noqa: E402

DAY = 86400 * 1000
BASE = H.day_ms("2026-11-01")
Z = "0x" + "0" * 64
clock = [BASE]
H.now_ms = lambda: clock[0]


async def a1_gap():
    ev = [{"id": f"e{i}", "ms": BASE + i * DAY + 3600_000} for i in range(20)]  # one a day, Nov 1 to 20
    calls = []

    async def fetch(addr, a, b):
        calls.append((a, b))
        return [dict(e) for e in ev if a <= e["ms"] <= b]

    addr = "0x" + "cd" * 20
    clock[0] = BASE + 5 * DAY
    items, _ = await H.synced("zerion", addr, H.day_ms("2026-01-01"), fetch, True)
    assert len(items) == 5, len(items)
    clock[0] = BASE + 15 * DAY  # ten days later, a question about the last two days only
    items, _ = await H.synced("zerion", addr, H.day_ms("2026-11-14"), fetch, True)
    assert len(items) == 2, len(items)  # the answer is still only what was asked for
    assert calls[-1][0] <= BASE + 5 * DAY, "the sync must resume from where the last one ended"
    clock[0] = BASE + 19 * DAY + 7200_000
    items, _ = await H.synced("zerion", addr, H.day_ms("2026-01-01"), fetch, True)
    assert {e["id"] for e in items} == {e["id"] for e in ev}, sorted(e["id"] for e in items)
    print("A1 ok: no gap,", len(items), "of 20")


async def a2_hashless():
    led = [{"time": BASE + i * DAY + 1000, "hash": Z, "delta": {"type": "rewardsClaim", "amount": "5.0", "token": "USDC"}}
           for i in range(3)]
    led.append({"time": BASE + 1000, "hash": "0x" + "ab" * 32, "delta": {"type": "deposit", "usdc": "10"}})

    async def post(body):
        if body["type"] == "userNonFundingLedgerUpdates":
            return [x for x in led if body["startTime"] <= x["time"] <= body["endTime"]]
        return []

    async def names():
        return {}

    H.hl_post, H.hl_spot_names = post, names
    addr = "0x" + "ab" * 20
    f = lambda a, s, e: H.hl_fetch(a, "T", s, e)  # noqa: E731
    clock[0] = BASE + 3 * DAY
    items, _ = await H.synced("hyperliquid", addr, H.day_ms("2026-01-01"), f, True)
    ids0 = sorted(e["id"] for e in items)
    assert len(items) == 4, len(items)
    for d in (1, 2, 3):
        clock[0] += DAY
        items, info = await H.synced("hyperliquid", addr, H.day_ms("2026-01-01"), f, True)
        assert sorted(e["id"] for e in items) == ids0 and not info["rechecked"], (d, len(items))
    assert f"hyperliquid:ledger:0x{'ab' * 32}:deposit" in ids0  # entries with a hash keep their id
    # a store written by the old code: the same entry under three position-based ids
    addr2 = "0x" + "ef" * 20
    one = H.hl_ledger_event(addr2, "T", addr2, led[0])
    old = {f"hyperliquid:ledger:{Z}:rewardsClaim:{one['ms']}:{i}": dict(one, id=f"hyperliquid:ledger:{Z}:rewardsClaim:{one['ms']}:{i}")
           for i in range(3)}
    H._save(H._store_path("hyperliquid", addr2), {"v": 1, "from": H.day_ms("2026-01-01"), "through": clock[0],
                                                  "synced": clock[0], "items": old})
    items, _ = await H.synced("hyperliquid", addr2, H.day_ms("2026-01-01"), f, False)
    assert [e["id"] for e in items] == [one["id"]], [e["id"] for e in items]
    print("A2 ok: stable ids, old copies collapsed to 1")


async def a3_hyperevm():
    addr = "0x5f62ecE59FbBA2Bae1654e9b69bbfA4e4d5b329b"
    toks = S.hyperevm_token_list()
    assert len(toks) >= 3
    mode = {"m": "ok", "n": 0}

    async def post(url, body, headers=None, retries=4):
        mode["n"] += 1
        if isinstance(body, list):
            if mode["m"] == "nobatch":
                return {"error": "batch not supported"}
            out = []
            for q in body:
                i = q["id"]
                if i == 0:
                    out.append({"id": 0, "result": hex(2 * 10**18)})
                elif i == 1 and mode["m"] == "limited":
                    out.append({"id": 1, "error": {"code": -32005, "message": "rate limited"}})
                elif i == 2:
                    out.append({"id": 2, "error": {"code": 3, "message": "execution reverted"}})
                else:
                    out.append({"id": i, "result": hex(5 * 10**18) if i == 1 else "0x"})
            return out
        if q := body.get("params"):
            if body["method"] == "eth_getBalance":
                return {"result": hex(2 * 10**18)}
            if q[0]["data"] == "0x313ce567":
                return {"result": hex(18)}
            return {"result": hex(5 * 10**18) if q[0]["to"] == toks[0]["address"] else "0x"}
        return {}

    async def px(refresh=False):
        return {"HYPE": S.D(40)}

    S.post_json, S.hl_spot_prices = post, px
    prices = {"usd": {}, "fx": None}
    r = await S.hyperevm(addr, prices, True)
    assert r["error"] is None and {t["symbol"] for t in r["tokens"]} == {"HYPE", toks[0]["symbol"]}, r
    n_ok = mode["n"]
    mode["m"] = "limited"
    r = await S.hyperevm(addr, prices, True)
    assert r["error"] and toks[0]["symbol"] in r["error"] and toks[1]["symbol"] not in r["error"], r["error"]
    mode["m"] = "nobatch"
    r = await S.hyperevm(addr, prices, True)
    assert r["error"] is None and len(r["tokens"]) == 2, r
    print("A3 ok: 1 batched request + 1 decimals lookup (%d calls); unread balance reported: %s" % (n_ok, "limited"))


async def a4_cut():
    async def post(body):  # every request returns a full batch of new funding entries
        lo = body["startTime"]
        return [{"time": lo + i + 1, "hash": Z, "delta": {"type": "funding", "coin": "xyz:BE", "usdc": "-0.01"}} for i in range(500)]

    async def names():
        return {}

    H.hl_post, H.hl_spot_names, H.HL_MAX_CALLS = post, names, 3
    addr = "0x" + "12" * 20
    clock[0] = BASE + 40 * DAY
    items, info = await H.synced("hyperliquid", addr, H.day_ms("2026-01-01"), lambda a, s, e: H.hl_fetch(a, "T", s, e), True)
    assert info.get("cut") and "cut off after 3 requests" in info["cut"], info
    _, cov = await H._job("hyperliquid", {"address": addr, "nickname": "T"}, H.day_ms("2026-01-01"), False)
    assert "may be incomplete" in (cov["note"] or ""), cov
    H.HL_MAX_CALLS = 400
    print("A4 ok:", cov["note"][:90])


async def a5_builds():
    async def post(body):
        return []

    async def none(*a, **k):
        return []

    H.hl_post, H.blockscout_fetch = post, none
    for i in range(6):
        w = [{"address": "0x" + f"{i:02x}" * 20, "nickname": f"w{i}"}]
        r = await H.history(w, None, "2026-01-01", None, None, "all", 1.0, False, 0, 10, 30)
        assert r["status"] == "ready", r
    assert len(H._builds) <= H.BUILD_KEEP + 1, len(H._builds)
    print("A5 ok: builds kept:", len(H._builds))


async def main():
    await a1_gap()
    await a2_hashless()
    await a3_hyperevm()
    await a4_cut()
    await a5_builds()
    print("AUDIT CHECKS PASSED")


asyncio.run(main())

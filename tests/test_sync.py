"""Offline test of the Nov 7, 2026 sync changes: yearly windows, merge-only store, recount, 429 cool-down,
stored events kept on failure, and the Bank of Canada rates tool. No network."""
import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
os.environ["DATA_DIR"] = tempfile.mkdtemp()
os.environ["ZERION_API_KEY"] = "zk_test"
os.environ["MCP_PATH_TOKEN"] = "x" * 30

import httpx  # noqa: E402

import history as H  # noqa: E402
import sources as S  # noqa: E402

ADDR = "0x5f62ecE59FbBA2Bae1654e9b69bbfA4e4d5b329b"
DAY = 86400 * 1000
T0 = H.day_ms("2020-01-01")
# one event every 3 days from 2020 to now
ALL = [{"id": f"base:0xe{i}:{ADDR.lower()[:10]}", "ms": T0 + i * 3 * DAY, "src": "zerion", "chain": "base",
        "wallet": "", "address": ADDR, "time": H._iso(T0 + i * 3 * DAY), "hash": f"0xe{i}", "op": "approve",
        "app": None, "status": "confirmed", "legs": [], "fee": None}
       for i in range((H.now_ms() - T0) // (3 * DAY))]
CALLS = []
DROP = {"on": False}
FAIL = {"code": None}


def err(code, retry_after=None):
    req = httpx.Request("GET", "https://api.zerion.io/v1/wallets/x/transactions/")
    return httpx.HTTPStatusError("x", request=req, response=httpx.Response(
        code, request=req, headers={"retry-after": str(retry_after)} if retry_after else {}))


async def fetch(addr, a, b):
    CALLS.append((a, b))
    if FAIL["code"]:
        raise err(FAIL["code"], FAIL.get("ra"))
    out = [dict(e) for e in ALL if a <= e["ms"] <= b]
    if DROP["on"]:  # what Zerion did on Nov 6: silently miss a block of events once
        DROP["on"] = False
        out = out[: len(out) // 2]
    return out


def ids(items):
    return {e["id"] for e in items}


async def main():
    s2026 = H.day_ms("2026-01-01")
    want26 = {e["id"] for e in ALL if e["ms"] >= s2026}
    # 1. first load from 2026: one window
    items, info = await H.synced("zerion", ADDR, s2026, fetch, False, 365, "zerion")
    assert ids(items) == want26 and len(CALLS) == 1 and not info["error"], (len(CALLS), info)
    # 2. earlier since: only the missing part, in yearly windows, newest first; nothing stored is lost
    CALLS.clear()
    items, info = await H.synced("zerion", ADDR, T0, fetch, False, 365, "zerion")
    assert ids(items) == {e["id"] for e in ALL}, len(items)
    assert len(CALLS) >= 6 and all(b - a < 366 * DAY for a, b in CALLS), CALLS
    assert CALLS[0][0] > CALLS[-1][0], "back-fill should go newest window first"
    assert max(b for _, b in CALLS) < s2026 + 3 * DAY, "should not refetch 2026"
    print("windows", len(CALLS), "events", len(items))
    # 3. a refresh where the API drops half the window: refetched once, union kept
    CALLS.clear()
    DROP["on"] = True
    H._mem_store[str(H._store_path("zerion", ADDR))]["through"] = H.day_ms("2026-03-01")  # re-read Mar 2026 on
    items, info = await H.synced("zerion", ADDR, T0, fetch, True, 365, "zerion")
    assert ids(items) == {e["id"] for e in ALL} and info["rechecked"] == 1 and len(CALLS) == 2, (info, CALLS)
    # 3b. even a short fetch that is not rechecked never removes stored events
    st = H._mem_store[str(H._store_path("zerion", ADDR))]
    assert len(st["items"]) == len(ALL)
    # 4. HTTP 429: stored events come back with the error, and the source pauses
    CALLS.clear()
    FAIL["code"] = 429
    items, info = await H.synced("zerion", ADDR, s2026, fetch, True, 365, "zerion")
    assert ids(items) == want26 and info["error"].startswith("HTTP 429") and info["kept"] == len(want26), info
    assert H.cooling("zerion") and len(CALLS) == 1
    items, info = await H.synced("zerion", ADDR, s2026, fetch, True, 365, "zerion")
    assert len(CALLS) == 1, "no request while cooling"
    assert "paused until" in info["error"] and ids(items) == want26, info
    w1 = H._cool["zerion"]["wait"]
    # cool-down doubles on a second 429 soon after, and honours a long Retry-After
    H._cool["zerion"]["until"] = H.time.time() - 1
    FAIL["ra"] = 7200
    await H.synced("zerion", ADDR, s2026, fetch, True, 365, "zerion")
    assert H._cool["zerion"]["wait"] == 7200 and H._cool["zerion"]["wait"] >= w1 * 2, H._cool
    # 5. recovery: success clears the pause
    H._cool["zerion"]["until"] = H.time.time() - 1
    FAIL["code"] = None
    items, info = await H.synced("zerion", ADDR, s2026, fetch, True, 365, "zerion")
    assert not info["error"] and "zerion" not in H._cool and ids(items) == want26
    # 6. a store written by the old code (no "through") keeps working
    p = H._store_path("zerion", "0xold0000000000000000000000000000000000000")
    old_items = {e["id"]: e for e in ALL if e["ms"] >= s2026}
    H._save(p, {"v": 1, "from": s2026, "synced": H.now_ms() - 3600_000, "items": old_items})
    CALLS.clear()
    items, info = await H.synced("zerion", "0xold0000000000000000000000000000000000000", s2026, fetch, False, 365)
    assert ids(items) == want26 and len(CALLS) == 1 and CALLS[0][0] > H.now_ms() - 3 * DAY, CALLS
    # 7. through history(): coverage row has error + kept, events still there
    H._mem_store.clear()

    async def fake_get(url, params=None, headers=None, retries=4):
        if url.endswith("/chains/"):
            return {"data": [{"id": c, "attributes": {"name": c, "flags": {"supports_transactions": True}}}
                             for c in ("ethereum", "base", "robinhood")]}
        if "/wallets/" in url:
            raise err(429)
        if "valet/observations/FXUSDCAD" in url:
            assert params["start_date"] == "2026-11-02"
            return {"observations": [{"d": "2026-11-02", "FXUSDCAD": {"v": "1.3944"}},
                                     {"d": "2026-11-03", "FXUSDCAD": {"v": "1.3981"}},
                                     {"d": "2026-11-04", "FXUSDCAD": {"v": ""}}]}
        raise AssertionError(url)

    async def fake_post(url, body, headers=None, retries=4):
        return [] if body.get("type") != "spotMeta" else {"tokens": [], "universe": []}

    S.get_json = fake_get
    S.post_json = fake_post
    H.hl_post = lambda body: fake_post(S.HL_INFO, body)
    H._cool.clear()
    H.SYNC_TTL = 0
    r = await H.history([{"address": ADDR, "nickname": "Primary"}], None, "2026-01-01", None, None, "all", 1.0,
                        True, 0, 1000, 30)
    assert r["status"] == "ready", r
    z = next(c for c in r["coverage"] if c["source"] == "zerion")
    rh = next(c for c in r["coverage"] if c["source"] == "robinhood")
    print("COV", z, rh)
    assert z["error"].startswith("HTTP 429") and z["kept"] and z["events"] == len(want26), z
    assert not rh.get("error"), rh
    assert sum(1 for e in r["events"] if e["src"] != "hyperliquid") == len(want26)
    # 8. Bank of Canada rates
    fx = await H.boc_rates("FXUSDCAD", "2026-11-02", None)
    assert fx["rates"] == {"2026-11-02": 1.3944, "2026-11-03": 1.3981} and fx["last"] == "2026-11-03", fx
    assert "error" in await H.boc_rates("XXX", "2026-11-02", None)
    import server
    names = [t.name for t in await server.mcp.list_tools()]
    assert "get_fx_rates" in names and "get_history" in names, names
    print("tools:", names)
    print("SYNC CHECKS PASSED")


asyncio.run(main())

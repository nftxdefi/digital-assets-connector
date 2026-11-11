"""Offline test of the Nov 11, 2026 changes (python tests/test_owner.py). No network.
B1 wallets valued at the Bank of Canada USD/CAD rate, CoinGecko as fallback; B2 funding summed per day on request;
B10 the debugging routes are gone."""
import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
os.environ["DATA_DIR"] = tempfile.mkdtemp()
os.environ["MCP_PATH_TOKEN"] = "x" * 30

import history as H  # noqa: E402
import sources as S  # noqa: E402

state = {"boc": "ok", "cg": "ok", "boc_calls": 0}


async def fake_get_json(url, params=None, headers=None, retries=4):
    if "bankofcanada" in url:
        state["boc_calls"] += 1
        if state["boc"] == "down":
            raise S.httpx.ConnectError("no route")
        return {"observations": [{"d": "2026-11-06", "FXUSDCAD": {"v": "1.4113"}},
                                 {"d": "2026-11-10", "FXUSDCAD": {"v": "1.4123"}},
                                 {"d": "2026-11-09", "FXUSDCAD": {"v": "1.4150"}}]}
    if "mempool" in url:
        return {"USD": 100000, "CAD": 141900}
    raise AssertionError("unexpected url " + url)


async def fake_cg(ids, refresh=False):
    if state["cg"] == "down":
        raise S.httpx.ConnectError("cg down")
    return {"bitcoin": {"usd": 100000, "cad": 141870}, "ethereum": {"usd": 3000, "cad": 4256.1}}


S.get_json = fake_get_json
S.cg_simple = fake_cg


def reset():
    S._cache.clear()
    S._boc_fail = 0.0


async def b1():
    reset()
    p = await S.native_prices()
    assert p["fx_usd_cad"] == 1.4123 and p["fx_source"] == "Bank of Canada 2026-11-10", p  # newest day, not the last row
    assert p["usd"]["bitcoin"] == 100000
    await S.native_prices()
    assert state["boc_calls"] == 1, "cached for an hour"
    # Bank of Canada down: CoinGecko's rate, and no second try for 10 minutes
    reset(); state["boc"] = "down"; state["boc_calls"] = 0
    p = await S.native_prices()
    assert abs(p["fx_usd_cad"] - 1.4187) < 1e-9 and p["fx_source"].startswith("CoinGecko"), p
    await S.native_prices()
    assert state["boc_calls"] == 1, state
    # both down: mempool.space
    reset(); state["cg"] = "down"
    p = await S.native_prices()
    assert abs(p["fx_usd_cad"] - 1.419) < 1e-9 and p["fx_source"].startswith("mempool"), p
    # CoinGecko down, Bank of Canada up: the BoC rate, bitcoin from mempool.space
    reset(); state["boc"] = "ok"
    p = await S.native_prices()
    assert p["fx_usd_cad"] == 1.4123 and p["usd"]["bitcoin"] == 100000, p
    state["cg"] = "ok"
    # CADC (price "cad") is then worth exactly one Canadian dollar at the same rate the ledger uses
    print("b1 ok")


def fev(i, ms, wallet, coin, usdc):
    return {"id": f"hyperliquid:funding:{ms}:{coin}", "src": "hyperliquid", "class": "funding", "auto": True, "ms": ms,
            "wallet": wallet, "address": "0x" + ("ab" if wallet == "Tria" else "cd") * 20, "op": "funding",
            "funding": {"coin": coin, "sym": coin.rpartition(":")[2], "usdc": usdc}}


async def b2():
    h = 3600_000
    t0 = H.day_ms("2026-11-10")  # 00:00 UTC = Nov 9, 19:00 in Toronto
    evs = [fev(i, t0 + i * h, "Tria", "xyz:BE", -0.01) for i in range(10)]
    evs += [fev(100 + i, t0 + i * h, "Tria", "BTC", 0.02) for i in range(3)]
    evs += [{"id": "s1", "src": "zerion", "class": "swap", "auto": True, "ms": t0, "wallet": "Primary",
             "address": "0x" + "cd" * 20, "op": "trade", "legs": []},
            {"id": "s2", "src": "zerion", "class": "send", "auto": False, "ms": t0 + h, "wallet": "Primary",
             "address": "0x" + "cd" * 20, "op": "send", "legs": []}]
    rest, rows, tz = H.funding_by_day(evs, "America/Toronto")
    assert tz == "America/Toronto" and [e["id"] for e in rest] == ["s1", "s2"]
    be = {r["d"]: r for r in rows if r["sym"] == "BE"}
    # Toronto is UTC-5: hours 0 to 4 UTC are Nov 9 there, 5 to 9 are Nov 10
    assert be["2026-11-09"]["n"] == 5 and be["2026-11-10"]["n"] == 5, be
    assert abs(be["2026-11-09"]["usdc"] + 0.05) < 1e-9 and be["2026-11-09"]["coin"] == "xyz:BE"
    assert sum(r["n"] for r in rows) == 13
    _, rows_utc, tz2 = H.funding_by_day(evs, "Not/AZone")
    assert tz2 == "UTC" and {r["d"] for r in rows_utc} == {"2026-11-10"}
    # through history(): funding leaves events, total and paging; counts keep it; the list comes on the first page
    key_build = {"at": __import__("time").time(), "task": None, "error": None,
                 "result": {"built_at": "x", "events": evs, "coverage": []}}
    H._builds.clear()

    async def fake_build(*a, **k):
        return key_build["result"]
    H._build = fake_build
    ws = [{"address": "0x" + "ab" * 20, "nickname": "Tria"}, {"address": "0x" + "cd" * 20, "nickname": "Primary"}]
    r = await H.history(ws, None, "2026-01-01", None, None, "all", 1.0, False, 0, 1, 5, "daily", "America/Toronto")
    assert r["status"] == "ready" and r["total"] == 2 and r["next_offset"] == 1 and len(r["events"]) == 1, r
    assert r["counts"]["funding"] == 13 and r["funding_events"] == 13 and len(r["funding_daily"]) == 3, r
    assert r["funding_tz"] == "America/Toronto"
    assert r["perps_summary"] and abs(sum(a["funding"] for a in r["perps_summary"]) - (-0.1 + 0.06)) < 1e-9
    r2 = await H.history(ws, None, "2026-01-01", None, None, "all", 1.0, False, 1, 1, 5, "daily", "America/Toronto")
    assert r2["funding_daily"] is None and r2["funding_events"] == 13 and r2["next_offset"] is None
    r3 = await H.history(ws, None, "2026-01-01", None, None, "review", 1.0, False, 0, 50, 5, "daily", "UTC")
    assert [e["id"] for e in r3["events"]] == ["s2"] and r3["review"] == 1
    r4 = await H.history(ws, None, "2026-01-01", None, None, "all", 1.0, False, 0, 50, 5)
    assert r4["total"] == 15 and "funding_daily" not in r4, "the default is unchanged"
    print("b2 ok")


async def b10():
    import server
    paths = [getattr(r, "path", "") for r in server.app.routes]
    assert not any("/api/" in p for p in paths), paths
    assert "/healthz" in paths
    tool = [t for t in await server.mcp.list_tools() if t.name == "get_history"][0]
    props = tool.inputSchema["properties"]
    assert props["funding"]["default"] == "events" and props["tz"]["default"] == "UTC"
    bad = await server.get_history(funding="weekly")
    assert "error" in bad
    print("b10 ok")


asyncio.run(b1())
asyncio.run(b2())
asyncio.run(b10())
print("all ok")

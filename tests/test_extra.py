import asyncio, os, sys, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
os.environ["DATA_DIR"] = tempfile.mkdtemp(); os.environ["MCP_PATH_TOKEN"] = "x"*30
os.environ.pop("ZERION_API_KEY", None)
import sources as S, history as H
BANKR = "0xf93f154841256e0d006ef2b7b99dccbf8cac52f7"

async def fake_bs(chain, path):
    assert chain == "robinhood", chain
    if "/transactions" in path:
        return {"items": [{"hash": "0xa1", "timestamp": "2026-09-27T10:00:00Z", "from": {"hash": BANKR}, "to": {"hash": "0xrelay"},
                           "value": str(12*10**15), "fee": {"value": str(10**13)}, "status": "ok", "method": "deposit"}],
                "next_page_params": None}
    if "/token-transfers" in path:
        assert "type=ERC-20" in path
        return {"items": [{"transaction_hash": "0xa2", "timestamp": "2026-10-01T00:00:00Z", "from": {"hash": "0xsomeone"},
                           "to": {"hash": BANKR}, "token": {"symbol": "USDG", "name": "Global Dollar", "address_hash": "0xusdg",
                           "decimals": "6"}, "total": {"value": "1383679", "decimals": "6"}},
                          {"transaction_hash": "0xa3", "timestamp": "2026-10-01T00:00:00Z", "from": {"hash": "0xscam"},
                           "to": {"hash": BANKR}, "token": {"symbol": "SCAM", "reputation": "scam", "decimals": "18"},
                           "total": {"value": "1", "decimals": "18"}}], "next_page_params": None}
    if "/internal-transactions" in path:
        return {"items": [], "next_page_params": None}
    raise AssertionError(path)

async def fake_get(url, params=None, headers=None, retries=4):
    if "/coins/ethereum/history" in url:
        return {"market_data": {"current_price": {"usd": 2695.6}}}
    raise AssertionError(url)

calls = []
async def fake_hl(body):
    calls.append((body["type"], body["startTime"], body["endTime"]))
    if body["type"] != "userFillsByTime": return []
    # 1200 fills from t=1000..2199; API returns at most 500, newest first (worst case)
    allf = [{"tid": i, "time": 1000 + i} for i in range(1200)]
    inr = [f for f in allf if body["startTime"] <= f["time"] <= body["endTime"]]
    return sorted(inr, key=lambda f: -f["time"])[:500]

S.bs_get = fake_bs; S.get_json = fake_get; H.hl_post = fake_hl

async def main():
    evs = await H.blockscout_fetch("robinhood", BANKR, "Bankr", 0, H.now_ms())
    for e in evs: print(e["hash"], e["legs"], e["fee"])
    assert {e["hash"] for e in evs} == {"0xa1", "0xa2"}
    a1 = next(e for e in evs if e["hash"] == "0xa1")
    assert a1["legs"][0]["amt"] == 0.012 and abs(a1["legs"][0]["usd"] - 32.3472) < 0.01 and a1["fee"]["usd"]
    fills = await H._hl_paged("userFillsByTime", BANKR, 0, 10**13, lambda x: x["tid"])
    print("fills", len(fills), "calls", len(calls))
    assert len(fills) == 1200
    print("sources without Zerion:", H.sources_for(BANKR), H.sources_for("G8i4SziqvFFfzvDUcpkkPn32XH9a6HLbbsUCDkwpDPkq"))
    import server
    tools = await server.mcp.list_tools()
    print("tools:", [t.name for t in tools])
    assert "get_history" in [t.name for t in tools]
    print("EXTRA OK")
asyncio.run(main())

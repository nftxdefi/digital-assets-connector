"""History check from the VPS:  docker compose run --rm digital-assets python historytest.py [since] [--dump file.json] [address ...]

Builds the transaction history for the saved wallets (or the addresses given) from `since` (default
2026-01-01) and prints, per wallet and source, how many events were found and any error, then the count
per class and the first few events that need review. Addresses are shortened in the output.
"""
import asyncio
import re
import sys
import time

import history as H
import sources as S


def short(a: str) -> str:
    return a if not a or len(a) <= 14 else f"{a[:6]}…{a[-4:]}"


async def main():
    args = sys.argv[1:]
    dump = None
    if "--dump" in args:
        i = args.index("--dump")
        dump = args[i + 1]
        del args[i:i + 2]
    since = args.pop(0) if args and re.fullmatch(r"\d{4}-\d{2}-\d{2}", args[0]) else "2026-01-01"
    ws = [{"address": a, "nickname": ""} for a in args] or S.default_wallets()
    if not ws:
        print("No addresses given and WALLETS is empty in .env")
        return
    print(f"Zerion key: {'set' if S.env('ZERION_API_KEY') else 'NOT SET (no EVM or Solana history)'}")
    print(f"History store: {H.HIST}  (writable: {H.DATA.exists()})")
    t0 = time.time()
    r = {"status": "building"}
    while r.get("status") == "building":
        r = await H.history(ws, None, since, None, None, "all", 1.0, False, 0, 100000, 30)
        if r.get("status") == "building":
            p = r["progress"]
            print(f"  building… {p['done']}/{p['total']} sources ({time.time() - t0:.0f}s)")
    if r.get("status") != "ready":
        print("ERROR:", r)
        return
    print(f"\nBuilt in {time.time() - t0:.0f}s, {r['total']} events since {since}\n")
    for c in r["coverage"]:
        flag = "ERR" if c.get("error") else "OK "
        print(f"  [{flag}] {(c.get('wallet') or short(c.get('address') or '')):<14} {str(c.get('source')):<12}"
              f" {c.get('events', 0):>5} events" + (f"  new {c['fetched']}" if c.get("fetched") else "")
              + (f"   <- {c['error']}" + (" (saved events kept)" if c.get("kept") else "") if c.get("error") else "")
              + (f"  rechecked {c['rechecked']}" if c.get("rechecked") else "") + (f"   ({c['note']})" if c.get("note") else ""))
    print("\nBy class:", ", ".join(f"{k} {v}" for k, v in sorted(r["counts"].items(), key=lambda x: -x[1])))
    print(f"Need review: {r['review']}")
    for p in r["perps_summary"]:
        print(f"  perps {p['wallet']:<10} {p['coin']:<12} fills {p['fills']:>4}  closed P&L {p['closed_pnl']:>10.2f}"
              f"  fees {p['fees']:>8.2f}  funding {p['funding']:>8.2f}")
    print("\nFirst 15 to review:")
    for e in [e for e in r["events"] if not e["auto"]][:15]:
        legs = "; ".join(f"{l['dir']} {l['amt']} {l['sym']}" + (f" (${l['usd']:.2f})" if l.get("usd") else "")
                         for l in e["legs"])
        print(f"  {e['time'][:16]} {e['wallet']:<10} {e['chain']:<10} {e['class']:<8} {legs[:80]}  | {e['why']}")
    if dump:
        import json
        with open(dump, "w") as fh:
            json.dump(r, fh, separators=(",", ":"))
        print(f"\nWrote all {r['total']} events to {dump}")
    await S.client().aclose()


asyncio.run(main())

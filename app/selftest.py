"""Quick check from the VPS:  docker compose run --rm digital-assets python selftest.py [address ...]

With no addresses it uses WALLETS from .env. Prints one line per chain so failing sources stand out.
Addresses are shortened in the output.
"""
import asyncio
import sys

import sources as S


def short(a: str) -> str:
    return a if len(a) <= 14 else f"{a[:6]}…{a[-4:]}"


async def main():
    ws = [{"address": a, "nickname": ""} for a in sys.argv[1:]] or S.default_wallets()
    if not ws:
        print("No addresses given and WALLETS is empty in .env")
        return
    p = await S.portfolio(ws)
    print(f"FX USD->CAD: {p['fx_usd_cad']}   price errors: {p['price_errors'] or 'none'}")
    for w in p["wallets"]:
        label = w["nickname"] or short(w["address"])
        print(f"\n== {label} ({w['address_type']}): US${w['total_usd']:,.2f}")
        if w.get("error"):
            print("   ERROR:", w["error"])
        for c in w["chains"]:
            status = "OK " if not c["error"] else "ERR"
            print(f"   [{status}] {c['name']:<16} US${c['total_usd']:>12,.2f}  {c['token_count']:>3} tokens"
                  + (f"   <- {c['error']}" if c["error"] else ""))
    print(f"\nTOTAL US${p['total_usd']:,.2f}  /  CA${(p['total_cad'] or 0):,.2f}")
    await S.client().aclose()


asyncio.run(main())

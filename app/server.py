"""Digital Assets MCP connector: read-only wallet balances across chains.

Served over streamable HTTP at /<MCP_PATH_TOKEN>/mcp so the address itself is the secret.
"""
from __future__ import annotations

import logging
import os
import re
import sys

import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import BaseModel, Field
from starlette.requests import Request
from starlette.responses import PlainTextResponse

import history as H
import sources as S

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("digital-assets")

TOKEN = S.env("MCP_PATH_TOKEN")
if len(TOKEN) < 24:
    sys.exit("MCP_PATH_TOKEN must be set in .env and be at least 24 characters (use: openssl rand -hex 24)")

mcp = FastMCP(
    "Digital Assets",
    instructions=(
        "Read-only balances for the user's crypto wallets on Ethereum, Base, Arbitrum, Optimism, Polygon, "
        "Robinhood Chain, HyperEVM, Hyperliquid (spot, perps on every dex incl. HIP-3 markets such as trade.xyz stocks, staked HYPE, vaults), Solana and Bitcoin, "
        "valued in USD and CAD. Use get_portfolio for totals across wallets (no arguments uses the saved wallet "
        "list), get_wallet for one address, list_chains to see what an address type covers. Values are estimates "
        "from public explorers and price feeds. Aave deposits are valued at the underlying token and Aave debts "
        "are negative, so totals are net. Unpriced tokens (mostly spam) are left out unless include_unpriced is true. "
        "get_history returns past transactions labelled for tax review (swaps, bridges, moves between wallets, "
        "Hyperliquid fills with closed P&L and funding). get_fx_rates returns Bank of Canada daily exchange rates "
        "(USD/CAD by default)."
    ),
    host="0.0.0.0",
    port=int(os.getenv("PORT", "8080")),
    streamable_http_path=f"/{TOKEN}/mcp",
    stateless_http=True,
    json_response=True,
    # Reached only through the Cloudflare tunnel; the secret path is the access control.
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)


class WalletIn(BaseModel):
    address: str = Field(description="Wallet address: 0x… (EVM + Hyperliquid), Solana base58, or Bitcoin bc1/1/3")
    nickname: str | None = Field(default=None, description="Friendly name shown in results")
    chains: list[str] | None = Field(default=None, description="Optional subset of chain keys to check")


@mcp.tool()
async def list_chains() -> dict:
    """List supported chains and which address formats map to them."""
    return {
        "chains": [{"chain": k, "name": v["name"]} for k, v in S.ALL_CHAINS.items()],
        "address_types": {
            "evm (0x…)": S.EVM_ADDRESS_CHAINS,
            "solana (base58)": ["solana"],
            "bitcoin (bc1… / 1… / 3…)": ["bitcoin"],
        },
        "saved_wallets": len(S.default_wallets()),
    }


@mcp.tool()
async def get_wallet(
    address: str,
    nickname: str | None = None,
    chains: list[str] | None = None,
    min_usd: float = 0.0,
    include_unpriced: bool = False,
    refresh: bool = False,
) -> dict:
    """Balances for one wallet address across every chain its format supports.

    Args:
        address: 0x… EVM address, Solana address, or Bitcoin address.
        nickname: Optional label.
        chains: Optional subset of chain keys (see list_chains).
        min_usd: Hide priced tokens worth less than this (they are counted in `hidden`).
        include_unpriced: Include tokens with no market price (mostly airdrop spam; counted in `hidden` otherwise).
        refresh: Bypass the 60-second cache.
    """
    return await S.wallet(address, nickname, chains, min_usd, include_unpriced, refresh)


@mcp.tool()
async def get_portfolio(
    wallets: list[WalletIn] | None = None,
    min_usd: float = 0.0,
    include_unpriced: bool = False,
    refresh: bool = False,
) -> dict:
    """Combined balances for several wallets: totals in USD and CAD, split by chain, by token and by wallet.

    Args:
        wallets: List of {address, nickname}. Omit to use the wallets saved on the server.
        min_usd: Hide priced tokens worth less than this per wallet (counted in each wallet's `hidden`).
        include_unpriced: Include tokens with no market price (mostly airdrop spam; counted in `hidden` otherwise).
        refresh: Bypass the 60-second cache.
    """
    ws = [w.model_dump() for w in wallets] if wallets else S.default_wallets()
    if not ws:
        return {"error": "No wallets given and none saved on the server (WALLETS in .env)."}
    return await S.portfolio(ws, min_usd, include_unpriced, refresh)


@mcp.tool()
async def get_history(
    wallets: list[WalletIn] | None = None,
    own_addresses: list[str] | None = None,
    since: str = "2026-01-01",
    until: str | None = None,
    only: list[str] | None = None,
    status: str = "all",
    dust_usd: float = 1.0,
    refresh: bool = False,
    offset: int = 0,
    limit: int = 300,
    wait_seconds: float = 40,
    funding: str = "events",
    tz: str = "UTC",
) -> dict:
    """Transaction history of the wallets, one event per wallet per transaction, labelled for tax review.

    Covers EVM chains and Solana (Zerion, with the USD value of each transfer at the time), Hyperliquid
    (perps and spot fills with closed P&L and fees, funding, deposits, withdrawals, transfers), Bitcoin and
    Robinhood Chain. Each event has `class` (self, bridge, swap, wrap, send, receive, reward, defi, perp,
    funding, gas, approve, failed, dust, unpriced, other), `auto` (true = certain, no review needed) and
    `why`. Moves between the given wallets (and own_addresses) are `self`; matched cross-chain legs carry `pair`.

    If a source can't be reached (for example Zerion answering HTTP 429), its coverage row has `error` and
    `kept: true`, and the events stored from earlier syncs are still returned. Long ranges are fetched in
    yearly windows and merged with what is stored, so a re-fetch never removes events.

    The first call for a wallet set can take a few minutes: it returns status "building" with progress;
    call again with the same arguments until status is "ready". Later calls only fetch what is new.

    Args:
        wallets: List of {address, nickname}. Omit to use the wallets saved on the server.
        own_addresses: Extra addresses that are also yours (counted as moves between your wallets).
        since: First day, YYYY-MM-DD (UTC).
        until: Last day, YYYY-MM-DD (UTC); default today.
        only: Return events of these addresses only (classification still uses all wallets).
        status: "all", "review" (only events that need a decision) or "auto".
        dust_usd: Transfers worth less than this (USD) are labelled dust.
        refresh: Fetch new transactions now instead of using the last sync (at most every 10 minutes otherwise).
        offset: Paging start.
        limit: Events per page (max 1000).
        wait_seconds: How long to wait for a running build before returning "building" (max 50).
        funding: "events" (default: one event per hourly funding payment) or "daily": funding events are left
            out of `events` (and of `total` and paging) and returned summed per day, wallet and coin in
            `funding_daily` [{d, wallet, address, coin, sym, usdc, n}] on the first page (offset 0), with
            `funding_events` (how many were summed). `counts` still includes them.
        tz: Time zone for the days of `funding_daily`, e.g. "America/Toronto" (default UTC).
    """
    ws = [w.model_dump() for w in wallets] if wallets else S.default_wallets()
    if not ws:
        return {"error": "No wallets given and none saved on the server (WALLETS in .env)."}
    for d in (since, until):
        if d and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", d):
            return {"error": f"Dates must be YYYY-MM-DD, got {d!r}"}
    if funding not in ("events", "daily"):
        return {"error": f'funding must be "events" or "daily", got {funding!r}'}
    return await H.history(ws, own_addresses, since, until, only, status, dust_usd, refresh,
                           max(0, offset), max(1, min(limit, 1000)), wait_seconds, funding, tz)


@mcp.tool()
async def get_fx_rates(series: str = "FXUSDCAD", since: str = "2026-01-01", until: str | None = None) -> dict:
    """Bank of Canada daily exchange rates (the rates the CRA accepts), one per business day.

    Args:
        series: Valet series, default "FXUSDCAD" (CAD per 1 USD). Also FXEURCAD, FXGBPCAD and other FX<CUR>CAD.
        since: First day, YYYY-MM-DD.
        until: Last day, YYYY-MM-DD; default the latest published rate.
    Returns {series, source, count, first, last, rates: {"YYYY-MM-DD": rate}}. Cached for an hour.
    """
    for d in (since, until):
        if d and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", d):
            return {"error": f"Dates must be YYYY-MM-DD, got {d!r}"}
    try:
        return await H.boc_rates(series, since, until)
    except Exception as e:  # noqa: BLE001
        return {"error": "Bank of Canada didn't answer: " + S.short_err(e)}


@mcp.custom_route("/healthz", methods=["GET"])
async def healthz(_: Request):
    return PlainTextResponse("ok")


app = mcp.streamable_http_app()

if __name__ == "__main__":
    log.info("Digital Assets connector listening on :%s", mcp.settings.port)
    uvicorn.run(app, host="0.0.0.0", port=mcp.settings.port, log_level="warning", proxy_headers=True,
                forwarded_allow_ips="*")

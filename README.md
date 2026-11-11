# Digital Assets connector

Read-only MCP server for wallet balances on Ethereum, Base, Arbitrum, Optimism, Polygon, Robinhood Chain,
HyperEVM, Hyperliquid (spot, perps positions on the main and HIP-3 dexes such as trade.xyz stocks, staked HYPE, vaults), Solana and Bitcoin. Prices in USD, totals in USD and CAD.
Runs in Docker on the VPS and is reached at `https://assets.swatfly.xyz/<MCP_PATH_TOKEN>/mcp` through the Cloudflare tunnel.

## Files
- `docker-compose.yml`: the connector plus the cloudflared tunnel
- `app/server.py`: MCP server (tools: `get_portfolio`, `get_wallet`, `list_chains`, `get_history`, `get_fx_rates`)
- `app/history.py`: transaction history for tax review (Zerion for EVM chains and Solana, Hyperliquid fills/funding/ledger, Bitcoin, Robinhood Chain via Blockscout if Zerion lacks it), kept in the `history-data` Docker volume
- `app/historytest.py`: history check (per wallet and source: events found, errors; counts by class; first items to review)
- `app/sources.py`: chain and price fetchers
- `app/hyperevm_tokens.json`: HyperEVM tokens to check (add a line for any token you hold there)
- `app/defi_positions.json`: DeFi vault positions to value (ERC-4626 vaults such as Superform SuperVaults; add one entry per vault, with a `cg:<coingecko id>` price for its underlying asset)
- `app/core_tokens.json`: tokens whose balance is read straight from the chain and overrides the token API (USDC on Ethereum, Base, Arbitrum, Optimism, Polygon and CADC on Base; add a line for any token that must never be missed)
- `app/selftest.py`: per-chain health check
- `.env.example`: settings that go into `~/digital-assets/.env`

## Everyday commands (run in ~/digital-assets)
- Status: `docker compose ps`
- Logs: `docker compose logs --tail 50 digital-assets`
- Self-test: `docker compose run --rm digital-assets python selftest.py`
- History test: `docker compose run --rm digital-assets python historytest.py [since]` (first run takes a few minutes; later runs only fetch what is new)
- Reset the history cache: `docker compose down && docker volume rm digital-assets_history-data && docker compose up -d`
- After changing files: `docker compose up -d --build`
- After changing .env only: `docker compose up -d --force-recreate`

## Data sources
Balances: Alchemy Portfolio API (Ethereum, Base, Arbitrum, Optimism, Polygon; `ALCHEMY_API_KEY`), Blockscout
(Robinhood Chain; `BLOCKSCOUT_API_KEY`), Hyperliquid info API, HyperEVM RPC, Helius (Solana; `HELIUS_API_KEY`),
mempool.space (Bitcoin), on-chain reads for core tokens and vaults (Alchemy RPC, public RPC as fallback),
CoinGecko (native prices and vault underlyings). USD/CAD: Bank of Canada daily rate (Valet, no key), CoinGecko then
mempool.space as fallbacks; `fx_source` in `get_portfolio` says which was used.
History: Zerion (`ZERION_API_KEY`, free Developer plan), Hyperliquid info API, mempool.space, Blockscout.
Exchange rates (`get_fx_rates`): Bank of Canada Valet API, no key.

## History syncing (Nov 7, 2026)
- Zerion ranges longer than a year are fetched in yearly windows (`HISTORY_WINDOW_DAYS`, default 365) and merged into the store by event id; a re-fetch never removes stored events. Asking for an earlier `since` fetches only the missing earlier part.
- If a window comes back with fewer events than are stored for it, it is fetched once more and both results are kept (`rechecked` in coverage).
- If Zerion keeps answering HTTP 429, the connector pauses Zerion for 15 minutes (doubling up to 4 hours, or the Retry-After it sends) and serves the stored events meanwhile.
- Any source that fails returns its stored events with `error` and `kept: true` in its coverage row.

## Core tokens and Alchemy paging (Nov 9, 2026)
- A 54.74 USDC deposit to Primary on Base (Nov 9) was on-chain but missing from the balances: the connector stopped reading Alchemy's token list after 10 pages, and USDC on Base was past the cut-off (Primary has 675 tokens on Base).
- Balances of the tokens in `app/core_tokens.json` are now read on-chain (`balanceOf`, one batched RPC request per wallet and chain, through Alchemy's RPC or the public one) and replace what the token API reported: a missing token is added, a wrong amount corrected, a spent one removed. If the chain can't be read, the token API's figure stays.
- Alchemy's token list is read until its last page (it stopped after 10 pages before; limit `ALCHEMY_MAX_PAGES`, default 80). If the limit is ever reached the chain row says "token list cut off".

## Audit fixes (Nov 10, 2026)
- A history call with a later `since` than the last sync no longer leaves a gap: every sync resumes from where the last one ended.
- Hyperliquid ledger entries without a transaction hash get an id from their content (it used their position in the fetched list, so one entry could be stored several times). Entries stored under the old ids are rekeyed the first time the store is read.
- HyperEVM balances are read in one batched request per wallet (one call each if the RPC refuses a batch). A balance that can't be read (rate limit, timeout) is named in the chain row's `error`; only a contract that reverts is skipped quietly.
- A fetch that stops at a safety limit says so: the coverage row's `note` reads "History may be incomplete: ..." (kept in the store), and Solana's chain row says "asset list cut off". Hyperliquid's limit is 400 requests per kind and sync (`HISTORY_HL_MAX_CALLS`; it was 80).
- Only the 3 most recent history builds are kept in memory.
- Previous versions: `backup-2026-11-10-audit`.

## Owner decisions (Nov 11, 2026)
- USD/CAD for wallet totals (and CADC, worth exactly C$1) is the Bank of Canada's latest daily rate, the same one the dashboard uses for ledger USD balances. Cached 1 hour; if the Bank of Canada can't be reached, CoinGecko's rate is used and the Bank of Canada is tried again after 10 minutes.
- `get_history(funding="daily", tz="America/Toronto")` leaves Hyperliquid funding events out of `events` (and out of `total` and paging) and returns them summed per day, wallet and coin in `funding_daily` on the first page. The default (`funding="events"`) is unchanged. Funding is one event per open position per hour, more than half of all events.
- The two debugging routes (`/<token>/api/portfolio` and `/<token>/api/history`) are removed. `/healthz` stays (Docker's health check uses it).
- `tzdata` added to `requirements.txt` so the time zone for `funding_daily` always resolves.
- Previous versions: `backup-2026-11-11-owner`.

## Tests (on a PC, no network)
`python tests/test_core.py`, `python tests/test_history.py`, `python tests/test_extra.py`, `python tests/test_sync.py`, `python tests/test_audit.py` and `python tests/test_owner.py` (need `pip install mcp httpx uvicorn tzdata`).

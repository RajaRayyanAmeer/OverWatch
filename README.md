# OverWatch — n8n + SQLite build package

This package turns the OverWatch Build Plan v1.1 into a modular n8n implementation scaffold.

## Architecture

The production design is intentionally split into independent sub-workflows:

`OW-01 Scraper → OW-02 Scoring → OW-03 Writer → OW-04 Fact Checker → OW-05 Grouping → OW-06 Media/R2 → OW-07 Caption → OW-14 Queue → OW-08 Scheduler/Publisher`

Cross-cutting workflows:

`OW-09 Watchtower`, `OW-10 Telegram Bot`, `OW-11 Error Handler`, `OW-12 Insights`, `OW-13 Human Review`.

`OW-DB Init` is a one-time utility. `OW-00 Master Orchestrator` is a smoke-test/manual chaining template, not the preferred production scheduler.

## Why SQLite

Use one SQLite file at `/data/overwatch.db`. It is the shared state backbone for every stage, gives idempotent status transitions, and keeps the audit trail local. WAL mode and transactional writes are enabled in the schema.

The implementation puts low-level SQLite access in `agent_worker.py` and invokes it from n8n's Execute Command node. This is deliberate: it keeps parameterized SQL and filesystem/media work in a deterministic, versioned helper instead of depending on a particular n8n Code-node runtime having every Python/SQLite package available.

## Other storage you actually need

SQLite is enough for metadata/state. It is not the public media host. The generated PNGs live in `/data/media` and should be uploaded to a Cloudflare R2 bucket so Instagram receives stable public HTTPS image URLs. The database stores those public URLs.

The package also creates `/data/audit` and `/data/backups`.

## Safety defaults

The database seeds:

- `review_mode=on`
- `auto_publish=false`
- `publish_enabled=false`
- `ingest_enabled=true`
- `llm_enabled=true`
- NewsBlue active theme
- Asia/Karachi audience timezone
- 72-hour freshness expiry
- 3 posts/day
- 3-hour minimum spacing

Do not enable `publish_enabled` until the dry run, human review and API test are complete.

## Installation

1. Copy `.env.example` to `.env` and fill in your secrets. Pin `N8N_VERSION` to a version you have tested; do not leave it as `latest`.
2. Create the local persistent directories with `mkdir -p data/{media,audit,backups} n8n_data`.
3. Build/start with `docker compose up -d --build`.
4. Open n8n and import `n8n/OW-DB_Init.json`. Run it once.
5. Import all OW-01 through OW-14 workflow JSONs. Keep them inactive initially.
6. Import `OW-00_Master_Orchestrator_Smoke_Test.json` last. Replace its `REPLACE_WITH_WORKFLOW_ID` targets by selecting the imported sub-workflows.
7. Configure your Telegram credential in OW-10/OW-11 and your Gemini/Groq/Meta/R2 environment variables.
8. Run the dry-run sequence with `publish_enabled=false` and `review_mode=on`.

## Credentials / secrets

Do not put API keys in workflow JSON or the SQLite database. The helper reads them from the container environment:

`GEMINI_API_KEY`, `GROQ_API_KEY`, `OLLAMA_URL`, `META_ACCESS_TOKEN`, `META_IG_USER_ID`, `R2_*`, `TELEGRAM_*`.

For OW-11, select the imported Telegram credential and replace the chat ID placeholder in the node after import.

## Connecting the sub-workflows

For production, each workflow's schedule should remain active and the Execute Workflow Trigger can be used for manual or parent-workflow invocation.

The hand-off is DB state based, not JSON payload based. The usual pattern is:

1. OW-01 writes `headlines.status=pending`.
2. OW-02 scores and creates `scored` + `pool` records.
3. OW-03 creates/updates `articles`.
4. OW-04 sets `articles.status` to `verified`, `pending_review` for rewrite, or `quarantined`.
5. OW-05 creates the group/audit bundle.
6. OW-06 renders cards and uploads them to R2.
7. OW-07 writes caption variants and opens the human-review queue when review mode is on.
8. OW-14 assigns future publish slots.
9. OW-08 publishes only due posts and only after review/publish guards pass.

## Notes on quota pacing

OW-03 is limited to about 15 new/rewrite articles per run. The source design deliberately spreads long-form generation over multiple days instead of forcing the entire 30–40 candidate pool through one burst.

## Semantic dedup

The worker supports an optional local `sentence-transformers` embedding model. If that package/model is not present, it uses a deterministic lexical-vector fallback so the pipeline remains operational. For the full source design, install the embedding model on the VM and set `EMBEDDING_MODEL=all-MiniLM-L6-v2`.

## R2

Set:

- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET`
- `R2_PUBLIC_BASE_URL`

Use a scoped token. The public base URL should point to the R2 public/custom domain that serves the uploaded PNGs over HTTPS.

## Telegram

OW-10 exposes `/webhook/overwatch/telegram` through n8n. Connect Telegram's webhook to that endpoint through the tunnel URL. The allow-listed commands shipped here are:

`/status`, `/quota`, `/digest`, `/pause`, `/resume`

`/run-cycle` is intentionally left as a wiring point because it needs the imported n8n workflow IDs. Add it after the master workflow has been configured.

OW-13 expects:

`/review approve G-...`

or

`/review reject G-...`

## Meta / Instagram

The package does not embed a fixed Meta API version because Meta changes versions independently of your deployment. Set `META_API_VERSION` after you create/test your app, and verify the current publishing permissions, token lifecycle and allowed Insights metrics in Meta's documentation before enabling production publishing.

## Tests

`tests/smoke_test.py` checks DB initialization and queue slot assignment without requiring any external API.

The build plan's production acceptance gates are stronger: 40-item scoring golden set, 20-item fact-checker golden set, 3,000-word/8-section writer gate, 10-card contract test, manual phone test of 3 decks, and two complete dry-run cycles before go-live.

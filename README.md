# Counterparty Extraction API v3.0.0

## Run locally
```
pip install -r requirements.txt
python app.py
# Open http://localhost:5000
```

## Deploy to Render (free tier)
1. Push this folder to a GitHub repo
2. Go to render.com → New Web Service → connect repo
3. Build command: `pip install -r requirements.txt`
4. Start command: `gunicorn app:app --bind 0.0.0.0:$PORT`
5. Done — Render gives you a public URL

## Update list versions
Replace the files in the `data/` folder with latest versions:
- `data/Counterparty_Search_Exclusion_List_v9.xlsx (or newer)`
- `data/counterparty_whitelist_v8.xlsx (or newer)`

Update the env vars EXCLUSION_LIST_PATH and WHITELIST_PATH to match.

## API endpoints
- GET  /health   — health check + list sizes
- GET  /stats    — pattern count + list versions loaded
- POST /extract  — single: {"description": "...", "transaction_id": "optional"}
                   batch:  {"transactions": [{"description":"...", "transaction_id":"..."}]}

## Response fields
- counterparty         — extracted counterparty name
- pattern_code         — internal pattern identifier
- pattern_name         — plain English pattern name
- pattern_description  — what was extracted and why
- entity_type          — BUSINESS / INDIVIDUAL / UNKNOWN
- disposition          — WHITELISTED / EXCLUDED / REQUIRES_EXTERNAL_RESEARCH
- canonical_name       — canonical name from whitelist (if whitelisted)
- sector               — industry sector from whitelist (if whitelisted)
- transaction_id       — echoed back from input
- raw                  — original input string

## List versions loaded
v9 exclusion list (228 terms + 6 regex patterns), v8 whitelist (18,735 aliases)

#!/bin/bash
# Smoke-test the local oMLX model server. No network required.
#   ./scripts/ask-local.sh "write a one paragraph summary of this project"
set -euo pipefail
HOST="${OMLX_HOST:-http://127.0.0.1:8000/v1}"
MODEL="${OMLX_MODEL:-Qwen3.8-27B-oQ4e-mtp}"
# oMLX requires its own API key; it lives in the app's settings rather than an env var.
KEY="${OMLX_API_KEY:-$(python3 -c "import json,os;print(json.load(open(os.path.expanduser('~/.omlx/settings.json')))['auth']['api_key'])")}"
PROMPT="${1:-write a one paragraph summary of this project}"

jq -n --arg m "$MODEL" --arg p "$PROMPT" \
   '{model:$m, temperature:0.3, max_tokens:500, messages:[{role:"user", content:$p}]}' \
| curl -s "$HOST/chat/completions" -H "Authorization: Bearer $KEY" \
       -H 'Content-Type: application/json' --max-time 600 -d @- \
| jq -r '.choices[0].message.content'

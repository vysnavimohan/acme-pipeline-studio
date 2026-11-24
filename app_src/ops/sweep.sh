#!/bin/bash
# De-brand sweep: fetch every HTML page and the main JSON APIs of a RUNNING app
# and count forbidden words in the responses. Exit 1 if any hit.
#
# Usage: ops/sweep.sh <base-url> [outdir]
#   ops/sweep.sh http://127.0.0.1:8773
#   ops/sweep.sh https://<app-url>
#
# Patterns are case-insensitive unless noted. Uppercase BILLING / NETWORK / PCT
# are matched case-sensitively (the lowercase forms are real Databricks system
# tables such as system.billing.usage, and "network" is an ordinary word).
set -u

BASE="${1:-}"
if [ -z "$BASE" ]; then
  echo "usage: $0 <base-url> [outdir]" >&2
  exit 2
fi
BASE="${BASE%/}"
OUT="${2:-/tmp/sweep_$(date +%H%M%S)}"
mkdir -p "$OUT"

PAGES="/ /request/new /tracker /governance /releases /agent-config /connectors /about"
APIS="/api/requests /api/requests/meta/team /api/agent-config /api/connectors/builtin-sources /api/connectors/connect-sources /api/connectors/connections /api/connectors/sources /api/connectors/ready-datasets /api/datasets /api/governance/summary /api/observability/health /api/observability/traces /api/releases /api/releases/candidates /api/sources"

# Case-insensitive terms.
CI_TERMS='blackwoods|cfs|colonial|meridian|monday fund|monday-fund|superannuation|super-fund|telco|msisdn|perpetual|securitis|pool tape|fund pulse|fo_sample|campaign prepaid|funding optimisation|jeremy'
# Case-sensitive terms.
CS_TERMS='BILLING|NETWORK|PCT'

total=0
fail_list=""

printf '%-34s %-6s %-6s %s\n' "PATH" "HTTP" "HITS" "BY TERM"
printf '%-34s %-6s %-6s %s\n' "----" "----" "----" "-------"

check() {
  local path="$1"
  local name
  name=$(echo "$path" | tr '/' '_' | sed 's/^_//; s/^$/home/')
  local file="$OUT/${name}.body"
  local code
  code=$(curl -s -m 120 -L ${SWEEP_TOKEN:+-H "Authorization: Bearer $SWEEP_TOKEN"} -o "$file" -w '%{http_code}' "${BASE}${path}")
  local hits=0
  local detail=""
  if [ -s "$file" ]; then
    local ci cs
    ci=$(grep -oiE "$CI_TERMS" "$file" | tr 'A-Z' 'a-z' | sort | uniq -c | awk '{c=$1; $1=""; sub(/^ /,""); printf "%s=%s ", $0, c}')
    cs=$(grep -oE "$CS_TERMS" "$file" | sort | uniq -c | awk '{c=$1; $1=""; sub(/^ /,""); printf "%s=%s ", $0, c}')
    hits=$(grep -oiE "$CI_TERMS" "$file" | wc -l | tr -d ' ')
    hits=$((hits + $(grep -oE "$CS_TERMS" "$file" | wc -l | tr -d ' ')))
    detail="$ci$cs"
    # Keep the matching lines for the report.
    grep -niE "$CI_TERMS" "$file" | cut -c1-200 > "$OUT/${name}.hits" 2>/dev/null
    grep -nE "$CS_TERMS" "$file" | cut -c1-200 >> "$OUT/${name}.hits" 2>/dev/null
  fi
  printf '%-34s %-6s %-6s %s\n' "$path" "$code" "$hits" "$detail"
  total=$((total + hits))
  if [ "$code" != "200" ]; then
    fail_list="$fail_list $path($code)"
  fi
}

for p in $PAGES; do check "$p"; done
for p in $APIS; do check "$p"; done

echo
echo "Output dir: $OUT   (per-path *.hits files list each matching line)"
echo "TOTAL forbidden-word hits: $total"
if [ -n "$fail_list" ]; then
  echo "Non-200 responses:$fail_list"
fi
if [ "$total" -gt 0 ]; then
  echo "SWEEP FAILED"
  exit 1
fi
echo "SWEEP CLEAN"
exit 0

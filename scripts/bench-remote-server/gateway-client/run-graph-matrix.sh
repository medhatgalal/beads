#!/usr/bin/env bash
set -euo pipefail

if [[ "${BEADS_PERF_LAB:-}" != "1" ]]; then
  echo "refusing to run: BEADS_PERF_LAB must equal 1" >&2
  exit 2
fi

client="${GATEWAY_CLIENT:-/var/lib/beads-perf-lab/binaries/beads-perf-gateway-client}"
token_file="${GATEWAY_TOKEN_FILE:-/var/lib/beads-perf-lab/private/gateway-token}"
project_id="${GATEWAY_PROJECT_ID:-756f20ac-7a3d-4ba6-bf3e-2022a3bf74ba}"
payload="${GATEWAY_GRAPH_PAYLOAD:-/var/lib/beads-perf-lab/gateway-a/graph-100-200.json}"
result_dir="${GATEWAY_RESULT_DIR:-/var/lib/beads-perf-lab/gateway-a/results/graph-matrix}"
read -r -a rtts <<<"${RTTS:-20 50 100 150 200}"
samples="${SAMPLES:-5}"
warmups="${WARMUPS:-0}"
key_prefix="${KEY_PREFIX:-matrix}"

[[ -x "$client" ]]
[[ -f "$token_file" ]]
[[ -f "$payload" ]]
[[ "$result_dir" == /var/lib/beads-perf-lab/gateway-a/results/* ]]
[[ "$samples" =~ ^[1-9][0-9]*$ ]]
[[ "$warmups" =~ ^[0-9]+$ ]]
[[ "$key_prefix" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$ ]]
[[ "${#rtts[@]}" -gt 0 ]]
for rtt in "${rtts[@]}"; do
  [[ "$rtt" =~ ^[0-9]+$ ]]
done
install -d -m 0700 "$result_dir"

for rtt in "${rtts[@]}"; do
  output="${result_dir}/graph-100-200-rtt${rtt}.json"
  "$client" \
    -project-id "$project_id" \
    -token-file "$token_file" \
    -method POST \
    -resource 'operations?wait_ms=30000' \
    -body-file "$payload" \
    -idempotency-key "${key_prefix}-graph-100-200-rtt${rtt}-{{ordinal}}" \
    -warmups "$warmups" \
    -samples "$samples" \
    -simulated-rtt-ms "$rtt" \
    -timeout 11m >"$output"
  jq -c '{resource:"graph-100-200",simulated_rtt_ms,samples,passed,p50_ms,p95_ms,p99_ms}' "$output"
done

sha256sum "$result_dir"/*.json >"${result_dir}/SHA256SUMS"

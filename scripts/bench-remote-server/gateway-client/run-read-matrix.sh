#!/usr/bin/env bash
set -euo pipefail

if [[ "${BEADS_PERF_LAB:-}" != "1" ]]; then
  echo "refusing to run: BEADS_PERF_LAB must equal 1" >&2
  exit 2
fi

client="${GATEWAY_CLIENT:-/var/lib/beads-perf-lab/binaries/beads-perf-gateway-client}"
token_file="${GATEWAY_TOKEN_FILE:-/var/lib/beads-perf-lab/private/gateway-token}"
project_id="${GATEWAY_PROJECT_ID:-756f20ac-7a3d-4ba6-bf3e-2022a3bf74ba}"
show_id="${GATEWAY_SHOW_ID:-gw-yw9}"
result_dir="${GATEWAY_RESULT_DIR:-/var/lib/beads-perf-lab/gateway-a/results/read-matrix}"
read -r -a rtts <<<"${RTTS:-20 50 100 150 200}"
samples="${SAMPLES:-20}"
warmups="${WARMUPS:-2}"

[[ -x "$client" ]]
[[ -f "$token_file" ]]
[[ "$result_dir" == /var/lib/beads-perf-lab/gateway-a/results/* ]]
[[ "$samples" =~ ^[1-9][0-9]*$ ]]
[[ "$warmups" =~ ^[0-9]+$ ]]
[[ "${#rtts[@]}" -gt 0 ]]
for rtt in "${rtts[@]}"; do
  [[ "$rtt" =~ ^[0-9]+$ ]]
done
install -d -m 0700 "$result_dir"

cases=(
  "ping|ping"
  "list|issues?limit=50"
  "ready|ready?limit=50"
  "show|issues/${show_id}"
)

for mode in warm cold; do
  for rtt in "${rtts[@]}"; do
    for entry in "${cases[@]}"; do
      IFS='|' read -r name resource <<<"$entry"
      output="${result_dir}/${mode}-${name}-rtt${rtt}.json"
      "$client" \
        -project-id "$project_id" \
        -token-file "$token_file" \
        -resource "$resource" \
        -warmups "$warmups" \
        -samples "$samples" \
        -connection-mode "$mode" \
        -simulated-rtt-ms "$rtt" \
        -timeout 2m >"$output"
      jq -c '{connection_mode,resource,simulated_rtt_ms,samples,passed,p50_ms,p95_ms,p99_ms}' "$output"
    done
  done
done

sha256sum "$result_dir"/*.json >"${result_dir}/SHA256SUMS"

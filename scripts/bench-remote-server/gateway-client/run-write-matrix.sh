#!/usr/bin/env bash
set -euo pipefail

if [[ "${BEADS_PERF_LAB:-}" != "1" ]]; then
  echo "refusing to run: BEADS_PERF_LAB must equal 1" >&2
  exit 2
fi

client="${GATEWAY_CLIENT:-/var/lib/beads-perf-lab/binaries/beads-perf-gateway-client}"
token_file="${GATEWAY_TOKEN_FILE:-/var/lib/beads-perf-lab/private/gateway-token}"
project_id="${GATEWAY_PROJECT_ID:-756f20ac-7a3d-4ba6-bf3e-2022a3bf74ba}"
payload_dir="${GATEWAY_PAYLOAD_DIR:-/var/lib/beads-perf-lab/gateway-a}"
result_dir="${GATEWAY_RESULT_DIR:-/var/lib/beads-perf-lab/gateway-a/results/write-matrix}"
read -r -a rtts <<<"${RTTS:-20 50 100 150 200}"
samples="${SAMPLES:-20}"
warmups="${WARMUPS:-0}"
key_prefix="${KEY_PREFIX:-matrix}"

[[ -x "$client" ]]
[[ -f "$token_file" ]]
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
  create_output="${result_dir}/create-rtt${rtt}.json"
  "$client" \
    -project-id "$project_id" \
    -token-file "$token_file" \
    -method POST \
    -resource 'operations?wait_ms=30000' \
    -body-file "${payload_dir}/create.json" \
    -idempotency-key "${key_prefix}-create-rtt${rtt}-{{ordinal}}" \
    -warmups "$warmups" \
    -samples "$samples" \
    -simulated-rtt-ms "$rtt" \
    -include-response \
    -timeout 2m >"$create_output"
  jq -c '{resource:"create",simulated_rtt_ms,samples,passed,p50_ms,p95_ms,p99_ms}' "$create_output"

  update_output="${result_dir}/update-rtt${rtt}.json"
  "$client" \
    -project-id "$project_id" \
    -token-file "$token_file" \
    -method POST \
    -resource 'operations?wait_ms=30000' \
    -body-file "${payload_dir}/update-matrix.json" \
    -idempotency-key "${key_prefix}-update-rtt${rtt}-{{ordinal}}" \
    -warmups "$warmups" \
    -samples "$samples" \
    -simulated-rtt-ms "$rtt" \
    -timeout 2m >"$update_output"
  jq -c '{resource:"update",simulated_rtt_ms,samples,passed,p50_ms,p95_ms,p99_ms}' "$update_output"

  mapfile -t issue_ids < <(jq -r '.records[] | select(.phase=="sample") | .response.result.id' "$create_output")
  if [[ "${#issue_ids[@]}" -ne "$samples" ]]; then
    echo "create response did not contain ${samples} issue IDs" >&2
    exit 1
  fi
  close_dir="${result_dir}/close-rtt${rtt}"
  install -d -m 0700 "$close_dir"
  for i in "${!issue_ids[@]}"; do
    body="${close_dir}/body-${i}.json"
    output="${close_dir}/sample-${i}.json"
    jq -n --arg id "${issue_ids[$i]}" \
      '{kind:"issue.close",payload:{id:$id,reason:"synthetic write-matrix close"}}' >"$body"
    "$client" \
      -project-id "$project_id" \
      -token-file "$token_file" \
      -method POST \
      -resource 'operations?wait_ms=30000' \
      -body-file "$body" \
      -idempotency-key "${key_prefix}-close-rtt${rtt}-${i}" \
      -warmups 0 \
      -samples 1 \
      -simulated-rtt-ms "$rtt" \
      -timeout 2m >"$output"
  done
  jq -s --argjson rtt "$rtt" --argjson expected "$samples" '
    [.[].records[] | select(.phase=="sample") | .wall_ms] | sort as $walls |
    ($walls | length) as $count |
    {resource:"close",simulated_rtt_ms:$rtt,samples:$count,passed:($count==$expected),
     p50_ms:$walls[((($count * 0.50) | ceil) - 1)],
     p95_ms:$walls[((($count * 0.95) | ceil) - 1)],
     p99_ms:$walls[((($count * 0.99) | ceil) - 1)]}
  ' "$close_dir"/sample-*.json | tee "${result_dir}/close-rtt${rtt}.summary.json"
done

find "$result_dir" -type f -name '*.json' -print0 | sort -z | xargs -0 sha256sum >"${result_dir}/SHA256SUMS"

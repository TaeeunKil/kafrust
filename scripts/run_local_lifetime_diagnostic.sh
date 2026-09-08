#!/usr/bin/env bash
set -euo pipefail

# Manually invoked, non-qualifying WSL/Docker lifetime diagnostic. Every
# resource is run-scoped; this script never prunes unrelated Docker state.

die() {
  echo "local lifetime diagnostic: $*" >&2
  exit 1
}

get_or_default() {
  local name="$1"
  local fallback="$2"
  local value
  value="$(printenv "$name" 2>/dev/null || true)"
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
  else
    printf '%s' "$fallback"
  fi
}

for command_name in cargo docker df python3 setsid timeout; do
  command -v "$command_name" >/dev/null 2>&1 || die "missing command: $command_name"
done
[[ "$(uname -s)" == "Linux" ]] || die "run this launcher inside Linux/WSL"
[[ "$(uname -m)" == "x86_64" ]] || die "the launcher requires Linux x86_64"
docker info >/dev/null 2>&1 || die "Docker is not available"

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
source "$repo_root/scripts/local_lifetime_launcher_guard.sh"
duration_seconds="$(get_or_default KAFRUST_LOCAL_DURATION_SECONDS 21600)"
rate_records_per_second="$(get_or_default KAFRUST_LOCAL_RATE_RECORDS_PER_SECOND 100)"
payload_bytes="$(get_or_default KAFRUST_LOCAL_PAYLOAD_BYTES 64)"
disk_watermark_gib="$(get_or_default KAFRUST_LOCAL_DISK_WATERMARK_GIB 100)"
disk_budget_gib="$(get_or_default KAFRUST_LOCAL_DISK_BUDGET_GIB 20)"
kafka_version="$(get_or_default KAFRUST_LOCAL_KAFKA_VERSION 4.3.1)"
run_id="$(get_or_default KAFRUST_LOCAL_RUN_ID "$(date -u +%Y%m%dT%H%M%SZ)-$$")"

for value in "$duration_seconds" "$rate_records_per_second" "$payload_bytes" "$disk_watermark_gib" "$disk_budget_gib"; do
  [[ "$value" =~ ^[0-9]+$ ]] || die "numeric inputs must be non-negative integers"
done
[[ "$run_id" =~ ^[A-Za-z0-9._-]+$ ]] || die "run ID contains unsupported characters"
((duration_seconds >= 60 && duration_seconds <= 86400)) ||
  die "duration must be between 60 seconds and 24 hours"
((rate_records_per_second >= 1 && rate_records_per_second <= 100)) ||
  die "rate must be between 1 and 100 records/s"
((payload_bytes >= 1 && payload_bytes <= 256)) ||
  die "payload must be between 1 and 256 bytes"
((disk_watermark_gib >= 100 && disk_watermark_gib <= 200)) ||
  die "disk watermark must be between 100 and 200 GiB"
((disk_budget_gib >= 1 && disk_budget_gib <= 20)) ||
  die "disk growth budget must be between 1 and 20 GiB"

resource_prefix="kafrust-local-lifetime-$run_id-"
network_name="kafrust-local-lifetime-$run_id"
topic_name="kafrust-local-lifetime-$run_id"
broker_image="apache/kafka:$kafka_version"
(( ${#resource_prefix} + 1 <= 63 )) ||
  die "run ID is too long for Docker hostnames; use a shorter KAFRUST_LOCAL_RUN_ID"
output_dir="$(get_or_default KAFRUST_LOCAL_OUTPUT_DIR "$repo_root/target/local-lifetime/$run_id")"
project_dir="$output_dir/external-project"
result_file="$output_dir/local-lifetime.json"
descriptor_file="$output_dir/local-lifetime-descriptor.json"
resource_samples_file="$output_dir/resource-samples.jsonl"
capacity_before_file="$output_dir/capacity-before.txt"
capacity_after_file="$output_dir/capacity-after.txt"
[[ ! -e "$output_dir" ]] || die "output directory already exists: $output_dir"
mkdir -p "$output_dir"
cargo_target_dir="$(get_or_default KAFRUST_LOCAL_CARGO_TARGET_DIR "$project_dir/target")"
if [[ "$cargo_target_dir" != /* ]]; then
  cargo_target_dir="$repo_root/$cargo_target_dir"
fi
mkdir -p "$cargo_target_dir"

capacity_path=/
if df -Pk /mnt/t >/dev/null 2>&1; then
  capacity_path=/mnt/t
fi
docker_root="$(docker info --format '{{.DockerRootDir}}')"
disk_budget_state="$output_dir/disk-budget.json"
python3 "$repo_root/scripts/local_lifetime_disk_budget.py" start "$disk_budget_state" \
  --paths "$capacity_path" "$docker_root" "$output_dir" "$cargo_target_dir" \
  --seconds "$duration_seconds" --rate "$rate_records_per_second" \
  --payload "$payload_bytes" --reserve-gib "$disk_watermark_gib" \
  --budget-gib "$disk_budget_gib"

free_gib() {
  local path="$1"
  local available_kib
  available_kib="$(df -Pk "$path" | awk 'NR == 2 {print $4}')"
  [[ "$available_kib" =~ ^[0-9]+$ ]] || die "unable to read free space for $path"
  echo $((available_kib / 1024 / 1024))
}

record_capacity() {
  local destination="$1"
  {
    echo "capacity_path=$capacity_path free_gib=$(free_gib "$capacity_path")"
    echo "docker_root=$docker_root free_gib=$(free_gib "$docker_root")"
    docker system df
  } | tee "$destination"
}

host_free="$(free_gib "$capacity_path")"
docker_free="$(free_gib "$docker_root")"
((host_free >= disk_watermark_gib)) || die "host free space is below the watermark"
((docker_free >= disk_watermark_gib)) || die "Docker free space is below the watermark"
if docker network inspect "$network_name" >/dev/null 2>&1; then
  die "network already exists: $network_name"
fi
if docker ps -a --format '{{.Names}}' |
  awk -v prefix="$resource_prefix" 'index($0, prefix) == 1 {found=1} END {exit found}'; then
  :
else
  die "a container with the run prefix already exists: $resource_prefix"
fi

build_pid=""
soak_pid=""
sampler_pid=""
fault_pid=""

cleanup() {
  set +e
  cleanup_run_resources "$resource_prefix" "$network_name" \
    "$build_pid" "$soak_pid" "$sampler_pid" "$fault_pid"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

record_capacity "$capacity_before_file"
docker network create "$network_name" >/dev/null
cluster_id="$(docker run --rm "$broker_image" /opt/kafka/bin/kafka-storage.sh random-uuid)"
voter_one="$resource_prefix"1
voter_two="$resource_prefix"2
voter_three="$resource_prefix"3
for node in 1 2 3; do
  external_port=$((19091 + node))
  container_name="$resource_prefix$node"
  docker run -d --name "$container_name" \
    --hostname "$container_name" \
    --network "$network_name" \
    --cpus=1.0 \
    --memory=2g \
    --pids-limit=512 \
    --log-driver json-file \
    --log-opt max-size=50m \
    --log-opt max-file=3 \
    -p "$external_port"':9092' \
    -e KAFKA_CLUSTER_ID="$cluster_id" \
    -e KAFKA_NODE_ID="$node" \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://$container_name:29092,EXTERNAL://localhost:$external_port \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@$voter_one:9093,2@$voter_two:9093,3@$voter_three:9093 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    "$broker_image" >/dev/null
done

for attempt in {1..120}; do
  if docker exec "$voter_one" /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "$voter_one:29092" >/dev/null 2>&1; then
    break
  fi
  ((attempt == 120)) && die "Kafka did not become ready"
  sleep 2
done

docker exec "$voter_one" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$voter_one:29092" \
  --create --topic "$topic_name" --partitions 3 --replication-factor 3 >/dev/null

python3 "$repo_root/scripts/wait_for_kafka_topic.py" \
  --topic "$topic_name" --partitions 3 --replicas 3 \
  --containers "$voter_one" "$voter_two" "$voter_three"

mkdir -p "$project_dir/src"
cp "$repo_root/.github/published-multi-soak-smoke/src/main.rs" "$project_dir/src/main.rs"
cat > "$project_dir/Cargo.toml" <<EOF
[package]
name = "kafrust-local-lifetime-diagnostic"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
kafrust = { path = "$repo_root/crates/kafrust" }
sha2 = "0.10"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
EOF

export KAFRUST_BOOTSTRAP_SERVERS="localhost:19092,localhost:19093,localhost:19094"
export KAFRUST_TOPIC="$topic_name"
export KAFRUST_SOAK_SECONDS="$duration_seconds"
export KAFRUST_SOAK_BATCH_SIZE=50
export KAFRUST_SOAK_PAYLOAD_BYTES="$payload_bytes"
export KAFRUST_SOAK_RECORDS_PER_SECOND="$rate_records_per_second"
export CARGO_TARGET_DIR="$cargo_target_dir"

# Compile before starting the broker fault timer. A cold build can otherwise
# consume the entire short smoke window before the workload has started.
disk_budget_check() {
  python3 "$repo_root/scripts/local_lifetime_disk_budget.py" check "$disk_budget_state"
}

setsid timeout --kill-after=10s 900s cargo build --quiet --release \
  --manifest-path "$project_dir/Cargo.toml" &
build_pid=$!
build_aborted=0
while kill -0 "$build_pid" 2>/dev/null; do
  if ! disk_budget_check; then
    echo "disk budget or reserve reached during helper build" >&2
    build_aborted=1
    break
  fi
  sleep 10
done
((build_aborted == 0)) || die "diagnostic helper build aborted by disk guard"
build_status=0
wait "$build_pid" || build_status=$?
build_pid=""
((build_status == 0)) || die "diagnostic helper build failed with status $build_status"
disk_budget_check ||
  die "disk budget or reserve reached during helper build"

# A separate process group lets disk aborts terminate cargo AND its helper.
# Include a bounded drain allowance; a hung helper cannot run forever.
setsid timeout --kill-after=10s "$((duration_seconds + 900))s" \
  "$cargo_target_dir/release/kafrust-local-lifetime-diagnostic" >"$result_file" &
soak_pid=$!
setsid timeout --kill-after=10s "$((duration_seconds + 900))s" \
  python3 "$repo_root/scripts/local_lifetime_resource_sampler.py" \
  --pid "$soak_pid" --output "$resource_samples_file" --interval-seconds 10 \
  --docker-container "$voter_one" --docker-container "$voter_two" \
  --docker-container "$voter_three" --disk-path "$capacity_path" \
  --disk-path "$docker_root" &
sampler_pid=$!
setsid timeout --kill-after=10s "$((duration_seconds + 900))s" bash -c '
  set -euo pipefail
  sleep "$1"
  docker stop -t 1 "$2" >/dev/null
  sleep 10
  docker start "$2" >/dev/null
' bash "$((duration_seconds / 2))" "$voter_one" &
fault_pid=$!

aborted=0
while kill -0 "$soak_pid" 2>/dev/null; do
  host_free="$(free_gib "$capacity_path")"
  docker_free="$(free_gib "$docker_root")"
  printf 'watermark check: host=%sGiB docker=%sGiB threshold=%sGiB\n' \
    "$host_free" "$docker_free" "$disk_watermark_gib"
  if ! disk_budget_check; then
    echo "disk budget or reserve reached; stopping diagnostic" >&2
    aborted=1
    break
  fi
  sleep 10
done

((aborted == 0)) || die "diagnostic aborted by disk guard"
soak_status=0
wait "$soak_pid" || soak_status=$?
((soak_status == 0)) || die "diagnostic helper failed with status $soak_status"
sampler_status=0
wait "$sampler_pid" || sampler_status=$?
sampler_pid=""
((sampler_status == 0)) || die "resource sampler failed with status $sampler_status"
[[ -s "$resource_samples_file" ]] || die "resource sampler did not produce samples"
fault_status=0
wait "$fault_pid" || fault_status=$?
((fault_status == 0)) || die "broker-restart helper failed with status $fault_status"

python3 - "$result_file" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    result = json.load(handle)
for key, expected in {
    "recovered": True,
    "unknown_outcomes": 0,
    "in_flight_requests": 0,
    "buffered_records": 0,
}.items():
    if result.get(key) != expected:
        raise SystemExit(f"result field {key!r} did not equal {expected!r}")
count_fields = ("attempted_records", "acknowledged_records", "consumed_unique_records")
counts = {key: result.get(key) for key in count_fields}
if any(not isinstance(value, int) or isinstance(value, bool) or value < 0
       for value in counts.values()):
    raise SystemExit("result record counts were missing or invalid")
if counts["attempted_records"] != counts["acknowledged_records"]:
    raise SystemExit("attempted and acknowledged record counts did not reconcile")
if counts["acknowledged_records"] != counts["consumed_unique_records"]:
    raise SystemExit("acknowledged and consumed-unique record counts did not reconcile")
if result.get("records") != counts["acknowledged_records"]:
    raise SystemExit("reported records and acknowledged record counts did not reconcile")
reconciliation = result.get("record_id_reconciliation", {})
if reconciliation.get("loss_count") != 0 or reconciliation.get("duplicate_count") != 0:
    raise SystemExit("record-ID reconciliation reported loss or duplicates")
if not result.get("record_id_reconciliation", {}).get("qualified"):
    raise SystemExit("record-ID reconciliation was not qualified")
PY

record_capacity "$capacity_after_file"
source_commit="$(git -C "$repo_root" rev-parse HEAD)"
source_dirty=false
[[ -z "$(git -C "$repo_root" status --porcelain)" ]] || source_dirty=true
export KAFRUST_LOCAL_DESCRIPTOR="$descriptor_file"
export KAFRUST_LOCAL_RESULT_FILE="$result_file"
export KAFRUST_LOCAL_SOURCE_COMMIT="$source_commit"
export KAFRUST_LOCAL_SOURCE_DIRTY="$source_dirty"
export KAFRUST_LOCAL_KAFKA_VERSION="$kafka_version"
export KAFRUST_LOCAL_RUN_ID="$run_id"
export KAFRUST_LOCAL_DURATION_SECONDS="$duration_seconds"
export KAFRUST_LOCAL_RATE_RECORDS_PER_SECOND="$rate_records_per_second"
export KAFRUST_LOCAL_PAYLOAD_BYTES="$payload_bytes"
export KAFRUST_LOCAL_DISK_WATERMARK_GIB="$disk_watermark_gib"
export KAFRUST_LOCAL_DISK_BUDGET_GIB="$disk_budget_gib"
export KAFRUST_LOCAL_CAPACITY_BEFORE="$capacity_before_file"
export KAFRUST_LOCAL_CAPACITY_AFTER="$capacity_after_file"
export KAFRUST_LOCAL_RESOURCE_SAMPLES="$resource_samples_file"
python3 - <<'PY'
import json
import os
import platform

with open(os.environ["KAFRUST_LOCAL_RESULT_FILE"], encoding="utf-8") as handle:
    result = json.load(handle)
descriptor = {
    "schema_version": 1,
    "status": "diagnostic",
    "qualified": False,
    "qualification_reason": "local rate-limited lifetime diagnostic; not V1-21 evidence",
    "campaign_id": os.environ["KAFRUST_LOCAL_RUN_ID"],
    "artifact": {
        "source_commit": os.environ["KAFRUST_LOCAL_SOURCE_COMMIT"],
        "working_tree_dirty": os.environ["KAFRUST_LOCAL_SOURCE_DIRTY"] == "true",
    },
    "runner": {"os": platform.system(), "architecture": platform.machine()},
    "broker": {"image": os.environ["KAFRUST_LOCAL_KAFKA_VERSION"]},
    "workload": {
        "duration_seconds": int(os.environ["KAFRUST_LOCAL_DURATION_SECONDS"]),
        "rate_records_per_second": int(os.environ["KAFRUST_LOCAL_RATE_RECORDS_PER_SECOND"]),
        "payload_bytes": int(os.environ["KAFRUST_LOCAL_PAYLOAD_BYTES"]),
        "replication_factor": 3,
        "partitions": 3,
        "disk_watermark_gib": int(os.environ["KAFRUST_LOCAL_DISK_WATERMARK_GIB"]),
        "disk_growth_budget_gib": int(os.environ["KAFRUST_LOCAL_DISK_BUDGET_GIB"]),
    },
    "fault": {"mode": "single-broker-restart", "broker": 1, "outage_seconds": 10},
    "result_file": os.path.basename(os.environ["KAFRUST_LOCAL_RESULT_FILE"]),
    "capacity_before": os.path.basename(os.environ["KAFRUST_LOCAL_CAPACITY_BEFORE"]),
    "capacity_after": os.path.basename(os.environ["KAFRUST_LOCAL_CAPACITY_AFTER"]),
    "resource_samples": os.path.basename(os.environ["KAFRUST_LOCAL_RESOURCE_SAMPLES"]),
    "result": result,
    "non_claims": [
        "not V1-21 throughput evidence",
        "not V1-22 SLO evidence",
        "not published-artifact evidence",
        "not service-canary evidence",
        "not release authorization",
    ],
}
with open(os.environ["KAFRUST_LOCAL_DESCRIPTOR"], "w", encoding="utf-8") as handle:
    json.dump(descriptor, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY

echo "local lifetime diagnostic completed: $output_dir"

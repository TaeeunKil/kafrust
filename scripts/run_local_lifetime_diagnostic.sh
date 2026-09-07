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

for command_name in cargo docker df python3; do
  command -v "$command_name" >/dev/null 2>&1 || die "missing command: $command_name"
done
[[ "$(uname -s)" == "Linux" ]] || die "run this launcher inside Linux/WSL"
[[ "$(uname -m)" == "x86_64" ]] || die "the launcher requires Linux x86_64"
docker info >/dev/null 2>&1 || die "Docker is not available"

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
duration_seconds="$(get_or_default KAFRUST_LOCAL_DURATION_SECONDS 21600)"
rate_records_per_second="$(get_or_default KAFRUST_LOCAL_RATE_RECORDS_PER_SECOND 100)"
payload_bytes="$(get_or_default KAFRUST_LOCAL_PAYLOAD_BYTES 64)"
disk_watermark_gib="$(get_or_default KAFRUST_LOCAL_DISK_WATERMARK_GIB 20)"
kafka_version="$(get_or_default KAFRUST_LOCAL_KAFKA_VERSION 4.3.1)"
run_id="$(get_or_default KAFRUST_LOCAL_RUN_ID "$(date -u +%Y%m%dT%H%M%SZ)-$$")"

for value in "$duration_seconds" "$rate_records_per_second" "$payload_bytes" "$disk_watermark_gib"; do
  [[ "$value" =~ ^[0-9]+$ ]] || die "numeric inputs must be non-negative integers"
done
[[ "$run_id" =~ ^[A-Za-z0-9._-]+$ ]] || die "run ID contains unsupported characters"
((duration_seconds >= 60 && duration_seconds <= 86400)) ||
  die "duration must be between 60 seconds and 24 hours"
((rate_records_per_second >= 1 && rate_records_per_second <= 100)) ||
  die "rate must be between 1 and 100 records/s"
((payload_bytes >= 1 && payload_bytes <= 256)) ||
  die "payload must be between 1 and 256 bytes"
((disk_watermark_gib >= 10 && disk_watermark_gib <= 200)) ||
  die "disk watermark must be between 10 and 200 GiB"

resource_prefix="kafrust-local-lifetime-$run_id-"
network_name="kafrust-local-lifetime-$run_id"
topic_name="kafrust-local-lifetime-$run_id"
broker_image="apache/kafka:$kafka_version"
output_dir="$(get_or_default KAFRUST_LOCAL_OUTPUT_DIR "$repo_root/target/local-lifetime/$run_id")"
project_dir="$output_dir/external-project"
result_file="$output_dir/local-lifetime.json"
descriptor_file="$output_dir/local-lifetime-descriptor.json"
capacity_before_file="$output_dir/capacity-before.txt"
capacity_after_file="$output_dir/capacity-after.txt"
[[ ! -e "$output_dir" ]] || die "output directory already exists: $output_dir"
mkdir -p "$output_dir"

capacity_path=/
if df -Pk /mnt/t >/dev/null 2>&1; then
  capacity_path=/mnt/t
fi
docker_root="$(docker info --format '{{.DockerRootDir}}')"

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

soak_pid=""
fault_pid=""
cleanup() {
  set +e
  if [[ -n "$soak_pid" ]] && kill -0 "$soak_pid" 2>/dev/null; then
    kill -TERM "$soak_pid" 2>/dev/null || true
    wait "$soak_pid" 2>/dev/null || true
  fi
  if [[ -n "$fault_pid" ]] && kill -0 "$fault_pid" 2>/dev/null; then
    kill -TERM "$fault_pid" 2>/dev/null || true
    wait "$fault_pid" 2>/dev/null || true
  fi
  while IFS= read -r container_name; do
    [[ -n "$container_name" ]] || continue
    docker rm -f -v "$container_name" >/dev/null 2>&1 || true
  done < <(
    docker ps -a --format '{{.Names}}' |
      awk -v prefix="$resource_prefix" 'index($0, prefix) == 1'
  )
  docker network rm "$network_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

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

(cargo run --quiet --release --manifest-path "$project_dir/Cargo.toml" | tee "$result_file") &
soak_pid=$!
(
  sleep "$((duration_seconds / 2))"
  docker stop -t 1 "$voter_one" >/dev/null
  sleep 10
  docker start "$voter_one" >/dev/null
) &
fault_pid=$!

aborted=0
while kill -0 "$soak_pid" 2>/dev/null; do
  host_free="$(free_gib "$capacity_path")"
  docker_free="$(free_gib "$docker_root")"
  printf 'watermark check: host=%sGiB docker=%sGiB threshold=%sGiB\n' \
    "$host_free" "$docker_free" "$disk_watermark_gib"
  if ((host_free < disk_watermark_gib || docker_free < disk_watermark_gib)); then
    echo "disk watermark reached; stopping diagnostic" >&2
    kill -TERM "$soak_pid" 2>/dev/null || true
    aborted=1
    break
  fi
  sleep 30
done

soak_status=0
wait "$soak_pid" || soak_status=$?
((aborted == 0)) || die "diagnostic aborted by disk watermark"
((soak_status == 0)) || die "diagnostic helper failed with status $soak_status"
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
if result.get("produced") != result.get("consumed"):
    raise SystemExit("produced and consumed counts did not reconcile")
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
export KAFRUST_LOCAL_CAPACITY_BEFORE="$capacity_before_file"
export KAFRUST_LOCAL_CAPACITY_AFTER="$capacity_after_file"
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
    },
    "fault": {"mode": "single-broker-restart", "broker": 1, "outage_seconds": 10},
    "result_file": os.path.basename(os.environ["KAFRUST_LOCAL_RESULT_FILE"]),
    "capacity_before": os.path.basename(os.environ["KAFRUST_LOCAL_CAPACITY_BEFORE"]),
    "capacity_after": os.path.basename(os.environ["KAFRUST_LOCAL_CAPACITY_AFTER"]),
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

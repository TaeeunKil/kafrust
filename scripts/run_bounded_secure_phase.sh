#!/usr/bin/env bash
set -euo pipefail

# Run one run-scoped SASL_SSL phase. The caller owns sequencing and the
# shared disk guard; this helper owns its containers, network, credentials,
# and temporary external Cargo project.

die() {
  echo "bounded secure phase: $*" >&2
  exit 1
}

usage() {
  cat >&2 <<'EOF'
usage: run_bounded_secure_phase.sh group|soak [options]
  --protocol classic|kip-848
  --cycles N
  --duration-seconds N
  --rate-records-per-second N
  --payload-bytes N
  --batch-size N
  --run-id ID
  --output-dir PATH
EOF
  exit 2
}

phase_kind="${1:-}"
[[ "$phase_kind" == "group" || "$phase_kind" == "soak" ]] || usage
shift

protocol="classic"
member_exit="drop"
cycles=100
duration_seconds=21600
rate_records_per_second=100
payload_bytes=64
batch_size=50
run_id=""
output_dir=""
kafka_version=""

while (($# > 0)); do
  case "$1" in
    --protocol) protocol="${2:?missing protocol}"; shift 2 ;;
    --member-exit) member_exit="${2:?missing member exit}"; shift 2 ;;
    --cycles) cycles="${2:?missing cycles}"; shift 2 ;;
    --duration-seconds) duration_seconds="${2:?missing duration}"; shift 2 ;;
    --rate-records-per-second) rate_records_per_second="${2:?missing rate}"; shift 2 ;;
    --payload-bytes) payload_bytes="${2:?missing payload}"; shift 2 ;;
    --batch-size) batch_size="${2:?missing batch size}"; shift 2 ;;
    --run-id) run_id="${2:?missing run id}"; shift 2 ;;
    --output-dir) output_dir="${2:?missing output directory}"; shift 2 ;;
    --kafka-version) kafka_version="${2:?missing Kafka version}"; shift 2 ;;
    *) usage ;;
  esac
done

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
source "$repo_root/scripts/local_lifetime_launcher_guard.sh"
source_root="${KAFRUST_LOCAL_SOURCE_ROOT:-$repo_root}"
[[ "$source_root" = /* && -d "$source_root" ]] || die "KAFRUST_LOCAL_SOURCE_ROOT must be an absolute directory"
[[ -f "$source_root/crates/kafrust/Cargo.toml" ]] || die "local source root has no crates/kafrust/Cargo.toml"
for command_name in cargo docker git openssl python3 setsid timeout; do
  command -v "$command_name" >/dev/null 2>&1 || die "missing command: $command_name"
done
[[ "$(uname -s)" == "Linux" ]] || die "run this helper inside Linux/WSL"
[[ "$(uname -m)" == "x86_64" ]] || die "the helper requires Linux x86_64"
docker info >/dev/null 2>&1 || die "Docker is not available"

[[ "$run_id" =~ ^[A-Za-z0-9._-]+$ ]] || die "run ID must contain only letters, digits, dot, underscore, or hyphen"
[[ "$output_dir" = /* ]] || die "output directory must be absolute"
[[ "$output_dir" != "$repo_root" && "$output_dir" != "$repo_root/"* ]] || die "output directory must be outside the repository"
[[ "$cycles" =~ ^[0-9]+$ && "$cycles" -ge 1 && "$cycles" -le 100 ]] || die "cycles must be between 1 and 100"
[[ "$duration_seconds" =~ ^[0-9]+$ && "$duration_seconds" -ge 60 && "$duration_seconds" -le 86400 ]] || die "duration must be between 60 seconds and 24 hours"
[[ "$rate_records_per_second" =~ ^[0-9]+$ && "$rate_records_per_second" -ge 1 && "$rate_records_per_second" -le 100 ]] || die "rate must be between 1 and 100 records/s"
[[ "$payload_bytes" =~ ^[0-9]+$ && "$payload_bytes" -ge 1 && "$payload_bytes" -le 256 ]] || die "payload must be between 1 and 256 bytes"
[[ "$batch_size" =~ ^[0-9]+$ && "$batch_size" -ge 1 && "$batch_size" -le 100 ]] || die "batch size must be between 1 and 100"
[[ "$member_exit" == leave || "$member_exit" == drop ]] || die "member exit must be leave or drop"

if [[ "$phase_kind" == group ]]; then
  case "$protocol" in
    classic) helper_protocol=classic ;;
    kip-848) helper_protocol=consumer ;;
    *) die "group protocol must be classic or kip-848" ;;
  esac
  helper_source="$source_root/.github/published-secure-group-rebalance-smoke/src/main.rs"
  manifest_source="$source_root/.github/published-secure-group-rebalance-smoke/Cargo.toml"
  phase_name="group-$protocol-$member_exit"
  external_port_base=23091
  topic_partitions=6
else
  helper_source="$source_root/.github/published-secure-multi-soak-smoke/src/main.rs"
  manifest_source="$source_root/.github/published-secure-multi-soak-smoke/Cargo.toml"
  phase_name="secure-soak"
  external_port_base=22091
  topic_partitions=3
  grep -q 'KAFRUST_SOAK_RECORDS_PER_SECOND' "$helper_source" ||
    die "secure soak helper has no explicit KAFRUST_SOAK_RECORDS_PER_SECOND contract"
  grep -q 'RateLimiter' "$helper_source" ||
    die "secure soak helper has no explicit RateLimiter"
fi

[[ -f "$helper_source" && -f "$manifest_source" ]] || die "selected published helper source is missing"
[[ ! -e "$output_dir" ]] || die "output directory already exists: $output_dir"
mkdir -p "$output_dir"
touch "$output_dir/.active"

kafka_version="${kafka_version:-${KAFRUST_KAFKA_VERSION:-3.7.2}}"
published_version="${KAFRUST_PUBLISHED_VERSION:-0.3.6}"
[[ "$kafka_version" == 3.7.2 || "$kafka_version" == 4.3.1 ]] || die "Kafka version must be 3.7.2 or 4.3.1"
[[ "$published_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9.-]+)?$ ]] || die "published version is invalid"
resource_prefix="kafrust-bounded-${run_id}-${phase_name}-"
network_name="kafrust-bounded-${run_id}-${phase_name}"
topic_name="kafrust-bounded-${run_id}-${phase_name}"
group_id="kafrust-bounded-${run_id}-${phase_name}"
broker_image="apache/kafka:$kafka_version"
secrets_dir="$output_dir/secrets"
project_dir="$output_dir/external-project"
result_file="$output_dir/result.log"
cargo_target_dir="${KAFRUST_BOUNDED_CARGO_TARGET_DIR:-$output_dir/cargo-target}"
[[ "$cargo_target_dir" = /* ]] || die "cargo target directory must be absolute"
build_pid=""
soak_pid=""
fault_pid=""
sampler_pid=""
resource_samples_file="$output_dir/resource-samples.jsonl"
mkdir -p "$secrets_dir" "$project_dir/src" "$cargo_target_dir"
chmod 700 "$output_dir" "$secrets_dir"
touch "$output_dir/.credentials-active"

cleanup() {
  set +e
  stop_process_group "$build_pid"
  stop_process_group "$soak_pid"
  stop_process_group "$fault_pid"
  stop_process_group "$sampler_pid"
  while IFS= read -r container_name; do
    [[ -n "$container_name" ]] || continue
    docker rm -f -v "$container_name" >/dev/null 2>&1 || true
  done < <(
    docker ps -a --format '{{.Names}}' 2>/dev/null |
      awk -v prefix="$resource_prefix" 'index($0, prefix) == 1'
  )
  docker network rm "$network_name" >/dev/null 2>&1 || true
  rm -f "$secrets_dir"/* "$output_dir/.credentials-active" "$output_dir/.active"
}
if docker network inspect "$network_name" >/dev/null 2>&1; then
  rm -f "$output_dir/.credentials-active" "$output_dir/.active"
  die "network already exists: $network_name"
fi
if docker ps -a --format '{{.Names}}' |
  awk -v prefix="$resource_prefix" 'index($0, prefix) == 1 {found=1} END {exit found}'; then
  :
else
  rm -f "$output_dir/.credentials-active" "$output_dir/.active"
  die "a container with the run prefix already exists: $resource_prefix"
fi

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

build_project() {
  setsid timeout --kill-after=30s 900s cargo build --quiet --release \
    --manifest-path "$project_dir/Cargo.toml" &
  build_pid=$!
  local status=0
  wait "$build_pid" || status=$?
  build_pid=""
  ((status == 0)) || die "external secure helper build failed with status $status"
}

start_sampler() {
  local disk_path=/
  [[ ! -d /mnt/t ]] || disk_path=/mnt/t
  setsid python3 "$repo_root/scripts/local_lifetime_resource_sampler.py" \
    --pid "$soak_pid" --output "$resource_samples_file" --interval-seconds 10 \
    --docker-container "${resource_prefix}1" --docker-container "${resource_prefix}2" \
    --docker-container "${resource_prefix}3" --disk-path "$disk_path" \
    --disk-path "$output_dir" &
  sampler_pid=$!
}

finish_sampler() {
  local status=0
  wait "$sampler_pid" || status=$?
  sampler_pid=""
  ((status == 0)) || die "resource sampler failed with status $status"
  [[ -s "$resource_samples_file" ]] || die "resource sampler produced no samples"
}

source_mode=published
source_commit=""
source_dirty=false
if [[ -n "${KAFRUST_LOCAL_SOURCE_ROOT:-}" ]]; then
  source_mode=local
  source_commit="$(git -C "$source_root" rev-parse HEAD)" || die "local source root is not a git checkout"
  [[ -z "$(git -C "$source_root" status --porcelain)" ]] || source_dirty=true
fi
export KAFRUST_BOUNDED_SOURCE_PROVENANCE="$output_dir/source-provenance.json"
export KAFRUST_BOUNDED_SOURCE_MODE="$source_mode"
export KAFRUST_BOUNDED_SOURCE_ROOT="$source_root"
export KAFRUST_BOUNDED_SOURCE_COMMIT="$source_commit"
export KAFRUST_BOUNDED_SOURCE_DIRTY="$source_dirty"
python3 - <<'PY'
import json
import os
from pathlib import Path

payload = {
    "schema_version": 1,
    "source_mode": os.environ["KAFRUST_BOUNDED_SOURCE_MODE"],
    "source_root": os.environ["KAFRUST_BOUNDED_SOURCE_ROOT"],
    "source_commit": os.environ["KAFRUST_BOUNDED_SOURCE_COMMIT"] or None,
    "working_tree_dirty": os.environ["KAFRUST_BOUNDED_SOURCE_DIRTY"] == "true",
}
with Path(os.environ["KAFRUST_BOUNDED_SOURCE_PROVENANCE"]).open("w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY

umask 077
sasl_password="$(openssl rand -hex 24)" || die "could not generate SASL password"
broker_password="$(openssl rand -hex 24)" || die "could not generate broker password"
printf '%s\n' "$sasl_password" > "$secrets_dir/client-password"
printf '%s\n' "$broker_password" > "$secrets_dir/broker-password"

openssl req -x509 -newkey rsa:2048 -days 1 -nodes \
  -subj "/CN=kafrust-bounded-test-ca" \
  -keyout "$secrets_dir/ca.key" -out "$secrets_dir/ca.crt" >/dev/null 2>&1
openssl x509 -in "$secrets_dir/ca.crt" -outform DER -out "$secrets_dir/ca.der"
openssl req -newkey rsa:2048 -nodes \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -keyout "$secrets_dir/server.key" -out "$secrets_dir/server.csr" >/dev/null 2>&1
openssl x509 -req -in "$secrets_dir/server.csr" \
  -CA "$secrets_dir/ca.crt" -CAkey "$secrets_dir/ca.key" -CAcreateserial \
  -days 1 -sha256 -copy_extensions copy -out "$secrets_dir/server.crt" >/dev/null 2>&1
openssl pkcs12 -export -in "$secrets_dir/server.crt" -inkey "$secrets_dir/server.key" \
  -certfile "$secrets_dir/ca.crt" -name localhost \
  -out "$secrets_dir/kafka.server.keystore.p12" \
  -passout "file:$secrets_dir/broker-password" >/dev/null 2>&1
printf '%s' "$broker_password" > "$secrets_dir/keystore-password"
printf '%s' "$broker_password" > "$secrets_dir/key-password"
cat > "$secrets_dir/kafka_server_jaas.conf" <<EOF
KafkaServer {
  org.apache.kafka.common.security.scram.ScramLoginModule required
  username="broker"
  password="$broker_password";
};
EOF

docker network create "$network_name" >/dev/null
cluster_id="$(docker run --rm "$broker_image" /opt/kafka/bin/kafka-storage.sh random-uuid)"
voter_one="${resource_prefix}1"
voter_two="${resource_prefix}2"
voter_three="${resource_prefix}3"
for node in 1 2 3; do
  external_port=$((external_port_base + node))
  container_name="${resource_prefix}${node}"
  docker run -d --name "$container_name" \
    --hostname "$container_name" \
    --network "$network_name" \
    --cpus=1.0 \
    --memory=2g \
    --pids-limit=512 \
    --log-driver json-file \
    --log-opt max-size=50m \
    --log-opt max-file=3 \
    -p "${external_port}:9095" \
    -v "$secrets_dir:/etc/kafka/secrets:ro" \
    -e KAFKA_OPTS="-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_server_jaas.conf" \
    -e KAFKA_CLUSTER_ID="$cluster_id" \
    -e KAFKA_NODE_ID="$node" \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=INTERNAL://:29092,SASL_SSL://:9095,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS="INTERNAL://${container_name}:29092,SASL_SSL://localhost:${external_port}" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,SASL_SSL:SASL_SSL \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@${voter_one}:9093,2@${voter_two}:9093,3@${voter_three}:9093" \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 \
    -e KAFKA_MIN_INSYNC_REPLICAS=2 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e KAFKA_SASL_ENABLED_MECHANISMS=SCRAM-SHA-256,SCRAM-SHA-512 \
    -e KAFKA_SSL_KEYSTORE_FILENAME=kafka.server.keystore.p12 \
    -e KAFKA_SSL_KEYSTORE_CREDENTIALS=keystore-password \
    -e KAFKA_SSL_KEY_CREDENTIALS=key-password \
    -e KAFKA_SSL_KEYSTORE_TYPE=PKCS12 \
    -e KAFKA_SSL_CLIENT_AUTH=none \
  "$broker_image" >/dev/null
done

for node in 1 2 3; do
  for attempt in {1..120}; do
    if docker exec "${resource_prefix}${node}" /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server "${resource_prefix}${node}:29092" >/dev/null 2>&1; then
      break
    fi
    ((attempt == 120)) && die "Kafka broker ${node} did not become ready"
    sleep 2
  done
done

topic_created=0
for attempt in {1..20}; do
  if docker exec "${resource_prefix}1" /opt/kafka/bin/kafka-configs.sh \
      --bootstrap-server "${resource_prefix}1:29092" --alter \
      --add-config "SCRAM-SHA-256=[iterations=4096,password=$sasl_password]" \
      --entity-type users --entity-name kafrust >/dev/null 2>&1 &&
    docker exec "${resource_prefix}1" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "${resource_prefix}1:29092" --create \
      --topic "$topic_name" --partitions "$topic_partitions" --replication-factor 3 >/dev/null 2>&1; then
    topic_created=1
    break
  fi
  sleep 2
done
((topic_created == 1)) || die "Kafka fixture setup failed before workload start"

python3 "$repo_root/scripts/wait_for_kafka_topic.py" \
  --topic "$topic_name" \
  --partitions "$topic_partitions" \
  --replicas 3 \
  --containers "${resource_prefix}1" "${resource_prefix}2" "${resource_prefix}3" \
  --timeout-seconds 120 >/dev/null

for port in $((external_port_base + 1)) $((external_port_base + 2)) $((external_port_base + 3)); do
  openssl s_client -connect "127.0.0.1:${port}" -servername localhost \
    -CAfile "$secrets_dir/ca.crt" -verify_return_error </dev/null >/dev/null 2>&1 ||
    die "TLS listener verification failed on port $port"
done

if [[ "$source_mode" == local ]]; then
  cat > "$project_dir/Cargo.toml" <<EOF
[package]
name = "kafrust-bounded-secure-phase"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
kafrust = { path = "$source_root/crates/kafrust", features = ["tls"] }
sha2 = "0.10"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
EOF
else
  sed \
    -e 's/^name = ".*"$/name = "kafrust-bounded-secure-phase"/' \
    -e "s/__KAFRUST_VERSION__/${published_version}/" \
    -e 's/__KAFRUST_FEATURES__/\["tls"\]/' \
    "$manifest_source" > "$project_dir/Cargo.toml"
fi
cp "$helper_source" "$project_dir/src/main.rs"

export CARGO_TARGET_DIR="$cargo_target_dir"
export KAFRUST_BOOTSTRAP_SERVERS="localhost:$((external_port_base + 1)),localhost:$((external_port_base + 2)),localhost:$((external_port_base + 3))"
export KAFRUST_SECURITY_PROTOCOL=sasl_tls
export KAFRUST_SASL_MECHANISM=scram-sha-256
export KAFRUST_SASL_USERNAME=kafrust
export KAFRUST_SASL_PASSWORD="$sasl_password"
export KAFRUST_TLS_SERVER_NAME=localhost
export KAFRUST_TLS_ROOT_CERT_DER_PATH="$secrets_dir/ca.der"
export KAFRUST_TOPIC="$topic_name"
export KAFRUST_GROUP_ID="$group_id"

if [[ "$phase_kind" == group ]]; then
  export KAFRUST_GROUP_PROTOCOL="$helper_protocol"
  export KAFRUST_MEMBER_EXIT="$member_exit"
  export KAFRUST_GROUP_CHURN_CYCLES="$cycles"
  build_project
  setsid timeout --kill-after=30s "$((cycles * 90 + 600))s" \
    "$cargo_target_dir/release/kafrust-bounded-secure-phase" > "$result_file" &
  soak_pid=$!
  start_sampler
  status=0
  wait "$soak_pid" || status=$?
  soak_pid=""
  ((status == 0)) || die "secure group helper failed with status $status"
  finish_sampler
  cat "$result_file"
  python3 - "$result_file" "$cycles" "$helper_protocol" "$member_exit" <<'PY'
import re
import sys
from pathlib import Path

result = Path(sys.argv[1]).read_text(encoding="utf-8")
expected_cycles, expected_protocol, expected_exit = sys.argv[2:]
matches = re.findall(
    r"published secure group churn passed protocol=(Classic|Consumer) exit=(leave|drop) cycles=(\d+)",
    result,
)
if not matches:
    raise SystemExit("secure group helper did not emit a final typed summary")
protocol, member_exit, cycles_observed = matches[-1]
if cycles_observed != expected_cycles or member_exit != expected_exit:
    raise SystemExit("secure group helper final summary did not match the requested phase")
if (expected_protocol == "classic" and protocol != "Classic") or (
    expected_protocol == "consumer" and protocol != "Consumer"
):
    raise SystemExit("secure group helper final summary used the wrong protocol")
PY
else
  export KAFRUST_SOAK_SECONDS="$duration_seconds"
  export KAFRUST_SOAK_BATCH_SIZE="$batch_size"
  export KAFRUST_SOAK_PAYLOAD_BYTES="$payload_bytes"
  export KAFRUST_SOAK_RECORDS_PER_SECOND="$rate_records_per_second"
  build_project
  setsid timeout --kill-after=30s "$((duration_seconds + 900))s" \
    "$cargo_target_dir/release/kafrust-bounded-secure-phase" > "$result_file" &
  soak_pid=$!
  start_sampler
  setsid bash -c '
    set -euo pipefail
    sleep "$1"
    docker stop -t 1 "$2" >/dev/null
    sleep "$3"
    docker start "$2" >/dev/null
  ' bash "$((duration_seconds / 2))" "${resource_prefix}1" "${KAFRUST_OUTAGE_SECONDS:-10}" &
  fault_pid=$!
  status=0
  wait "$soak_pid" || status=$?
  soak_pid=""
  if ((status != 0)); then
    stop_process_group "$fault_pid"
    fault_pid=""
    exit "$status"
  fi
  wait "$fault_pid"
  fault_pid=""
  finish_sampler
  python3 - "$result_file" "$duration_seconds" "$rate_records_per_second" "$payload_bytes" "$batch_size" <<'PY'
import json
import sys
from pathlib import Path

result = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
expected_duration = int(sys.argv[2])
expected_rate = int(sys.argv[3])
expected_payload = int(sys.argv[4])
batch_size = int(sys.argv[5])
if result.get("recovered") is not True:
    raise SystemExit("secure soak did not report recovery")
if result.get("in_flight_requests") != 0 or result.get("buffered_records") != 0:
    raise SystemExit("secure soak ended with non-zero resource gauges")
if result.get("duration_seconds", 0) < expected_duration:
    raise SystemExit("secure soak ended before the requested duration")
records = result.get("records")
if not isinstance(records, int) or isinstance(records, bool) or records <= 0:
    raise SystemExit("secure soak result did not contain an integer record count")
max_records = expected_rate * expected_duration + 3 * batch_size
if records > max_records:
    raise SystemExit("secure soak exceeded the aggregate rate budget")
if result.get("unknown_outcomes") != 0:
    raise SystemExit("secure soak reported unknown outcomes")
if result.get("attempted_records") != records:
    raise SystemExit("secure soak attempted records did not reconcile")
if result.get("records") != result.get("consumed_unique_records"):
    raise SystemExit("secure soak produced and consumed counts did not reconcile")
if result.get("acknowledged_records") != result.get("consumed_unique_records"):
    raise SystemExit("secure soak acknowledgements did not reconcile")
if result.get("record_id_reconciliation", {}).get("qualified") is not True:
    raise SystemExit("secure soak record reconciliation was not qualified")
if result.get("record_id_reconciliation", {}).get("loss_count") != 0:
    raise SystemExit("secure soak reported lost records")
if result.get("record_id_reconciliation", {}).get("duplicate_count") != 0:
    raise SystemExit("secure soak reported duplicate records")
if result.get("payload_bytes") != expected_payload:
    raise SystemExit("secure soak payload size did not match the requested phase")
PY
fi

echo "bounded secure phase completed: $phase_name"

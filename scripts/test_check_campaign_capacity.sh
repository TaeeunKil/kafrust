#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
script="$repo_root/scripts/check_campaign_capacity.sh"
fake_root="$(mktemp -d)"
trap 'rm -rf "$fake_root"' EXIT
mkdir -p "$fake_root/bin"

cat >"$fake_root/bin/df" <<'EOF'
#!/usr/bin/env bash
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
printf 'fake 0 0 %s 0%% /\n' "${FAKE_FREE_KIB:?}"
EOF
cat >"$fake_root/bin/docker" <<'EOF'
#!/usr/bin/env bash
case "${1:-}" in
  info) printf '/var/lib/docker\n' ;;
  system) printf 'fake docker system df\n' ;;
  *) exit 1 ;;
esac
EOF
chmod +x "$fake_root/bin/df" "$fake_root/bin/docker"

guard() {
  local free_gib="$1"
  shift
  local free_kib=$((free_gib * 1024 * 1024))
  FAKE_FREE_KIB="$free_kib" PATH="$fake_root/bin:$PATH" \
    env "$@" bash "$script" >/dev/null
}

if guard 2500 KAFRUST_SOAK_SECONDS=21600; then
  echo "six-hour guard accepted an unsafe 2,500-GiB host" >&2
  exit 1
fi
guard 3000 KAFRUST_SOAK_SECONDS=21600
guard 700 KAFRUST_SOAK_SECONDS=120
if guard 3500 KAFRUST_BENCH_WARMUP_SECONDS=7200 KAFRUST_BENCH_MEASURED_SECONDS=21600; then
  echo "eight-hour guard accepted an unsafe 3,500-GiB host" >&2
  exit 1
fi
guard 4000 KAFRUST_BENCH_WARMUP_SECONDS=7200 KAFRUST_BENCH_MEASURED_SECONDS=21600

echo "capacity guard policy tests passed"

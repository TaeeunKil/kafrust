# Current host capacity preflight (2026-09-10)

This read-only preflight covers the workstation that owns the `wsl-ubuntu-t9`
runner. It is an execution-safety record, not a V1-21 or V1-22 qualification
result.

## Environment

- source head: `7932c0e23a5e64318165438f5b9d360924d96889`
- published candidate: `kafrust` and `kafrust-protocol` `0.4.0`
- Windows: AMD Ryzen 7 260, 8 physical/16 logical CPUs, 31.3 GiB RAM
- Ubuntu-T9 WSL2: 16 CPUs, 15 GiB visible memory, systemd enabled
- campaign volume (`T:`/`/mnt/t`): 733 GiB free
- Docker root (`/var/lib/docker`): 846 GiB free
- registered runner: `wsl-ubuntu-t9`, online and idle
- runner service: enabled and active under systemd
- Windows AC standby and hibernate timeouts: disabled

The only active Docker containers at inspection were the unrelated
`power21-runtime-web-1` and `power21-runtime-db-1`; they were left untouched.

## Capacity finding

The published multi-broker helper is throughput-seeking unless a diagnostic
rate limiter is explicitly configured. The recent 15-minute plaintext run
[`34439794733`](https://github.com/TaeeunKil/kafrust/actions/runs/34439794733)
processed 28,786,200 1-KiB records at roughly 31,985 records/s. At replication
factor three this is about 82 GiB of broker payload per 15 minutes, or about
1,978 GiB for six hours before Kafka indexes, segments, metadata, retries, and
filesystem overhead. The secure rerun
[`34441268698`](https://github.com/TaeeunKil/kafrust/actions/runs/34441268698)
was slower but still projects to more than 1.5 TiB of six-hour payload.

The previous 700-GiB preflight floor therefore did not provide a safe margin
for an unbounded six-hour qualification workload. The capacity checker now
keeps the 700-GiB floor for short diagnostics, requires at least 2,600 GiB for
six-hour soak jobs, and requires at least 4,000 GiB for the eight-hour V1-22
jobs. It refuses an environment override that lowers those calculated floors.
On this workstation the long jobs fail fast before Kafka is started.

V1-22 is also a 120-job matrix with two-hour warmup plus six-hour measurement;
its maximum-throughput profiles cannot be converted into official evidence by
lowering the rate. The current host can run the matrix code sequentially only
after a larger storage-backed runner is available.

## Disposition

No six-hour V1-21 campaign or V1-22 matrix job was dispatched from this host
after this preflight. The safe next step is to provide a runner with several
TiB of free space (and preserve the same published artifact, broker images,
thresholds, and adjudicators), then run the exact manifest sequentially. The
existing short diagnostics and the three hosted 100-cycle probes dispatched on
2026-09-10 remain separate evidence and do not close either official gate.

#!/usr/bin/env python3
"""Wait for the fixture topic's leader and replicas on every named broker."""

import argparse
import re
import subprocess
import time


def ready_topic(description, partitions, replicas):
    observed = {}
    for line in description.splitlines():
        match = re.search(r"\bPartition:\s*(\d+)\s+Leader:\s*(-?\d+)\s+Replicas:\s*([\d,]+)\s+Isr:\s*([\d,]*)", line)
        if not match:
            continue
        partition, leader = int(match[1]), int(match[2])
        assigned = set(match[3].split(","))
        in_sync = set(match[4].split(",")) - {""}
        if partition in observed or leader < 0 or str(leader) not in in_sync:
            return False
        if len(assigned) != replicas or in_sync != assigned:
            return False
        observed[partition] = leader
    return set(observed) == set(range(partitions))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--partitions", type=int, required=True)
    parser.add_argument("--replicas", type=int, default=3)
    parser.add_argument("--containers", nargs="+", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args()
    if min(args.partitions, args.replicas, args.timeout_seconds) < 1:
        parser.error("counts and timeout must be positive")
    deadline = time.monotonic() + args.timeout_seconds
    while time.monotonic() < deadline:
        all_ready = True
        for container in args.containers:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                all_ready = False
                break
            try:
                result = subprocess.run(
                    ["docker", "exec", container, "/opt/kafka/bin/kafka-topics.sh",
                     "--bootstrap-server", f"{container}:29092", "--describe", "--topic", args.topic],
                    capture_output=True, text=True, timeout=min(15, remaining), check=False,
                )
            except subprocess.TimeoutExpired:
                all_ready = False
                break
            if result.returncode or not ready_topic(result.stdout, args.partitions, args.replicas):
                all_ready = False
                break
        if all_ready:
            print(f"topic fixture ready: {args.topic} ({args.partitions} partitions, RF{args.replicas})")
            return
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    parser.exit(1, f"topic fixture did not become fully replicated before deadline: {args.topic}\n")


if __name__ == "__main__":
    main()

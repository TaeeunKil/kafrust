#!/usr/bin/env python3
"""Plan and monitor the local soak's disk reserve and growth budget."""

import argparse
import json
import shutil
from pathlib import Path

GIB = 1024**3


def estimate_bytes(seconds, rate, payload):
    # RF3, 1 KiB/record overhead allowance, 2x margin, and 2 GiB fixed allowance.
    # This is admission planning, not an upper bound on Kafka or build storage.
    return seconds * rate * (payload + 1024) * 3 * 2 + 2 * GIB


def check_limits(baseline, current, reserve, budget):
    for path, initial in baseline.items():
        available = current[path]
        if available <= reserve:
            raise ValueError(f"{path}: free space reached the reserve")
        if initial - available >= budget:
            raise ValueError(f"{path}: disk growth reached the run budget")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("start", "check"))
    parser.add_argument("state", type=Path)
    parser.add_argument("--paths", nargs="+")
    parser.add_argument("--seconds", type=int, default=21600)
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--payload", type=int, default=64)
    parser.add_argument("--reserve-gib", type=int, default=100)
    parser.add_argument("--budget-gib", type=int, default=20)
    args = parser.parse_args()
    try:
        if args.mode == "start":
            if not args.paths or not (60 <= args.seconds <= 86400
                    and 1 <= args.rate <= 100 and 1 <= args.payload <= 256
                    and 100 <= args.reserve_gib <= 200
                    and 1 <= args.budget_gib <= 20):
                raise ValueError("invalid local workload, reserve, budget, or paths")
            estimate = estimate_bytes(args.seconds, args.rate, args.payload)
            budget = args.budget_gib * GIB
            reserve = args.reserve_gib * GIB
            if estimate > budget:
                raise ValueError("estimated storage exceeds budget; reduce rate or duration")
            baseline = {str(Path(p).resolve()): shutil.disk_usage(p).free for p in args.paths}
            for path, available in baseline.items():
                if available < reserve + budget:
                    raise ValueError(f"{path}: need reserve plus full run budget before starting")
            state = dict(baseline=baseline, reserve=reserve, budget=budget,
                         estimated_bytes=estimate)
            with args.state.open("x", encoding="utf-8") as handle:
                json.dump(state, handle, indent=2)
            print(f"disk plan: estimated {estimate / GIB:.2f} GiB; "
                  f"growth limit {args.budget_gib} GiB; reserve {args.reserve_gib} GiB")
        else:
            state = json.loads(args.state.read_text(encoding="utf-8"))
            current = {p: shutil.disk_usage(p).free for p in state["baseline"]}
            check_limits(state["baseline"], current, state["reserve"], state["budget"])
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, f"local disk budget: {error}\n")


if __name__ == "__main__":
    main()

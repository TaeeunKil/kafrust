#!/usr/bin/env python3
"""Validate and normalize the V1-21 broker-fault schedule input."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass


MODES = frozenset(("leader", "coordinator", "combined", "simultaneous"))
MIN_DURATION_SECONDS = 60
MAX_DURATION_SECONDS = 21_600


@dataclass(frozen=True)
class FaultEvent:
    mode: str
    planned_percent: int
    planned_offset_seconds: int


def parse_schedule(schedule: str, duration_seconds: int) -> tuple[FaultEvent, ...]:
    if not isinstance(schedule, str) or not schedule.strip():
        raise ValueError("fault schedule must not be empty")
    if not MIN_DURATION_SECONDS <= duration_seconds <= MAX_DURATION_SECONDS:
        raise ValueError("duration must be between 60 and 21600 seconds")

    events: list[FaultEvent] = []
    previous_percent = 0
    for raw_entry in schedule.split(","):
        mode, separator, raw_percent = raw_entry.partition("@")
        if not separator or mode not in MODES:
            raise ValueError(f"unsupported fault schedule entry: {raw_entry!r}")
        try:
            percent = int(raw_percent, 10)
        except ValueError as error:
            raise ValueError(f"fault schedule percentage is not an integer: {raw_entry!r}") from error
        if percent <= previous_percent or percent >= 100:
            raise ValueError("fault schedule percentages must increase and stay below 100")
        events.append(
            FaultEvent(
                mode=mode,
                planned_percent=percent,
                planned_offset_seconds=duration_seconds * percent // 100,
            )
        )
        previous_percent = percent
    return tuple(events)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--duration-seconds", required=True, type=int)
    args = parser.parse_args()
    try:
        events = parse_schedule(args.schedule, args.duration_seconds)
    except ValueError as error:
        print(f"v1 fault schedule check failed: {error}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            [
                {
                    "mode": event.mode,
                    "planned_percent": event.planned_percent,
                    "planned_offset_seconds": event.planned_offset_seconds,
                }
                for event in events
            ],
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Publish a continuous stream of agent_events records to Kafka.

Intentionally seeds three streaming data-quality problems, all
parameterized via config.yaml:
  - burst: a short high-rate spike every N seconds
  - late arrival: a fraction of events carry a backdated event_timestamp
  - duplicate: a fraction of events are re-published with the same
    event_id shortly after their first publish
"""

from __future__ import annotations

import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from confluent_kafka import Producer

AGENT_IDS = [f"agent_{i:04d}" for i in range(50)]
TOOL_IDS = [f"tool_{i:03d}" for i in range(12)]


def load_config(path: str | Path) -> dict:
    """Load config.yaml next to this script."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def make_event(cfg: dict, late: bool) -> dict:
    """Build one agent_events record, optionally with a backdated timestamp."""
    now = datetime.now(timezone.utc)
    if late:
        delay = random.uniform(
            cfg["generator"]["late_arrival"]["delay_min_sec"],
            cfg["generator"]["late_arrival"]["delay_max_sec"],
        )
        event_timestamp = (now - timedelta(seconds=delay)).isoformat()
    else:
        event_timestamp = now.isoformat()

    return {
        "event_id": str(uuid.uuid4()),
        "agent_id": random.choice(AGENT_IDS),
        "tool_id": random.choice(TOOL_IDS),
        "event_timestamp": event_timestamp,
        "published_at": now.isoformat(),
        "duration_ms": random.randint(20, 5000),
    }


def publish(producer: Producer, topic: str, event: dict, label: str) -> None:
    """Publish one event to Kafka and echo it to stdout with a label."""
    producer.produce(topic, value=json.dumps(event).encode("utf-8"))
    producer.poll(0)
    print(f"[{label}] {json.dumps(event)}", flush=True)


def main() -> None:
    """Continuously publish events, cycling through burst/late/duplicate."""
    cfg = load_config(Path(__file__).parent / "config.yaml")
    producer = Producer({"bootstrap.servers": cfg["kafka"]["broker"]})
    topic = cfg["kafka"]["topic"]

    late_pct = cfg["generator"]["late_arrival"]["late_pct"]
    dup_pct = cfg["generator"]["duplicate"]["duplicate_pct"]
    burst_cfg = cfg["generator"]["burst"]
    base_rate = cfg["generator"]["base_rate_per_sec"]

    last_event: dict | None = None
    start = time.time()
    next_burst_at = start + burst_cfg["every_n_seconds"] if burst_cfg["enabled"] else None

    try:
        while True:
            now_t = time.time()
            in_burst = next_burst_at is not None and now_t >= next_burst_at
            if in_burst:
                rate = burst_cfg["burst_rate_per_sec"]
                if now_t >= next_burst_at + burst_cfg["burst_duration_sec"]:
                    next_burst_at = now_t + burst_cfg["every_n_seconds"]
                    in_burst = False
                    rate = base_rate
            else:
                rate = base_rate

            is_late = random.random() < late_pct
            event = make_event(cfg, late=is_late)
            label = "BURST" if in_burst else ("LATE" if is_late else "clean")
            publish(producer, topic, event, label)
            last_event = event

            if last_event is not None and random.random() < dup_pct:
                publish(producer, topic, last_event, "DUPLICATE")

            time.sleep(1.0 / rate)
    finally:
        producer.flush()


if __name__ == "__main__":
    main()

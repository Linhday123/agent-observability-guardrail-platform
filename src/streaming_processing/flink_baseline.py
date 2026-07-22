"""Baseline Flink job over agent_events.

Runs on the real Flink JobManager/TaskManager cluster.

Baseline limitations:
- parallelism = 1
- watermark tolerance = 1 second
- no duplicate handling
- no allowed lateness
- no late-event side output
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import (
    Time,
    TumblingEventTimeWindows,
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "agent_events")
GROUP_ID = os.getenv("FLINK_GROUP_ID", "flink-baseline")

WINDOW_MIN = 1
PARALLELISM = 1


def build_env() -> StreamExecutionEnvironment:
    """Get the execution environment provided by the Flink cluster."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    return env


def parse_timestamp(timestamp: str) -> int:
    """Convert an ISO-8601 timestamp to UTC epoch milliseconds."""
    dt = datetime.fromisoformat(timestamp)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return int(dt.timestamp() * 1000)


class EventTimestampAssigner(TimestampAssigner):
    """Extract event_timestamp from the Kafka JSON message."""

    def extract_timestamp(
        self,
        value: str,
        record_timestamp: int,
    ) -> int:
        del record_timestamp

        record = json.loads(value)
        return parse_timestamp(record["event_timestamp"])


def parse_event(raw: str) -> tuple[str, str, int]:
    """Convert JSON into (agent_id, tool_id, duration_ms)."""
    record = json.loads(raw)

    return (
        record["agent_id"],
        record["tool_id"],
        int(record["duration_ms"]),
    )


class EventAggFunction(AggregateFunction):
    """Maintain (count, total_duration_ms) incrementally."""

    def create_accumulator(self):
        return (0, 0)

    def add(self, value, accumulator):
        _, _, duration_ms = value
        count, total_duration = accumulator

        return (
            count + 1,
            total_duration + duration_ms,
        )

    def get_result(self, accumulator):
        count, total_duration = accumulator

        avg_duration_ms = (
            round(total_duration / count, 1)
            if count
            else 0.0
        )

        return (
            count,
            avg_duration_ms,
        )

    def merge(self, left, right):
        return (
            left[0] + right[0],
            left[1] + right[1],
        )


class WindowResultFunction(ProcessWindowFunction):
    """Attach key and window boundaries to the aggregate."""

    def process(self, key, context, elements):
        agent_id, tool_id = key
        num_calls, avg_duration_ms = next(iter(elements))

        window = context.window()

        yield json.dumps(
            {
                "agent_id": agent_id,
                "tool_id": tool_id,
                "window_start": datetime.fromtimestamp(
                    window.start / 1000,
                    tz=timezone.utc,
                ).isoformat(),
                "window_end": datetime.fromtimestamp(
                    window.end / 1000,
                    tz=timezone.utc,
                ).isoformat(),
                "num_calls": num_calls,
                "avg_duration_ms": avg_duration_ms,
            }
        )


def main() -> None:
    """Build and execute the baseline job."""
    env = build_env()

    event_type = Types.TUPLE(
        [
            Types.STRING(),
            Types.STRING(),
            Types.INT(),
        ]
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(
            KafkaOffsetsInitializer.latest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(
            Duration.of_seconds(1)
        )
        .with_timestamp_assigner(
            EventTimestampAssigner()
        )
    )

    result = (
        env.from_source(
            source,
            watermark_strategy,
            "Kafka agent_events",
        )
        .map(
            parse_event,
            output_type=event_type,
        )
        .key_by(
            lambda event: (
                event[0],
                event[1],
            )
        )
        .window(
            TumblingEventTimeWindows.of(
                Time.minutes(WINDOW_MIN)
            )
        )
        .aggregate(
            EventAggFunction(),
            WindowResultFunction(),
            output_type=Types.STRING(),
        )
    )

    result.print("baseline-window-result")

    env.execute("agent_events_baseline")


if __name__ == "__main__":
    main()

"""Optimized PyFlink job for the agent_events Kafka topic.

Optimizations compared with the baseline:

1. Parallelism is increased from 1 to 4 to distribute burst traffic.
2. Duplicate event IDs are filtered with keyed ValueState.
3. Deduplication state is bounded by a 10-minute TTL.
4. Event-time processing uses a 10-second watermark tolerance.
5. Windows accept late events for an additional five minutes.

The job is submitted to the real Flink cluster with:

    docker compose -f infra/flink/docker-compose.yml exec jobmanager \
      flink run -py /opt/flink/usrlib/flink_optimized.py -d

The Kafka connector JAR is mounted into /opt/flink/lib, so the script
does not call add_jars() or configure a local Flink environment.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Iterable

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.state import (
    StateTtlConfig,
    ValueStateDescriptor,
)
from pyflink.datastream.window import (
    Time,
    TumblingEventTimeWindows,
)


KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "kafka:19092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "agent_events",
)

GROUP_ID = os.getenv(
    "FLINK_GROUP_ID",
    "flink-optimized",
)

PARALLELISM = 4
WINDOW_MINUTES = 1
WATERMARK_SECONDS = 10
ALLOWED_LATENESS_MINUTES = 5
DEDUP_STATE_TTL_MINUTES = 10


def build_env() -> StreamExecutionEnvironment:
    """Create the execution environment supplied by the Flink cluster."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)

    return env


def parse_iso_timestamp(timestamp_value: str) -> int:
    """Convert an ISO-8601 timestamp into UTC epoch milliseconds."""
    timestamp = datetime.fromisoformat(timestamp_value)

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(
            tzinfo=timezone.utc,
        )
    else:
        timestamp = timestamp.astimezone(
            timezone.utc,
        )

    return int(timestamp.timestamp() * 1000)


class EventTimestampAssigner(TimestampAssigner):
    """Assign event time from the event_timestamp JSON field."""

    def extract_timestamp(
        self,
        value: str,
        record_timestamp: int,
    ) -> int:
        del record_timestamp

        event = json.loads(value)

        return parse_iso_timestamp(
            event["event_timestamp"]
        )


def parse_for_dedup(
    raw_event: str,
) -> tuple[str, str]:
    """Return event_id together with the original JSON message."""
    event = json.loads(raw_event)

    return (
        str(event["event_id"]),
        raw_event,
    )


def dedup_key_selector(
    event: tuple[str, str],
) -> str:
    """Select event_id as the keyed-state key."""
    return event[0]


def extract_raw_event(
    event: tuple[str, str],
) -> str:
    """Remove the temporary event_id tuple field after deduplication."""
    return event[1]


def parse_event(
    raw_event: str,
) -> tuple[str, str, int]:
    """Convert JSON into agent_id, tool_id and duration_ms."""
    event = json.loads(raw_event)

    return (
        str(event["agent_id"]),
        str(event["tool_id"]),
        int(event["duration_ms"]),
    )


def aggregation_key_selector(
    event: tuple[str, str, int],
) -> tuple[str, str]:
    """Group events by agent_id and tool_id."""
    return (
        event[0],
        event[1],
    )


class DedupFunction(KeyedProcessFunction):
    """Filter duplicate event IDs using TTL-bounded keyed state."""

    def open(self, runtime_context) -> None:
        """Initialize the keyed ValueState for the current task."""
        ttl_configuration = (
            StateTtlConfig
            .new_builder(
                Time.minutes(
                    DEDUP_STATE_TTL_MINUTES
                )
            )
            .set_update_type(
                StateTtlConfig.UpdateType.OnCreateAndWrite
            )
            .set_state_visibility(
                StateTtlConfig.StateVisibility.NeverReturnExpired
            )
            .build()
        )

        state_descriptor = ValueStateDescriptor(
            "seen-event-id",
            Types.BOOLEAN(),
        )

        state_descriptor.enable_time_to_live(
            ttl_configuration
        )

        self.seen_state = runtime_context.get_state(
            state_descriptor
        )

    def process_element(
        self,
        value: tuple[str, str],
        context,
    ):
        """Emit the event only when its event_id has not been seen."""
        del context

        already_seen = self.seen_state.value()

        if already_seen is None:
            self.seen_state.update(True)
            yield value


class WindowAggregationFunction(ProcessWindowFunction):
    """Calculate count and average duration for each event-time window."""

    def process(
        self,
        key: tuple[str, str],
        context,
        elements: Iterable[tuple[str, str, int]],
    ):
        """Aggregate all events belonging to one keyed time window."""
        agent_id, tool_id = key

        event_count = 0
        total_duration_ms = 0

        for event in elements:
            event_count += 1
            total_duration_ms += int(event[2])

        average_duration_ms = (
            round(
                total_duration_ms / event_count,
                1,
            )
            if event_count > 0
            else 0.0
        )

        window = context.window()

        window_start = datetime.fromtimestamp(
            window.start / 1000,
            tz=timezone.utc,
        ).isoformat()

        window_end = datetime.fromtimestamp(
            window.end / 1000,
            tz=timezone.utc,
        ).isoformat()

        result = {
            "agent_id": agent_id,
            "tool_id": tool_id,
            "window_start": window_start,
            "window_end": window_end,
            "num_calls": event_count,
            "avg_duration_ms": average_duration_ms,
        }

        yield json.dumps(result)


def build_kafka_source() -> KafkaSource:
    """Build the Kafka source used by the optimized streaming job."""
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(
            KAFKA_BROKER
        )
        .set_topics(
            KAFKA_TOPIC
        )
        .set_group_id(
            GROUP_ID
        )
        .set_starting_offsets(
            KafkaOffsetsInitializer.latest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )


def build_watermark_strategy() -> WatermarkStrategy:
    """Build the event-time watermark strategy."""
    return (
    WatermarkStrategy
    .for_bounded_out_of_orderness(
        Duration.of_seconds(
            WATERMARK_SECONDS
        )
    )
    .with_timestamp_assigner(
        EventTimestampAssigner()
    )
    .with_idleness(
        Duration.of_seconds(15)
    )
)


def main() -> None:
    """Build and execute the optimized Flink streaming pipeline."""
    env = build_env()

    dedup_event_type = Types.TUPLE(
        [
            Types.STRING(),
            Types.STRING(),
        ]
    )

    parsed_event_type = Types.TUPLE(
        [
            Types.STRING(),
            Types.STRING(),
            Types.INT(),
        ]
    )

    kafka_source = build_kafka_source()
    watermark_strategy = build_watermark_strategy()

    raw_stream = env.from_source(
        kafka_source,
        watermark_strategy,
        "Kafka agent_events",
    )

    deduplicated_stream = (
        raw_stream
        .map(
            parse_for_dedup,
            output_type=dedup_event_type,
        )
        .key_by(
            dedup_key_selector
        )
        .process(
            DedupFunction(),
            output_type=dedup_event_type,
        )
        .map(
            extract_raw_event,
            output_type=Types.STRING(),
        )
    )

    result_stream = (
        deduplicated_stream
        .map(
            parse_event,
            output_type=parsed_event_type,
        )
        .key_by(
            aggregation_key_selector
        )
        .window(
            TumblingEventTimeWindows.of(
                Time.minutes(
                    WINDOW_MINUTES
                )
            )
        )
        .allowed_lateness(
            Time.minutes(
                ALLOWED_LATENESS_MINUTES
            ).to_milliseconds()
        )
        .process(
            WindowAggregationFunction(),
            output_type=Types.STRING(),
        )
    )

    result_stream.print(
        "optimized-window-result"
    )

    env.execute(
        "agent_events_optimized"
    )


if __name__ == "__main__":
    main()

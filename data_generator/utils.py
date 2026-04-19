"""
StreamPulse — Shared Utilities
Helper functions for ID generation, timestamps, and Event Hub publishing.
"""

import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Any

from azure.eventhub import EventHubProducerClient, EventData

from data_generator.live_data.config import EventHubConfig

logger = logging.getLogger("streampulse")


def generate_uuid() -> str:
    """Generate a random UUID v4 string."""
    return str(uuid.uuid4())


def generate_content_id() -> str:
    """Generate a content ID in the format tt-<uuid>."""
    return f"tt-{uuid.uuid4().hex[:8]}"


def generate_user_id_pool(num_users: int, prefix: str = "usr") -> list[str]:
    """Generate a deterministic user ID pool shared across generators.

    IDs are stable for a given num_users/prefix combination, e.g.:
    usr-000001, usr-000002, ...
    """
    if num_users <= 0:
        return []
    return [f"{prefix}-{index:06d}" for index in range(1, num_users + 1)]


def generate_content_id_pool(num_content_items: int, prefix: str = "tt") -> list[str]:
    """Generate a deterministic content ID pool shared across generators."""
    if num_content_items <= 0:
        return []
    return [f"{prefix}-{index:06d}" for index in range(1, num_content_items + 1)]


def generate_session_id(user_id: str, sequence: int, prefix: str = "ses") -> str:
    """Generate deterministic session IDs for a user and sequence number."""
    normalized_user = user_id.replace("-", "")
    return f"{prefix}-{normalized_user}-{sequence:06d}"


def now_iso() -> str:
    """Return current UTC timestamp in ISO 8601 format with milliseconds."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
           f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"


def create_producer(hub_config: EventHubConfig) -> EventHubProducerClient:
    """Create an Event Hub producer client from config."""
    return EventHubProducerClient.from_connection_string(
        conn_str=hub_config.connection_string,
        eventhub_name=hub_config.eventhub_name,
    )


def send_events(
    producer: EventHubProducerClient,
    events: list[dict[str, Any]],
    hub_name: str,
) -> int:
    """Send a batch of events to Event Hub.

    Args:
        producer: The Event Hub producer client.
        events: List of event dictionaries to serialize as JSON.
        hub_name: Name of the Event Hub (for logging).

    Returns:
        Number of events successfully sent.
    """
    if not events:
        return 0

    event_data_batch = producer.create_batch()
    sent_count = 0

    for event in events:
        try:
            event_data = EventData(json.dumps(event))
            event_data_batch.add(event_data)
            sent_count += 1
        except ValueError:
            # Batch is full — send current batch and start a new one
            producer.send_batch(event_data_batch)
            logger.info(f"[{hub_name}] Sent batch of {sent_count} events")
            event_data_batch = producer.create_batch()
            event_data = EventData(json.dumps(event))
            event_data_batch.add(event_data)
            sent_count = 1

    # Send remaining events
    if sent_count > 0:
        producer.send_batch(event_data_batch)
        logger.info(f"[{hub_name}] Sent batch of {sent_count} events")

    return sent_count


def setup_logging(level: int = logging.INFO) -> None:
    """Configure structured logging for the application."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

"""
StreamPulse — Playback Event Publisher
Simulates video playback telemetry: heartbeats, play/pause/stop lifecycle events,
buffering, bitrate shifts, and playback errors. Publishes to Azure Event Hub.

Usage:
    python -m data_generator.live_data.eventhub
"""

import random
import time
import logging
import signal
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path

# Support running this file directly as well as via `python -m`.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from data_generator.models import PlaybackEvent
from data_generator.live_data.config import load_config, SimulationConfig
from data_generator.utils import (
    generate_uuid,
    generate_content_id_pool,
    generate_user_id_pool,
    generate_session_id,
    now_iso,
    create_producer,
    send_events,
    setup_logging,
)

logger = logging.getLogger("streampulse.playback")

# ---------------------------------------------------------------------------
# Constants — realistic value pools
# ---------------------------------------------------------------------------

DEVICE_TYPES = ["smart_tv", "mobile", "tablet", "desktop", "console"]
DEVICE_OS_MAP: dict[str, list[str]] = {
    "smart_tv": ["tvOS", "Roku", "FireOS", "Android"],
    "mobile": ["Android", "iOS"],
    "tablet": ["Android", "iOS"],
    "desktop": ["Windows", "macOS"],
    "console": ["PlayStation", "Xbox"],
}
RESOLUTIONS = ["480p", "720p", "1080p", "4K"]
RESOLUTION_WEIGHTS = [0.05, 0.15, 0.55, 0.25]  # most users on 1080p
BITRATE_BY_RESOLUTION: dict[str, tuple[int, int]] = {
    "480p": (1500, 3000),
    "720p": (3000, 5000),
    "1080p": (5000, 8000),
    "4K": (15000, 25000),
}
ISP_LIST = ["Comcast", "Verizon", "AT&T", "T-Mobile", "Spectrum", "Cox", "CenturyLink"]
GEO_REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-south-1", "ap-southeast-1"]
GEO_COUNTRIES = ["US", "US", "GB", "DE", "IN", "SG"]  # aligned with regions
APP_VERSIONS = ["5.12.3", "5.12.2", "5.11.0", "5.10.1", "4.9.8"]

LIFECYCLE_EVENTS = ["play", "pause", "resume", "stop", "seek"]
ERROR_TYPES = ["decoder_error", "drm_error", "network_timeout", "segment_404"]


# ---------------------------------------------------------------------------
# Session simulation
# ---------------------------------------------------------------------------

@dataclass
class ViewerSession:
    """Represents an active viewer's playback session."""

    user_id: str
    session_id: str = ""
    content_id: str = ""
    device_type: str = ""
    device_os: str = ""
    resolution: str = "1080p"
    app_version: str = "5.12.3"
    geo_index: int = 0  # index into GEO_REGIONS / GEO_COUNTRIES
    isp: str = "Comcast"
    playback_position_sec: int = 0
    is_playing: bool = True
    is_buffering: bool = False
    content_duration_sec: int = 3600  # default 1 hour

    def __post_init__(self) -> None:
        if not self.device_type:
            self.device_type = random.choice(DEVICE_TYPES)
        if not self.device_os:
            self.device_os = random.choice(DEVICE_OS_MAP[self.device_type])
        self.resolution = random.choices(RESOLUTIONS, RESOLUTION_WEIGHTS)[0]
        self.app_version = random.choice(APP_VERSIONS)
        self.geo_index = random.randrange(len(GEO_REGIONS))
        self.isp = random.choice(ISP_LIST)
        self.content_duration_sec = random.randint(20 * 60, 150 * 60)  # 20 min – 2.5 hrs


def _random_bitrate(resolution: str) -> int:
    lo, hi = BITRATE_BY_RESOLUTION[resolution]
    return random.randint(lo, hi)


def generate_heartbeat(session: ViewerSession) -> dict:
    """Generate a heartbeat event for an active session."""
    return PlaybackEvent(
        user_id=session.user_id,
        session_id=session.session_id,
        content_id=session.content_id,
        event_type="heartbeat",
        playback_position_sec=session.playback_position_sec,
        bitrate_kbps=_random_bitrate(session.resolution),
        resolution=session.resolution,
        buffer_duration_ms=0,
        latency_ms=random.randint(10, 120),
        event_timestamp=now_iso(),
        device_type=session.device_type,
        device_os=session.device_os,
        app_version=session.app_version,
        geo_country=GEO_COUNTRIES[session.geo_index],
        geo_region=GEO_REGIONS[session.geo_index],
        isp=session.isp,
    ).model_dump()


def generate_lifecycle_event(session: ViewerSession) -> dict:
    """Generate a random lifecycle event (play, pause, seek, etc.)."""
    event_type = random.choice(LIFECYCLE_EVENTS)

    if event_type == "seek":
        session.playback_position_sec = random.randint(0, session.content_duration_sec)
    elif event_type == "pause":
        session.is_playing = False
    elif event_type in ("play", "resume"):
        session.is_playing = True

    return PlaybackEvent(
        user_id=session.user_id,
        session_id=session.session_id,
        content_id=session.content_id,
        event_type=event_type,
        playback_position_sec=session.playback_position_sec,
        bitrate_kbps=_random_bitrate(session.resolution),
        resolution=session.resolution,
        buffer_duration_ms=0,
        latency_ms=random.randint(10, 120),
        event_timestamp=now_iso(),
        device_type=session.device_type,
        device_os=session.device_os,
        app_version=session.app_version,
        geo_country=GEO_COUNTRIES[session.geo_index],
        geo_region=GEO_REGIONS[session.geo_index],
        isp=session.isp,
    ).model_dump()


def generate_buffer_event(session: ViewerSession, start: bool = True) -> dict:
    """Generate a buffer_start or buffer_end event."""
    session.is_buffering = start
    return PlaybackEvent(
        user_id=session.user_id,
        session_id=session.session_id,
        content_id=session.content_id,
        event_type="buffer_start" if start else "buffer_end",
        playback_position_sec=session.playback_position_sec,
        bitrate_kbps=_random_bitrate(session.resolution) if not start else 0,
        resolution=session.resolution,
        buffer_duration_ms=random.randint(500, 8000) if not start else 0,
        latency_ms=random.randint(50, 500),
        event_timestamp=now_iso(),
        device_type=session.device_type,
        device_os=session.device_os,
        app_version=session.app_version,
        geo_country=GEO_COUNTRIES[session.geo_index],
        geo_region=GEO_REGIONS[session.geo_index],
        isp=session.isp,
    ).model_dump()


def generate_error_event(session: ViewerSession) -> dict:
    """Generate a playback error event."""
    return PlaybackEvent(
        user_id=session.user_id,
        session_id=session.session_id,
        content_id=session.content_id,
        event_type="error",
        playback_position_sec=session.playback_position_sec,
        bitrate_kbps=0,
        resolution=session.resolution,
        buffer_duration_ms=0,
        latency_ms=random.randint(200, 2000),
        event_timestamp=now_iso(),
        device_type=session.device_type,
        device_os=session.device_os,
        app_version=session.app_version,
        geo_country=GEO_COUNTRIES[session.geo_index],
        geo_region=GEO_REGIONS[session.geo_index],
        isp=session.isp,
    ).model_dump()


# ---------------------------------------------------------------------------
# Main simulation loop
# ---------------------------------------------------------------------------

def run_simulation(
    sim_config: SimulationConfig,
    producer,
    hub_name: str,
    stop_event: threading.Event | None = None,
    counter: list[int] | None = None,
) -> None:
    """Run the playback event simulation loop.

    Each tick (~heartbeat_interval_sec):
      - All active sessions emit a heartbeat
      - ~5% of sessions trigger a lifecycle event (pause/seek/etc.)
      - ~2% of sessions experience a buffering episode
      - ~0.5% of sessions hit an error
      - ~3% of sessions end (stop) and are replaced by new ones

    Args:
        stop_event: When set, the loop exits cleanly (used when called from a thread).
        counter:    A one-element list ``[int]`` incremented with every event sent
                    (allows the caller to track throughput without shared state).
    """
    # Pre-generate user pool
    user_ids = generate_user_id_pool(sim_config.num_users)
    content_ids = generate_content_id_pool(sim_config.num_content_items)
    user_session_counters: dict[str, int] = {user_id: 0 for user_id in user_ids}

    def _next_session_id(user_id: str) -> str:
        user_session_counters[user_id] += 1
        return generate_session_id(user_id, user_session_counters[user_id])

    # Initialize active sessions (50% of users are watching at any time)
    active_count = max(10, sim_config.num_users // 2)
    sessions: list[ViewerSession] = []
    for i in range(active_count):
        sessions.append(ViewerSession(
            user_id=user_ids[i % len(user_ids)],
            session_id=_next_session_id(user_ids[i % len(user_ids)]),
            content_id=content_ids[random.randrange(len(content_ids))],
        ))

    running = True

    # Signal handlers can only be installed from the main thread.
    # When called from a background thread (e.g. web app), use stop_event instead.
    if stop_event is None:
        def _signal_handler(sig, frame):
            nonlocal running
            logger.info("Shutdown signal received, finishing current batch...")
            running = False

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

    start_time = time.time()
    total_sent = 0

    logger.info(f"Starting playback simulation: {len(sessions)} active sessions")

    while running and (stop_event is None or not stop_event.is_set()):
        events: list[dict] = []

        for session in sessions:
            # Advance playback position for playing sessions
            if session.is_playing and not session.is_buffering:
                session.playback_position_sec += int(sim_config.heartbeat_interval_sec)

            # Session ended (watched past content duration)?
            if session.playback_position_sec >= session.content_duration_sec:
                events.append(generate_lifecycle_event(session))  # 'stop'
                # Reset session with new content
                session.session_id = _next_session_id(session.user_id)
                session.content_id = content_ids[random.randrange(len(content_ids))]
                session.playback_position_sec = 0
                session.is_playing = True
                session.is_buffering = False
                continue

            # Heartbeat (every active session)
            if session.is_playing:
                events.append(generate_heartbeat(session))

            # Lifecycle event (~5%)
            if random.random() < 0.05:
                events.append(generate_lifecycle_event(session))

            # Buffering episode (~2%)
            if random.random() < 0.02 and not session.is_buffering:
                events.append(generate_buffer_event(session, start=True))
                events.append(generate_buffer_event(session, start=False))

            # Error (~0.5%)
            if random.random() < 0.005:
                events.append(generate_error_event(session))

        # Session churn: ~3% of sessions end and get replaced
        for i in range(len(sessions)):
            if random.random() < 0.03:
                events.append(PlaybackEvent(
                    user_id=sessions[i].user_id,
                    session_id=sessions[i].session_id,
                    content_id=sessions[i].content_id,
                    event_type="stop",
                    playback_position_sec=sessions[i].playback_position_sec,
                    event_timestamp=now_iso(),
                    device_type=sessions[i].device_type,
                    device_os=sessions[i].device_os,
                    geo_country=GEO_COUNTRIES[sessions[i].geo_index],
                    geo_region=GEO_REGIONS[sessions[i].geo_index],
                    isp=sessions[i].isp,
                ).model_dump())
                replacement_user_id = random.choice(user_ids)
                sessions[i] = ViewerSession(
                    user_id=replacement_user_id,
                    session_id=_next_session_id(replacement_user_id),
                    content_id=content_ids[random.randrange(len(content_ids))],
                )

        # Send in batches
        for batch_start in range(0, len(events), sim_config.batch_size):
            batch = events[batch_start : batch_start + sim_config.batch_size]
            sent = send_events(producer, batch, hub_name)
            total_sent += sent
            if counter is not None:
                counter[0] += sent

        logger.info(
            f"Tick complete: {len(events)} events generated, "
            f"{total_sent} total sent, "
            f"{len(sessions)} active sessions"
        )

        # Check duration limit
        if sim_config.run_duration_sec > 0:
            elapsed = time.time() - start_time
            if elapsed >= sim_config.run_duration_sec:
                logger.info(f"Duration limit reached ({sim_config.run_duration_sec}s). Stopping.")
                break

        time.sleep(sim_config.batch_delay_sec)

    logger.info(f"Playback simulation ended. Total events sent: {total_sent}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging()
    config = load_config()

    logger.info(f"Connecting to Event Hub: {config.playback_hub.eventhub_name}")
    producer = create_producer(config.playback_hub)

    try:
        run_simulation(config.simulation, producer, config.playback_hub.eventhub_name)
    finally:
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    main()

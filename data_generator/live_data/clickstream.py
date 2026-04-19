"""
StreamPulse — Clickstream Event Publisher
Simulates UI interaction events: page views, searches, title clicks,
watchlist actions, ratings, and shares. Publishes to Azure Event Hub.

Usage:
    python -m data_generator.live_data.clickstream
"""

import random
import time
import logging
import signal
import sys
import threading
from pathlib import Path

# Support running this file directly as well as via `python -m`.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from data_generator.models import ClickstreamEvent
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

logger = logging.getLogger("streampulse.clickstream")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EVENT_TYPES = [
    "page_view", "search", "browse", "click_title",
    "add_watchlist", "remove_watchlist", "rate", "share",
]
EVENT_WEIGHTS = [0.30, 0.12, 0.20, 0.18, 0.08, 0.03, 0.05, 0.04]

PAGES = ["home", "search", "browse_genre", "title_detail", "my_list", "settings"]
PAGE_WEIGHTS = [0.30, 0.15, 0.20, 0.25, 0.07, 0.03]

DEVICE_TYPES = ["smart_tv", "mobile", "tablet", "desktop"]
DEVICE_WEIGHTS = [0.35, 0.30, 0.15, 0.20]

DEVICE_OS_MAP: dict[str, list[str]] = {
    "smart_tv": ["tvOS", "Roku", "FireOS", "Android"],
    "mobile": ["Android", "iOS"],
    "tablet": ["Android", "iOS"],
    "desktop": ["Windows", "macOS"],
}

BROWSERS = ["in_app", "Chrome", "Safari", "Firefox", "Edge"]
BROWSER_WEIGHTS = [0.60, 0.15, 0.10, 0.08, 0.07]

SEARCH_QUERIES = [
    "stranger things", "breaking bad", "the witcher", "squid game",
    "wednesday", "dark", "narcos", "black mirror", "the crown",
    "money heist", "ozark", "bridgerton", "cobra kai", "arcane",
    "action movies", "comedy series", "horror films", "new releases",
    "top rated", "anime", "korean drama", "documentaries",
]

GENRES = [
    "action", "comedy", "drama", "horror", "sci-fi", "thriller",
    "romance", "documentary", "animation", "fantasy", "crime",
]


# ---------------------------------------------------------------------------
# Event generation
# ---------------------------------------------------------------------------

def generate_clickstream_event(
    user_id: str,
    session_id: str,
    content_ids: list[str],
) -> dict:
    """Generate a single clickstream event with realistic distributions."""
    event_type = random.choices(EVENT_TYPES, EVENT_WEIGHTS)[0]
    page = random.choices(PAGES, PAGE_WEIGHTS)[0]
    device_type = random.choices(DEVICE_TYPES, DEVICE_WEIGHTS)[0]
    device_os = random.choice(DEVICE_OS_MAP[device_type])
    browser = random.choices(BROWSERS, BROWSER_WEIGHTS)[0]

    # Context-dependent fields
    content_id = None
    search_query = None

    if event_type in ("click_title", "add_watchlist", "remove_watchlist", "rate", "share"):
        content_id = random.choice(content_ids)
        page = "title_detail"
    elif event_type == "search":
        search_query = random.choice(SEARCH_QUERIES)
        page = "search"
    elif event_type == "browse":
        page = "browse_genre"

    return ClickstreamEvent(
        user_id=user_id,
        session_id=session_id,
        event_type=event_type,
        page=page,
        content_id=content_id,
        search_query=search_query,
        event_timestamp=now_iso(),
        device_type=device_type,
        device_os=device_os,
        browser=browser,
    ).model_dump()


# ---------------------------------------------------------------------------
# User session simulation
# ---------------------------------------------------------------------------

class BrowsingSession:
    """Represents a user browsing session with variable activity rate."""

    def __init__(self, user_id: str, session_id: str, content_ids: list[str]) -> None:
        self.user_id = user_id
        self.session_id = session_id
        self.content_ids = content_ids
        # Average events per tick for this session (some users browse more)
        self.activity_rate = random.uniform(0.3, 2.0)
        # Session TTL in ticks
        self.ttl = random.randint(5, 60)
        self.age = 0

    @property
    def is_expired(self) -> bool:
        return self.age >= self.ttl

    def tick(self) -> list[dict]:
        """Generate events for this tick based on activity rate."""
        self.age += 1
        events = []
        # Poisson-like: generate 0-N events per tick
        num_events = int(self.activity_rate) + (1 if random.random() < (self.activity_rate % 1) else 0)
        for _ in range(num_events):
            events.append(generate_clickstream_event(
                self.user_id, self.session_id, self.content_ids
            ))
        return events


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
    """Run the clickstream simulation.

    Maintains a pool of active browsing sessions. Each tick:
      - Active sessions generate 0-N click events
      - Expired sessions are replaced with new ones
      - ~10% of idle users start a new session

    Args:
        stop_event: When set, the loop exits cleanly (used when called from a thread).
        counter:    A one-element list ``[int]`` incremented with every event sent.
    """
    user_ids = generate_user_id_pool(sim_config.num_users)
    content_ids = generate_content_id_pool(sim_config.num_content_items)
    user_session_counters: dict[str, int] = {user_id: 0 for user_id in user_ids}

    def _next_session_id(user_id: str) -> str:
        user_session_counters[user_id] += 1
        return generate_session_id(user_id, user_session_counters[user_id])

    # Start with ~30% of users having active browsing sessions
    active_count = max(10, sim_config.num_users * 3 // 10)
    sessions: list[BrowsingSession] = [
        BrowsingSession(user_ids[i], _next_session_id(user_ids[i]), content_ids) for i in range(active_count)
    ]

    running = True

    # Signal handlers can only be installed from the main thread.
    # When called from a background thread (e.g. web app), use stop_event instead.
    if stop_event is None:
        def _signal_handler(sig, frame):
            nonlocal running
            logger.info("Shutdown signal received...")
            running = False

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

    start_time = time.time()
    total_sent = 0
    next_allowed_send_time = time.monotonic()

    logger.info(
        "Starting clickstream simulation: %s active sessions, max_events_per_sec=%s",
        len(sessions),
        sim_config.max_events_per_sec if sim_config.max_events_per_sec > 0 else "unlimited",
    )

    while running and (stop_event is None or not stop_event.is_set()):
        events: list[dict] = []

        # Generate events from active sessions
        alive_sessions: list[BrowsingSession] = []
        for session in sessions:
            events.extend(session.tick())
            if not session.is_expired:
                alive_sessions.append(session)

        # Replace expired sessions + random new ones
        new_session_count = len(sessions) - len(alive_sessions)
        new_session_count += int(random.uniform(0, active_count * 0.1))
        new_session_count = min(new_session_count, sim_config.num_users - len(alive_sessions))

        for _ in range(max(0, new_session_count)):
            user_id = random.choice(user_ids)
            alive_sessions.append(BrowsingSession(user_id, _next_session_id(user_id), content_ids))

        sessions = alive_sessions

        # Send events
        for batch_start in range(0, len(events), sim_config.batch_size):
            batch = events[batch_start : batch_start + sim_config.batch_size]

            if sim_config.max_events_per_sec > 0:
                now_mono = time.monotonic()
                if now_mono < next_allowed_send_time:
                    time.sleep(next_allowed_send_time - now_mono)

            sent = send_events(producer, batch, hub_name)
            total_sent += sent
            if counter is not None:
                counter[0] += sent

            if sim_config.max_events_per_sec > 0 and sent > 0:
                pacing_interval_sec = sent / sim_config.max_events_per_sec
                next_allowed_send_time = max(next_allowed_send_time, time.monotonic()) + pacing_interval_sec

        logger.info(
            f"Tick: {len(events)} events, {len(sessions)} sessions, {total_sent} total sent"
        )

        # Duration check
        if sim_config.run_duration_sec > 0:
            if time.time() - start_time >= sim_config.run_duration_sec:
                logger.info(f"Duration limit reached. Stopping.")
                break

        time.sleep(sim_config.batch_delay_sec)

    logger.info(f"Clickstream simulation ended. Total events sent: {total_sent}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging()
    config = load_config()

    logger.info(f"Connecting to Event Hub: {config.clickstream_hub.eventhub_name}")
    producer = create_producer(config.clickstream_hub)

    try:
        run_simulation(config.simulation, producer, config.clickstream_hub.eventhub_name)
    finally:
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    main()

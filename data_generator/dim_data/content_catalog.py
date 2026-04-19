"""
StreamPulse — Content Catalog Publisher
Seeds and periodically updates the content catalog (movies, series, documentaries)
as CDC records written to Azure Data Lake Storage Gen2 for the content dimension table.

Usage:
    python -m data_generator.dim_data.content_catalog
"""

import json
import random
import time
import logging
import signal
import sys
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

# Support running this file directly as well as via `python -m`.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from azure.storage.blob import BlobServiceClient

from data_generator.models import ContentCatalogEvent
from data_generator.live_data.config import load_config, SimulationConfig, ADLSConfig
from data_generator.utils import (
    generate_content_id,
    generate_content_id_pool,
    generate_uuid,
    now_iso,
    setup_logging,
)

logger = logging.getLogger("streampulse.content")

# ---------------------------------------------------------------------------
# Constants — realistic content catalog data
# ---------------------------------------------------------------------------

CONTENT_TYPES = ["movie", "series", "documentary", "short"]
CONTENT_TYPE_WEIGHTS = [0.40, 0.35, 0.15, 0.10]

GENRES = [
    "action", "comedy", "drama", "horror", "sci-fi", "thriller",
    "romance", "documentary", "animation", "fantasy", "crime",
    "mystery", "adventure", "family", "musical",
]

MATURITY_RATINGS = ["G", "PG", "PG-13", "TV-14", "TV-MA", "R"]
MATURITY_WEIGHTS = [0.05, 0.10, 0.25, 0.25, 0.20, 0.15]

LANGUAGES = ["en", "es", "ko", "ja", "de", "fr", "pt", "hi", "it", "zh"]
LANGUAGE_WEIGHTS = [0.45, 0.12, 0.10, 0.08, 0.05, 0.05, 0.05, 0.04, 0.03, 0.03]

# Title pools for realistic generation
TITLE_PREFIXES = [
    "The", "A", "Dark", "Last", "Beyond", "Under", "Silent", "Lost",
    "Eternal", "Broken", "Hidden", "Iron", "Glass", "Shadow", "Crimson",
]
TITLE_NOUNS = [
    "Kingdom", "Empire", "Signal", "Protocol", "Horizon", "Legacy",
    "Cipher", "Frontier", "Paradox", "Eclipse", "Requiem", "Labyrinth",
    "Odyssey", "Syndicate", "Vendetta", "Gambit", "Revelation", "Dominion",
]
TITLE_SUFFIXES = ["", "", "", " Returns", " Reloaded", " Uprising", " Chronicles"]

DIRECTORS = [
    "Sarah Chen", "Marcus Rivera", "Anya Petrova", "James O'Brien",
    "Yuki Tanaka", "Sofia Andersson", "David Kim", "Lucia Fernandez",
    "Hans Mueller", "Priya Sharma", "Carlos Vega", "Elena Volkov",
]

ACTORS = [
    "Emma Stone", "Oscar Isaac", "Zendaya", "Pedro Pascal",
    "Florence Pugh", "Dev Patel", "Saoirse Ronan", "John Boyega",
    "Lupita Nyong'o", "Timothée Chalamet", "Anya Taylor-Joy",
    "Rami Malek", "Jenna Ortega", "Jonathan Majors", "Maitreyi Ramakrishnan",
    "Ke Huy Quan", "Stephanie Hsu", "Barry Keoghan", "Rachel Zegler",
    "Austin Butler", "Hailee Steinfeld", "Tom Holland",
]


def _generate_title() -> str:
    """Generate a plausible movie/series title."""
    prefix = random.choice(TITLE_PREFIXES)
    noun = random.choice(TITLE_NOUNS)
    suffix = random.choice(TITLE_SUFFIXES)
    return f"{prefix} {noun}{suffix}"


def _random_cast(min_size: int = 2, max_size: int = 6) -> list[str]:
    """Pick a random subset of actors for a cast list."""
    size = random.randint(min_size, max_size)
    return random.sample(ACTORS, min(size, len(ACTORS)))


def _random_genres(min_count: int = 1, max_count: int = 3) -> list[str]:
    """Pick random genres for a title."""
    count = random.randint(min_count, max_count)
    return random.sample(GENRES, count)


# ---------------------------------------------------------------------------
# Content generation
# ---------------------------------------------------------------------------

def generate_content_item(content_id: str | None = None) -> dict:
    """Generate a single content catalog entry."""
    content_type = random.choices(CONTENT_TYPES, CONTENT_TYPE_WEIGHTS)[0]

    season_number = None
    episode_number = None
    if content_type == "series":
        season_number = random.randint(1, 8)
        episode_number = random.randint(1, 13)

    duration = {
        "movie": random.randint(85, 180),
        "series": random.randint(22, 65),
        "documentary": random.randint(45, 120),
        "short": random.randint(5, 25),
    }[content_type]

    return ContentCatalogEvent(
        content_id=content_id or generate_content_id(),
        title=_generate_title(),
        content_type=content_type,
        genre=_random_genres(),
        release_year=random.randint(2015, 2026),
        maturity_rating=random.choices(MATURITY_RATINGS, MATURITY_WEIGHTS)[0],
        duration_min=duration,
        season_number=season_number,
        episode_number=episode_number,
        director=random.choice(DIRECTORS),
        cast=_random_cast(),
        language=random.choices(LANGUAGES, LANGUAGE_WEIGHTS)[0],
        updated_at=now_iso(),
    ).model_dump()


def generate_seed_catalog(num_items: int) -> tuple[list[dict], list[str]]:
    """Generate the initial content catalog seed.

    Returns:
        Tuple of (events list, content_id list for later updates).
    """
    events = []
    content_ids = []
    for content_id in generate_content_id_pool(num_items):
        event = generate_content_item(content_id=content_id)
        events.append(event)
        content_ids.append(event["content_id"])
    return events, content_ids


def generate_catalog_updates(content_ids: list[str], num_updates: int) -> list[dict]:
    """Generate CDC updates for existing content items.

    Simulates metadata corrections, new seasons/episodes, rating changes.
    """
    events = []
    for _ in range(num_updates):
        cid = random.choice(content_ids)
        events.append(generate_content_item(content_id=cid))
    return events


# ---------------------------------------------------------------------------
# Main simulation loop
# ---------------------------------------------------------------------------

def upload_events_to_adls(
    container_client,
    base_path: str,
    events: list[dict],
) -> int:
    """Write a list of events as a single JSON file to ADLS Gen2.

    File naming: <base_path>/year=YYYY/month=MM/day=DD/<timestamp>_<uuid>.json
    """
    if not events:
        return 0

    now = datetime.now(timezone.utc)
    partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
    filename = f"{now.strftime('%Y%m%dT%H%M%S')}_{generate_uuid()[:8]}.json"
    blob_path = f"{base_path}/{partition}/{filename}"

    ndjson = "\n".join(json.dumps(e) for e in events)
    data = BytesIO(ndjson.encode("utf-8"))

    blob_client = container_client.get_blob_client(blob_path)
    blob_client.upload_blob(data, overwrite=True)

    logger.info(f"[ADLS] Wrote {len(events)} content records → {blob_path}")
    return len(events)

def run_simulation(sim_config: SimulationConfig, container_client, adls_config: ADLSConfig) -> None:
    """Run the content catalog simulation.

    1. Seed the initial catalog (batch upload to ADLS).
    2. Periodically write updates: new releases + metadata changes.
    """
    logger.info(f"Seeding content catalog with {sim_config.num_content_items} titles...")
    seed_events, content_ids = generate_seed_catalog(sim_config.num_content_items)

    # Write seed in batches
    total_written = 0
    for batch_start in range(0, len(seed_events), sim_config.batch_size):
        batch = seed_events[batch_start : batch_start + sim_config.batch_size]
        total_written += upload_events_to_adls(container_client, adls_config.base_path, batch)

    logger.info(f"Seed complete: {total_written} catalog entries written to ADLS")

    # Continuous update loop
    running = True

    def _signal_handler(sig, frame):
        nonlocal running
        logger.info("Shutdown signal received...")
        running = False

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    start_time = time.time()
    update_interval_sec = 30  # publish updates every 30 seconds

    logger.info("Entering continuous update loop (new releases + metadata changes)...")

    while running:
        time.sleep(update_interval_sec)

        # ~2-5 new releases per cycle
        new_count = random.randint(2, 5)
        new_events = []
        for _ in range(new_count):
            event = generate_content_item()
            new_events.append(event)
            content_ids.append(event["content_id"])

        # ~3-8 metadata updates per cycle
        update_count = random.randint(3, 8)
        update_events = generate_catalog_updates(content_ids, update_count)

        all_events = new_events + update_events
        written = upload_events_to_adls(container_client, adls_config.base_path, all_events)
        total_written += written

        logger.info(
            f"Cycle: {new_count} new releases, {update_count} updates, "
            f"{len(content_ids)} total titles, {total_written} total written"
        )

        if sim_config.run_duration_sec > 0:
            if time.time() - start_time >= sim_config.run_duration_sec:
                logger.info("Duration limit reached. Stopping.")
                break

    logger.info(f"Content catalog simulation ended. Total records written: {total_written}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging()
    config = load_config()
    adls_config = config.content_catalog_adls

    logger.info(
        f"Connecting to ADLS Gen2: container={adls_config.container_name}, "
        f"path={adls_config.base_path}"
    )
    blob_service = BlobServiceClient.from_connection_string(adls_config.connection_string)
    container_client = blob_service.get_container_client(adls_config.container_name)

    try:
        container_client.get_container_properties()
    except Exception:
        container_client.create_container()
        logger.info(f"Created container: {adls_config.container_name}")

    try:
        run_simulation(config.simulation, container_client, adls_config)
    finally:
        blob_service.close()
        logger.info("ADLS client closed.")


if __name__ == "__main__":
    main()

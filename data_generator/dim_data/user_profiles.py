"""
StreamPulse — User Profiles Publisher
Seeds user profiles and simulates subscription changes (plan upgrades/downgrades,
cancellations, reactivations) as CDC events for SCD Type 2 tracking.
Writes JSON files to Azure Data Lake Storage Gen2 (picked up by Auto Loader on Databricks).

Usage:
    python -m data_generator.dim_data.user_profiles
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

from data_generator.models import UserProfileEvent
from data_generator.live_data.config import load_config, SimulationConfig, ADLSConfig
from data_generator.utils import (
    generate_uuid,
    generate_user_id_pool,
    now_iso,
    setup_logging,
)

logger = logging.getLogger("streampulse.users")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUBSCRIPTION_PLANS = ["free_trial", "basic", "standard", "premium"]
PLAN_WEIGHTS = [0.10, 0.25, 0.40, 0.25]

SUBSCRIPTION_STATUSES = ["active", "cancelled", "paused", "expired"]

COUNTRIES = ["US", "GB", "DE", "IN", "BR", "JP", "KR", "FR", "CA", "AU", "MX", "SG"]
COUNTRY_WEIGHTS = [0.30, 0.08, 0.07, 0.10, 0.08, 0.06, 0.05, 0.05, 0.06, 0.05, 0.05, 0.05]

LANGUAGES = ["en", "es", "de", "hi", "pt", "ja", "ko", "fr", "zh", "it"]

FIRST_NAMES = [
    "Emma", "Liam", "Sophia", "Noah", "Olivia", "Ethan", "Ava", "Mason",
    "Isabella", "Lucas", "Mia", "James", "Amelia", "Alexander", "Harper",
    "Aiden", "Evelyn", "Daniel", "Abigail", "Matthew", "Aria", "Priya",
    "Wei", "Yuki", "Carlos", "Fatima", "Igor", "Sakura", "Hans", "Elif",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Kim", "Lee", "Chen", "Wang",
    "Tanaka", "Mueller", "Fernandez", "Silva", "Patel", "Andersson",
    "O'Brien", "Volkov", "Nakamura", "Ali", "Kowalski",
]

EMAIL_DOMAINS = [
    "gmail.com", "outlook.com", "yahoo.com", "proton.me",
    "icloud.com", "hotmail.com", "mail.com",
]


def _generate_username(first: str, last: str) -> str:
    """Generate a plausible username from name components."""
    style = random.choice(["dot", "underscore", "number"])
    if style == "dot":
        return f"{first.lower()}.{last.lower()}"
    elif style == "underscore":
        return f"{first.lower()}_{last.lower()}{random.randint(1, 99)}"
    else:
        return f"{first.lower()}{random.randint(100, 9999)}"


def _generate_email(username: str) -> str:
    return f"{username}@{random.choice(EMAIL_DOMAINS)}"


# ---------------------------------------------------------------------------
# User generation
# ---------------------------------------------------------------------------

def generate_user_profile(user_id: str | None = None) -> dict:
    """Generate a single user profile event."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    username = _generate_username(first, last)
    country = random.choices(COUNTRIES, COUNTRY_WEIGHTS)[0]

    # Language often correlates with country
    lang_map = {
        "US": "en", "GB": "en", "CA": "en", "AU": "en",
        "DE": "de", "FR": "fr", "BR": "pt", "MX": "es",
        "IN": "hi", "JP": "ja", "KR": "ko", "SG": "en",
    }
    preferred_lang = lang_map.get(country, "en")

    return UserProfileEvent(
        user_id=user_id or generate_uuid(),
        username=username,
        email=_generate_email(username),
        subscription_plan=random.choices(SUBSCRIPTION_PLANS, PLAN_WEIGHTS)[0],
        subscription_status="active",
        country=country,
        preferred_language=preferred_lang,
        profile_count=random.randint(1, 5),
        created_at=now_iso(),
        updated_at=now_iso(),
    ).model_dump()


def generate_seed_users(user_ids: list[str]) -> tuple[list[dict], list[dict]]:
    """Generate the initial user base.

    Returns:
        Tuple of (events list, user records for tracking changes).
    """
    events = []
    user_records = []
    for user_id in user_ids:
        event = generate_user_profile(user_id=user_id)
        events.append(event)
        user_records.append(event.copy())
    return events, user_records


def generate_subscription_change(user_record: dict) -> dict:
    """Generate a subscription change event for an existing user.

    Simulates realistic transitions:
      - active → paused, cancelled
      - paused → active, cancelled
      - cancelled → active (win-back)
      - free_trial → basic, standard (conversion)
      - basic → standard, premium (upgrade)
      - premium → standard, basic (downgrade)
    """
    current_plan = user_record["subscription_plan"]
    current_status = user_record["subscription_status"]

    # Status transitions
    if current_status == "active":
        new_status = random.choices(
            ["active", "paused", "cancelled"],
            [0.70, 0.15, 0.15],
        )[0]
    elif current_status == "paused":
        new_status = random.choices(
            ["active", "paused", "cancelled"],
            [0.50, 0.30, 0.20],
        )[0]
    elif current_status in ("cancelled", "expired"):
        new_status = random.choices(
            ["active", "cancelled"],
            [0.30, 0.70],  # 30% win-back rate
        )[0]
    else:
        new_status = "active"

    # Plan transitions (only if still active or reactivating)
    new_plan = current_plan
    if new_status == "active":
        plan_transitions: dict[str, list[tuple[str, float]]] = {
            "free_trial": [("basic", 0.4), ("standard", 0.4), ("free_trial", 0.2)],
            "basic": [("basic", 0.5), ("standard", 0.35), ("premium", 0.15)],
            "standard": [("standard", 0.6), ("premium", 0.25), ("basic", 0.15)],
            "premium": [("premium", 0.7), ("standard", 0.25), ("basic", 0.05)],
        }
        options = plan_transitions.get(current_plan, [("standard", 1.0)])
        plans, weights = zip(*options)
        new_plan = random.choices(list(plans), list(weights))[0]

    return UserProfileEvent(
        user_id=user_record["user_id"],
        username=user_record["username"],
        email=user_record["email"],
        subscription_plan=new_plan,
        subscription_status=new_status,
        country=user_record["country"],
        preferred_language=user_record["preferred_language"],
        profile_count=user_record["profile_count"],
        created_at=user_record["created_at"],
        updated_at=now_iso(),
    ).model_dump()


# ---------------------------------------------------------------------------
# ADLS Gen2 upload helper
# ---------------------------------------------------------------------------

def upload_events_to_adls(
    container_client,
    base_path: str,
    events: list[dict],
) -> int:
    """Write a list of events as a single JSON file to ADLS Gen2.

    File naming: <base_path>/year=YYYY/month=MM/day=DD/<timestamp>_<uuid>.json
    This partitioning scheme works well with Databricks Auto Loader.

    Returns:
        Number of events written.
    """
    if not events:
        return 0

    now = datetime.now(timezone.utc)
    partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
    filename = f"{now.strftime('%Y%m%dT%H%M%S')}_{generate_uuid()[:8]}.json"
    blob_path = f"{base_path}/{partition}/{filename}"

    # Write as newline-delimited JSON (NDJSON) — optimal for Spark ingestion
    ndjson = "\n".join(json.dumps(e) for e in events)
    data = BytesIO(ndjson.encode("utf-8"))

    blob_client = container_client.get_blob_client(blob_path)
    blob_client.upload_blob(data, overwrite=True)

    logger.info(f"[ADLS] Wrote {len(events)} events → {blob_path}")
    return len(events)


# ---------------------------------------------------------------------------
# Main simulation loop
# ---------------------------------------------------------------------------

def run_simulation(sim_config: SimulationConfig, container_client, adls_config: ADLSConfig) -> None:
    """Run the user profiles simulation.

    1. Seed the initial user base (batch upload to ADLS).
    2. Periodically write subscription changes and new sign-ups as JSON files.
    """
    logger.info(f"Seeding user profiles with {sim_config.num_users} users...")
    user_ids = generate_user_id_pool(sim_config.num_users)
    seed_events, user_records = generate_seed_users(user_ids)

    total_sent = 0
    for batch_start in range(0, len(seed_events), sim_config.batch_size):
        batch = seed_events[batch_start : batch_start + sim_config.batch_size]
        total_sent += upload_events_to_adls(container_client, adls_config.base_path, batch)

    logger.info(f"Seed complete: {total_sent} user profiles written to ADLS")

    # Continuous update loop
    running = True

    def _signal_handler(sig, frame):
        nonlocal running
        logger.info("Shutdown signal received...")
        running = False

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    start_time = time.time()
    update_interval_sec = 15  # check for changes every 15 seconds

    logger.info("Entering continuous update loop (subscription changes + new sign-ups)...")

    while running:
        time.sleep(update_interval_sec)

        events: list[dict] = []

        # ~1-3 new sign-ups per cycle
        new_signups = random.randint(1, 3)
        for _ in range(new_signups):
            event = generate_user_profile()
            events.append(event)
            user_records.append(event.copy())

        # ~5-15% of users have a subscription change per cycle
        change_count = max(1, int(len(user_records) * random.uniform(0.005, 0.015)))
        change_indices = random.sample(range(len(user_records)), min(change_count, len(user_records)))

        for idx in change_indices:
            change_event = generate_subscription_change(user_records[idx])
            events.append(change_event)
            # Update our tracking record
            user_records[idx]["subscription_plan"] = change_event["subscription_plan"]
            user_records[idx]["subscription_status"] = change_event["subscription_status"]
            user_records[idx]["updated_at"] = change_event["updated_at"]

        written = upload_events_to_adls(container_client, adls_config.base_path, events)
        total_sent += written

        # Count active/churned
        active = sum(1 for u in user_records if u["subscription_status"] == "active")
        logger.info(
            f"Cycle: {new_signups} new sign-ups, {len(change_indices)} changes, "
            f"{active}/{len(user_records)} active users, {total_sent} total written"
        )

        if sim_config.run_duration_sec > 0:
            if time.time() - start_time >= sim_config.run_duration_sec:
                logger.info("Duration limit reached. Stopping.")
                break

    logger.info(f"User profiles simulation ended. Total events written: {total_sent}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging()
    config = load_config()
    adls_config = config.user_profiles_adls

    logger.info(
        f"Connecting to ADLS Gen2: container={adls_config.container_name}, "
        f"path={adls_config.base_path}"
    )
    blob_service = BlobServiceClient.from_connection_string(adls_config.connection_string)
    container_client = blob_service.get_container_client(adls_config.container_name)

    # Ensure container exists
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

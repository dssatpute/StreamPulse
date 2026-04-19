"""
StreamPulse — Configuration Module
Loads Event Hub connection settings and simulation parameters from .env file.
"""

import os
import logging
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("streampulse.config")


@dataclass(frozen=True)
class EventHubConfig:
    """Connection configuration for a single Event Hub instance."""

    connection_string: str
    eventhub_name: str

    @property
    def full_connection_string(self) -> str:
        """Return connection string with EntityPath appended (if not already present)."""
        if "EntityPath" in self.connection_string:
            return self.connection_string
        return f"{self.connection_string};EntityPath={self.eventhub_name}"


@dataclass(frozen=True)
class SimulationConfig:
    """Controls the behavior of the data generators."""

    # Number of simulated concurrent users/sessions
    num_users: int = 200
    num_content_items: int = 500

    # Playback heartbeat interval in seconds
    heartbeat_interval_sec: float = 10.0

    # Events per batch before sending
    batch_size: int = 50

    # Delay between batches (seconds) to control throughput
    batch_delay_sec: float = 1.0

    # Maximum publish throughput to Event Hub (events/sec, 0 = unlimited)
    max_events_per_sec: int = 20

    # How long to run the simulation (0 = indefinite)
    run_duration_sec: int = 0


@dataclass(frozen=True)
class ADLSConfig:
    """Connection configuration for Azure Data Lake Storage Gen2."""

    connection_string: str
    container_name: str
    base_path: str = ""  # e.g. "raw/user-profiles"


@dataclass(frozen=True)
class AppConfig:
    """Top-level application configuration."""

    playback_hub: EventHubConfig
    clickstream_hub: EventHubConfig
    content_hub: EventHubConfig
    user_profiles_adls: ADLSConfig
    content_catalog_adls: ADLSConfig
    simulation: SimulationConfig




def load_config() -> AppConfig:
    """Load configuration from .env file / environment variables.

    Required env vars (see .env.example):
        EVENT_HUB_CONNECTION_STRING       — Event Hub namespace connection string
        ADLS_CONNECTION_STRING            — ADLS Gen2 storage account connection string
        PLAYBACK_EVENT_HUB_NAME           — Hub name for playback events
        CLICKSTREAM_EVENT_HUB_NAME        — Hub name for clickstream events
        CONTENT_EVENT_HUB_NAME            — Hub name for content catalog events
        ADLS_CONTAINER_NAME               — Blob container for dimension data
        ADLS_USER_PROFILES_PATH           — Base path for user profile JSON files
    """
    logger.info("Loading configuration from environment...")

    eventhub_conn_str = os.environ["EVENT_HUB_CONNECTION_STRING"]
    adls_conn_str = os.environ["ADLS_CONNECTION_STRING"]

    return AppConfig(
        playback_hub=EventHubConfig(
            connection_string=eventhub_conn_str,
            eventhub_name=os.getenv("PLAYBACK_EVENT_HUB_NAME", "playback-events"),
        ),
        clickstream_hub=EventHubConfig(
            connection_string=eventhub_conn_str,
            eventhub_name=os.getenv("CLICKSTREAM_EVENT_HUB_NAME", "clickstream-events"),
        ),
        content_hub=EventHubConfig(
            connection_string=eventhub_conn_str,
            eventhub_name=os.getenv("CONTENT_EVENT_HUB_NAME", "content-events"),
        ),
        user_profiles_adls=ADLSConfig(
            connection_string=adls_conn_str,
            container_name=os.getenv("ADLS_CONTAINER_NAME", "dimensions"),
            base_path=os.getenv("ADLS_USER_PROFILES_PATH", "user-profiles"),
        ),
        content_catalog_adls=ADLSConfig(
            connection_string=adls_conn_str,
            container_name=os.getenv("ADLS_CONTAINER_NAME", "dimensions"),
            base_path=os.getenv("ADLS_CONTENT_CATALOG_PATH", "content-catalog"),
        ),
        simulation=SimulationConfig(
            num_users=int(os.getenv("SIM_NUM_USERS", "200")),
            num_content_items=int(os.getenv("SIM_NUM_CONTENT", "500")),
            batch_size=int(os.getenv("SIM_BATCH_SIZE", "50")),
            batch_delay_sec=float(os.getenv("SIM_BATCH_DELAY_SEC", "1.0")),
            max_events_per_sec=int(os.getenv("SIM_MAX_EVENTS_PER_SEC", "250")),
            run_duration_sec=int(os.getenv("SIM_DURATION_SEC", "0")),
        ),
    )

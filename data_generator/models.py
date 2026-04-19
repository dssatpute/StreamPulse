"""
StreamPulse — Pydantic Models
Defines the schema for all event types produced by the data generators.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from data_generator.utils import generate_uuid, generate_content_id, now_iso


# ---------------------------------------------------------------------------
# Playback Events
# ---------------------------------------------------------------------------

class PlaybackEvent(BaseModel):
    """A single playback telemetry event (heartbeat, lifecycle, or error)."""

    event_id: str = Field(default_factory=generate_uuid)
    user_id: str
    session_id: str
    content_id: str
    event_type: str  # play|pause|resume|stop|seek|heartbeat|buffer_start|buffer_end|error
    playback_position_sec: int = 0
    bitrate_kbps: int = 5800
    resolution: str = "1080p"  # 1080p|720p|480p|4K
    buffer_duration_ms: int = 0
    latency_ms: int = 45
    event_timestamp: str = Field(default_factory=now_iso)
    device_type: str = "smart_tv"  # smart_tv|mobile|tablet|desktop|console
    device_os: str = "Android"  # tvOS|Android|iOS|Windows|macOS|Roku|FireOS
    app_version: str = "5.12.3"
    geo_country: str = "US"
    geo_region: str = "us-west-2"
    isp: str = "Comcast"


# ---------------------------------------------------------------------------
# Clickstream Events
# ---------------------------------------------------------------------------

class ClickstreamEvent(BaseModel):
    """A UI interaction event from the streaming app."""

    event_id: str = Field(default_factory=generate_uuid)
    user_id: str
    session_id: str
    event_type: str  # page_view|search|browse|click_title|add_watchlist|remove_watchlist|rate|share
    page: str = "home"  # home|search|browse_genre|title_detail|my_list|settings
    content_id: Optional[str] = None
    search_query: Optional[str] = None
    event_timestamp: str = Field(default_factory=now_iso)
    device_type: str = "mobile"
    device_os: str = "iOS"
    browser: str = "in_app"  # in_app|Chrome|Safari|Firefox|Edge


# ---------------------------------------------------------------------------
# Content Catalog (Dimension)
# ---------------------------------------------------------------------------

class ContentCatalogEvent(BaseModel):
    """A content catalog entry (movie / series episode)."""

    content_id: str = Field(default_factory=generate_content_id)
    title: str
    content_type: str = "movie"  # series|movie|documentary|short
    genre: list[str] = Field(default_factory=lambda: ["drama"])
    release_year: int = 2025
    maturity_rating: str = "TV-14"  # TV-14|PG-13|R|G|TV-MA
    duration_min: int = 120
    season_number: Optional[int] = None
    episode_number: Optional[int] = None
    director: str = "Unknown"
    cast: list[str] = Field(default_factory=list)
    language: str = "en"
    updated_at: str = Field(default_factory=now_iso)


# ---------------------------------------------------------------------------
# User Profiles (Dimension — SCD Type 2)
# ---------------------------------------------------------------------------

class UserProfileEvent(BaseModel):
    """A user profile change event for SCD Type 2 tracking."""

    user_id: str = Field(default_factory=generate_uuid)
    username: str
    email: str
    subscription_plan: str = "standard"  # basic|standard|premium|free_trial
    subscription_status: str = "active"  # active|cancelled|paused|expired
    country: str = "US"
    preferred_language: str = "en"
    profile_count: int = 1
    created_at: str = Field(default_factory=now_iso)
    updated_at: str = Field(default_factory=now_iso)

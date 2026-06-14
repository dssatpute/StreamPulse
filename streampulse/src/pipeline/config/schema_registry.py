"""
Azure Event Hubs Schema Registry - Local Schema Definitions

This file maintains versioned Avro schemas for Event Hubs topics.
When schemas evolve in Azure Schema Registry, update the versions here.

Schema Evolution Best Practices:
- Add new fields with default values (backward compatible)
- Never remove required fields
- Update CURRENT_VERSION when promoting a new schema
- Keep old versions for rollback capability
"""

# Click Stream Events Schema Versions
CLICK_STREAM_SCHEMAS = {
    "v1": """
    {
      "type": "record",
      "name": "ClickStreamEvent",
      "namespace": "com.streampulse.events",
      "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "session_id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "page_url", "type": ["null", "string"], "default": null},
        {"name": "element_id", "type": ["null", "string"], "default": null},
        {"name": "content_id", "type": ["null", "string"], "default": null},
        {"name": "event_timestamp", "type": "long"},
        {"name": "device_type", "type": ["null", "string"], "default": null},
        {"name": "client_ip", "type": ["null", "string"], "default": null}
      ]
    }
    """,
    "v2": """
    {
      "type": "record",
      "name": "ClickStreamEvent",
      "namespace": "com.streampulse.events",
      "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "session_id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "page_url", "type": ["null", "string"], "default": null},
        {"name": "element_id", "type": ["null", "string"], "default": null},
        {"name": "content_id", "type": ["null", "string"], "default": null},
        {"name": "event_timestamp", "type": "long"},
        {"name": "device_type", "type": ["null", "string"], "default": null},
        {"name": "client_ip", "type": ["null", "string"], "default": null},
        {"name": "user_agent", "type": ["null", "string"], "default": null},
        {"name": "referrer_url", "type": ["null", "string"], "default": null}
      ]
    }
    """
}

# Playback Events Schema Versions
PLAYBACK_EVENTS_SCHEMAS = {
    "v1": """
    {
      "type": "record",
      "name": "PlaybackEvent",
      "namespace": "com.streampulse.events",
      "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "session_id", "type": "string"},
        {"name": "content_id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "playback_position_ms", "type": ["null", "long"], "default": null},
        {"name": "buffer_duration_ms", "type": ["null", "long"], "default": null},
        {"name": "error_code", "type": ["null", "string"], "default": null},
        {"name": "event_timestamp", "type": "long"},
        {"name": "device_type", "type": ["null", "string"], "default": null},
        {"name": "bitrate_kbps", "type": ["null", "int"], "default": null}
      ]
    }
    """,
    "v2": """
    {
      "type": "record",
      "name": "PlaybackEvent",
      "namespace": "com.streampulse.events",
      "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "session_id", "type": "string"},
        {"name": "content_id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "playback_position_ms", "type": ["null", "long"], "default": null},
        {"name": "buffer_duration_ms", "type": ["null", "long"], "default": null},
        {"name": "error_code", "type": ["null", "string"], "default": null},
        {"name": "event_timestamp", "type": "long"},
        {"name": "device_type", "type": ["null", "string"], "default": null},
        {"name": "bitrate_kbps", "type": ["null", "int"], "default": null},
        {"name": "cdn_node", "type": ["null", "string"], "default": null},
        {"name": "stream_quality", "type": ["null", "string"], "default": null}
      ]
    }
    """
}

# Current active versions - update these when promoting new schemas
CURRENT_CLICK_STREAM_VERSION = "v1"
CURRENT_PLAYBACK_EVENTS_VERSION = "v1"


def get_click_stream_schema(version: str = None) -> str:
    """
    Get the Avro schema for click stream events.
    
    Args:
        version: Specific version to retrieve (e.g., "v1", "v2"). 
                 If None, returns the current production version.
    
    Returns:
        Avro schema definition as JSON string
    
    Raises:
        KeyError: If the specified version doesn't exist
    """
    version = version or CURRENT_CLICK_STREAM_VERSION
    if version not in CLICK_STREAM_SCHEMAS:
        available = ", ".join(CLICK_STREAM_SCHEMAS.keys())
        raise KeyError(f"Click stream schema version '{version}' not found. Available: {available}")
    
    return CLICK_STREAM_SCHEMAS[version]


def get_playback_events_schema(version: str = None) -> str:
    """
    Get the Avro schema for playback events.
    
    Args:
        version: Specific version to retrieve (e.g., "v1", "v2"). 
                 If None, returns the current production version.
    
    Returns:
        Avro schema definition as JSON string
    
    Raises:
        KeyError: If the specified version doesn't exist
    """
    version = version or CURRENT_PLAYBACK_EVENTS_VERSION
    if version not in PLAYBACK_EVENTS_SCHEMAS:
        available = ", ".join(PLAYBACK_EVENTS_SCHEMAS.keys())
        raise KeyError(f"Playback events schema version '{version}' not found. Available: {available}")
    
    return PLAYBACK_EVENTS_SCHEMAS[version]


def list_schema_versions(topic: str) -> list:
    """
    List all available schema versions for a topic.
    
    Args:
        topic: Either "click-stream" or "playback"
    
    Returns:
        List of version strings
    """
    if topic == "click-stream":
        return list(CLICK_STREAM_SCHEMAS.keys())
    elif topic == "playback":
        return list(PLAYBACK_EVENTS_SCHEMAS.keys())
    else:
        raise ValueError(f"Unknown topic: {topic}. Use 'click-stream' or 'playback'")

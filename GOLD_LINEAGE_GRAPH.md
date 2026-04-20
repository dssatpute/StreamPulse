# Gold Layer Lineage Graph

This document shows how Gold reporting tables depend on Silver source tables and intermediate Gold tables.

```mermaid
flowchart LR
  subgraph S[Silver Sources]
    S1[streampulse.silver.user_playback_events]
    S2[streampulse.silver.content_catalog_dim]
    S3[streampulse.silver.user_profile_dim]
  end

  subgraph G[Gold Reporting Tables]
    G1[streampulse.gold.trending_title_viewers_5m]
    G2[streampulse.gold.top_trending_titles]
    G3[streampulse.gold.content_session_stats]
    G4[streampulse.gold.content_engagement]
    G5[streampulse.gold.subscription_churn_signals]
  end

  S1 --> G1
  G1 --> G2
  S2 --> G2

  S1 --> G3
  G3 --> G4
  S2 --> G4

  S1 --> G5
  S3 --> G5
```

## Dependency List

1. streampulse.gold.trending_title_viewers_5m depends on streampulse.silver.user_playback_events.
2. streampulse.gold.top_trending_titles depends on:
   - streampulse.gold.trending_title_viewers_5m
   - streampulse.silver.content_catalog_dim
3. streampulse.gold.content_session_stats depends on streampulse.silver.user_playback_events.
4. streampulse.gold.content_engagement depends on:
   - streampulse.gold.content_session_stats
   - streampulse.silver.content_catalog_dim
5. streampulse.gold.subscription_churn_signals depends on:
   - streampulse.silver.user_playback_events
   - streampulse.silver.user_profile_dim

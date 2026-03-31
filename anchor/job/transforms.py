"""
Transform functions that convert new Spotify GraphQL API responses
into the old Anchor REST API data shapes expected by the backend.

Each function takes the raw GraphQL response dict and returns only the
`data` portion (the envelope with provider/meta/range is added by
open_podcast.py).
"""

from datetime import datetime, timezone
from loguru import logger


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _date_to_unix(date_str: str) -> int:
    """Convert a 'YYYY-MM-DD' date string to a Unix timestamp (UTC midnight)."""
    return int(datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def _extract_time_series_points(graphql_data: dict, *path_keys: str) -> list[dict]:
    """
    Walk into a nested GraphQL response following *path_keys* and return
    the ``points`` list from a TimeSeriesValue.

    Typical path: ``("showByShowUri", "playsDaily")``
    The function automatically traverses ``.analyticsValue.analyticsValue.points``.
    """
    node = graphql_data
    for key in path_keys:
        if not isinstance(node, dict):
            return []
        node = node.get(key)
    # Now descend through analyticsValue.analyticsValue.points
    if isinstance(node, dict):
        node = node.get("analyticsValue", {})
    if isinstance(node, dict):
        node = node.get("analyticsValue", {})
    if isinstance(node, dict):
        return node.get("points", [])
    return []


def _extract_analytics_value(graphql_data: dict, *path_keys: str):
    """
    Walk into a nested GraphQL response and return the innermost
    ``analyticsValue.analyticsValue`` dict.
    """
    node = graphql_data
    for key in path_keys:
        if not isinstance(node, dict):
            return {}
        node = node.get(key)
    if isinstance(node, dict):
        node = node.get("analyticsValue", {})
    if isinstance(node, dict):
        node = node.get("analyticsValue", {})
    return node if isinstance(node, dict) else {}


# ---------------------------------------------------------------------------
# Show-level transforms
# ---------------------------------------------------------------------------


def transform_plays(graphql_data: dict) -> dict:
    """
    getShowOnSpotifyStats → old ``plays`` shape.

    Old shape::

        {"kind": "plays", "data": {"rows": [[timestamp, count], ...],
         "columnHeaders": [{"name": "Time (UTC)", ...}, {"name": "Plays", ...}]}}
    """
    points = _extract_time_series_points(graphql_data, "showByShowUri", "playsDaily")
    rows = []
    for p in points:
        ts = _date_to_unix(p["date"])
        val = p.get("value", {}).get("value", 0)
        rows.append([ts, val])

    return {
        "stationId": 0,
        "kind": "plays",
        "parameters": {
            "timeRange": [rows[0][0], rows[-1][0]] if rows else [],
            "timeInterval": 86400,
        },
        "data": {
            "rows": rows,
            "columnHeaders": [
                {"name": "Time (UTC)", "type": "integer", "isDateTime": True},
                {"name": "Plays", "type": "integer"},
            ],
        },
    }


def transform_plays_by_app(graphql_data: dict) -> dict:
    """
    getShowAudienceAllPlatformsStats → old ``playsByApp`` shape.

    Old shape::

        {"kind": "playsByApp", "data": {"rows": [["Spotify", 0.66], ...],
         "columnHeaders": [{"name": "App", ...}, {"name": "Percent of Plays", ...}]}}
    """
    inner = _extract_analytics_value(
        graphql_data, "showByShowUri", "showStreamsAndDownloadsByApp"
    )
    apps = inner.get("apps", [])
    rows = [[a["displayName"], a["value"]] for a in apps]

    return {
        "stationId": 0,
        "kind": "playsByApp",
        "parameters": {
            "minValue": 0.02,
            "timeRange": [],
            "timeInterval": 86400,
        },
        "data": {
            "rows": rows,
            "columnHeaders": [
                {"name": "App", "type": "string"},
                {"name": "Percent of Plays", "type": "number"},
            ],
        },
    }


def transform_plays_by_device(graphql_data: dict) -> dict:
    """
    getShowAudienceAllPlatformsStats → old ``playsByDevice`` shape.
    """
    inner = _extract_analytics_value(
        graphql_data, "showByShowUri", "showStreamsAndDownloadsByDevice"
    )
    devices = inner.get("devices", [])
    rows = [[d["displayName"], d["value"]] for d in devices]

    translation_mapping = {r[0]: r[0] for r in rows}
    colors = {r[0]: "#26008D" for r in rows}

    return {
        "stationId": 0,
        "kind": "playsByDevice",
        "parameters": {
            "minValue": 0.02,
            "timeRange": [],
            "timeInterval": 86400,
        },
        "data": {
            "rows": rows,
            "columnHeaders": [
                {"name": "Device", "type": "string"},
                {"name": "Percent of Plays", "type": "number"},
            ],
            "translationMapping": translation_mapping,
            "colors": colors,
        },
    }


def transform_plays_by_geo(graphql_data: dict) -> dict:
    """
    getShowAudienceAllPlatformsGeoStats (GEO_COUNTRY) → old ``playsByGeo`` shape.

    Old shape::

        {"kind": "playsByGeo", "parameters": {"resultGeo": "geo_4"},
         "data": {"rows": [["Germany", 0.33], ...],
         "columnHeaders": [{"name": "Geo", ...}, {"name": "Percent of Plays", ...}]}}
    """
    inner = _extract_analytics_value(
        graphql_data, "showByShowUri", "showStreamsAndDownloadsByGeo"
    )
    geos = inner.get("geos", [])
    rows = [[g["displayName"], g["value"]] for g in geos]

    return {
        "stationId": 0,
        "kind": "playsByGeo",
        "parameters": {
            "geos": [None, None, None],
            "resultGeo": "geo_4",
            "timeRange": [],
            "timeInterval": 3600,
        },
        "data": {
            "rows": rows,
            "columnHeaders": [
                {"name": "Geo", "type": "string"},
                {"name": "Percent of Plays", "type": "number"},
            ],
            "assets": {"flagUrlByGeo": {}},
        },
    }


def transform_plays_by_geo_city(graphql_data: dict) -> dict:
    """
    getShowAudienceAllPlatformsGeoStats (GEO_CITY) → old ``playsByGeoCity`` shape.

    NOTE: The Spotify GraphQL API may return ``null`` for city-level geo data
    on some shows (DataFetchingException).  In that case we return empty rows.
    """
    # Guard: if the API returned null for the geo node, inner will be {}
    inner = _extract_analytics_value(
        graphql_data, "showByShowUri", "showStreamsAndDownloadsByGeo"
    )
    geos = inner.get("geos", [])
    rows = [[g["displayName"], g["value"]] for g in geos]

    return {
        "stationId": 0,
        "kind": "playsByGeo",
        "parameters": {
            "geos": [None, None, None],
            "resultGeo": "geo_3",
            "timeRange": [],
            "timeInterval": 3600,
        },
        "data": {
            "rows": rows,
            "columnHeaders": [
                {"name": "Geo", "type": "string"},
                {"name": "Percent of Plays", "type": "number"},
            ],
            "assets": {"flagUrlByGeo": {}},
        },
    }


def transform_plays_by_age_range(graphql_data: dict) -> dict:
    """
    getShowAudienceDemographicsStats → old ``playsByAgeRange`` shape.

    The new API returns age brackets with per-gender breakdowns.
    We sum each bracket's total / overall total to get the fraction,
    matching the old shape: ``[["18-22", 0.08], ...]``.
    """
    inner = _extract_analytics_value(
        graphql_data, "showByShowUri", "showStreamsFaceted"
    )
    age_breakdown = inner.get("ageBreakdown", [])
    total_value = inner.get("totalValue", 0)

    # Mapping from new API bracket names to old Anchor names
    _AGE_REMAP = {"60-150": "60+"}

    rows = []
    for bracket in age_breakdown:
        bracket_name = bracket.get("ageBracket", bracket.get("displayName", ""))
        # Skip unknown brackets
        if bracket_name == "unknown":
            continue
        bracket_name = _AGE_REMAP.get(bracket_name, bracket_name)
        bracket_total = bracket.get("genderBreakdown", {}).get("total", 0)
        fraction = bracket_total / total_value if total_value else 0.0
        rows.append([bracket_name, fraction])

    # Build translationMapping and colors from actual data rows
    translation_mapping = {r[0]: r[0] for r in rows}
    colors = {r[0]: "#26008D" for r in rows}

    return {
        "stationId": 0,
        "kind": "playsByAgeRange",
        "parameters": {
            "timeRange": [],
        },
        "data": {
            "rows": rows,
            "columnHeaders": [
                {"name": "Age Range", "type": "string"},
                {"name": "Percent of Plays", "type": "number"},
            ],
            "translationMapping": translation_mapping,
            "colors": colors,
        },
    }


def transform_plays_by_gender(graphql_data: dict) -> dict:
    """
    getShowAudienceDemographicsStats → old ``playsByGender`` shape.

    Uses the top-level ``genderBreakdown`` summary from the response.
    Old shape: ``[["Female", 0.78], ["Male", 0.19], ...]``.
    """
    inner = _extract_analytics_value(
        graphql_data, "showByShowUri", "showStreamsFaceted"
    )
    gender_breakdown = inner.get("genderBreakdown", {})
    counts = gender_breakdown.get("counts", [])

    rows = [[g["displayName"], g["percent"]] for g in counts]

    # Build translationMapping and colors from actual data rows
    gender_colors = {
        "Male": "#26008D", "Female": "#5925FF",
        "Not specified": "#9691FF", "Non-binary": "#D7DBFF",
    }
    translation_mapping = {r[0]: r[0] for r in rows}
    colors = {r[0]: gender_colors.get(r[0], "#26008D") for r in rows}

    return {
        "stationId": 0,
        "kind": "playsByGender",
        "parameters": {
            "timeRange": [],
        },
        "data": {
            "rows": rows,
            "columnHeaders": [
                {"name": "Gender", "type": "string"},
                {"name": "Percent of Plays", "type": "number"},
            ],
            "translationMapping": translation_mapping,
            "colors": colors,
        },
    }


def transform_unique_listeners(graphql_data: dict) -> dict:
    """
    getShowAudienceDiscoveryStats → old ``uniqueListeners`` shape.

    Old shape: ``{"kind": "uniqueListeners", "data": {"rows": [65]}}``

    Uses ``audienceSize`` from the discovery stats response.
    """
    inner = _extract_analytics_value(
        graphql_data, "showByShowUri", "audienceSize"
    )
    value = inner.get("value", 0)

    return {
        "stationId": 0,
        "kind": "uniqueListeners",
        "parameters": None,
        "data": {
            "rows": [value],
            "columnHeaders": [
                {"name": "Unique Listeners", "type": "integer"},
            ],
        },
    }


def transform_audience_size(graphql_data: dict) -> dict:
    """
    getShowAudienceDiscoveryStats → old ``audienceSize`` shape.

    Old shape: ``{"kind": "audienceSize", "data": {"rows": [123]}}``

    Same source as unique_listeners — the old API had two separate endpoints
    that returned the same concept.
    """
    inner = _extract_analytics_value(
        graphql_data, "showByShowUri", "audienceSize"
    )
    value = inner.get("value", 0)

    return {
        "stationId": 0,
        "kind": "audienceSize",
        "parameters": None,
        "data": {
            "rows": [value],
            "columnHeaders": [
                {"name": "Audience Size", "type": "integer"},
            ],
        },
    }


def transform_total_plays(graphql_data: dict) -> dict:
    """
    getShowOnSpotifyStats → old ``totalPlays`` shape.

    The GraphQL response carries a time-series under
    ``showByShowUri.playsDaily`` with daily play counts.
    We sum them to produce the total for the requested window.

    Old shape: ``{"kind": "totalPlays", "data": {"rows": [1234]}}``
    """
    points = _extract_time_series_points(
        graphql_data, "showByShowUri", "playsDaily"
    )
    value = sum(
        (p.get("value", {}).get("value", 0) if isinstance(p.get("value"), dict) else 0)
        for p in points
    )

    return {
        "stationId": 0,
        "kind": "totalPlays",
        "parameters": None,
        "data": {
            "rows": [value],
            "columnHeaders": [
                {"name": "Plays", "type": "integer"},
            ],
        },
    }


def transform_total_plays_by_episode(
    graphql_data: dict,
    episode_enrichment: dict | None = None,
) -> dict:
    """
    getShowTopEpisodes → old ``totalPlaysByEpisode`` shape.

    *episode_enrichment* is a ``{uri: episode_dict}`` lookup built from
    ``get_all_episodes()``.  Each episode dict is expected to carry an
    ``episodeId`` integer field (the legacy Anchor numeric ID).  When the
    lookup is provided the real ID is used; otherwise ``rank`` is used as
    a fallback.

    Old shape::

        {"kind": "totalPlaysByEpisode", "data": {"rows": [
            ["title", episodeId, streamsCount, publishTimestamp, rank, "episodeUri"],
            ...
        ]}}
    """
    enrichment = episode_enrichment or {}
    inner = _extract_analytics_value(
        graphql_data, "showByShowUri", "analytics"
    )
    top_episodes = inner.get("topEpisodes", [])

    rows = []
    for rank, ep in enumerate(top_episodes, start=1):
        title = ep.get("episode", {}).get("title", "")
        episode_uri = ep.get("episodeUri", "")
        count = ep.get("count", 0)
        publish_seconds = ep.get("episode", {}).get("publishedOn", {}).get("seconds", 0)
        # Use the real Anchor numeric episodeId when available (from
        # get_all_episodes enrichment), otherwise fall back to rank.
        ep_info = enrichment.get(episode_uri, {})
        episode_id = ep_info.get("episodeId", rank)
        rows.append([title, episode_id, count, publish_seconds, rank, episode_uri])

    return {
        "stationId": 0,
        "kind": "totalPlaysByEpisode",
        "parameters": None,
        "data": {
            "rows": rows,
            "columnHeaders": [
                {"name": "Episode Title", "type": "string"},
                {"name": "Episode ID", "type": "integer"},
                {"name": "Streams & Downloads", "type": "integer"},
                {"name": "Publish Time (UTC)", "type": "integer", "isDateTime": True},
                {"name": "Rank", "type": "integer"},
                {"name": "Episode URI", "type": "string"},
            ],
        },
    }


# ---------------------------------------------------------------------------
# Episode list transform
# ---------------------------------------------------------------------------


def transform_episodes_page(episodes_list: list[dict]) -> list[dict]:
    """
    get_all_episodes() list → old ``episodesPage`` shape.

    Each new episode dict is mapped to the old field names.
    Returns a flat list (the old API returned a generator/list of episode dicts).
    """
    result = []
    for ep in episodes_list:
        uri = ep.get("uri", "")
        episode_id = ep.get("episodeId", 0)
        title = ep.get("title")
        publish_seconds = ep.get("publishedOn", {}).get("seconds", 0)
        created_seconds = ep.get("createdOn", {}).get("seconds", 0)
        duration = ep.get("asset", {}).get("lengthMs", 0)
        total_plays = (
            (ep.get("analyticsStreamsAndDownloads") or {})
            .get("analyticsValue") or {}
        )
        total_plays = (total_plays.get("analyticsValue") or {}).get("value", 0)
        is_trailer = ep.get("episodeType") == "EPISODE_TYPE_TRAILER"
        is_video = ep.get("contentType") == "EPISODE_CONTENT_TYPE_VIDEO"

        result.append(
            {
                "episodeId": episode_id,
                "webEpisodeId": uri,
                "title": title,
                "publishOnUnixTimestamp": publish_seconds,
                "createdUnixTimestamp": created_seconds,
                "shareLinkPath": "",
                "shareLinkEmbedPath": "",
                "downloadUrl": ep.get("asset", {}).get("downloadUrl"),
                "totalPlays": total_plays,
                "duration": duration,
                "adCount": 0,
                "containsMusicSegments": False,
                "isPublishedToSpotifyExclusively": ep.get("isSpotifyExclusive", False),
                "wordpressPostMetadataId": None,
                "isTrailer": is_trailer,
                "isVideoEpisode": is_video,
                "audioCount": sum(
                    1
                    for mf in ep.get("asset", {}).get("mediaFiles", [])
                    if mf.get("mediaType") == "MEDIA_TYPE_AUDIO"
                ),
            }
        )
    return result


# ---------------------------------------------------------------------------
# Episode-level transforms
# ---------------------------------------------------------------------------


def transform_episode_plays(graphql_data: dict, episode_uri: str) -> dict:
    """
    getEpisodeStreamsAndDownloads → old ``episodePlays`` shape.

    Old shape::

        {"episodeId": N, "kind": "plays",
         "data": {"rows": [[timestamp, count], ...], "columnHeaders": [...]}}
    """
    points = _extract_time_series_points(
        graphql_data, "episodeByUri", "analytics"
    )
    rows = []
    for p in points:
        ts = _date_to_unix(p["date"])
        val = p.get("value", {}).get("value", 0)
        rows.append([ts, val])

    return {
        "episodeId": 0,
        "kind": "plays",
        "parameters": {
            "timeRange": [rows[0][0], rows[-1][0]] if rows else [],
            "timeInterval": 86400,
        },
        "data": {
            "rows": rows,
            "columnHeaders": [
                {"name": "Time (UTC)", "type": "integer", "isDateTime": True},
                {"name": "Plays", "type": "integer"},
            ],
        },
    }


def transform_episode_performance(graphql_data: dict, episode_uri: str) -> dict:
    """
    getEpisodePerformanceAllTime → old ``episodePerformance`` shape.

    Old shape: second-by-second rows ``[[0, "13"], [1, "13"], ...]``
    """
    inner = _extract_analytics_value(
        graphql_data, "episodeByUri", "episodePerformanceTotalAllTime"
    )
    points = inner.get("points", [])
    rows = [[p["second"], str(p["sampleCount"])] for p in points]

    return {
        "episodeId": 0,
        "kind": "performance",
        "parameters": None,
        "data": {
            "rows": rows,
            "columnHeaders": [
                {"name": "Second", "type": "integer"},
                {"name": "Sample Size", "type": "string"},
            ],
        },
    }


def transform_aggregated_performance(graphql_data: dict, episode_uri: str) -> dict:
    """
    getEpisodePerformanceAllTime → old ``aggregatedPerformance`` shape.

    Old shape::

        {"kind": "aggregatedPerformance", "data": {"rows": [
            ["percentile25", 57], ["percentile50", 50],
            ["percentile75", 36], ["percentile100", 21],
            ["averageListenSeconds", "1254"]
        ]}}

    The new API returns ``percentiles`` as a list of
    ``{audiencePercentage, completionPercentage}`` and
    ``medianCompletionSeconds``.
    """
    inner = _extract_analytics_value(
        graphql_data, "episodeByUri", "episodePerformanceTotalAllTime"
    )
    percentiles = inner.get("percentiles", [])
    median_seconds = inner.get("medianCompletionSeconds", 0)

    # Build a lookup: completionPercentage -> audiencePercentage
    pct_map = {}
    for p in percentiles:
        pct_map[p.get("completionPercentage")] = p.get("audiencePercentage", 0)

    rows = [
        ["percentile25", int(pct_map.get(25, 0))],
        ["percentile50", int(pct_map.get(50, 0))],
        ["percentile75", int(pct_map.get(75, 0))],
        ["percentile100", int(pct_map.get(100, 0))],
        ["averageListenSeconds", str(median_seconds) if median_seconds else None],
    ]

    return {
        "episodeId": 0,
        "kind": "aggregatedPerformance",
        "parameters": None,
        "data": {
            "rows": rows,
            "columnHeaders": [
                {"name": "Metric", "type": "string"},
                {"name": "Value", "type": "integer"},
            ],
        },
    }


def wrap_episode_metadata(
    graphql_data: dict,
    episode_uri: str,
    episode_enrichment: dict | None = None,
) -> dict:
    """
    getEpisodeMetadataForAnalytics → old ``podcastEpisode`` envelope shape.

    ``episode_enrichment`` is an optional dict from ``get_all_episodes()``
    keyed by episode URI.  It supplies ``duration``, ``created``,
    ``downloadUrl`` and ``description`` which the analytics metadata
    endpoint does not return.

    Old shape::

        {"allEpisodeWebIds": ["e215pm4"], "podcastId": "...",
         "podcastEpisodes": [{...}], "totalPodcastEpisodes": 1, ...}
    """
    ep = graphql_data.get("episodeByUri", {})
    enrich = (episode_enrichment or {}).get(episode_uri, {})

    publish_seconds = ep.get("publishedOn", {}).get("seconds", 0)
    thumbnail_images = ep.get("thumbnail", {}).get("images", [])
    episode_image = thumbnail_images[0].get("url") if thumbnail_images else None

    # Enrichment from get_all_episodes()
    duration_ms = enrich.get("asset", {}).get("lengthMs", 0)
    created_seconds = enrich.get("createdOn", {}).get("seconds", 0)
    download_url = enrich.get("asset", {}).get("downloadUrl", "")

    transformed_episode = {
        "adCount": 0,
        "created": "",
        "createdUnixTimestamp": created_seconds,
        "description": "",
        "duration": duration_ms,
        "hourOffset": 0,
        "isDeleted": False,
        "isPublished": True,
        "podcastEpisodeId": episode_uri,
        "publishOn": "",
        "publishOnUnixTimestamp": publish_seconds * 1000 if publish_seconds else 0,
        "title": ep.get("title", ""),
        "url": download_url or "",
        "trackedUrl": "",
        "episodeImage": episode_image,
        "shareLinkPath": "",
        "shareLinkEmbedPath": "",
    }

    return {
        "allEpisodeWebIds": [episode_uri],
        "podcastId": "",
        "podcastEpisodes": [transformed_episode],
        "totalPodcastEpisodes": 1,
        "vanitySlug": "",
        "stationCreatedDate": "",
    }


# ---------------------------------------------------------------------------
# Missing fields documentation
# ---------------------------------------------------------------------------
#
# The following fields existed in the old Anchor REST API fixtures but have
# NO direct equivalent in the new Spotify GraphQL API:
#
# Show-level:
#   - stationId (numeric) — removed, no longer needed
#   - parameters.timeRange — old API returned the Unix timestamp range
#   - parameters.timeInterval — old API returned the interval (e.g. 86400)
#   - parameters.minValue — old API returned a minimum threshold value
#   - data.translationMapping — old API returned label translation mappings
#   - data.colors — old API returned color hex values per category
#   - data.assets.flagUrlByGeo — old API returned flag image URLs
#
# Episode list (episodesPage):
#   - shareLinkPath — old API returned Anchor share link paths
#   - shareLinkEmbedPath — old API returned Anchor embed link paths
#   - containsMusicSegments — old API flagged music content
#   - wordpressPostMetadataId — old API returned WordPress integration ID
#
# Episode metadata (podcastEpisode):
#   - created / description / duration / hourOffset — not returned by
#     getEpisodeMetadataForAnalytics (only title, coverArt, publishedOn)
#   - url (audio file URL) — not returned by the analytics metadata endpoint
#   - trackedUrl (Spotify URL) — not returned by this endpoint
#   - vanitySlug / stationCreatedDate — old Anchor-specific fields
#
# Removed endpoint:
#   - playsByEpisode — old API returned a per-day-per-episode matrix
#     (timestamp × episodeIndex → count). No direct equivalent in new API.
#     Use totalPlaysByEpisode (all-time) + individual episodePlays instead.

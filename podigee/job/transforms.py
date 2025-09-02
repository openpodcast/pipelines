"""
Transform functions for Podigee data that can be tested independently.
"""
from job.date_utils import extract_date_str_from_iso, get_date_string, get_end_date_on_granularity


def transform_podigee_podcast_overview(overview_data):
    """
    Transform Podigee podcast overview data to OpenPodcast format.
    """
    if not overview_data or "meta" not in overview_data:
        return {"metrics": []}

    metrics = []

    if "unique_listeners_number" in overview_data:
        metrics.append({
            "start": extract_date_str_from_iso(overview_data["meta"]["from"]),
            "end": extract_date_str_from_iso(overview_data["meta"]["to"]),
            "dimension": "listeners",
            "subdimension": "unique",
            "value": overview_data["unique_listeners_number"]
        })
    if "unique_subscribers_number" in overview_data:
        metrics.append({
            "start": extract_date_str_from_iso(overview_data["meta"]["from"]),
            "end": extract_date_str_from_iso(overview_data["meta"]["to"]),
            "dimension": "subscribers",
            "subdimension": "unique",
            "value": overview_data["unique_subscribers_number"]
        })
    if "total_downloads" in overview_data:
        metrics.append({
            "start": extract_date_str_from_iso(overview_data["meta"]["from"]),
            "end": extract_date_str_from_iso(overview_data["meta"]["to"]),
            "dimension": "downloads",
            "subdimension": "downloads",
            "value": overview_data["total_downloads"]
        })

    return {"metrics": metrics}


def transform_podigee_analytics_to_metrics(analytics_data, store_downloads_only=False):
    """
    Transform Podigee analytics data to OpenPodcast metrics format.
    Expected format: {"metrics": [{"start": "date", "end": "date", "dimension": "string", "subdimension": "string", "value": number}]}
    """
    if not analytics_data or "objects" not in analytics_data:
        return {"metrics": []}

    aggregation_granularity = analytics_data.get("meta", {}).get("aggregation_granularity", "day")
    metrics = []
    
    for day_data in analytics_data["objects"]:
        date = extract_date_str_from_iso(day_data.get("downloaded_on", ""))
        if not date:
            continue
            
        start_date = get_date_string(date)
        end_date = get_end_date_on_granularity(aggregation_granularity, date)
            
        # Process downloads
        if "downloads" in day_data:
            for download_type, value in day_data["downloads"].items():
                metrics.append({
                    "start": start_date,
                    "end": end_date,
                    "dimension": "downloads",
                    "subdimension": download_type,
                    "value": value
                })

        if not store_downloads_only:
            # Process platforms
            if "platforms" in day_data:
                for platform, value in day_data["platforms"].items():
                    metrics.append({
                        "start": start_date,
                        "end": end_date,
                        "dimension": "platforms",
                        "subdimension": platform,
                        "value": value
                    })

            # Process clients
            if "clients" in day_data:
                for client, value in day_data["clients"].items():
                    metrics.append({
                        "start": start_date,
                        "end": end_date,
                        "dimension": "clients",
                        "subdimension": client,
                        "value": value
                    })
            
            # Process sources
            if "sources" in day_data:
                for source, value in day_data["sources"].items():
                    metrics.append({
                        "start": start_date,
                        "end": end_date,
                        "dimension": "sources",
                        "subdimension": source,
                        "value": value
                    })
                    
            # Process countries
            if "countries" in day_data:
                for country, value in day_data["countries"].items():
                    metrics.append({
                        "start": start_date,
                        "end": end_date,
                        "dimension": "countries",
                        "subdimension": country,
                        "value": value
                    })
    
    return {"metrics": metrics}

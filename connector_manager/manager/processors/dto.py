"""
Data Transfer Object for podcast processing tasks.
"""
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class PodcastTask:
    """Generic DTO for all podcast processing tasks."""
    account_id: str
    source_name: str
    source_podcast_id: str
    source_access_keys: Dict[str, Any]
    pod_name: str
    
    def get_access_key(self, key: str, default: str = "") -> str:
        """Helper to get access key value with default."""
        return self.source_access_keys.get(key, default)
    
    @property
    def openpodcast_api_token(self) -> str:
        return self.get_access_key("OPENPODCAST_API_TOKEN")
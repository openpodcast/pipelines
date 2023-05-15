from datetime import datetime
from typing import Any, Callable, Dict
from dataclasses import dataclass


@dataclass
class FetchParams:
    """
    A dataclass that holds the parameters for a single API call.
    """

    openpodcast_endpoint: str
    anchor_call: Callable[[], Any]
    start_date: datetime
    end_date: datetime
    meta: Dict[str, Any] = None

    def output_path(self) -> str:
        """
        Returns the path to the output file for this FetchParams object.
        """
        return f"{self.save_location}/{self.openpodcast_endpoint}-{self.start_date}-{self.end_date}.json"

from datetime import date
from unittest.mock import Mock, patch

import pytest
import requests

from job.open_podcast import OpenPodcastConnector


def test_failed_save_raises():
    connector = OpenPodcastConnector("https://example.invalid", "token", "show")
    response = Mock(status_code=500, text="private response")
    day = date(2026, 9, 29)

    with patch("job.open_podcast.requests.post", return_value=response):
        with pytest.raises(requests.HTTPError) as exc:
            connector.post("listeners", None, {}, day, day)
    assert exc.value.response is response
    assert "500" in str(exc.value)
    assert "private response" not in str(exc.value)

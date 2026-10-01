from datetime import date
from unittest.mock import Mock, patch

import pytest
import requests

from job.open_podcast import OpenPodcastConnector


@pytest.mark.parametrize("status", [200, 302, 400, 429, 500])
def test_ingestion_status_is_not_ignored(status):
    connector = OpenPodcastConnector("https://example.invalid", "fake-token", "show")
    response = Mock(status_code=status, text="secret-response")
    with patch("job.open_podcast.requests.post", return_value=response) as post:
        if status == 200:
            assert (
                connector.post(
                    "listeners", None, {}, date(2026, 9, 29), date(2026, 9, 29)
                )
                is response
            )
        else:
            with pytest.raises(requests.HTTPError) as exc:
                connector.post(
                    "listeners", None, {}, date(2026, 9, 29), date(2026, 9, 29)
                )
            assert str(status) in str(exc.value)
            assert "secret-response" not in str(exc.value)
            response.close.assert_called_once()
    assert post.call_count == 1
    assert post.call_args.kwargs["timeout"] == 60

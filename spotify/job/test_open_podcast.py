import datetime as dt
from unittest.mock import Mock, patch

import pytest
import requests

from job.open_podcast import OpenPodcastConnector


@pytest.fixture
def client():
    return OpenPodcastConnector("https://example.invalid", "fake-token", "show-id")


def response(status):
    result = Mock(spec=requests.Response)
    result.status_code = status
    result.text = "sensitive response body"
    return result


def post(client, data=None):
    return client.post(
        "listeners",
        None,
        data if data is not None else {"counts": []},
        dt.date(2026, 9, 29),
        dt.date(2026, 9, 29),
    )


def test_success_returns_response(client):
    success = response(200)
    with patch("job.open_podcast.requests.post", return_value=success) as request:
        assert post(client) is success
    assert request.call_count == 1
    assert request.call_args.kwargs["timeout"] == 60
    assert request.call_args.kwargs["allow_redirects"] is False


@pytest.mark.parametrize(
    "status", [201, 204, 301, 302, 400, 401, 403, 404, 422, 429, 500, 502, 503, 504]
)
def test_non_200_fails_without_retry_or_sensitive_error_text(client, status):
    failed = response(status)
    with patch("job.open_podcast.requests.post", return_value=failed) as request:
        with pytest.raises(requests.HTTPError) as exc:
            post(client)
    assert exc.value.response is failed
    assert str(status) in str(exc.value)
    assert "sensitive" not in str(exc.value)
    assert "fake-token" not in str(exc.value)
    assert request.call_count == 1
    failed.close.assert_called_once()


@pytest.mark.parametrize("error", [requests.Timeout, requests.ConnectionError])
def test_transport_errors_propagate_without_retry(client, error):
    with patch("job.open_podcast.requests.post", side_effect=error()) as request:
        with pytest.raises(error):
            post(client)
    assert request.call_count == 1


def test_generator_is_materialized(client):
    data = (entry for entry in [{"date": "2026-09-29", "count": 0}])
    with patch("job.open_podcast.requests.post", return_value=response(200)) as request:
        post(client, data)
    assert request.call_args.kwargs["json"]["data"] == {
        "listeners": [{"date": "2026-09-29", "count": 0}]
    }

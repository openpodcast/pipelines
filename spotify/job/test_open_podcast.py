import datetime as dt
from unittest.mock import Mock, patch

import pytest
import requests

from job.open_podcast import OpenPodcastConnector, retry_delay


@pytest.fixture
def client():
    return OpenPodcastConnector("https://example.invalid", "fake-token", "show-id")


def response(status, headers=None):
    result = Mock(spec=requests.Response)
    result.status_code = status
    result.headers = headers or {}
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


def test_success_does_not_retry(client):
    success = response(200)
    with patch("job.open_podcast.requests.post", return_value=success) as request:
        with patch("job.open_podcast.sleep") as sleep:
            assert post(client) is success
    assert request.call_count == 1
    assert request.call_args.kwargs["timeout"] == 60
    assert request.call_args.kwargs["allow_redirects"] is False
    sleep.assert_not_called()


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_status_retries_same_payload(client, status):
    failed, success = response(status), response(200)
    with patch(
        "job.open_podcast.requests.post", side_effect=[failed, success]
    ) as request:
        with patch("job.open_podcast.sleep") as sleep:
            assert post(client) is success
    assert request.call_count == 2
    assert (
        request.call_args_list[0].kwargs["json"]
        is request.call_args_list[1].kwargs["json"]
    )
    failed.close.assert_called_once()
    sleep.assert_called_once_with(1)


@pytest.mark.parametrize("status", [201, 204, 301, 302, 400, 401, 403, 404, 422])
def test_other_status_fails_without_retry(client, status):
    failed = response(status)
    with patch("job.open_podcast.requests.post", return_value=failed) as request:
        with patch("job.open_podcast.sleep") as sleep:
            with pytest.raises(requests.HTTPError) as exc:
                post(client)
    assert str(status) in str(exc.value)
    assert "sensitive" not in str(exc.value)
    assert "fake-token" not in str(exc.value)
    assert request.call_count == 1
    sleep.assert_not_called()
    failed.close.assert_called_once()


def test_exhausted_status_retries_fail(client):
    with patch(
        "job.open_podcast.requests.post", side_effect=[response(503) for _ in range(3)]
    ) as request:
        with patch("job.open_podcast.sleep") as sleep:
            with pytest.raises(requests.HTTPError):
                post(client)
    assert request.call_count == 3
    assert [c.args[0] for c in sleep.call_args_list] == [1, 2]


@pytest.mark.parametrize("error", [requests.Timeout, requests.ConnectionError])
def test_transport_failure_recovers(client, error):
    success = response(200)
    with patch(
        "job.open_podcast.requests.post", side_effect=[error(), success]
    ) as request:
        with patch("job.open_podcast.sleep"):
            assert post(client) is success
    assert request.call_count == 2


@pytest.mark.parametrize("error", [requests.Timeout, requests.ConnectionError])
def test_transport_failure_exhausted(client, error):
    with patch("job.open_podcast.requests.post", side_effect=error()) as request:
        with patch("job.open_podcast.sleep") as sleep:
            with pytest.raises(error):
                post(client)
    assert request.call_count == 3
    assert sleep.call_count == 2


def test_rate_limit_retry_after_is_bounded(client):
    with patch(
        "job.open_podcast.requests.post",
        side_effect=[response(429, {"Retry-After": "99999"}), response(200)],
    ):
        with patch("job.open_podcast.sleep") as sleep:
            post(client)
    sleep.assert_called_once_with(60)


@pytest.mark.parametrize(
    "header,expected",
    [(None, 2), ("invalid", 2), ("-10", 2), ("10", 10), ("99999", 60), ("inf", 2)],
)
def test_retry_after_values(header, expected):
    assert retry_delay(header, 2) == expected


def test_retry_after_http_date():
    from email.utils import format_datetime

    future = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=30)
    assert 28 <= retry_delay(format_datetime(future, usegmt=True), 2) <= 30


def test_generator_is_materialized_once(client):
    data = (entry for entry in [{"date": "2026-09-29", "count": 0}])
    with patch(
        "job.open_podcast.requests.post", side_effect=[response(503), response(200)]
    ) as request:
        with patch("job.open_podcast.sleep"):
            post(client, data)
    for call in request.call_args_list:
        assert call.kwargs["json"]["data"] == {
            "listeners": [{"date": "2026-09-29", "count": 0}]
        }


@pytest.mark.parametrize(
    "endpoint",
    [
        "metadata",
        "episodeMetadata",
        "performance",
        "impressions_funnel",
        "new-endpoint",
    ],
)
@pytest.mark.parametrize("failure", ["timeout", "status"])
def test_snapshot_and_unknown_endpoints_never_replay(client, endpoint, failure):
    # A timeout can occur after a commit. These handlers choose the server's
    # current date, so a replay after midnight could write a different row.
    outcome = requests.Timeout() if failure == "timeout" else response(503)
    with patch("job.open_podcast.requests.post", side_effect=[outcome]) as request:
        with patch("job.open_podcast.sleep") as sleep:
            with pytest.raises(requests.RequestException):
                client.post(
                    endpoint,
                    None,
                    {"snapshot": 1},
                    dt.date(2026, 9, 29),
                    dt.date(2026, 9, 29),
                )
    assert request.call_count == 1
    sleep.assert_not_called()

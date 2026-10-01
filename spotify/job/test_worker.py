from datetime import datetime
from unittest.mock import Mock, patch

import pytest
import requests

from job.fetch_params import FetchParams
from job.worker import fetch


@pytest.mark.parametrize(
    "data,meta,success,saved",
    [
        ({"counts": []}, None, False, False),
        (None, None, False, False),
        ({"counts": [{"date": "2026-09-29", "count": 0}]}, None, True, True),
        ({"counts": []}, {"episode": "episode-id"}, True, True),
    ],
)
def test_listener_results(data, meta, success, saved):
    params = FetchParams(
        "listeners",
        Mock(return_value=data),
        datetime(2026, 9, 29),
        datetime(2026, 9, 29),
        meta,
    )
    connector = Mock()
    assert fetch(connector, params) is success
    assert connector.post.called is saved
    if saved:
        connector.post.assert_called_once_with(
            "listeners", meta, data, params.start_date, params.end_date
        )


@pytest.mark.parametrize("failure_source", ["fetch", "post"])
@pytest.mark.parametrize("error", [requests.HTTPError, RuntimeError])
def test_failures_are_reported_without_secrets_and_delay_is_preserved(
    failure_source, error
):
    params = FetchParams(
        "metadata",
        Mock(return_value={"name": "show"}),
        datetime(2026, 9, 29),
        datetime(2026, 9, 29),
    )
    connector = Mock()
    failing_call = params.spotify_call if failure_source == "fetch" else connector.post
    failing_call.side_effect = error("secret-response")
    with patch("job.worker.sleep") as sleep, patch("job.worker.logger.error") as log:
        assert fetch(connector, params, 1) is False
    sleep.assert_called_once_with(1)
    template, *values = log.call_args.args
    message = template.format(*values)
    assert "metadata" in message and error.__name__ in message
    assert "secret-response" not in message

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
import requests
from spotifyconnector.connector import CredentialsExpired

from job import worker
from job.fetch_params import FetchParams


def task(data=None, endpoint="metadata", meta=None):
    return FetchParams(
        endpoint,
        Mock(return_value=data),
        datetime(2026, 7, 22),
        datetime(2026, 7, 23),
        meta,
    )


@pytest.mark.parametrize(
    "data", [None, {}, [], {"counts": None}, {"counts": {}}, {"counts": []}]
)
def test_empty_show_listeners_are_not_saved(data):
    connector = Mock()
    with pytest.raises(worker.EmptyListenerData):
        worker.fetch(connector, task(data, "listeners"))
    connector.post.assert_not_called()


@pytest.mark.parametrize(
    "data,meta",
    [
        ({"counts": [{"date": "2026-07-22", "count": 0}]}, None),
        ({"counts": []}, {"episode": "episode-id"}),
    ],
)
def test_explicit_zero_and_empty_episode_counts_are_saved_unchanged(data, meta):
    connector = Mock()
    params = task(data, "listeners", meta)
    worker.fetch(connector, params)
    connector.post.assert_called_once_with(
        "listeners", meta, data, params.start_date, params.end_date
    )


def test_empty_other_endpoint_is_still_skipped():
    connector = Mock()
    worker.fetch(connector, task(None))
    connector.post.assert_not_called()


@pytest.mark.parametrize("workers", [1, 3])
@pytest.mark.parametrize("failure_source", ["fetch", "post"])
@pytest.mark.parametrize(
    "error", [requests.HTTPError, CredentialsExpired, RuntimeError]
)
def test_failed_task_does_not_prevent_later_tasks(workers, failure_source, error):
    tasks = [task({"index": i}) for i in range(3)]
    connector = Mock()
    if failure_source == "fetch":
        tasks[0].spotify_call.side_effect = error("sensitive details")
    else:

        def post(endpoint, meta, data, start, end):
            if data["index"] == 0:
                raise error("sensitive details")

        connector.post.side_effect = post

    with patch.object(worker, "sleep") as sleep:
        assert worker.run_tasks(tasks, connector, 0.25, workers) == 1
    for params in tasks:
        params.spotify_call.assert_called_once_with()
    assert sorted(call.args[2]["index"] for call in connector.post.call_args_list) == (
        [1, 2] if failure_source == "fetch" else [0, 1, 2]
    )
    assert sleep.call_count == 3
    assert all(call.args == (0.25,) for call in sleep.call_args_list)


@pytest.mark.parametrize(
    "data,failures", [([], 0), ([{"name": "show"}], 0), ([{"counts": []}], 1)]
)
def test_success_empty_batch_and_invalid_listeners(data, failures):
    connector = Mock()
    tasks = [task(d, "listeners" if "counts" in d else "metadata") for d in data]
    assert worker.run_tasks(tasks, connector, 0, 2) == failures
    assert connector.post.call_count == len(tasks) - failures


@pytest.mark.parametrize(
    "workers,delay", [(0, 0), (-1, 0), (1, -1), (1, float("nan")), (1, float("inf"))]
)
def test_invalid_configuration_fails_before_work(workers, delay):
    params = task({"name": "show"})
    with pytest.raises(ValueError):
        worker.run_tasks([params], Mock(), delay, workers)
    params.spotify_call.assert_not_called()


def test_failure_log_has_context_without_exception_details():
    params = task({"name": "show"})
    response = requests.Response()
    response.status_code = 403
    params.spotify_call.side_effect = requests.HTTPError(
        "secret-response", response=response
    )
    with patch.object(worker.logger, "error") as log:
        assert worker.run_tasks([params], Mock(), 0, 1) == 1
    message, *values = log.call_args.args
    rendered = message.format(*values)
    assert "metadata" in rendered and "403" in rendered
    assert "secret-response" not in rendered

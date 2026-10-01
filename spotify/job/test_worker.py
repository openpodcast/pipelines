from pathlib import Path
import subprocess
import sys
import textwrap
from datetime import datetime
from unittest.mock import Mock

import pytest
import requests

from job.fetch_params import FetchParams
from job import worker


def make_params(data=None, endpoint="metadata", meta=None):
    return FetchParams(
        openpodcast_endpoint=endpoint,
        spotify_call=Mock(return_value=data),
        start_date=datetime(2026, 7, 22),
        end_date=datetime(2026, 7, 23),
        meta=meta,
    )


@pytest.mark.parametrize("status", [401, 429, 500])
def test_fetch_propagates_http_error_without_post(status):
    response = requests.Response()
    response.status_code = status
    error = requests.HTTPError(response=response)
    params = make_params()
    params.spotify_call.side_effect = error
    connector = Mock()

    with pytest.raises(requests.HTTPError) as raised:
        worker.fetch(connector, params)

    assert raised.value is error
    params.spotify_call.assert_called_once_with()
    connector.post.assert_not_called()


@pytest.mark.parametrize(
    "data",
    [
        None,
        {},
        [],
        "invalid",
        {"counts": None},
        {"counts": {}},
        {"counts": "invalid"},
        {"counts": []},
        {"error": "unavailable"},
    ],
)
@pytest.mark.parametrize("meta", [None, {"show": "show-id"}])
def test_fetch_rejects_missing_or_malformed_show_listener_counts(data, meta):
    params = make_params(data, "listeners", meta)
    connector = Mock()

    with pytest.raises(ValueError, match="daily counts"):
        worker.fetch(connector, params)

    params.spotify_call.assert_called_once_with()
    connector.post.assert_not_called()


@pytest.mark.parametrize(
    "data,meta",
    [
        ({"counts": [{"date": "2026-07-22", "count": 0}]}, None),
        ({"counts": []}, {"episode": "episode-id"}),
    ],
    ids=["explicit-show-zero", "empty-episode-counts"],
)
def test_fetch_posts_valid_listener_response_unchanged(data, meta):
    params = make_params(data, "listeners", meta)
    connector = Mock()

    worker.fetch(connector, params)

    params.spotify_call.assert_called_once_with()
    connector.post.assert_called_once_with(
        "listeners", meta, data, params.start_date, params.end_date
    )
    assert connector.post.call_args.args[2] is data


@pytest.mark.parametrize(
    "endpoint,meta",
    [
        ("metadata", None),
        ("listeners", {"episode": "episode-id"}),
    ],
)
@pytest.mark.parametrize("data", [None, {}, []])
def test_fetch_preserves_empty_non_show_response_behavior(endpoint, meta, data):
    connector = Mock()
    params = make_params(data, endpoint, meta)

    worker.fetch(connector, params)

    params.spotify_call.assert_called_once_with()
    connector.post.assert_not_called()


def test_fetch_propagates_ingestion_failure():
    error = requests.HTTPError("ingestion failed")
    connector = Mock()
    connector.post.side_effect = error
    params = make_params({"name": "test show"})

    with pytest.raises(requests.HTTPError) as raised:
        worker.fetch(connector, params)

    assert raised.value is error
    connector.post.assert_called_once_with(
        "metadata",
        None,
        {"name": "test show"},
        params.start_date,
        params.end_date,
    )


# A dead worker can leave Queue.join() (or a non-daemon thread) stuck forever.
# Isolate every run_tasks call so a regression is killed, not left inside pytest.
def run_worker_script(script, tmp_path):
    setup = """
from datetime import datetime
from unittest.mock import Mock, patch
import threading
import requests
from spotifyconnector.connector import CredentialsExpired
from job.fetch_params import FetchParams
from job import worker

requests.sessions.Session.request = Mock(side_effect=AssertionError("network forbidden"))

def task(data=None):
    return FetchParams("metadata", Mock(return_value=data), datetime(2026, 7, 22),
                       datetime(2026, 7, 23))
"""
    try:
        result = subprocess.run(
            [sys.executable, "-c", setup + textwrap.dedent(script)],
            cwd=tmp_path,
            env={"PYTHONPATH": str(Path(__file__).resolve().parents[1])},
            capture_output=True,
            text=True,
            timeout=10,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(
            "run_tasks did not terminate within 10 seconds (possible queue hang)"
        )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("num_workers", [1, 3])
@pytest.mark.parametrize("failure_source", ["fetch", "post"])
@pytest.mark.parametrize(
    "exception", ["requests.HTTPError", "CredentialsExpired", "RuntimeError"]
)
def test_run_tasks_counts_failures_and_continues(
    num_workers, failure_source, exception, tmp_path
):
    run_worker_script(
        f"""
        connector = Mock()
        tasks = [task({{"index": i}}) for i in range(8)]
        error = {exception}("test failure")
        if {failure_source!r} == "fetch":
            for i in (0, 1):
                tasks[i].spotify_call.side_effect = error
        else:
            def post(endpoint, meta, data, start, end):
                if data["index"] in (0, 1):
                    raise error
            connector.post.side_effect = post

        before = set(threading.enumerate())
        with patch.object(worker, "sleep") as sleep:
            failures = worker.run_tasks(tasks, connector, 0.25, {num_workers})
        assert failures == 2, failures
        for params in tasks:
            params.spotify_call.assert_called_once_with()
        posted = [call.args[2]["index"] for call in connector.post.call_args_list]
        expected = range(2, 8) if {failure_source!r} == "fetch" else range(8)
        assert sorted(posted) == list(expected), posted
        assert sleep.call_count == len(tasks), sleep.call_count
        assert all(call.args == (0.25,) for call in sleep.call_args_list)
        assert set(threading.enumerate()) == before, "worker threads were not joined"
        """,
        tmp_path,
    )


@pytest.mark.parametrize("num_workers", [1, 3])
@pytest.mark.parametrize("task_count", [0, 1, 8])
def test_run_tasks_success_and_empty_queue_terminate(num_workers, task_count, tmp_path):
    run_worker_script(
        f"""
        connector = Mock()
        tasks = [task({{"index": i}}) for i in range({task_count})]
        before = set(threading.enumerate())
        assert worker.run_tasks(tasks, connector, 0, {num_workers}) == 0
        for params in tasks:
            params.spotify_call.assert_called_once_with()
        assert connector.post.call_count == len(tasks)
        assert set(threading.enumerate()) == before
        """,
        tmp_path,
    )


def test_run_tasks_counts_invalid_show_listeners_and_continues(tmp_path):
    run_worker_script(
        """
        connector = Mock()
        tasks = [task(data) for data in ({"counts": []}, {"counts": None},
                                       {"counts": [{"date": "2026-07-22", "count": 0}]})]
        for params in tasks:
            params.openpodcast_endpoint = "listeners"
        assert worker.run_tasks(tasks, connector, 0, 1) == 2
        for params in tasks:
            params.spotify_call.assert_called_once_with()
        connector.post.assert_called_once_with(
            "listeners", None, tasks[2].spotify_call.return_value,
            tasks[2].start_date, tasks[2].end_date,
        )
        """,
        tmp_path,
    )


@pytest.mark.parametrize("num_workers,delay", [(0, 0), (-1, 0), (1, -0.01)])
def test_run_tasks_rejects_invalid_configuration_before_work(
    num_workers, delay, tmp_path
):
    run_worker_script(
        f"""
        connector = Mock()
        params = task({{"name": "test"}})
        with patch.object(worker.threading, "Thread") as thread:
            try:
                worker.run_tasks([params], connector, {delay}, {num_workers})
            except ValueError:
                pass
            else:
                raise AssertionError("invalid configuration was accepted")
        thread.assert_not_called()
        params.spotify_call.assert_not_called()
        connector.post.assert_not_called()
        """,
        tmp_path,
    )


@pytest.mark.parametrize("delay", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_delay_is_rejected(delay):
    from job.worker import run_tasks

    with pytest.raises(ValueError, match="finite"):
        run_tasks([], Mock(), delay, 1)

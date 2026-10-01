import os
import runpy
import sys
from types import ModuleType
from unittest.mock import Mock, patch

import pytest
import requests

from job import open_podcast, worker


@pytest.fixture
def main_dependencies(monkeypatch):
    # Replace both the connector import and environment loaders before runpy;
    # no real cookies, *_FILE paths, .env files, or network are consulted.
    connector_module = ModuleType("spotifyconnector")
    exceptions_module = ModuleType("spotifyconnector.connector")

    class CredentialsExpired(Exception):
        pass

    exceptions_module.CredentialsExpired = CredentialsExpired
    connector_module.connector = exceptions_module
    spotify = Mock()
    spotify.episodes.return_value = []
    connector_module.SpotifyConnector = Mock(return_value=spotify)
    monkeypatch.setitem(sys.modules, "spotifyconnector", connector_module)
    monkeypatch.setitem(sys.modules, "spotifyconnector.connector", exceptions_module)

    values = {
        "SPOTIFY_SP_DC": "fake-cookie",
        "SPOTIFY_SP_KEY": "fake-key",
        "SPOTIFY_PODCAST_ID": "test-show",
        "OPENPODCAST_API_TOKEN": "fake-token",
        "START_DATE": "2026-07-22",
        "END_DATE": "2026-07-22",
    }
    env_module = ModuleType("job.load_env")
    env_module.load_env = Mock(
        side_effect=lambda key, default=None: values.get(key, default)
    )
    env_module.load_file_or_env = env_module.load_env
    monkeypatch.setitem(sys.modules, "job.load_env", env_module)

    connector = Mock()
    connector.health.return_value.status_code = 200
    monkeypatch.setattr(
        open_podcast, "OpenPodcastConnector", Mock(return_value=connector)
    )
    run_tasks = Mock(return_value=0)
    monkeypatch.setattr(worker, "run_tasks", run_tasks)
    monkeypatch.setattr(
        requests.sessions.Session,
        "request",
        Mock(side_effect=AssertionError("network forbidden")),
    )
    with patch.dict(os.environ, {"NUM_WORKERS": "2", "TASK_DELAY": "0"}, clear=True):
        yield connector_module, connector, run_tasks


@pytest.mark.parametrize("failures", [0, 1, 3])
def test_main_exits_nonzero_only_when_tasks_failed(main_dependencies, failures, capsys):
    _, connector, run_tasks = main_dependencies
    run_tasks.return_value = failures

    if failures:
        with pytest.raises(SystemExit) as raised:
            runpy.run_module("job.__main__", run_name="__main__")
        assert raised.value.code == 1
    else:
        runpy.run_module("job.__main__", run_name="__main__")

    run_tasks.assert_called_once()
    tasks, actual_connector, delay, workers = run_tasks.call_args.args
    assert tasks
    assert any(task.openpodcast_endpoint == "listeners" for task in tasks)
    assert actual_connector is connector
    assert (delay, workers) == (0.0, 2)
    connector.health.assert_called_once_with()
    assert ("All items processed." in capsys.readouterr().out) == (failures == 0)


def test_main_exits_nonzero_on_expired_credentials(main_dependencies, capsys):
    connector_module, connector, run_tasks = main_dependencies
    connector_module.SpotifyConnector.side_effect = (
        connector_module.connector.CredentialsExpired("test expiration")
    )

    with pytest.raises(SystemExit) as raised:
        runpy.run_module("job.__main__", run_name="__main__")

    assert raised.value.code == 1
    connector.health.assert_not_called()
    run_tasks.assert_not_called()
    assert "All items processed." not in capsys.readouterr().out

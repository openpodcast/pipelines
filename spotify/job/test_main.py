import os
import runpy
from unittest.mock import Mock, patch

import pytest
import requests
import spotifyconnector

from job import load_env, open_podcast, worker


@pytest.mark.parametrize("failures", [0, 1])
@pytest.mark.parametrize("workers", [1, 3])
def test_job_exit_reflects_task_failures(monkeypatch, failures, workers, capsys):
    # No real environment files, credentials, or network calls.
    values = {
        "SPOTIFY_SP_DC": "fake-cookie",
        "SPOTIFY_SP_KEY": "fake-key",
        "SPOTIFY_PODCAST_ID": "test-show",
        "OPENPODCAST_API_TOKEN": "fake-token",
        "START_DATE": "2026-07-22",
        "END_DATE": "2026-07-22",
    }

    def loader(key, default=None):
        return values.get(key, default)

    monkeypatch.setattr(load_env, "load_env", loader)
    monkeypatch.setattr(load_env, "load_file_or_env", loader)
    spotify = Mock()
    spotify.episodes.return_value = []
    monkeypatch.setattr(
        spotifyconnector, "SpotifyConnector", Mock(return_value=spotify)
    )
    connector = Mock()
    connector.health.return_value.status_code = 200
    monkeypatch.setattr(
        open_podcast, "OpenPodcastConnector", Mock(return_value=connector)
    )
    fetch = Mock()
    fetch.side_effect = lambda api, params, delay: (
        not (failures and params.openpodcast_endpoint == "metadata")
    )
    monkeypatch.setattr(worker, "fetch", fetch)
    monkeypatch.setattr(
        requests.sessions.Session,
        "request",
        Mock(side_effect=AssertionError("network forbidden")),
    )

    with patch.dict(
        os.environ, {"NUM_WORKERS": str(workers), "TASK_DELAY": "0"}, clear=True
    ):
        if failures:
            with pytest.raises(SystemExit) as exc:
                runpy.run_module("job.__main__", run_name="__main__")
            assert exc.value.code == 1
        else:
            runpy.run_module("job.__main__", run_name="__main__")
    # Even when the first task fails, the pool must finish the remaining tasks.
    assert fetch.call_count > 1
    assert any(
        call.args[1].openpodcast_endpoint == "listeners"
        for call in fetch.call_args_list
    )
    assert all(
        call.args[0] is connector and call.args[2] == 0.0
        for call in fetch.call_args_list
    )
    assert ("All items processed." in capsys.readouterr().out) == (failures == 0)

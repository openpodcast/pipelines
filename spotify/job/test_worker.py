from datetime import datetime
from unittest.mock import Mock, patch

import pytest
import requests

from job.fetch_params import FetchParams
from job.worker import fetch


@pytest.mark.parametrize("failure", [None, "fetch", "save"])
def test_fetch_reports_task_outcome(failure):
    connector = Mock()
    source = Mock(return_value={"name": "show"})
    if failure:
        failing_call = source if failure == "fetch" else connector.post
        failing_call.side_effect = requests.HTTPError("request failed")
    day = datetime(2026, 9, 29)
    params = FetchParams("metadata", source, day, day)

    with patch("job.worker.sleep") as sleep:
        assert fetch(connector, params, delay=1) is (failure is None)
    sleep.assert_called_once_with(1)
    source.assert_called_once_with()
    assert connector.post.call_count == (0 if failure == "fetch" else 1)

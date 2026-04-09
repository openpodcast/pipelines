import datetime as dt
import unittest
from unittest.mock import Mock

from job.fetch_params import FetchParams
from job.worker import fetch


class TestWorker(unittest.TestCase):
    def test_fetch_skips_processing_error(self):
        openpodcast = Mock()
        params = FetchParams(
            openpodcast_endpoint="playsByGender",
            anchor_call=Mock(side_effect=AttributeError("bad payload")),
            start_date=dt.date(2026, 4, 6),
            end_date=dt.date(2026, 4, 8),
        )

        fetch(openpodcast, params)

        openpodcast.post.assert_not_called()


if __name__ == "__main__":
    unittest.main()

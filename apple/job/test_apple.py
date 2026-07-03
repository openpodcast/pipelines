import sys
import types
import unittest
from unittest.mock import Mock, call, patch

appleconnector = types.ModuleType("appleconnector")
appleconnector.AppleConnector = Mock
sys.modules.setdefault("appleconnector", appleconnector)

loguru = types.ModuleType("loguru")
loguru.logger = Mock()
sys.modules.setdefault("loguru", loguru)

from job.apple import fetch_all_cookies


class TestFetchAllCookies(unittest.TestCase):
    @patch("job.apple.requests.get")
    def test_sends_podcast_id_as_query_param(self, get_mock):
        response = Mock(status_code=200)
        response.json.return_value = []
        get_mock.return_value = response

        fetch_all_cookies("token", "https://automation.example.com/cookies", "12345")

        get_mock.assert_called_once_with(
            "https://automation.example.com/cookies",
            headers={"Authorization": "Bearer token"},
            params={"podcastId": "12345"},
            timeout=600,
        )

    @patch("job.apple.time.sleep")
    @patch("job.apple.requests.get")
    def test_retries_with_podcast_id_as_query_param(self, get_mock, sleep_mock):
        failed_response = Mock(status_code=500, text="nope")
        successful_response = Mock(status_code=200)
        successful_response.json.return_value = []
        get_mock.side_effect = [failed_response, successful_response]

        fetch_all_cookies("token", "https://automation.example.com/cookies", "12345")

        expected_call = call(
            "https://automation.example.com/cookies",
            headers={"Authorization": "Bearer token"},
            params={"podcastId": "12345"},
            timeout=600,
        )
        self.assertEqual(get_mock.call_args_list, [expected_call, expected_call])
        sleep_mock.assert_called_once_with(20 * 60)


if __name__ == "__main__":
    unittest.main()

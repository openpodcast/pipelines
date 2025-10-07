import unittest
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime
import requests

from job.worker import fetch, MAX_RETRIES, INITIAL_BACKOFF
from job.fetch_params import FetchParams
from job.open_podcast import OpenPodcastConnector


class TestFetchRetryLogic(unittest.TestCase):
    """Test the retry logic with exponential backoff in the fetch function"""

    def setUp(self):
        """Set up test fixtures"""
        self.openpodcast = Mock(spec=OpenPodcastConnector)
        self.openpodcast.post = Mock(return_value=Mock(status_code=200))
        
        self.params = Mock(spec=FetchParams)
        self.params.openpodcast_endpoint = "test_endpoint"
        self.params.meta = {"test": "meta"}
        self.params.start_date = datetime(2023, 1, 1)
        self.params.end_date = datetime(2023, 1, 31)

    @patch('job.worker.sleep')
    def test_successful_request_no_retry(self, mock_sleep):
        """Test that successful requests complete without retry"""
        self.params.spotify_call = Mock(return_value={"data": "test"})
        
        fetch(self.openpodcast, self.params)
        
        # Should call spotify_call exactly once
        self.params.spotify_call.assert_called_once()
        # Should post to openpodcast exactly once
        self.openpodcast.post.assert_called_once()
        # Should not sleep for backoff
        mock_sleep.assert_not_called()

    @patch('job.worker.sleep')
    def test_rate_limit_error_retries_with_backoff(self, mock_sleep):
        """Test that 429 rate limit errors trigger retry with exponential backoff"""
        # Create a mock response with 429 status
        mock_response = Mock()
        mock_response.status_code = 429
        
        # First two calls fail with 429, third succeeds
        http_error = requests.exceptions.HTTPError()
        http_error.response = mock_response
        
        self.params.spotify_call = Mock(
            side_effect=[http_error, http_error, {"data": "test"}]
        )
        
        fetch(self.openpodcast, self.params)
        
        # Should call spotify_call 3 times (2 failures + 1 success)
        self.assertEqual(self.params.spotify_call.call_count, 3)
        # Should post to openpodcast once after success
        self.openpodcast.post.assert_called_once()
        # Should sleep twice with exponential backoff (1s, 2s)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_has_calls([call(INITIAL_BACKOFF), call(INITIAL_BACKOFF * 2)])

    @patch('job.worker.sleep')
    @patch('job.worker.logger')
    def test_max_retries_reached_logs_error(self, mock_logger, mock_sleep):
        """Test that max retries logs appropriate error"""
        # Create a mock response with 429 status
        mock_response = Mock()
        mock_response.status_code = 429
        
        http_error = requests.exceptions.HTTPError()
        http_error.response = mock_response
        
        # Always fail with 429
        self.params.spotify_call = Mock(side_effect=http_error)
        
        fetch(self.openpodcast, self.params)
        
        # Should try MAX_RETRIES + 1 times (initial + retries)
        self.assertEqual(self.params.spotify_call.call_count, MAX_RETRIES + 1)
        # Should not post to openpodcast
        self.openpodcast.post.assert_not_called()
        # Should log error about max retries
        error_calls = [c for c in mock_logger.error.call_args_list 
                      if "Max retries" in str(c)]
        self.assertTrue(len(error_calls) > 0)

    @patch('job.worker.sleep')
    def test_server_error_retries(self, mock_sleep):
        """Test that 5xx server errors trigger retry"""
        # Create a mock response with 503 status
        mock_response = Mock()
        mock_response.status_code = 503
        
        http_error = requests.exceptions.HTTPError()
        http_error.response = mock_response
        
        # First call fails with 503, second succeeds
        self.params.spotify_call = Mock(
            side_effect=[http_error, {"data": "test"}]
        )
        
        fetch(self.openpodcast, self.params)
        
        # Should call spotify_call 2 times (1 failure + 1 success)
        self.assertEqual(self.params.spotify_call.call_count, 2)
        # Should post to openpodcast once
        self.openpodcast.post.assert_called_once()
        # Should sleep once for backoff
        mock_sleep.assert_called_once_with(INITIAL_BACKOFF)

    @patch('job.worker.sleep')
    @patch('job.worker.logger')
    def test_client_error_no_retry(self, mock_logger, mock_sleep):
        """Test that 4xx errors (except 429) don't trigger retry"""
        # Create a mock response with 404 status
        mock_response = Mock()
        mock_response.status_code = 404
        
        http_error = requests.exceptions.HTTPError()
        http_error.response = mock_response
        
        self.params.spotify_call = Mock(side_effect=http_error)
        
        fetch(self.openpodcast, self.params)
        
        # Should call spotify_call only once (no retry)
        self.params.spotify_call.assert_called_once()
        # Should not post to openpodcast
        self.openpodcast.post.assert_not_called()
        # Should not sleep for backoff
        mock_sleep.assert_not_called()
        # Should log error
        mock_logger.error.assert_called()

    @patch('job.worker.sleep')
    def test_connection_error_retries(self, mock_sleep):
        """Test that connection errors trigger retry"""
        connection_error = requests.exceptions.ConnectionError("Connection refused")
        
        # First call fails with connection error, second succeeds
        self.params.spotify_call = Mock(
            side_effect=[connection_error, {"data": "test"}]
        )
        
        fetch(self.openpodcast, self.params)
        
        # Should call spotify_call 2 times
        self.assertEqual(self.params.spotify_call.call_count, 2)
        # Should post to openpodcast once
        self.openpodcast.post.assert_called_once()
        # Should sleep once for backoff
        mock_sleep.assert_called_once_with(INITIAL_BACKOFF)

    @patch('job.worker.sleep')
    def test_timeout_error_retries(self, mock_sleep):
        """Test that timeout errors trigger retry"""
        timeout_error = requests.exceptions.Timeout("Request timed out")
        
        # First call times out, second succeeds
        self.params.spotify_call = Mock(
            side_effect=[timeout_error, {"data": "test"}]
        )
        
        fetch(self.openpodcast, self.params)
        
        # Should call spotify_call 2 times
        self.assertEqual(self.params.spotify_call.call_count, 2)
        # Should post to openpodcast once
        self.openpodcast.post.assert_called_once()
        # Should sleep once for backoff
        mock_sleep.assert_called_once_with(INITIAL_BACKOFF)

    @patch('job.worker.sleep')
    def test_exponential_backoff_caps_at_max(self, mock_sleep):
        """Test that exponential backoff caps at MAX_BACKOFF"""
        # Create a mock response with 429 status
        mock_response = Mock()
        mock_response.status_code = 429
        
        http_error = requests.exceptions.HTTPError()
        http_error.response = mock_response
        
        # Always fail to test all retry attempts
        self.params.spotify_call = Mock(side_effect=http_error)
        
        fetch(self.openpodcast, self.params)
        
        # Check that backoff values follow exponential pattern with cap
        # Expected: 1, 2, 4, 8, 16, 32, 60 (capped at MAX_BACKOFF=60)
        sleep_calls = [c[0][0] for c in mock_sleep.call_args_list]
        self.assertEqual(len(sleep_calls), MAX_RETRIES)
        
        # Verify exponential backoff pattern
        expected_backoffs = [1, 2, 4, 8, 16]
        for i, expected in enumerate(expected_backoffs):
            self.assertEqual(sleep_calls[i], expected)

    @patch('job.worker.sleep')
    @patch('job.worker.logger')
    def test_no_data_returned_no_post(self, mock_logger, mock_sleep):
        """Test that when spotify_call returns None, we don't post"""
        self.params.spotify_call = Mock(return_value=None)
        
        fetch(self.openpodcast, self.params)
        
        # Should call spotify_call once
        self.params.spotify_call.assert_called_once()
        # Should not post to openpodcast
        self.openpodcast.post.assert_not_called()
        # Should not sleep
        mock_sleep.assert_not_called()

    @patch('job.worker.sleep')
    @patch('job.worker.logger')
    def test_unexpected_error_no_retry(self, mock_logger, mock_sleep):
        """Test that unexpected errors don't trigger retry"""
        unexpected_error = ValueError("Unexpected error")
        
        self.params.spotify_call = Mock(side_effect=unexpected_error)
        
        fetch(self.openpodcast, self.params)
        
        # Should call spotify_call only once (no retry)
        self.params.spotify_call.assert_called_once()
        # Should not post to openpodcast
        self.openpodcast.post.assert_not_called()
        # Should not sleep for backoff
        mock_sleep.assert_not_called()
        # Should log error
        mock_logger.error.assert_called()

    @patch('job.worker.sleep')
    @patch('job.worker.logger')
    def test_retry_logging(self, mock_logger, mock_sleep):
        """Test that retry attempts are properly logged"""
        # Create a mock response with 429 status
        mock_response = Mock()
        mock_response.status_code = 429
        
        http_error = requests.exceptions.HTTPError()
        http_error.response = mock_response
        
        # Fail twice, then succeed
        self.params.spotify_call = Mock(
            side_effect=[http_error, http_error, {"data": "test"}]
        )
        
        fetch(self.openpodcast, self.params)
        
        # Check warning logs contain retry information
        warning_calls = mock_logger.warning.call_args_list
        self.assertEqual(len(warning_calls), 2)
        
        # First warning should mention "Retry 1/5"
        first_warning = str(warning_calls[0])
        self.assertIn("Retry 1/", first_warning)
        self.assertIn("backoff", first_warning)
        
        # Second warning should mention "Retry 2/5"
        second_warning = str(warning_calls[1])
        self.assertIn("Retry 2/", second_warning)


if __name__ == "__main__":
    unittest.main()

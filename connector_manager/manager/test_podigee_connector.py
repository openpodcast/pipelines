"""
Tests for Podigee OAuth connector functionality.
Tests the refresh token logic and database update scenarios.
"""
import pytest
import json
import requests
import mysql.connector
from unittest.mock import Mock, patch, MagicMock
from manager.podigee_connector import refresh_podigee_token, handle_podigee_refresh


class TestRefreshPodigeeToken:
    """Test the refresh_podigee_token function."""
    
    def test_successful_refresh(self):
        """Test successful token refresh."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "access_token": "new_access_token",
            "refresh_token": "new_refresh_token",
            "expires_in": 3600
        }
        mock_response.raise_for_status.return_value = None
        
        with patch('requests.post', return_value=mock_response) as mock_post:
            result = refresh_podigee_token(
                client_id="test_client_id",
                client_secret="test_client_secret", 
                refresh_token="test_refresh_token"
            )
            
            assert result is not None
            assert result["access_token"] == "new_access_token"
            assert result["refresh_token"] == "new_refresh_token"
            assert result["expires_in"] == 3600
            
            # Verify the request was made correctly
            mock_post.assert_called_once()
            args, kwargs = mock_post.call_args
            assert args[0] == "https://app.podigee.com/oauth/token"
            assert kwargs["data"]["client_id"] == "test_client_id"
            assert kwargs["data"]["refresh_token"] == "test_refresh_token"
            assert kwargs["data"]["grant_type"] == "refresh_token"
    
    def test_missing_refresh_token(self):
        """Test handling of missing refresh token."""
        result = refresh_podigee_token(
            client_id="test_client_id",
            client_secret="test_client_secret",
            refresh_token=None
        )
        assert result is None
    
    def test_api_request_failure(self):
        """Test handling of API request failure."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.RequestException("API Error")
        
        with patch('requests.post', return_value=mock_response):
            result = refresh_podigee_token(
                client_id="test_client_id",
                client_secret="test_client_secret",
                refresh_token="test_refresh_token"
            )
            assert result is None
    
    def test_custom_redirect_uri(self):
        """Test with custom redirect URI."""
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "token", "refresh_token": "token"}
        mock_response.raise_for_status.return_value = None
        
        custom_uri = "https://custom.example.com/callback"
        
        with patch('requests.post', return_value=mock_response) as mock_post:
            refresh_podigee_token(
                client_id="test_client_id",
                client_secret="test_client_secret",
                refresh_token="test_refresh_token",
                redirect_uri=custom_uri
            )
            
            args, kwargs = mock_post.call_args
            assert kwargs["data"]["redirect_uri"] == custom_uri


class TestHandlePodigeeRefresh:
    """Test the handle_podigee_refresh function."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_db = Mock()
        self.mock_cursor = Mock()
        
        # Properly mock the context manager for cursor
        cursor_context = Mock()
        cursor_context.__enter__ = Mock(return_value=self.mock_cursor)
        cursor_context.__exit__ = Mock(return_value=None)
        self.mock_db.cursor.return_value = cursor_context
        
        self.test_keys = {
            "PODIGEE_REFRESH_TOKEN": "old_refresh_token_with_sufficient_length_for_validation",
            "PODIGEE_ACCESS_TOKEN": "old_access_token_with_sufficient_length_for_validation", 
            "OTHER_KEY": "other_value"
        }
        
        self.new_token_data = {
            "access_token": "new_access_token_with_sufficient_length_for_validation",
            "refresh_token": "new_refresh_token_with_sufficient_length_for_validation",
            "expires_in": 3600
        }
    
    def test_non_podigee_source_passthrough(self):
        """Test that non-Podigee sources are passed through unchanged."""
        result = handle_podigee_refresh(
            db_connection=self.mock_db,
            account_id="test_account",
            source_name="spotify",  # Not podigee
            source_access_keys=self.test_keys,
            pod_name="Test Podcast",
            encryption_key="test_key"
        )
        
        assert result == self.test_keys
        # Database should not be touched
        self.mock_db.cursor.assert_not_called()
    
    def test_missing_refresh_token_in_keys(self):
        """Test handling when refresh token is missing from keys."""
        keys_without_refresh = {"OTHER_KEY": "value"}
        
        result = handle_podigee_refresh(
            db_connection=self.mock_db,
            account_id="test_account",
            source_name="podigee",
            source_access_keys=keys_without_refresh,
            pod_name="Test Podcast",
            encryption_key="test_key"
        )
        
        assert result == keys_without_refresh
    
    def test_missing_oauth_credentials(self):
        """Test handling of missing OAuth credentials."""
        result = handle_podigee_refresh(
            db_connection=self.mock_db,
            account_id="test_account",
            source_name="podigee",
            source_access_keys=self.test_keys,
            pod_name="Test Podcast",
            encryption_key="test_key",
            client_id=None,  # Missing
            client_secret="secret"
        )
        
        assert result is None
    
    @patch('manager.podigee_connector.refresh_podigee_token')
    @patch('manager.podigee_connector.encrypt_json')
    def test_successful_refresh_and_db_update(self, mock_encrypt, mock_refresh):
        """Test successful token refresh and database update."""
        # Mock the token refresh
        mock_refresh.return_value = self.new_token_data
        
        # Mock encryption
        mock_encrypt.return_value = "encrypted_keys"
        
        # Mock successful database update
        self.mock_cursor.rowcount = 1
        
        result = handle_podigee_refresh(
            db_connection=self.mock_db,
            account_id="test_account",
            source_name="podigee",
            source_access_keys=self.test_keys.copy(),
            pod_name="Test Podcast",
            encryption_key="test_key",
            client_id="client_id",
            client_secret="client_secret"
        )
        
        # Verify result contains updated tokens
        assert result is not None
        assert result["PODIGEE_ACCESS_TOKEN"] == "new_access_token_with_sufficient_length_for_validation"
        assert result["PODIGEE_REFRESH_TOKEN"] == "new_refresh_token_with_sufficient_length_for_validation"
        assert result["OTHER_KEY"] == "other_value"  # Preserved
        
        # Verify database operations (simplified - no transaction management)
        self.mock_cursor.execute.assert_called_once()
        
        # Verify the SQL query
        sql_call = self.mock_cursor.execute.call_args[0]
        assert "UPDATE podcastSources" in sql_call[0]
        assert sql_call[1] == ("encrypted_keys", "test_account")
    
    @patch('manager.podigee_connector.refresh_podigee_token')
    def test_failed_token_refresh(self, mock_refresh):
        """Test handling of failed token refresh."""
        mock_refresh.return_value = None  # Simulate failure
        
        result = handle_podigee_refresh(
            db_connection=self.mock_db,
            account_id="test_account",
            source_name="podigee",
            source_access_keys=self.test_keys,
            pod_name="Test Podcast",
            encryption_key="test_key",
            client_id="client_id",
            client_secret="client_secret"
        )
        
        assert result is None
        # Database should not be touched if token refresh fails
        self.mock_cursor.execute.assert_not_called()
    
    @patch('manager.podigee_connector.refresh_podigee_token')
    @patch('manager.podigee_connector.encrypt_json')
    def test_database_update_failure(self, mock_encrypt, mock_refresh):
        """Test handling of database update failure after successful refresh."""
        # Mock successful token refresh
        mock_refresh.return_value = self.new_token_data
        mock_encrypt.return_value = "encrypted_keys"
        
        # Mock database error
        self.mock_cursor.execute.side_effect = mysql.connector.Error("DB Error")
        
        result = handle_podigee_refresh(
            db_connection=self.mock_db,
            account_id="test_account",
            source_name="podigee",
            source_access_keys=self.test_keys.copy(),
            pod_name="Test Podcast",
            encryption_key="test_key",
            client_id="client_id",
            client_secret="client_secret"
        )
        
        # Should return None due to database failure
        assert result is None
        
        # Database execute should have been called but failed
        self.mock_cursor.execute.assert_called_once()
    
    @patch('manager.podigee_connector.refresh_podigee_token')
    @patch('manager.podigee_connector.encrypt_json')
    def test_no_rows_updated(self, mock_encrypt, mock_refresh):
        """Test handling when database update affects 0 rows."""
        mock_refresh.return_value = self.new_token_data
        mock_encrypt.return_value = "encrypted_keys"
        
        # Mock no rows updated (account_id not found)
        self.mock_cursor.rowcount = 0
        
        result = handle_podigee_refresh(
            db_connection=self.mock_db,
            account_id="nonexistent_account",
            source_name="podigee",
            source_access_keys=self.test_keys.copy(),
            pod_name="Test Podcast",
            encryption_key="test_key",
            client_id="client_id",
            client_secret="client_secret"
        )
        
        assert result is None
    
    @patch('manager.podigee_connector.refresh_podigee_token')
    def test_same_refresh_token_warning(self, mock_refresh):
        """Test warning when API returns the same refresh token."""
        # Mock returning the same refresh token (unusual scenario)
        same_token_data = {
            "access_token": "new_access_token_with_sufficient_length_for_validation", 
            "refresh_token": "old_refresh_token_with_sufficient_length_for_validation",  # Same as input
            "expires_in": 3600
        }
        mock_refresh.return_value = same_token_data
        
        with patch('manager.podigee_connector.encrypt_json'):
            self.mock_cursor.rowcount = 1
            
            result = handle_podigee_refresh(
                db_connection=self.mock_db,
                account_id="test_account",
                source_name="podigee",
                source_access_keys=self.test_keys.copy(),
                pod_name="Test Podcast",
                encryption_key="test_key",
                client_id="client_id",
                client_secret="client_secret"
            )
            
            # Should still work but log a warning
            assert result is not None
            assert result["PODIGEE_REFRESH_TOKEN"] == "old_refresh_token_with_sufficient_length_for_validation"


class TestIntegrationScenarios:
    """Integration tests for real-world scenarios."""
    
    @patch('manager.podigee_connector.refresh_podigee_token')
    @patch('manager.podigee_connector.encrypt_json')
    def test_cron_job_scenario(self, mock_encrypt, mock_refresh):
        """Test the scenario that was failing in cron jobs."""
        # Simulate the scenario where refresh token was already used
        mock_refresh.return_value = None  # Token refresh fails
        
        mock_db = Mock()
        mock_cursor = Mock()
        
        # Properly mock the context manager for cursor
        cursor_context = Mock()
        cursor_context.__enter__ = Mock(return_value=mock_cursor)
        cursor_context.__exit__ = Mock(return_value=None)
        mock_db.cursor.return_value = cursor_context
        
        test_keys = {
            "PODIGEE_REFRESH_TOKEN": "already_used_token_with_sufficient_length_for_validation",
            "PODIGEE_ACCESS_TOKEN": "old_access_token_with_sufficient_length_for_validation"
        }
        
        result = handle_podigee_refresh(
            db_connection=mock_db,
            account_id="test_account",
            source_name="podigee",
            source_access_keys=test_keys,
            pod_name="Test Podcast",
            encryption_key="test_key",
            client_id="client_id",
            client_secret="client_secret"
        )
        
        # Should return None indicating manual intervention needed
        assert result is None
        
        # Database should not be modified
        mock_db.start_transaction.assert_not_called()
    
    @patch('manager.podigee_connector.refresh_podigee_token')
    @patch('manager.podigee_connector.encrypt_json')
    def test_race_condition_recovery(self, mock_encrypt, mock_refresh):
        """Test recovery from race condition where DB fails after token refresh."""
        # Successful token refresh
        new_tokens = {
            "access_token": "new_access_token_with_sufficient_length_for_validation",
            "refresh_token": "new_refresh_token_with_sufficient_length_for_validation",
            "expires_in": 3600
        }
        mock_refresh.return_value = new_tokens
        mock_encrypt.return_value = "encrypted_data"
        
        # Database fails after successful token refresh
        mock_db = Mock()
        mock_cursor = Mock()
        
        # Properly mock the context manager for cursor
        cursor_context = Mock()
        cursor_context.__enter__ = Mock(return_value=mock_cursor)
        cursor_context.__exit__ = Mock(return_value=None)
        mock_db.cursor.return_value = cursor_context
        
        mock_cursor.execute.side_effect = mysql.connector.Error("Connection lost")
        
        test_keys = {
            "PODIGEE_REFRESH_TOKEN": "valid_token_with_sufficient_length_for_validation",
            "PODIGEE_ACCESS_TOKEN": "old_access_token_with_sufficient_length_for_validation"
        }
        
        result = handle_podigee_refresh(
            db_connection=mock_db,
            account_id="test_account",
            source_name="podigee",
            source_access_keys=test_keys,
            pod_name="Test Podcast",
            encryption_key="test_key",
            client_id="client_id",
            client_secret="client_secret"
        )
        
        # Should return None indicating failure and need for manual intervention
        assert result is None
        
        # Database execute should have been called but failed
        mock_cursor.execute.assert_called_once()


if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__, "-v"])

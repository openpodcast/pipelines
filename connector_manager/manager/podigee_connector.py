"""
Handles Podigee OAuth operations, including token refresh and database updates.
"""
import mysql.connector
import json
from loguru import logger
import os
import requests

def refresh_podigee_token(client_id, client_secret, refresh_token, redirect_uri=None):
    """
    Exchange a refresh token for a new access token and refresh token for Podigee.
    
    Args:
        client_id (str): The client ID for the Podigee OAuth app
        client_secret (str): The client secret for the Podigee OAuth app
        refresh_token (str): The refresh token to exchange
        redirect_uri (str, optional): The redirect URI. 
                                    Defaults to https://connect.openpodcast.app/auth/v1/podigee/callback
    
    Returns:
        dict: Token response containing access_token, refresh_token, etc.
              Returns None if the request fails
    """
    if not redirect_uri:
        redirect_uri = "https://connect.openpodcast.app/auth/v1/podigee/callback"
        
    if not refresh_token:
        logger.error("No refresh token provided for Podigee")
        return None
    
    token_url = "https://app.podigee.com/oauth/token"
    
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
        "redirect_uri": redirect_uri
    }
    
    try:
        response = requests.post(token_url, data=payload)
        response.raise_for_status()
        
        token_data = response.json()
        logger.info(f"Successfully refreshed Podigee access token, expires in {token_data.get('expires_in')} seconds")
        
        return token_data
    except requests.RequestException as e:
        logger.error(f"Failed to refresh Podigee token: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response: {e.response.text}")
        return None

def handle_podigee_refresh(db_connection, account_id, source_name, source_access_keys, pod_name, encryption_key, 
                    client_id=None, client_secret=None, redirect_uri=None):
    """
    Handle the refresh token logic for Podigee:
    1. Refresh the token
    2. Update the database with the new refresh token
    3. Return the updated source_access_keys
    
    Args:
        db_connection: MySQL database connection
        account_id (str): Account ID
        source_name (str): Source name (should be "podigee")
        source_access_keys (dict): Decrypted source access keys
        pod_name (str): Podcast name for logging
        encryption_key (str): Encryption key for re-encrypting the access keys
        client_id (str, optional): Client ID for the Podigee OAuth app
        client_secret (str, optional): Client secret for the Podigee OAuth app
        redirect_uri (str, optional): Redirect URI for the OAuth flow
        
    Returns:
        dict: Updated source_access_keys with the new access token, or None if failed
    """
    if source_name != "podigee" or "refreshToken" not in source_access_keys:
        return source_access_keys
        
    logger.info(f"Refreshing Podigee access token for {pod_name}")
    
    # Get refresh token from source_access_keys
    refresh_token = source_access_keys.get("refreshToken")
    
    if not client_id or not client_secret or not refresh_token:
        logger.error(f"Missing required OAuth credentials for Podigee: {pod_name}")
        return {}
        
    # Refresh the token
    token_data = refresh_podigee_token(client_id, client_secret, refresh_token, redirect_uri)
    
    if not token_data or "access_token" not in token_data or "refresh_token" not in token_data:
        logger.error(f"Failed to refresh Podigee token for {pod_name}")
        return {}

    source_access_keys = token_data.copy()
    
    # Update the refresh token in the database
    try:
        with db_connection.cursor() as cursor:
            # Encrypt the access keys before storing them
            access_keys_json = encrypt_json(source_access_keys, encryption_key)
            
            # Update the database with the new encrypted keys
            sql = """
                UPDATE podcastSources 
                SET source_access_keys_encrypted = %s
                WHERE account_id = %s AND source_name = "podigee"
            """
            cursor.execute(sql, (access_keys_json, account_id))
            db_connection.commit()
            logger.info(f"Updated Podigee refresh token for {pod_name}")
    except mysql.connector.Error as e:
        logger.error(f"Failed to update Podigee refresh token: {e}")
        
    return source_access_keys

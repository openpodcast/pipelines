"""
Handles Podigee OAuth operations, including token refresh and database updates.
"""
from manager.cryptography import encrypt_json
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
    if source_name != "podigee" or "PODIGEE_REFRESH_TOKEN" not in source_access_keys:
        return source_access_keys
        
    logger.info(f"Refreshing Podigee access token for {pod_name}")
    
    # Get refresh token from source_access_keys
    refresh_token = source_access_keys.get("PODIGEE_REFRESH_TOKEN")
    logger.debug(f"Using refresh token: {refresh_token[:10]}... for {pod_name}")
    if not client_id or not client_secret or not refresh_token:
        logger.error(f"Missing required OAuth credentials for Podigee: {pod_name}")
        return None

    # Store original refresh token for debugging
    original_refresh_token = refresh_token

    # Refresh the token
    token_data = refresh_podigee_token(client_id, client_secret, refresh_token, redirect_uri)
    
    if not token_data or "access_token" not in token_data or "refresh_token" not in token_data:
        logger.error(f"Failed to refresh Podigee token for {pod_name}")
        logger.error(f"This likely means the refresh token was already used or is invalid.")
        logger.error(f"Original refresh token was: {original_refresh_token[:10]}...")
        logger.error(f"Manual intervention may be required to re-authenticate the Podigee connection.")
        return None

    # Check if we got a new refresh token (it should be different from the original)
    new_refresh_token = token_data["refresh_token"]
    if new_refresh_token == original_refresh_token:
        logger.warning(f"Podigee returned the same refresh token for {pod_name}. This is unusual.")
    else:
        logger.info(f"Successfully obtained new refresh token for {pod_name}")

    # Update source_access_keys in-place; keep the remaining keys
    source_access_keys.update({
        "PODIGEE_ACCESS_TOKEN": token_data["access_token"],
        "PODIGEE_REFRESH_TOKEN": token_data["refresh_token"],
    })
    
    # Update the refresh token in the database
    # This is critical - if this fails, the refresh token is consumed but not saved,
    # which will cause the next execution to fail
    try:
        with db_connection.cursor() as cursor:
            # We only need to store the refresh token. Throw away the rest of the JSON
            refresh_token = source_access_keys.get("PODIGEE_REFRESH_TOKEN")
            if not refresh_token or len(refresh_token) < 20:
                raise ValueError(f"Invalid refresh token: {refresh_token}")

            # Encrypt the access keys before storing them
            source_access_keys_json = encrypt_json(source_access_keys, encryption_key)

            # Update the database with the new encrypted keys
            sql = """
                UPDATE podcastSources 
                SET source_access_keys_encrypted = %s
                WHERE account_id = %s AND source_name = "podigee"
            """
            cursor.execute(sql, (source_access_keys_json, account_id))
            
            # Verify the update worked
            if cursor.rowcount == 0:
                logger.error(f"No rows were updated - account_id '{account_id}' or source_name 'podigee' not found")
                logger.error(f"New refresh token that could not be saved: {token_data['refresh_token']}")
                return None
                
            logger.info(f"Successfully updated Podigee refresh token for {pod_name}")
            
    except (mysql.connector.Error, ValueError) as e:
        logger.error(f"Failed to update Podigee refresh token in database: {e}")
        logger.error(f"New refresh token that could not be saved: {token_data['refresh_token']} Account ID: {account_id}, Pod name: {pod_name}")
        logger.error(f"Manual intervention required: update the refresh token in the database manually.")
        return None
        
    return source_access_keys

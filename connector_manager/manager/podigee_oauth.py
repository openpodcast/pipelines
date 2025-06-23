"""
OAuth functionality for Podigee connector.
This module handles refreshing the access token for Podigee.
"""
import requests
from loguru import logger

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

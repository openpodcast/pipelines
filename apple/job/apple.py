from dataclasses import dataclass
from typing import List

from filecache import filecache
from appleconnector import AppleConnector
import requests
from loguru import logger

# Set a very long timeout for the cookie request as we use browser automation to
# get the cookies, which can take a while
COOKIE_TIMEOUT = 600  # seconds


@dataclass
class AppleCookies:
    """
    Holds all the relevant cookies for the Apple connector API
    """

    myacinfo: str
    itctx: str


# Add a cache decorator to the function that fetches the cookies
# so that we don't have to fetch them every time as we pay for every single
# login.
# The cache will be invalidated after 1 hour.
@filecache(60 * 60)
def fetch_all_cookies(bearer_token: str, apple_automation_endpoint: str):
    """
    Get Apple cookies from API
    """
    headers = {"Authorization": f"Bearer {bearer_token}"}
    response = requests.get(
        apple_automation_endpoint, headers=headers, timeout=COOKIE_TIMEOUT
    )

    logger.info(f"Got cookies response: {response.status_code}")
    if response.status_code != 200:
        raise Exception(f"Failed to get cookies: {response.text}")

    cookies = response.json()
    return cookies


def get_cookies(bearer_token: str, apple_automation_endpoint: str) -> AppleCookies:
    """
    Extract the relevant cookies from the response
    """
    cookies = fetch_all_cookies(bearer_token, apple_automation_endpoint)
    # Extract `myacinfo` cookie
    myacinfo_cookie = next(c for c in cookies if c["name"] == "myacinfo")
    myacinfo = myacinfo_cookie["value"]

    # Extract `itctx` cookie
    itctx_cookie = next(c for c in cookies if c["name"] == "itctx")
    itctx = itctx_cookie["value"]

    return AppleCookies(myacinfo, itctx)


def get_episode_ids(apple_connector: AppleConnector) -> List[str]:
    """
    Get all episode IDs from Apple
    """
    episodes = apple_connector.episodes()

    if not episodes or not episodes["content"] or not episodes["content"]["results"]:
        logger.error("No episodes found")
        # This might not be an error if the podcast is new
        return []

    # Convert keys to a list of episode IDs
    return list(episodes["content"]["results"].keys())

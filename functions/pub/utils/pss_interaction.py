import os
import logging
import requests
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


def pss_post(body, timeout=15.0):
    """
    Posts an XML body to the PSS endpoint and returns the parsed XML root element.

    Raises:
        ValueError: For configuration issues, HTTP errors,
                    connectivity issues, or XML parsing failures.
    """
    # 1) Validate environment configuration
    url = os.getenv("PSSBaseURL")
    if not url:
        raise ValueError("Missing required environment variable: 'PSSBaseURL'.")

    if not (url.startswith("http://") or url.startswith("https://")):
        raise ValueError("Invalid PSSBaseURL '{}'. Expected a valid http(s) URL.".format(url))

    # 2) Validate input body
    if body is None or (isinstance(body, str) and body.strip() == ""):
        raise ValueError("Request body is empty. Expected non-empty XML string.")

    headers = {
        "Content-Type": "text/xml; charset=utf-8"
    }

    # 3) Perform POST with timeout and proper error handling

    try:
        logger.debug(f"Posting to PSS endpoint: {url}")
        response = requests.post(url, data=body, headers=headers, timeout=timeout)

    except requests.exceptions.ConnectTimeout as ex:
        # Timeout while establishing the TCP connection
        logger.exception(f"Timed out connecting to PSS endpoint after {timeout} seconds.")
        raise ValueError(f"Timed out connecting to PSS endpoint after {timeout} seconds.") from ex

    except requests.exceptions.ReadTimeout as ex:
        # Server accepted connection but did not send a response in time
        logger.exception(f"PSS endpoint did not respond within {timeout} seconds (read timeout).")
        raise ValueError(f"PSS endpoint did not respond within {timeout} seconds (read timeout).") from ex

    except requests.exceptions.SSLError as ex:
        logger.exception(f"SSL/TLS error when connecting to PSS endpoint: {ex}")
        raise ValueError("SSL/TLS error when connecting to PSS endpoint.") from ex

    except requests.exceptions.ConnectionError as ex:
        # Covers DNS failures, connection refused, etc.
        logger.exception(f"Could not connect to PSS endpoint at '{url}': {ex}")
        raise ValueError(f"Could not connect to PSS endpoint at '{url}'.") from ex

    except requests.exceptions.RequestException as ex:
        # Fallback for any other requests-related error
        logger.exception(f"Unexpected error posting to PSS endpoint: {ex}")
        raise ValueError("Unexpected error posting to PSS endpoint.") from ex


    # 4) Check HTTP status code
    if not (200 <= response.status_code < 300):
        snippet = (response.text or "").strip()
        if len(snippet) > 300:
            snippet = snippet[:300] + "…"
        raise ValueError(
            "PSS endpoint returned HTTP {}. Response snippet: {}".format(
                response.status_code,
                snippet or "<empty response>"
            )
        )

    # 5) Validate response body
    if response.text is None or response.text.strip() == "":
        raise ValueError("PSS endpoint returned an empty response body; expected XML.")

    # 6) Parse XML response
    try:
        root = ET.fromstring(response.text)
    except ET.ParseError as ex:
        snippet = response.text.strip()
        if len(snippet) > 300:
            snippet = snippet[:300] + "…"
        raise ValueError(
            "Failed to parse XML response from PSS endpoint: {}. Snippet: {}".format(ex, snippet)
        ) from ex

    logger.debug("Successfully received and parsed XML from PSS endpoint.")
    return root

import requests
import os
import logging

def get_release_package():
    """
    Retrieves the active Semarchy Release Package.

    Returns:
        ReleasePackageID (str or None)

    Raises:
        ValueError: if multiple release packages are returned
                    or if the API request fails.
    """

    api_key = os.getenv("SemarchyAPIKey")
    url = os.getenv("SemarchyGetReleasePackageNamedQueryURL")
    headers = {"API-Key": api_key}

    # -----------------------------
    # Perform API request
    # -----------------------------
    logging.info("RELEASE PACKAGE HANDLING | Retrieval - START")

    try:
        response = requests.get(url=url, headers=headers)
        response.raise_for_status()
        records = response.json().get("records", [])
        logging.info(
            f"RELEASE PACKAGE HANDLING | Retrieval - SUCCESS "
            f"(records_returned={len(records)})"
        )
    except Exception as e:
        logging.error(
            f"RELEASE PACKAGE HANDLING | Retrieval - FAILED ({e})"
        )
        raise ValueError(f"Failed to retrieve Release Package: {e}") from e

    # -----------------------------
    # No Release Packages
    # -----------------------------
    if not records:
        logging.info(
            "RELEASE PACKAGE HANDLING | No open PNLD Release Packages found"
        )
        return None

    # -----------------------------
    # Single Release Package
    # -----------------------------
    if len(records) == 1:
        rp_id = records[0].get("ReleasePackageID")
        logging.info(
            f"RELEASE PACKAGE HANDLING | One Release Package found: {rp_id}"
        )
        return rp_id

    # -----------------------------
    # Multiple Release Packages
    # -----------------------------
    rp_ids = [r.get("ReleasePackageID") for r in records]
    msg = f"Multiple open PNLD Release Packages found: {rp_ids}"

    logging.error(f"RELEASE PACKAGE HANDLING | {msg}")
    raise ValueError(msg)
import requests
import os
import logging
import time
from datetime import datetime, timezone

def create_release_package():
    """
    Creates a Release Package in Semarchy and waits for the load to complete.

    Returns:
        "DONE", "WARNING", or "ERROR"

    Raises:
        ValueError for submission failures, missing load ID, or timeout.
    """

    logging.info("RELEASE PACKAGE HANDLING | Creation - START")

    # ------------------------------------------
    # Resolve environment variables
    # ------------------------------------------
    base_url = f'{os.getenv("SemarchyBaseURL")}/loads/CSDS'
    api_key = os.getenv("SemarchyAPIKey")

    if not base_url or not api_key:
        msg = "Missing Semarchy configuration (URL or API key)."
        logging.error(f"RELEASE PACKAGE HANDLING | Creation - FAILED ({msg})")
        raise ValueError(f"Release Package creation failed: {msg}")

    headers = {"API-Key": api_key}

    # Timestamp (no milliseconds)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------------------
    # Build Release Package record
    # ------------------------------------------
    release_package = [{
        "Description": f"PNLD Release Package",
        "Notes": "PNLD Upload Release Package Creation",
        "ReleasePackageType": "PNLD"
    }]

    post_body = {
        "action": "CREATE_LOAD_AND_SUBMIT",
        "programName": "UPDATE_DATA_REST_API",
        "loadDescription": "PNLD Release Package Creation",
        "jobName": "ReleasePackageIntegrationLoad",
        "persistOptions": {
            "defaultPublisherId": "PNLD",
            "optionsPerEntity": {
                "ReleasePackage": {
                    "enrichers": ["SetDefaultOpenStatusEnricher"]
                }
            },
            "missingIdBehavior": "GENERATE",
            "persistMode": "IF_NO_ERROR_OR_MATCH"
        },
        "persistRecords": {
            "ReleasePackage": release_package
        }
    }

    # ------------------------------------------
    # Submit Release Package load
    # ------------------------------------------
    try:
        logging.info("RELEASE PACKAGE HANDLING | Load Submission - START")

        response = requests.post(
            url=base_url,
            json=post_body,
            headers=headers,
            timeout=(5, 60)
        )
        response.raise_for_status()

        load_id = response.json().get("load", {}).get("loadId")

        logging.info(
            f"RELEASE PACKAGE HANDLING | Load Submission - SUCCESS "
            f"(load_id={load_id})"
        )

    except Exception as e:
        logging.error(
            f"RELEASE PACKAGE HANDLING | Load Submission - FAILED ({e})"
        )
        raise ValueError(f"Release Package creation failed during submission: {e}")

    # ------------------------------------------
    # Validate returned load_id
    # ------------------------------------------
    if not load_id:
        msg = "Semarchy returned no load_id for Release Package creation."
        logging.error(f"RELEASE PACKAGE HANDLING | Load Validation - FAILED ({msg})")
        raise ValueError(msg)

    # ------------------------------------------
    # Poll load status
    # ------------------------------------------
    status_url = f"{base_url}/{load_id}"
    max_attempts = 60
    interval = 1  # seconds

    logging.info(
        f"RELEASE PACKAGE HANDLING | Status Polling - START "
        f"(load_id={load_id}, attempts={max_attempts}, interval={interval}s)"
    )

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(
                url=status_url,
                headers=headers,
                timeout=(5, 30)
            )
            response.raise_for_status()

            body = response.json()
            load_status = body.get("loadStatus")

            logging.info(
                f"RELEASE PACKAGE HANDLING | Status Polling - ATTEMPT {attempt}/{max_attempts} "
                f"(load_id={load_id}, status={load_status})"
            )

            if load_status in ("DONE", "WARNING", "ERROR", "SUSPENDED"):
                logging.info(
                    f"RELEASE PACKAGE HANDLING | Status Polling - COMPLETE "
                    f"(load_id={load_id}, final_status={load_status})"
                )
                return load_status

        except Exception as e:
            msg = f"POLLING - ATTEMPT FAILED (attempt={attempt}, load_id={load_id}, error={e})"
            logging.warning(
                f"RELEASE PACKAGE HANDLING | POST RELEASE PACKAGE | POLLING - ATTEMPT FAILED "
                f"(attempt={attempt}, load_id={load_id}, error={e})"
            )
            raise ValueError(msg)

        time.sleep(interval)

    # ------------------------------------------
    # Timeout reached — load did not finish
    # ------------------------------------------
    total_wait = max_attempts * interval
    msg = (
        f"Release Package creation timed out after {total_wait}s "
        f"(load_id={load_id})."
    )

    logging.error(f"RELEASE PACKAGE HANDLING | Status Polling - TIMEOUT ({msg})")

    raise ValueError(msg)
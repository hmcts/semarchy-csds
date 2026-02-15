import requests
import os
import logging
import time

def post_offences(offences):
    """
    Submit Offence Revision records to Semarchy and poll until the load completes.

    Returns:
        (load_status, batch_id)

    Raises:
        ValueError on submission failure, missing load_id, or timeout.

    Logging format:
        OFFENCE HANDLING | <step> - <status> (details)
    """

    # Remove non-persisted helper field(s)
    offences = [{k: v for k, v in off.items() if k != "xml_file_id"} for off in offences]

    logging.info(
        f"OFFENCE HANDLING | POST OFFENCES | START "
        f"(offences={len(offences)})"
    )

    # ------------------------------------------
    # Resolve environment variables
    # ------------------------------------------
    base_url = os.getenv("SemarchyLoadURL")
    api_key = os.getenv("SemarchyAPIKey")

    if not base_url or not api_key:
        msg = "Missing Semarchy configuration (LoadURL or APIKey)."
        logging.error(f"OFFENCE HANDLING | POST OFFENCES | FAILED ({msg})")
        raise ValueError(f"Offence Revision load failed: {msg}")

    headers = {"API-Key": api_key}

    post_body = {
        "action": "CREATE_LOAD_AND_SUBMIT",
        "programName": "UPDATE_DATA_REST_API",
        "loadDescription": "Offence Revision Load",
        "jobName": "OffenceRevisionIntegrationLoad",
        "persistOptions": {
            "defaultPublisherId": "PNLD",
            "optionsPerEntity": {
                "OffenceRevision": {
                    "enrichers": ["SetVersionNumber", "CreateOffenceHeaderPNLD"]
                }
            },
            "missingIdBehavior": "GENERATE",
            "persistMode": "IF_NO_ERROR_OR_MATCH"
        },
        "persistRecords": {
            "OffenceRevision": offences
        }
    }

    # ------------------------------------------
    # Submit Offence Revision load
    # ------------------------------------------
    logging.info("OFFENCE HANDLING | POST OFFENCES | SUBMIT - START")

    try:
        response = requests.post(
            url=base_url,
            json=post_body,
            headers=headers,
            timeout=(5, 60)
        )
        response.raise_for_status()

        load_id = response.json().get("load", {}).get("loadId")
        batch_id = response.json().get("load", {}).get("batchId")

        logging.info(
            f"OFFENCE HANDLING | POST OFFENCES | SUBMIT - SUCCESS "
            f"(load_id={load_id}, batch_id={batch_id})"
        )

    except Exception as e:
        logging.error(
            f"OFFENCE HANDLING | POST OFFENCES | SUBMIT - FAILED ({e})"
        )
        raise ValueError(f"Offence Revision load submission failed: {e}")

    # ------------------------------------------
    # Validate returned load_id
    # ------------------------------------------
    if not load_id:
        msg = "Semarchy returned no load_id for Offence Revision load."
        logging.error(
            f"OFFENCE HANDLING | POST OFFENCES | LOAD VALIDATION - FAILED ({msg})"
        )
        raise ValueError(msg)

    # ------------------------------------------
    # Poll load status
    # ------------------------------------------
    status_url = f"{base_url}/{load_id}"
    max_attempts = 60
    interval = 1  # seconds

    logging.info(
        f"OFFENCE HANDLING | POST OFFENCES | POLLING - START "
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
                f"OFFENCE HANDLING | POST OFFENCES | POLLING - ATTEMPT {attempt}/{max_attempts} "
                f"(load_id={load_id}, status={load_status})"
            )

            # Terminal states
            if load_status in ("DONE", "WARNING", "ERROR"):
                logging.info(
                    f"OFFENCE HANDLING | POST OFFENCES | POLLING - COMPLETE "
                    f"(load_id={load_id}, final_status={load_status})"
                )
                return load_status, batch_id

        except Exception as e:
            logging.warning(
                f"OFFENCE HANDLING | POST OFFENCES | POLLING - ATTEMPT FAILED "
                f"(attempt={attempt}, load_id={load_id}, error={e})"
            )

        time.sleep(interval)

    # ------------------------------------------
    # Timeout reached — load did not finish
    # ------------------------------------------
    total_wait = max_attempts * interval
    msg = (
        f"Offence Revision load timed out after {total_wait}s "
        f"(load_id={load_id})."
    )
    logging.error(f"OFFENCE HANDLING | POST OFFENCES | POLLING - TIMEOUT ({msg})")
    raise ValueError(msg)
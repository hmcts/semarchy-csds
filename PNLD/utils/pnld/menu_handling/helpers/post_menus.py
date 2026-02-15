import requests
import os
import logging
import time

def post_menus(menus, menu_options, rp_id):
    """
    Submits PNLD Menu data and Menu Options to Semarchy using the CREATE_LOAD_AND_SUBMIT API action.
    Handles submission, validation, and polling of load status.

    Logging format:
        MENU HANDLING | <step> - <status> (details)
    """

    logging.info(
        f"MENU HANDLING | POST MENUS | START "
        f"(menus={len(menus)}, menu_options={len(menu_options)}, rp_id={rp_id})"
    )

    # ------------------------------------------
    # Prepare Menu records
    # ------------------------------------------
    menus = [{
        'Name': menu['Name'],
        'PNLDHashMD5': menu['PNLDHashMD5'],
        'AuthoringStatus': 'Final',
        'PublishingStatus': 'Not Published',
        'FID_ReleasePackage': rp_id
    } for menu in menus]

    # ------------------------------------------
    # Resolve environment variables
    # ------------------------------------------
    base_url = os.getenv("SemarchyLoadURL")
    api_key = os.getenv("SemarchyAPIKey")

    if not base_url or not api_key:
        msg = "Missing Semarchy configuration (LoadURL or APIKey)."
        logging.error(f"MENU HANDLING | POST MENUS | FAILED ({msg})")
        raise ValueError(f"Menu load failed: {msg}")

    headers = {"API-Key": api_key}

    # ------------------------------------------
    # Construct Semarchy payload for Menu load
    # ------------------------------------------
    post_body = {
        'action': 'CREATE_LOAD_AND_SUBMIT',
        'programName': 'UPDATE_DATA_REST_API',
        'loadDescription': 'Process PNLD Menu XML files',
        'jobName': 'OffenceMenusIntegrationLoad',
        'persistOptions': {
            'defaultPublisherId': 'PNLD',
            'optionsPerEntity': {
                'OTEMenuOptions': {
                    'enrichers': ['GetMenuId']
                }
            },
            'missingIdBehavior': 'GENERATE',
            'persistMode': 'IF_NO_ERROR_OR_MATCH'
        },
        'persistRecords': {
            'OTEMenu': menus,
            'OTEMenuOptions': menu_options
        }
    }

    # ------------------------------------------
    # Submit Menu load to Semarchy
    # ------------------------------------------
    logging.info("MENU HANDLING | POST MENUS | SUBMIT - START")

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
            f"MENU HANDLING | POST MENUS | SUBMIT - SUCCESS "
            f"(load_id={load_id}, batch_id={batch_id})"
        )

    except Exception as e:
        logging.error(f"MENU HANDLING | POST MENUS | SUBMIT - FAILED ({e})")
        raise ValueError(f"PNLD Menu load submission failed: {e}")

    # ------------------------------------------
    # Validate returned load_id
    # ------------------------------------------
    if not load_id:
        msg = "Semarchy returned no load_id for Menu load."
        logging.error(f"MENU HANDLING | POST MENUS | LOAD VALIDATION - FAILED ({msg})")
        raise ValueError(msg)

    # ------------------------------------------
    # Poll load status
    # ------------------------------------------
    status_url = f"{base_url}/{load_id}"
    max_attempts = 60
    interval = 1  # seconds

    logging.info(
        f"MENU HANDLING | POST MENUS | POLLING - START "
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
                f"MENU HANDLING | POST MENUS | POLLING - ATTEMPT {attempt}/{max_attempts} "
                f"(load_id={load_id}, status={load_status})"
            )

            # Terminal states
            if load_status in ("DONE", "WARNING", "ERROR"):
                logging.info(
                    f"MENU HANDLING | POST MENUS | POLLING - COMPLETE "
                    f"(load_id={load_id}, final_status={load_status})"
                )
                return load_status

        except Exception as e:
            logging.warning(
                f"MENU HANDLING | POST MENUS | POLLING - ATTEMPT FAILED "
                f"(attempt={attempt}, load_id={load_id}, error={e})"
            )

        time.sleep(interval)

    # ------------------------------------------
    # Timeout reached — load did not finish
    # ------------------------------------------
    total_wait = max_attempts * interval
    msg = (
        f"Menu load timed out after {total_wait}s "
        f"(load_id={load_id})."
    )

    logging.error(f"MENU HANDLING | POST MENUS | POLLING - TIMEOUT ({msg})")
    raise ValueError(msg)
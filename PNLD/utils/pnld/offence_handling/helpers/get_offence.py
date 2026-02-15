
import os
import logging
import requests

def get_offences(batch_id):

    logging.info(f"Retrieving Offence Revisions for Batch ID: {batch_id}")

    api_key = os.getenv("SemarchyAPIKey")
    base_url = os.getenv("SemarchyGetOffenceRevisionBatchNamedQueryURL")

    # -----------------------------
    # Validate configuration
    # -----------------------------
    if not api_key:
        msg = "Semarchy API Key missing — unable to retrieve Offence Revisions."
        logging.error(msg)
        raise ValueError(msg)

    if not base_url:
        msg = "Semarchy GET Offence Revision URL missing — unable to perform lookup."
        logging.error(msg)
        raise ValueError(msg)

    url = f"{base_url}?BATCH_ID={batch_id}"
    headers = {"API-Key": api_key}

    logging.debug(f"GET Request URL: {url}")

    # -----------------------------
    # Perform API request
    # -----------------------------
    try:
        logging.info("Sending GET request to Semarchy...")
        response = requests.get(url=url, headers=headers, timeout=(5, 30))
        response.raise_for_status()

        logging.info(f"GET request successful for Batch ID {batch_id}")

        # -----------------------------
        # Parse the records safely
        # -----------------------------
        json_body = response.json()
        records = json_body.get("records", [])

        logging.info(
            f"Retrieved {len(records)} Offence Revision record(s) for Batch ID {batch_id}"
        )

        return records

    except requests.exceptions.Timeout:
        msg = f"Timeout occurred while retrieving Offence Revisions for Batch ID {batch_id}"
        logging.error(msg)
        raise ValueError(msg)

    except requests.exceptions.HTTPError as e:
        msg = (
            f"HTTP error while retrieving Offence Revisions for Batch ID {batch_id}: "
            f"Status {response.status_code}, Error: {e}"
        )
        logging.error(msg)
        raise ValueError(msg)

    except requests.exceptions.RequestException as e:
        msg = f"Network error retrieving Offence Revisions for Batch ID {batch_id}: {e}"
        logging.error(msg)
        raise ValueError(msg)

    except Exception as e:
        msg = f"Unexpected error retrieving Offence Revisions for Batch ID {batch_id}: {e}"
        logging.error(msg)
        raise ValueError(msg)

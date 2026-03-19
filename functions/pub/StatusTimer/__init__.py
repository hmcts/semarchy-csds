import logging
import os
import time
import requests
import azure.functions as func


def main(myTimer: func.TimerRequest):

    logging.info("STATUS UPDATE PROCESS | START")

    # ------------------------------------------
    # Resolve environment configuration
    # ------------------------------------------
    base = os.getenv("SemarchyBaseURL")
    api_key = os.getenv("SemarchyAPIKey")

    if not base or not api_key:
        msg = "Missing SemarchyBaseURL, or SemarchyAPIKey."
        logging.error(f"CSDS POST DATA | Configuration Error ({msg})")
        raise ValueError(msg)

    post_url = f"{base.rstrip('/')}/loads/CSDS"
    headers = {"API-Key": api_key}

    # ------------------------------------------
    # Build Semarchy load submission
    # ------------------------------------------
    post_body = {
        "action": "CREATE_LOAD_AND_SUBMIT",
        "programName": "UPDATE_DATA_REST_API",
        "loadDescription": "Update Offence + Menu Status",
        "jobName": "ReleasePackageIntegrationLoad",
        "persistOptions": {
            "defaultPublisherId": "STATUS_UPDATE",
            "optionsPerEntity": {},
            "missingIdBehavior": "GENERATE",
            "persistMode": "IF_NO_ERROR_OR_MATCH"
        },
        "persistRecords": {}
    }

    # ------------------------------------------
    # SUBMIT LOAD (with full HTTP exception logging)
    # ------------------------------------------
    try:
        logging.info(f"SEMARCHY POST | SEND (url={post_url})")

        with requests.Session() as session:
            response = session.post(
                url=post_url,
                json=post_body,
                headers=headers,
                timeout=(5, 60)
            )

        # Trigger HTTPError if status code >= 400
        response.raise_for_status()

        load_id = response.json().get("load", {}).get("loadId")

        logging.info(
            f"SEMARCHY POST | SUCCESS "
            f"(status={response.status_code}, load_id={load_id})"
        )

    except requests.exceptions.HTTPError as http_err:
        status = getattr(http_err.response, "status_code", "UNKNOWN")
        body_preview = (
            http_err.response.text[:500] + "..."
            if http_err.response and http_err.response.text
            else "NO BODY"
        )

        logging.error(
            "SEMARCHY POST | HTTP ERROR DURING SUBMISSION "
            f"(status={status}, body_preview={body_preview}, error={http_err})"
        )
        return

    except requests.exceptions.Timeout as timeout_err:
        logging.error(
            f"SEMARCHY POST | TIMEOUT DURING SUBMISSION (error={timeout_err})"
        )
        return

    except requests.exceptions.RequestException as req_err:
        logging.error(
            f"SEMARCHY POST | NETWORK FAILURE DURING SUBMISSION (error={req_err})"
        )
        return

    except Exception as e:
        logging.exception(
            f"SEMARCHY POST | UNHANDLED FAILURE DURING SUBMISSION (error={e})"
        )
        return

    # ------------------------------------------
    # Validate load ID
    # ------------------------------------------
    if not load_id:
        logging.error("SEMARCHY LOAD | ERROR no loadId returned")
        return

    # ------------------------------------------
    # POLLING for load status
    # ------------------------------------------
    status_url = f"{post_url}/{load_id}"
    max_attempts = 60
    interval = 1  # seconds

    logging.info(
        f"SEMARCHY POLLING | START "
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

            status_body = response.json()
            load_status = status_body.get("loadStatus")

            logging.info(
                f"SEMARCHY POLLING | ATTEMPT {attempt}/{max_attempts} "
                f"(load_id={load_id}, status={load_status})"
            )

            # Exit early if job finished
            if load_status in ("DONE", "WARNING", "ERROR"):
                logging.info(
                    f"SEMARCHY POLLING | COMPLETE "
                    f"(load_id={load_id}, final_status={load_status})"
                )
                break

        except requests.exceptions.HTTPError as http_err:
            status = getattr(http_err.response, "status_code", "UNKNOWN")
            body_preview = (
                http_err.response.text[:300] + "..."
                if http_err.response and http_err.response.text
                else "NO BODY"
            )
            logging.warning(
                "SEMARCHY POLLING | HTTP ERROR "
                f"(attempt={attempt}, load_id={load_id}, status={status}, "
                f"body_preview={body_preview}, error={http_err})"
            )

        except Exception as e:
            logging.warning(
                f"SEMARCHY POLLING | ATTEMPT FAILED "
                f"(attempt={attempt}, load_id={load_id}, error={e})"
            )

        time.sleep(interval)

    else:
        # Polling timed out
        logging.error(
            f"SEMARCHY POLLING | TIMEOUT (load_id={load_id})"
        )
        return

    # ------------------------------------------
    # END — Completed process
    # ------------------------------------------
    logging.info("STATUS UPDATE PROCESS | COMPLETE")
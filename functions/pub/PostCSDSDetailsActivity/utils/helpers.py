import requests
import logging
import time


# ------------------------------------------------------
# Submit load request
# ------------------------------------------------------
def submit_load(base_url, headers, post_body):
    """Submit a CSDS load request and return the load_id."""
    try:
        logging.info("CSDS POST DATA | Load Submission - START")

        response = requests.post(
            url=base_url,
            json=post_body,
            headers=headers,
            timeout=(5, 60)
        )
        response.raise_for_status()

        load_id = response.json().get("load", {}).get("loadId")

        if not load_id:
            raise ValueError("CSDS POST DATA | Load Validation Failed (Semarchy returned no load_id for CSDS POST)")

        logging.info(
            f"CSDS POST DATA | Load Submission - SUCCESS (load_id={load_id})"
        )
        return load_id

    except Exception as e:
        raise ValueError(f"CSDS POST DATA | Load Submission - FAILED ({e})") from e


# ------------------------------------------------------
# Poll for completion
# ------------------------------------------------------
def poll_status(base_url, load_id, headers, max_attempts=60, interval=1):
    """Poll Semarchy until the load reaches a final state."""
    status_url = f"{base_url}/{load_id}"

    logging.info(
        f"CSDS POST DATA | Status Polling - START "
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

            data = response.json()
            status = data.get("loadStatus")

            logging.info(
                f"CSDS POST DATA | Poll Attempt {attempt}/{max_attempts} "
                f"(load_id={load_id}, status={status})"
            )

            if status in ("DONE", "WARNING", "ERROR", "SUSPENDED"):
                logging.info(
                    f"CSDS POST DATA | Polling Complete "
                    f"(load_id={load_id}, final_status={status})"
                )

                if status=="DONE":
                    return status
                else:
                    msg = (
                        f"CSDS POST DATA | Poll Attempt FAILED "
                        f"(Completed with status {status})"
                    )
                    raise ValueError(msg)
                

        except Exception as e:
            msg = (
                f"CSDS POST DATA | Poll Attempt FAILED "
                f"(attempt={attempt}, load_id={load_id}, error={e})"
            )
            raise ValueError(msg) from e

        time.sleep(interval)

    # Timeout
    total_wait = max_attempts * interval
    msg = f"CSDS POST DATA | Polling Timeout | Timed out after {total_wait}s (load_id={load_id})"
    raise ValueError(msg)


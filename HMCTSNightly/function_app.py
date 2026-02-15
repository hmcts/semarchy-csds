import os
import time
import uuid
import logging
import requests
from datetime import datetime

import azure.functions as func
import azure.functions.decorators as dec

# =============================
# Azure Function App
# =============================

app = dec.FunctionApp()

@app.schedule(
    schedule="*/30 * * * * *",
    arg_name="mytimer",
    run_on_startup=False
)
def update_offence_revisions(mytimer: func.TimerRequest):
    """
    Timer-triggered function:
      - Executes Named Queries
      - Builds OffenceRevision + OTEMenu payload
      - Submits a Semarchy load ONLY if updates exist
    """

    run_id = str(uuid.uuid4())
    start_ts = time.time()

    logging.info(f"[{run_id}] Function execution started.")

    # -----------------------------
    # Environment configuration
    # -----------------------------

    base_url = os.getenv("SemarchyBaseURL")
    api_key = os.getenv("SemarchyAPIKey")
    load_url = os.getenv("SemarchyLoadURL")

    missing = [
        name for name, val in {
            "SemarchyBaseURL": base_url,
            "SemarchyAPIKey": api_key,
            "SemarchyLoadURL": load_url,
        }.items() if not val
    ]

    if missing:
        logging.error(f"[{run_id}] Missing environment variables: {', '.join(missing)}")
        return

    headers = {"API-Key": api_key}

    # -----------------------------
    # Execute named queries
    # -----------------------------

    try:
        or_records, menu_records = process_named_queries(base_url, headers)
        logging.info(
            f"[{run_id}] Retrieved records → "
            f"OffenceRevisions={len(or_records)}, Menus={len(menu_records)}"
        )
    except Exception as exc:
        logging.exception(f"[{run_id}] Named query processing failed: {exc}")
        return

    # 🚨 EARLY EXIT — nothing to update
    if not or_records and not menu_records:
        logging.info(
            f"[{run_id}] No update candidates found. "
            "Semarchy load submission skipped."
        )
        return

    # -----------------------------
    # Build POST body
    # -----------------------------

    post_body = generate_post_body(or_records, menu_records)

    if not post_body:
        logging.info(f"[{run_id}] POST body empty. Semarchy POST skipped.")
        return

    # -----------------------------
    # Submit to Semarchy
    # -----------------------------

    post_url = f"{base_url}/{load_url}"
    logging.info(f"[{run_id}] Submitting Semarchy load → {post_url}")

    try:
        response = requests.post(
            url=post_url,
            json=post_body,
            headers=headers,
            timeout=(5, 60)
        )
    except requests.exceptions.Timeout:
        logging.error(f"[{run_id}] Semarchy POST timed out.")
        return
    except requests.exceptions.RequestException as exc:
        logging.exception(f"[{run_id}] Semarchy POST failed: {exc}")
        return

    # -----------------------------
    # Handle response
    # -----------------------------

    if 200 <= response.status_code < 300:
        logging.info(
            f"[{run_id}] Semarchy POST succeeded "
            f"(status={response.status_code})."
        )
    else:
        logging.error(
            f"[{run_id}] Semarchy POST failed "
            f"(status={response.status_code}): "
            f"{response.text[:1000]}"
        )

    duration_ms = int((time.time() - start_ts) * 1000)
    logging.info(
        f"[{run_id}] Function completed in {duration_ms} ms."
    )


# =============================
# Helper Functions
# =============================

def process_named_queries(base_url: str, headers: dict):
    """
    Executes all environment variables prefixed with NQ_
    and aggregates OffenceRevision + OTEMenu updates.
    """

    nq_keys = sorted(k for k in os.environ if k.startswith("NQ_"))

    if not nq_keys:
        logging.warning("No NQ_* environment variables found.")
        return [], []

    persisted_or = []
    persisted_menus = []
    seen_or_ids = set()
    seen_menu_ids = set()

    for key in nq_keys:
        nq_path = os.getenv(key)
        if not nq_path:
            continue

        nq_url = f"{base_url}/{nq_path}"
        logging.info(f"Executing named query {key} → {nq_url}")

        api_body = get_api_body(nq_url, headers)
        records = api_body.get("records", [])

        logging.info(f"{key} returned {len(records)} records.")

        for record in records:
            or_id = record.get("OffenceRevisionID")
            if or_id is None or or_id in seen_or_ids:
                continue

            seen_or_ids.add(or_id)

            # -------------------------
            # Process Menus
            # -------------------------

            menus_text = record.get("Menus") or ""
            menu_ids = [
                int(part) for part in menus_text.split("-")
                if part.strip()
            ]

            for menu_id in menu_ids:
                if menu_id not in seen_menu_ids:
                    seen_menu_ids.add(menu_id)
                    persisted_menus.append({"OTEMenuID": menu_id})

            # -------------------------
            # Process OffenceRevision
            # -------------------------

            cleaned = {
                k: v for k, v in record.items()
                if v is not None and k != "Menus"
            }

            cleaned["ChangedDate"] = datetime.utcnow().strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
            cleaned["ChangedBy"] = "AzureFunction"

            persisted_or.append(cleaned)

    logging.info(
        f"Aggregated totals → "
        f"OffenceRevisions={len(persisted_or)}, "
        f"Menus={len(persisted_menus)}"
    )

    return persisted_or, persisted_menus


def generate_post_body(persisted_or, persisted_menus):
    """
    Builds Semarchy load payload.
    Returns None if nothing to persist.
    """

    if not persisted_or and not persisted_menus:
        return None

    return {
        "action": "CREATE_LOAD_AND_SUBMIT",
        "programName": "UPDATE_DATA_REST_API",
        "loadDescription": "Update Offence Revision + Menu Status",
        "jobName": "OffenceStatus",
        "persistOptions": {
            "responsePayload": "SUMMARY",
            "optionsPerEntity": {
                "OTEMenu": {
                    "enrichers": ["OffenceMenuUpdateFromRevision"]
                }
            },
            "missingIdBehavior": "GENERATE",
            "persistMode": "IF_NO_ERROR_OR_MATCH"
        },
        "persistRecords": {
            "OffenceRevision": persisted_or,
            "OTEMenu": persisted_menus
        }
    }


def get_api_body(url: str, headers: dict, timeout=(5, 30)):
    """
    Executes GET request and returns parsed JSON.
    Returns {} on failure.
    """

    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        logging.error(f"GET failed for {url}: {exc}")
        return {}

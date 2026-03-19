import azure.durable_functions as df
from datetime import datetime as dt, timedelta
import logging
import os


def orchestrator(context: df.DurableOrchestrationContext):

    # -------------------------------------------
    # Helper to stop execution on any fatal error
    # -------------------------------------------
    def fail(message: str):
        logging.error(message)
        raise Exception(message)   # Durable-safe deterministic fail

    # Helper to avoid duplicate logs on replay
    def log_info(message: str):
        if not context.is_replaying:
            logging.info(message)

    # ----------------------------------------------------------
    # 1. Read & Validate Input
    # ----------------------------------------------------------
    rp_input = context.get_input() or {}

    rp_id = rp_input.get("ReleasePackageID")
    rp_urgent = rp_input.get("Urgent", "No")

    if not rp_id:
        fail(f'RELEASE PACKAGE ID: None | No Release Package ID')

    log_info(f'RELEASE PACKAGE ID: {rp_id} | URGENT | Urgent Flag is "{rp_urgent}"')

    is_urgent = str(rp_urgent).strip().lower() in ["yes", "true", "1"]

    # ----------------------------------------------------------
    # 2. Conditional Delay (Timer)
    # ----------------------------------------------------------
    if is_urgent:
        delay_seconds = int(os.getenv("UrgentWaitPeriodSeconds"))
        deadline = context.current_utc_datetime + timedelta(seconds=delay_seconds)

        log_info(
            f'RELEASE PACKAGE ID: {rp_id} | URGENT | '
            f'Pausing execution for {delay_seconds} seconds. '
            f'Timer deadline = {deadline}'
        )

        yield context.create_timer(deadline)

    # ----------------------------------------------------------
    # 3. Fetch Release Package Details
    # ----------------------------------------------------------
    rp_details_list = yield context.call_activity(
        "GetCSDSDetailsActivity",
        {
            "ReleasePackageID": rp_id,
            "Type": "ReleasePackage"
        }
    )

    if not rp_details_list or not isinstance(rp_details_list, list):
        fail(f'RELEASE PACKAGE ID: {rp_id} | GET CSDS DETAILS | Invalid or no details received')

    rp_volume = len(rp_details_list)

    if rp_volume == 0:
        fail(f'RELEASE PACKAGE ID: {rp_id} | GET CSDS DETAILS | No details returned')

    if rp_volume > 1:
        fail(f'RELEASE PACKAGE ID: {rp_id} | GET CSDS DETAILS | Multiple details returned')

    log_info(f'RELEASE PACKAGE ID: {rp_id} | GET CSDS DETAILS | 1 detail record received')

    rp_details = rp_details_list[0]

    # ----------------------------------------------------------
    # 4. Validate Business Rules
    # ----------------------------------------------------------
    status = rp_details.get("Status")

    if status != "Publish to Live":
        log_info(f'RELEASE PACKAGE ID: {rp_id} | GET CSDS DETAILS | Release Package is not set to Publish to Live')
        return None

    publish_date_str = rp_details.get("PublishDate")
    if not publish_date_str:
        fail(f'RELEASE PACKAGE ID: {rp_id} | PUBLISH DATE VALIDATION | Missing PublishDate')

    try:
        publish_dt = dt.fromisoformat(publish_date_str.replace("Z", "+00:00"))
    except Exception:
        fail(
            f'RELEASE PACKAGE ID: {rp_id} | PUBLISH DATE VALIDATION | '
            f'Invalid PublishDate format "{publish_date_str}"'
        )

    current_dt = context.current_utc_datetime

    if publish_dt > current_dt:
        log_info(f'RELEASE PACKAGE ID: {rp_id} | GET CSDS DETAILS | Publish Date is in the past: {publish_dt}')
        return None

    # ----------------------------------------------------------
    # 6. Persist the Result
    # ----------------------------------------------------------
    persist_payload = {
        "ReleasePackage": [
            {
                'ReleasePackageID': rp_id,
                'Status': 'Published'
            }
        ]
    }

    load_status = yield context.call_activity(
        "PostCSDSDetailsActivity",
        persist_payload
    )

    if load_status == 'DONE':
        log_info(f'RELEASE PACKAGE ID: {rp_id} | SEMARCHY LOAD | DONE')
    else:
        fail(f'RELEASE PACKAGE ID: {rp_id} | SEMARCHY LOAD | Failed with status {load_status}')


main = df.Orchestrator.create(orchestrator)
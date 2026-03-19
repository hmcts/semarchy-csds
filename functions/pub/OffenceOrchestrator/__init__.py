import azure.functions as func
import azure.durable_functions as df
from azure.durable_functions import RetryOptions
import logging

from OffenceOrchestrator.utils.generate_csds_payload import generate_csds_payload


def orchestrator(context: df.DurableOrchestrationContext):
    """
    Orchestrator function that retrieves offence revision details and processes them in parallel.
    
    1. Calls the GetCSDSDetailsActivity to fetch offence details based on the Release Package ID input.
    2. If offence details are retrieved, it creates parallel tasks to process each offence item using the OffenceActivity function.
    3. Waits for all parallel tasks to complete and returns the results.
    """
    # Helper to avoid duplicate logs on replay
    def log_info(message: str):
        if not context.is_replaying:
            logging.info(message)

    # Define retry options for activities - TODO: remove this?
    retry_options = RetryOptions(
        first_retry_interval_in_milliseconds=5000,
        max_number_of_attempts=3
    )
    retry_options.backoff_coefficient = 2.0

    # ----------------------------------------------------------
    # 1. Read Input
    # ----------------------------------------------------------
    rp_id = context.get_input()
    get_config = {
        "ReleasePackageID": rp_id,
        "Type": "Offence"
    }

    # Handle missing rp_id - TODO: Check with TG about how to log these
    if not context.is_replaying:
        if not rp_id:
            return {
                "status": "Error",
                "message": "ReleasePackageID missing from input."
            }

    log_prefix = f"RELEASE PACKAGE ID: {rp_id} | OFFENCE ORCHESTRATOR |"

    try:
        # ----------------------------------------------------------
        # 2. Fetch Offence Details
        # ----------------------------------------------------------
        log_info(f'{log_prefix} GET CSDS DETAILS | Started')

        offence_details = yield context.call_activity_with_retry(
            "GetCSDSDetailsActivity",
            retry_options,
            get_config
        )

        log_info(f'{log_prefix} GET CSDS DETAILS | Completed Successfully - {len(offence_details)} Offence Items Returned')

        # Handle empty or invalid results from Named Query - TODO: Check with TG about how to log these
        if not context.is_replaying:
            if not offence_details:
                return {
                    "status": "Error",
                    "message": "GetCSDSDetailsActivity returned no results."
                }

        # ----------------------------------------------------------
        # 3. Perform Offence Processing
        # ----------------------------------------------------------
        log_info(f'{log_prefix} OFFENCE ACTIVITY | Started')

        parallel_tasks = [
            context.call_activity_with_retry(
                "OffenceActivity",
                retry_options,
                item
            )
            for item in offence_details
        ]

        # If there are zero offences, return empty result without calling task_all - TODO: Check with TG about how to log these
        if not parallel_tasks:
            if not context.is_replaying:
                logging.info(f"rp_id: {rp_id} - No offence items to process.")
            return []

        # Wait for all parallel tasks to complete and gather results
        offence_results = yield context.task_all(parallel_tasks)
        
        log_info(f'{log_prefix} OFFENCE ACTIVITY | Completed - {len(offence_results)} Offence Items Processed')

        status = "Offences Submitted"
        message = None

    except Exception as e:
        offence_results = []
        status = "Error (Live)"
        message = str(e)

    # ----------------------------------------------------------
    # 4. Send to CSDS
    # ----------------------------------------------------------

    # Generate Persist Records Payload based on output of Offence Activity
    persist_payload = generate_csds_payload(offence_results)

    # Generate Release Package Payload
    release_package_payload = [{
        "ReleasePackageID": rp_id,
        "PublishingStatus": status
        # "PublishingErrorMessage": message     TODO: Add back in once error logging has been added
    }]

    # Add Release Package payload to the Persist Record Payload
    persist_payload["ReleasePackage"] = release_package_payload
    log_info(f'{log_prefix} POST CSDS DETAILS | Started')

    # Post to CSDS
    load_status = yield context.call_activity(
        "PostCSDSDetailsActivity",
        persist_payload
    )

    log_info(f'{log_prefix} POST CSDS DETAILS | Completed - {load_status}')

    # ----------------------------------------------------------
    # Final Structured Result
    # ----------------------------------------------------------
    return {
        "Status": "SUCCESS"
    }


# Azure Functions entrypoint
main = df.Orchestrator.create(orchestrator)

import azure.durable_functions as df
import logging

from OffenceMenuOrchestrator.utils.generate_csds_payload import generate_csds_payload

from utils.fail_handling import fail
from utils.release_package_content_handling import get_release_package_content_id

def orchestrator(context: df.DurableOrchestrationContext):
    """
    Orchestrator function that retrieves offence menu details and processes them in parallel.
    
    1. Calls the GetCSDSDetailsActivity to fetch offence menu details based on the Release Package ID input.
    2. If menu details are retrieved, it creates parallel tasks to process each menu item using the MenuActivity function.
    3. Waits for all parallel tasks to complete and returns the results.
    """
    # Helper to avoid duplicate logs on replay
    def log_info(message: str):
        if not context.is_replaying:
            logging.info(message)

    # ----------------------------------------------------------
    # 1. Read Input
    # ----------------------------------------------------------
    rp_id = context.get_input()
    get_config = {
        "ReleasePackageID": rp_id,
        "Type": "OffenceMenu"
    }

    log_prefix = f"RELEASE PACKAGE ID: {rp_id} | MENU ORCHESTRATOR |"

    try:
        # ----------------------------------------------------------
        # 2. Fetch Offence Menu Details
        # ----------------------------------------------------------
        log_info(f'{log_prefix} GET CSDS DETAILS | Offence Menu - Started')

        menu_details = yield context.call_activity(
            "GetCSDSDetailsActivity",
            get_config
        )

        log_info(f'{log_prefix} GET CSDS DETAILS | Offence Menu - Completed Successfully - {len(menu_details)} Offence Menu Items Returned')

        # ----------------------------------------------------------
        # 3. Perform OM Processing
        # ----------------------------------------------------------
        log_info(f'{log_prefix} OFFENCE MENU ACTIVITY | Started')

        parallel_tasks = [
            context.call_activity(
                "OffenceMenuActivity",
                menu
            )
            for menu in menu_details
        ]

        # Wait for all parallel tasks to complete and gather results
        menu_results = yield context.task_all(parallel_tasks)
        log_info(f'{log_prefix} OFFENCE MENU ACTIVITY | Completed - {len(menu_results)} Offence Menu Items Processed')

        # ----------------------------------------------------------
        # 4. Handle OM Process Ouput
        # ----------------------------------------------------------
        # Raise Failure if not all Menus were processed
        if len(menu_results)!=len(menu_details):
            fail(f"{log_prefix} Recieved {len(menu_details)} Menu Items but processed {len(menu_results)} Menu Items")
        
        # Determine if any Menus failed processing
        failed_count = sum(1 for menu in menu_results if menu.get("ActivityStatus") == "FAILED")
        success_count = sum(1 for menu in menu_results if menu.get("ActivityStatus") == "SUCCESS")
        log_info(f"{log_prefix} Successful Menu Items: {success_count}, Failed Menu Items: {failed_count}")

        # If no Menus Failed, Decalre Release Package as Menus Submitted
        if failed_count==0:

            # Declare Release Package Details
            status_attribute = "PublishingStatus"
            rp_status = "Menus Submitted"
            rp_error_message = None
        
        else:

            # ----------------------------------------------------------
            # 4.1. Get Release Package Contents and filter for only Menus
            # ----------------------------------------------------------
            get_config = {
                "ReleasePackageID": rp_id,
                "Type": "ReleasePackageContents"
            }
            
            log_info(f'{log_prefix} GET CSDS DETAILS | Release Package Contents - Started')
            rp_contents = yield context.call_activity(
                "GetCSDSDetailsActivity",
                get_config
            )
            log_info(f'{log_prefix} GET CSDS DETAILS | Release Package Contents - Completed Successfully - {len(menu_details)} Release Package Contents Returned')
            
            # Filter so only Menus remain
            menu_rp_contents = [
                item for item in rp_contents
                if item.get("ReleasePackageContentType") == "Offence Menu"
            ]

            # Fail if number of menus in contents does not match what is expected
            if len(menu_rp_contents)!=len(menu_results):
                fail(f"{log_prefix} Recieved {len(menu_rp_contents)} Menu Items within Release Package Contents but processed {len(menu_results)} Menu Items")

            # ----------------------------------------------------------
            # 4.2. Update OM Payload to include Release Package Contents
            # ----------------------------------------------------------
            menu_results = get_release_package_content_id(log_prefix, menu_results, menu_rp_contents, "OffenceMenu", "OffenceMenuID")

            # Declare Release Package Details
            status_attribute = "Status"
            rp_status = "Error (Live)"
            rp_error_message = f"Failed to process {failed_count} Menus"

    except Exception as e:
        menu_results = []
        status_attribute = "Status"
        rp_status = "Error (Live)"
        rp_error_message = str(e).strip().split("\n")[0].strip()

    # ----------------------------------------------------------
    # 5. Send to CSDS
    # ----------------------------------------------------------
    # Generate Persist Records Payload based on output of Menu Activity
    persist_payload = generate_csds_payload(menu_results)

    # Generate Release Package Payload
    release_package_payload = [{
        "ReleasePackageID": rp_id,
        status_attribute: rp_status,
        "PublishingErrorMessage": rp_error_message
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
    
    # If there were any failures processing the Menu, then the Orchestrator is in ERROR
    if rp_status=="Error (Live)":
        orchestrator_status = "FAILED"
    else:
        orchestrator_status = "SUCCESS"

    # ----------------------------------------------------------
    # Final Structured Result
    # ----------------------------------------------------------
    return {
        "OrchestratorStatus": orchestrator_status
    }


# Azure Functions entrypoint
main = df.Orchestrator.create(orchestrator)
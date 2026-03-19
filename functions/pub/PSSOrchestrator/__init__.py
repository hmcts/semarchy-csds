import azure.durable_functions as df
import logging



def orchestrator(context: df.DurableOrchestrationContext):

    # Helper to avoid duplicate logs on replay
    def log_info(message: str):
        if not context.is_replaying:
            logging.info(message)

    # ----------------------------------------------------------
    # 1. Read & Validate Input
    # ----------------------------------------------------------
    rp_input = context.get_input() or {}
    rp_id = rp_input.get("ReleasePackageID")

    if rp_id is None:
        raise ValueError("RELEASE PACKAGE | Missing Release Package ID")
    
    log_prefix = f"RELEASE PACKAGE ID: {rp_id} |"

    # ----------------------------------------------------------
    # 2. Call Create Release Package Orchestrator
    # ----------------------------------------------------------
    log_info(f"{log_prefix} CREATE RELEASE PACKAGE | START")

    # Returns the Status of the Create Release Package Orchestrator
    create_rp_result = yield context.call_sub_orchestrator("CreateReleasePackageOrchestrator", rp_input)
    create_rp_status = create_rp_result.get("Status")
    log_info(f"{log_prefix} CREATE RELEASE PACKAGE | {create_rp_status}")


    # If the Status is not SUCCESS then the Release Package is no longer suitable to continue to PSS
    # Whole Orchestrator stops
    if create_rp_status != "SUCCESS":

        # Extract Reason for no longer being sutiable and log
        reason = create_rp_result.get("Reason")
        logging.info(f'{log_prefix} CREATE RELEASE PACKAGE | {create_rp_status} | {reason}')

        return None
        

    # ----------------------------------------------------------
    # 3. Call Offence Menu Orchestrator
    # ----------------------------------------------------------
    log_info(f"{log_prefix} OFFENCE MENU | START")

    # Returns the Status of the Create Offence Menu Orchestrator
    om_result = yield context.call_sub_orchestrator("OffenceMenuOrchestrator", rp_id)
    om_status = om_result.get("Status")
    log_info(f"{log_prefix} OFFENCE MENU | {om_status}")

    if om_status=="FAILED":
        return 

    # ----------------------------------------------------------
    # 4. Call Offence Orchestrator
    # ----------------------------------------------------------
    ofr_result = yield context.call_sub_orchestrator("OffenceOrchestrator", rp_id)

    # ----------------------------------------------------------
    # 5. Call Get Release Package Orchestrator
    # ----------------------------------------------------------

main = df.Orchestrator.create(orchestrator)
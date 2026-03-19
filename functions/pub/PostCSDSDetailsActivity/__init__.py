import os
import logging

from utils.fail_handling import fail

from PostCSDSDetailsActivity.utils.helpers import submit_load
from PostCSDSDetailsActivity.utils.helpers import poll_status

def main(persist_records):
    """Entry point for posting data to CSDS and monitoring load status."""


    # ------------------------------------------
    # Environment resolution (kept inside main)
    # ------------------------------------------
    base = os.getenv("SemarchyBaseURL")
    api_key = os.getenv("SemarchyAPIKey")

    if not base or not api_key:
        fail("CSDS POST DATA | Configuration Error (Missing SemarchyBaseURL, or SemarchyAPIKey)")

    base_url = f"{base.rstrip('/')}/loads/CSDS"
    headers = {"API-Key": api_key}

    # ------------------------------------------
    # Body definition (kept inside main)
    # ------------------------------------------
    post_body = {
        "action": "CREATE_LOAD_AND_SUBMIT",
        "programName": "UPDATE_DATA_REST_API",
        "loadDescription": "Publishing Integration Load",
        "jobName": "ReleasePackageIntegrationLoad",
        "persistOptions": {
            "defaultPublisherId": "",
            "optionsPerEntity": {},
            "missingIdBehavior": "GENERATE",
            "persistMode": "IF_NO_ERROR_OR_MATCH"
        },
        "persistRecords": persist_records
    }

    # ------------------------------------------
    # Submit load
    # ------------------------------------------
    load_id = submit_load(base_url, headers, post_body)

    if not load_id:
        fail("CSDS POST DATA | Load Validation Failed (Semarchy returned no load_id for CSDS POST)")

    # ------------------------------------------
    # Poll for final status
    # ------------------------------------------
    load_status = poll_status(base_url, load_id, headers)

    if load_status!='DONE':
        fail(f'CSDS Load failed with status {load_status}')
    
    return load_status
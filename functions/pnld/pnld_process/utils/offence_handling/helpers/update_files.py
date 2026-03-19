
from pnld_process.utils.message_handling import add_message

def update_files(source_files, messages, loaded_offences, offences, error_message=None):

    # Convert to lookup maps for fast access
    loaded_lookup = {item["CJSCode"]: item["OffenceRevisionID"] for item in loaded_offences}
    source_lookup = {item["SourceFileID"]: item for item in source_files}

    for offence in offences:
        cjs = offence["CJSCode"]
        xml_file_id = offence["xml_file_id"]

        src = source_lookup[xml_file_id]

        # ------------------------------
        # CASE: Offence exists → SUCCESS
        # ------------------------------
        if cjs in loaded_lookup:
            offence_id = loaded_lookup[cjs]

            has_cleanses = any(
                    msg.get("FID_SourceFile") == xml_file_id 
                    and msg.get("MessageType") == "CLEANSE"
                    for msg in messages
                )

            # If xml_file_id has Cleanses then mark that within the status
            if has_cleanses:   
                src["FID_SourceStatus"] = "Scheduling Complete - Clean Up Made"
            else:
                src["FID_SourceStatus"] = "Scheduling Complete"

            src["MessageCount"] += 1

            messages = add_message(
                    messages=messages,
                    file_id=xml_file_id,
                    code='CO-NSDT-COMPLETED-001',
                    msg_type='COMPLETION',
                    issue='File Successfully Transformed and Offence ingested into CSDS',
                    cause='Completed as expected',
                    resolution='Follow the View Offence link found within the PNLD File Overview'
                )

            # Add revision to offence record
            src["FID_OffenceRevision"] = offence_id

        # ------------------------------
        # CASE: Offence not loaded → FAILURE
        # ------------------------------
        else:
             # Update source file
            src["FID_SourceStatus"] = "Failed"
            src["MessageCount"] += 1

            # Add Failure message
            messages = add_message(
                    messages=messages,
                    file_id=xml_file_id,
                    code='ER-SUPP-OFFENCELOAD-001',
                    msg_type='ERROR',
                    issue='Associated Offence within File failed ingestion into CSDS',
                    cause=error_message,
                    resolution='CONTACT SUPPORT TEAM'
                )

    return source_files, messages

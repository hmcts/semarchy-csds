

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

            # Update source file
            src["FID_SourceStatus"] = "Scheduling Complete"
            src["MessageCount"] += 1

            # Add success message
            messages.append({
                "FID_SourceFile": xml_file_id,
                "SourceFileMessageCode": "AA-100",
                "SourceFileMessageType": "COMPLETION",
                "SourceFileMessage": 'Offence Ingested Successfully',
            })

            # Add revision to offence record
            src["FID_OffenceRevision"] = offence_id

        # ------------------------------
        # CASE: Offence not loaded → FAILURE
        # ------------------------------
        else:
             # Update source file
            src["FID_SourceStatus"] = "Failed"
            src["MessageCount"] += 1

            # Add success message
            messages.append({
                "FID_SourceFile": xml_file_id,
                "SourceFileMessageCode": "XX-100",
                "SourceFileMessageType": "ERROR",
                "SourceFileMessage": f'Offence Failed to ingest',
            })

    return source_files, messages

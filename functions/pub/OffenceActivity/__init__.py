import logging
import random

from OffenceActivity.utils.create_change_set_header import build_payload_create_change_set_header, build_soap_xml_create_change_set_header
from OffenceActivity.utils.get_offence_full import build_payload_get_offence_full, build_soap_xml_get_offence_full
from OffenceActivity.utils.get_offence_summary import build_payload_get_offence_summary, build_soap_xml_get_offence_summary
from OffenceActivity.utils.update_offence_full import build_payload_update_offence_full, build_soap_xml_update_offence_full
from OffenceActivity.utils.generate_csds_payload import generate_ote_payload
from OffenceActivity.utils.extract_offence_revision_full import extract_offence_revision_json_from_root
from utils.pss_interaction import pss_post


def main(offence_details: dict):
    """
    Process Offence requests for sending to PSS.
+
        0) Validate the input data and check for key fields
+       1) Build the createChangeSetHeader SOAP XML request
+       2) Build the updateOffenceFull SOAP XML request
+       3) Build the getOffenceSummary SOAP XML request
+       4) Build the getOffenceFull SOAP XML request
+   
    All XML outputs are temporarily uploaded to Blob Storage for testing purposes
    """
    #**********************************************************
    # Step 0: Validate input
    #**********************************************************
    # Validate the item is a dict
    if not isinstance(offence_details, dict):
        raise ValueError(f"OFFENCE ACTIVITY | Expected a dict but got {type(offence_details).__name__}")

    # Validate required keys
    if "OffenceRevisionID" not in offence_details:
        raise ValueError(f"OFFENCE ACTIVITY | Missing OffenceRevisionID in item: {offence_details}")

    ofr_id = offence_details.get("OffenceRevisionID")

    if ofr_id is None:
        raise ValueError(f"OFFENCE ACTIVITY | OffenceRevisionID is missing from offence_details: {offence_details}")

    log_prefix = f"OFFENCE ACTIVITY | OFFENCE REVISION ID: {ofr_id} |"

    try:
        #**********************************************************
        # Step 1: Send Offence createChangeSetHeader to PSS and handle response
        #**********************************************************
        # Build JSON payload
        logging.info(f"{log_prefix} Building createChangeSetHeader payload")
        csh_payload = build_payload_create_change_set_header(offence_details)
        logging.info(f"{log_prefix} createChangeSetHeader payload built successfully")
        
        logging.info(f"{log_prefix} Building full createChangeSetHeader SOAP XML")
        csh_soap_xml = build_soap_xml_create_change_set_header(csh_payload)
        logging.info(f"{log_prefix} createChangeSetHeader XML built successfully")


        # Send Change Set Header XML to PSS and get reponse body back
        pss_csh_response = pss_post(csh_soap_xml)
    
        # Namespace mappings used to search the XML response
        ns = {
            "soap": "http://schemas.xmlsoap.org/soap/envelope/",
            "tns": "http://www.justice.gov.uk/magistrates/pss/CreateChangeSetHeaderRequest",
            "s0": "http://www.justice.gov.uk/magistrates/pss/CreateChangeSetHeaderResponse"
        }

        # Extract the returned PSS Release Package ID
        pss_csh_pk = pss_csh_response.find(".//ChangeSetHeaderPK", ns).text

        if not pss_csh_pk:
            raise ValueError(f"{log_prefix} PSS CSH not returned")
        logging.info(f"{log_prefix} Change Set Header ID returned: {pss_csh_pk}")


        #**********************************************************
        # Step 2: Send updateOffenceFull to PSS and handle response
        #**********************************************************
        # Build JSON payload
        logging.info(f"{log_prefix} Building updateOffenceFull payload")
        update_ofr_full_payload = build_payload_update_offence_full(offence_details, pss_csh_pk)
        logging.info(f"{log_prefix} updateOffenceFull payload built successfully")
        
        logging.info(f"{log_prefix} Building full updateOffenceFull SOAP XML")
        update_ofr_full_soap_xml = build_soap_xml_update_offence_full(update_ofr_full_payload)
        logging.info(f"{log_prefix} updateOffenceFull XML built successfully")

        # Send Update Offence Full XML to PSS and get response body back
        pss_update_ofr_full_response = pss_post(update_ofr_full_soap_xml)

        # Namespace mappings used to search the XML response
        ns = {
            "soap": "http://schemas.xmlsoap.org/soap/envelope/",
            "ack": "http://www.justice.gov.uk/magistrates/ack"
        }

        # Extract the returned PSS Release Package ID
        update_ofr_full_ack = pss_update_ofr_full_response.find(".//MessageStatus", ns).text

        if not update_ofr_full_ack:
            raise ValueError(f"{log_prefix} updateOffenceFull Acknowledgment not returned")
        logging.info(f"{log_prefix} updateOffenceFull Acknowledgment returned: {update_ofr_full_ack}")


        #**********************************************************
        # Step 3: End here if the change is an edit
        # TODO May want to update this to be if the PSS ID = Semarchy PSS ID??????
        #**********************************************************
        if not offence_details.get("VersionType") in ["Initial", "New Revision"]:
            logging.info(f"{log_prefix} Version Type is not Initial or New Revision ({offence_details.get('VersionType')})")
            # Return the IDs and status to the caller
            ofr_payload = [{
                "OffenceRevisionID": int(ofr_id),
                "PSSChangeSetHeaderID": int(pss_csh_pk),
                "PublishingStatus": "Submitted"
            }]

            return {
                "OffenceMenu": ofr_payload,
                "Status": "SUCCESS"
            }


        #**********************************************************
        # Step 4: Send getOffenceSummary to PSS and handle response (Initial / New Revision only)
        #**********************************************************
        # Build JSON payload
        logging.info(f"{log_prefix} Building getOffenceSummary payload")
        get_ofr_summary_payload = build_payload_get_offence_summary(offence_details)
        logging.info(f"{log_prefix} getOffenceSummary payload built successfully")
        
        logging.info(f"{log_prefix} Building full getOffenceSummary SOAP XML")
        get_ofr_summary_soap_xml = build_soap_xml_get_offence_summary(get_ofr_summary_payload)
        logging.info(f"{log_prefix} getOffenceSummary XML built successfully")

        # Send Get Offence Summary XML to PSS and get response body back
        pss_get_ofr_summary_response = pss_post(get_ofr_summary_soap_xml)

        # Namespace mappings used to search the XML response
        ns = {
            "soap": "http://schemas.xmlsoap.org/soap/envelope/",
            "ns7":   "http://www.justice.gov.uk/magistrates/pss/GetReferenceDataResponse",
        }

        # Extract the returned PSS Offence Revision ID
        pss_ofr_pk = pss_get_ofr_summary_response.find('.//DataItem[@ColumnOrder="1"]/Value', ns).text
    
        if not pss_ofr_pk:
            raise ValueError(f"{log_prefix} PSS OFR ID not returned")
        logging.info(f"{log_prefix} PSS OFR ID returned: {pss_ofr_pk}")


        #**********************************************************
        # Step 5: Send getOffenceFull to PSS and handle response (Initial / New Revision only)
        #**********************************************************
        # Build JSON payload
        logging.info(f"{log_prefix} Building getOffenceFull payload")
        get_ofr_full_payload = build_payload_get_offence_full(offence_details, pss_ofr_pk)
        logging.info(f"{log_prefix} getOffenceFull payload built successfully")
        
        logging.info(f"{log_prefix} Building full getOffenceFull SOAP XML")
        get_ofr_full_soap_xml = build_soap_xml_get_offence_full(get_ofr_full_payload)
        logging.info(f"{log_prefix} getOffenceFull XML built successfully")

        # Send Get Offence Full XML to PSS and get response body back
        pss_get_ofr_full_response = pss_post(get_ofr_full_soap_xml)
        # Namespace mappings used to search the XML response
        ns = {
            "soap": "http://schemas.xmlsoap.org/soap/envelope/",
            "ns58": "http://www.justice.gov.uk/magistrates/pss/GetOffenceFullResponse",
        }

        # Extract the returned PSS Offence Revision payload
        pss_offence_revision = extract_offence_revision_json_from_root(pss_get_ofr_full_response, ns)


        #**********************************************************
        # Step 6: Generate the Semarchy payloads
        #**********************************************************
        # Generate Offence Revision Payload
        ofr_payload = [{
            "OffenceRevisionID": int(ofr_id),
            "PSSOffenceRevisionID": int(pss_offence_revision.get("OFR_ID")),
            "PSSChangeSetHeaderID": int(pss_csh_pk) ,
            "PublishingStatus": "Submitted"
        }]

        ###### Generate Offence Header and Terminal Entries Payloads
        # ote_payload = generate_ote_payload(offence_details, pss_offence_revision)
        # oh_payload = generate_oh_payload(offence_details, pss_offence_revision)

        # Return the IDs and status to the caller
        return {
            # "OffenceHeader": oh_payload,
            "OffenceRevision": ofr_payload,
            # "OffenceTerminalEntries": ote_payload,
            "ActivityStatus": "SUCCESS"
        }

    except Exception as ex:
        logging.error(f"{log_prefix} {str(ex)}")
        error_payload = [{
            "OffenceRevisionID": int(ofr_id),
            "PublishingStatus": "Error"
        }]
        return {
            "OffenceRevisionID": error_payload,
            "ActivityStatus": "Error (Live)",
            "ActivityReason": str(ex)
        }

import logging
import random
import xml.etree.ElementTree as ET

from OffenceMenuActivity.utils.create_change_set_header import build_payload_create_change_set_header, build_soap_xml_create_change_set_header
from OffenceMenuActivity.utils.get_offence_menu_full import build_payload_get_offence_menu_full, build_soap_xml_get_offence_menu_full
from OffenceMenuActivity.utils.get_offence_menu_summary import build_payload_get_offence_menu_summary, build_soap_xml_get_offence_menu_summary
from OffenceMenuActivity.utils.update_offence_menu_full import build_payload_update_offence_menu_full, build_soap_xml_update_offence_menu_full
from OffenceMenuActivity.utils.generate_csds_payload import (generate_omo_payload, generate_oed_payload)
from OffenceMenuActivity.utils.extract_offence_menu_full import extract_offence_menu_json_from_root

from utils.pss_interaction import pss_post

def main(menu_details: dict):
    """
    Process Offence Menu requests for sending to PSS.
+
        0) Validate the input data and check for key fields
+       1) Build the createChangeSetHeader SOAP XML request
+       2) Build the updateOffenceMenuFull SOAP XML request
+       3) Build the getOffenceMenuSummary SOAP XML request
+       4) Build the getOffenceMenuFull SOAP XML request
+   
    All XML outputs are temporarily uploaded to Blob Storage for testing purposes
    """

    om_id = menu_details.get("OffenceMenuID")

    log_prefix = f"MENU ACTIVITY | MENU ID: {om_id} |"
    
    try:

        #**********************************************************
        # Step 1: Build Offence Menu createChangeSetHeader SOAP XML
        #**********************************************************
        # Build JSON payload
        logging.info(f"{log_prefix} Building createChangeSetHeader payload")
        csh_payload = build_payload_create_change_set_header(menu_details)
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
        logging.info(f"{log_prefix} Change Set Header Id returned: {pss_csh_pk}")


        #**********************************************************
        # Step 3: Send Offence Menu updateOffenceMenuFull to PSS and handle response
        #**********************************************************

        # Build JSON payload
        logging.info(f"{log_prefix} Building updateOffenceMenuFull payload")
        update_om_full_payload = build_payload_update_offence_menu_full(menu_details, pss_csh_pk)
        logging.info(f"{log_prefix} updateOffenceMenuFull payload built successfully")
        
        logging.info(f"{log_prefix} Building full updateOffenceMenuFull SOAP XML")
        update_om_full_soap_xml = build_soap_xml_update_offence_menu_full(update_om_full_payload)
        logging.info(f"{log_prefix} updateOffenceMenuFull XML built successfully")

        
        # Send Update Offence Menu Full XML to PSS and get reponse body back
        pss_update_om_full_response = pss_post(update_om_full_soap_xml)

        # Namespace mappings used to search the XML response
        ns = {
            "soap": "http://schemas.xmlsoap.org/soap/envelope/",
            "ack": "http://www.justice.gov.uk/magistrates/ack"
        }

        # Extract the returned PSS Release Package ID
        update_om_full_ack = pss_update_om_full_response.find(".//MessageStatus", ns).text
        
        if not update_om_full_ack:
            raise ValueError(f"{log_prefix} updateOffenceMenuFull Acknowledgment not returned")
        logging.info(f"{log_prefix} updateOffenceMenuFull Acknowledgment returned: {update_om_full_ack}")

        #**********************************************************
        # Step 4: End here if Offence Menu is an Edit
        # TODO May want to update this to be if the PSS ID = Semarchy PSS ID??????
        #**********************************************************
        if not menu_details.get("VersionType") == "Initial":
            logging.info(f"{log_prefix} Version Type is not Initial ({menu_details.get('VersionType')})")
            # Return the IDs and status to the caller
            om_payload = [{
                "OffenceMenuID": int(om_id),
                "PSSChangeSetHeaderID": int(pss_csh_pk) ,
                "PublishingStatus": "Submitted"
            }]

            return {
                "OffenceMenu": om_payload,
                "ActivityStatus": "SUCCESS"
            }


        #**********************************************************
        # Step 5: Send Offence Menu getOffenceMenuSummary to PSS and handle response (Initial only)
        #**********************************************************

        # Build JSON payload
        logging.info(f"{log_prefix} Building getOffenceMenuSummary payload")
        get_om_summary_payload = build_payload_get_offence_menu_summary(menu_details)
        logging.info(f"{log_prefix} getOffenceMenuSummary payload built successfully")
        
        logging.info(f"{log_prefix} Building full getOffenceMenuSummary SOAP XML")
        get_om_summary_soap_xml = build_soap_xml_get_offence_menu_summary(get_om_summary_payload)
        logging.info(f"{log_prefix} getOffenceMenuSummary XML built successfully")

        # Send Get Offence Menu Summary XML to PSS and get reponse body back
        pss_get_om_summary_response = pss_post(get_om_summary_soap_xml)

        # Namespace mappings used to search the XML response
        ns = {
            "soap": "http://schemas.xmlsoap.org/soap/envelope/",
            "ns7":   "http://www.justice.gov.uk/magistrates/pss/GetReferenceDataResponse",
        }

        # Extract the returned PSS Release Package ID
        pss_om_pk = pss_get_om_summary_response.find('.//DataItem[@ColumnOrder="1"]/Value', ns).text
    
        if not pss_om_pk:
            raise ValueError(f"{log_prefix} PSS OM ID not returned")
        logging.info(f"{log_prefix} PSS OM ID returned: {pss_om_pk}")


        #**********************************************************
        # Step 7:Send Offence Menu getOffenceMenuFull to PSS and handle response (Initial only)
        #**********************************************************

        # Build JSON payload
        logging.info(f"{log_prefix} Building getOffenceMenuFull payload")
        get_om_full_payload = build_payload_get_offence_menu_full(menu_details, pss_om_pk)
        logging.info(f"{log_prefix} getOffenceMenuFull payload built successfully")
        
        logging.info(f"{log_prefix} Building full getOffenceMenuFull SOAP XML")
        get_om_full_soap_xml = build_soap_xml_get_offence_menu_full(get_om_full_payload)
        logging.info(f"{log_prefix} getOffenceMenuFull XML built successfully")

        # Send Get Offence Menu Full XML to PSS and get reponse body back
        pss_get_om_full_response = pss_post(get_om_full_soap_xml)

        # Namespace mappings used to search the XML response
        ns = {
            "soap": "http://schemas.xmlsoap.org/soap/envelope/",
            "ns33": "http://www.justice.gov.uk/magistrates/pss/GetOffenceMenuFullResponse",
        }

        # Extract the returned PSS Release Package ID
        pss_offence_menu = extract_offence_menu_json_from_root(pss_get_om_full_response, ns)


        #**********************************************************
        # Step 9: Generate the Semarchy Payload
        #**********************************************************

        ###### Generate Menu Payload
        om_payload = [{
            "OffenceMenuID": int(om_id),
            "PSSOffenceMenuID": int(pss_offence_menu.get("OM_ID")),
            "PSSChangeSetHeaderID": int(pss_csh_pk) ,
            "PublishingStatus": "Submitted" # Update to be "Submitted (Draft)"
        }]

        ###### Generate Menu Options Payload
        omo_payload = generate_omo_payload(menu_details, pss_offence_menu)
        oed_payload = generate_oed_payload(menu_details, pss_offence_menu)

        # Return the IDs and status to the caller
        return {
            "OffenceMenu": om_payload,
            "OffenceMenuOptions": omo_payload,
            "OffenceMenuElementDefinition": oed_payload,
            "ActivityStatus": "SUCCESS"
        }

    except Exception as ex:
        logging.error(f"{log_prefix} FAILED:{str(ex)}")
        message = str(ex).strip().split("\n")[0].strip()

        om_payload = [{
            "OffenceMenuID": int(om_id),
            "PublishingStatus": "Error"
        }]

        return {
            "OffenceMenu": om_payload,
            "ActivityStatus": "FAILED",
            "ActivityErrorMessage": message
        }

import os
import xml.etree.ElementTree as ET

from CreateReleasePackageActivity.utils.helpers import generate_soap_xml
from utils.pss_interaction import pss_post


def main(record):
    
    # Get the Release Package ID from the incoming record
    rp_id = record.get("ReleasePackageID")

    # Build the Release Package details to send to PSS
    rp_details = {
        "ReleasePackageType": {
            "Description": record.get("Description"),
            "Status": "Open",   # Default status until all Change Set Headers are final
            "UpdateType": "N"   # Always set to "N" unless it's a maintenance update
        },
        "AuditingInformation": {
            "ChangedBy": os.getenv("SYSTEM_USER"),
            "ChangedDate": record.get("PublishDate")
        }
    }

    # Convert the details into a SOAP XML message
    soap_xml = generate_soap_xml(rp_details)

    # Send the SOAP request and get the response
    response_xml = pss_post(soap_xml)
    
    # Namespace mappings used to search the XML response
    ns = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "tns": "http://www.justice.gov.uk/magistrates/pss/CreateReleasePackageRequest",
        "s0": "http://www.justice.gov.uk/magistrates/pss/CreateReleasePackageResponse"
    }


    # Extract the returned PSS Release Package ID
    pss_rp_id = response_xml.find(".//ReleasePackagePK", ns).text

    # Return the IDs and status to the caller
    return {
        'ReleasePackageID': rp_id,
        'PSSReleasePackageID': int(pss_rp_id),
        'PublishingStatus': 'Release Package Created'
    }
import os
import random
import uuid
import xml.etree.ElementTree as ET

from utils.build_xml import dict_to_xml


def build_payload_create_change_set_header(offence_details: dict) -> dict:
    """
    Extracts the required Offence attributes from the Semarchy Named Query response.
    """

    # Generate random NewRecordID by combining the OffenceRevisionID with a random number to ensure uniqueness
    offence_revision_id = str(offence_details.get("OffenceRevisionID"))
    remaining_length = 13 - len(str(offence_revision_id))
    rand_new_record_id = int(offence_revision_id + str(random.randint(10**(remaining_length-1), 10**remaining_length - 1)))

    payload = {
        "ChangeSetHeaderType": {
            "ChangeSetHeaderPK": -1,                                                            # Primary key of the Change Set Header. Use -1 when creating a new Change Set; PSS will generate the actual ID.
            "ChangeSetHeaderDescription": offence_details.get("CJSTitle", ""),                  # Human‑readable description for the Change Set (often includes reference code and date). Used for audit and UI display.
            "ReleasePackagePK": None,                                                           # PSS Identifier of the Release Package if already linked. Set to nil when creating a new Change Set not yet assigned to a Release Package.
            "ReleasePackageStatus": "",                                                         # Current status of the linked Release Package (e.g. Draft/Open/Publish). Typically empty on creation.
            "ReleasePackageDescription": offence_details.get("ReleasePackageDescription", ""),  # Descriptive text for the associated Release Package, if known.
            "ReferenceType": offence_revision_id,                                               # Business reference identifying the item being changed (e.g. offence code). Used to prevent concurrent Change Sets for the same record.
            "ReferenceTypePK": "",                                                              # Database primary key of the reference record being changed, if it already exists. Blank for new records.
            "NewRecordID": rand_new_record_id,                                                  # Client‑generated temporary identifier used when creating a brand‑new record. Allows PSS to correlate draft changes before a real PK exists.
            "ReferenceTypeDescription": offence_details.get("CJSTitle", ""),                    # Business description of the reference record, used in release notes and audit output.
            "Status": "Draft",                                                                  # Lifecycle status of the Change Set (Draft or Final). Creation always starts as Draft.
            "ChangeReason": offence_details.get("OffenceNotes", ""),                            # Free‑text explanation of why the change is being made (audit and governance).
            "ParentRefType": "Offences",                                                        # High‑level reference category (e.g. Offences). Determines which stored procedures and validation rules apply.
            "RelatedItemsIdentifier": "",                                                       # Optional identifier for grouping related items within the same Change Set.
            "RelatedItemsIdentifierIndex": "",                                                  # Index used when multiple related identifiers exist.
        },
        "AuditingInformation": {
            "ChangedBy": os.getenv("SYSTEM_USER"),                                              # User or system identifier performing the change.
            "ChangedDate": offence_details.get("ReleasePackage", {}).get("PublishDate", ""),    # Timestamp of the change, in ISO‑8601 format.
        }
    }

    return payload


def build_soap_xml_create_change_set_header(json_obj: dict) -> str:
    """
    Builds a SOAP envelope with WS-Addressing headers and an XML body generated from the given dictionary.
    """

    # Generate MessageID
    message_id = f"{uuid.uuid4()}"

    # Envelope
    envelope = ET.Element(
        "soapenv:Envelope",
        attrib={
            "xmlns:soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
            "xmlns:req": "http://www.justice.gov.uk/magistrates/pss/GetOrganisationsRequest",
            "xmlns:ns12": "http://www.justice.gov.uk/magistrates/pss/CreateChangeSetHeaderRequest",
            "xmlns:wsa": "http://schemas.xmlsoap.org/ws/2004/08/addressing",

        }
    )

    # Header
    header = ET.SubElement(envelope, "soapenv:Header")

    ET.SubElement(header, "wsa:Action").text = "createChangeSetHeader"
    ET.SubElement(header, "wsa:MessageID").text = message_id
    ET.SubElement(header, "wsa:To").text = "pss_db"
    ET.SubElement(header, "wsa:RelatesTo").text = ""

    reply_to = ET.SubElement(header, "wsa:ReplyTo")
    ET.SubElement(reply_to, "wsa:Address").text = \
        "http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous"

    from_elem = ET.SubElement(header, "wsa:From")
    ET.SubElement(from_elem, "wsa:Address").text = "pss_sdui"

    # Body
    body = ET.SubElement(envelope, "soapenv:Body")
    payload = ET.SubElement(body, "ns12:CreateChangeSetHeaderRequest")

    dict_to_xml(payload, json_obj, nil_fields={"ReleasePackagePK"})

    return ET.tostring(envelope, encoding="utf-8", xml_declaration=True, short_empty_elements=False).decode("utf-8")

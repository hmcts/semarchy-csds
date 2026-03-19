import os
import uuid
import xml.etree.ElementTree as ET

from utils.build_xml import dict_to_xml


def build_payload_get_offence_menu_full(menu_details: dict, pss_offence_menu_id: int) -> dict:
    """
    Extracts the required Offence Menu attributes from the Semarchy Named Query response.
    """

    payload = {
  #       "GetOffenceMenuFullRequest": {
            "OM_ID": pss_offence_menu_id,                           # The PSS OM_ID
            "Status": "DRAFT",                                      # TODO: Always set to DRAFT?
  #      },
        "AuditingInformation": {
            "ChangedBy": os.getenv("SYSTEM_USER"),                  # User or system identifier performing the change.  Will be defaulted to a system user ID.
            "ChangedDate": menu_details.get("PublishDate", "")      # Timestamp of the change, in ISO‑8601 format.
        }
    }

    return payload


def build_soap_xml_get_offence_menu_full(json_obj: dict) -> str:
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
            "xmlns:ns55": "http://www.justice.gov.uk/magistrates/pss/GetOffenceSummaryRequest",
            "xmlns:ns62": "http://www.justice.gov.uk/magistrates/pss/GetOffenceMenuSummaryRequest",
            "xmlns:ns59": "http://www.justice.gov.uk/magistrates/pss/GetOffenceMenuFullRequest",
            "xmlns:wsa": "http://schemas.xmlsoap.org/ws/2004/08/addressing",

        }
    )

    # Header
    header = ET.SubElement(envelope, "soapenv:Header")

    ET.SubElement(header, "wsa:Action").text = "getOffenceMenuFull"
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
    payload = ET.SubElement(body, "ns59:GetOffenceMenuFullRequest")

    dict_to_xml(payload, json_obj)

    return ET.tostring(envelope, encoding="utf-8", xml_declaration=True, short_empty_elements=False).decode("utf-8")

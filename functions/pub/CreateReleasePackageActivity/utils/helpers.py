import xml.etree.ElementTree as ET
import uuid

def dict_to_xml(parent, data: dict):
    """Recursively converts dict → XML."""
    for key, value in data.items():
        elem = ET.SubElement(parent, key)
        if isinstance(value, dict):
            dict_to_xml(elem, value)
        else:
            elem.text = str(value)


def generate_soap_xml(payload):

    # Generate MessageID
    message_id = f"{uuid.uuid4()}"

    # Envelope
    envelope = ET.Element(
        "soapenv:Envelope",
        attrib={
            "xmlns:soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
            "xmlns:req": "http://www.justice.gov.uk/magistrates/pss/GetOrganisationsRequest",
            "xmlns:ns12": "http://www.justice.gov.uk/magistrates/pss/CreateChangeSetHeaderRequest",
            "xmlns:ns48": "http://www.justice.gov.uk/magistrates/pss/CreateReleasePackageRequest",
            "xmlns:wsa": "http://schemas.xmlsoap.org/ws/2004/08/addressing",

        }
    )

    # Header
    header = ET.SubElement(envelope, "soapenv:Header")

    ET.SubElement(header, "wsa:Action").text = "createReleasePackage"
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
    payload_elem = ET.SubElement(body, "ns48:CreateReleasePackageRequest")

    dict_to_xml(payload_elem, payload)

    return ET.tostring(envelope, encoding="utf-8", xml_declaration=True).decode("utf-8")
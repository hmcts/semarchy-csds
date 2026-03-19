import os
import uuid
import xml.etree.ElementTree as ET

from utils.build_xml import dict_to_xml
from utils.value_mappings import map_edit_type, map_version_type


def build_payload_update_offence_menu_full(menu_details: dict, pss_change_set_header_pk: int) -> dict:
    """
    Builds the full UpdateOffenceMenuFull payload, supporting multiple menu options
    and multiple option elements per option.
    """

    operation_type = map_version_type(menu_details.get("VersionType"))

    # Build OTEMenuOptions list
    menu_options_payload = []

    related_identifier = 1

    for opt in menu_details.get("OTEMenuOptions", []):
        option_elements_payload = []
        
        related_identifier_index = 1

        for elem in opt.get("OffenceMenuElementDefinitions", []):
            option_elements_payload.append({
                "OMOP_ID": elem.get("PSSMenuOptionElementDefinitionID", ""),        # Primary key of the Menu Option Element. Blank when creating a new element.
                "OperationType": operation_type,                                    # Indicates the operation to perform on the Menu Option Element (C, U, D).
                "ElementNumber": elem.get("ElementNumber"),                         # Numeric index corresponding to the placeholder [n] in OptionText.
                "VersionNumber": elem.get("ElementVersionNumber"),                  # Version number of the element record. Required when updating.
                "OMO_OMO_ID": opt.get("PSSOffenceMenuOptionsID", ""),               # Foreign key reference to the parent Menu Option (OMO_ID).
                "OED_OED_ID": elem.get("PSSMenuOptionElementDefinitionID", ""),     # Foreign key linking to the associated Element Definition.
                "CSH_CSH_ID": pss_change_set_header_pk,                             # Change Set Header reference.

                "OTEElementDefinitions": {
                    "OED_ID": elem.get("PSSMenuOptionElementDefinitionID", ""),             # Primary key of the Element Definition. Blank when creating a new definition.
                    "OperationType": operation_type,                                        # Indicates the operation to perform on the Element Definition (C, U, D).
                    "OEDMin": elem.get("OTEElementMin", ""),                                # Minimum allowed length or occurrence value for the element.
                    "OEDMax": elem.get("OTEElementMax", ""),                                # Maximum allowed length or occurrence value for the element.
                    "EntryFormat": elem.get("EntryFormat"),                                 # Format of the element (e.g., TXT for free text, MNU for menu-based input).
                    "EntryPrompt": elem.get("EntryPrompt"),                                 # Prompt displayed to the user when entering data for this element.
                    "VersionNumber": elem.get("ElementVersionNumber"),                      # Version number of the element definition record. Required when updating.
                    "CSH_CSH_ID": pss_change_set_header_pk,                                 # Change Set Header reference under which the change is staged.
                    "RelatedItemsIdentifier": related_identifier,                           # Correlation identifier for nested object creation.
                    "RelatedItemsIdentifierIndex": related_identifier_index,                # Index used for grouping nested related elements.
                },

                "RelatedItemsIdentifier": related_identifier,                       # Correlation identifier for nested object creation.
                "RelatedItemsIdentifierIndex": related_identifier_index             # Index used when multiple related nested elements are created.
            })
            
            related_identifier_index += 1

        menu_options_payload.append({
            "OMO_ID": opt.get("PSSOffenceMenuOptionsID", ""),           # Primary key of the Menu Option. Leave blank when creating a new option. Required when updating or deleting.
            "OperationType": operation_type,                            # Indicates the operation to perform on the Menu Option (C, U, D).
            "OptionNumber": opt.get("OptionNumber"),                    # Sequential number identifying the position of the option within the menu. Must be unique within the same menu.
            "OptionText": opt.get("OptionText"),                        # Text displayed to users. May contain element placeholders such as [1], [2], etc.
            "VersionNumber": opt.get("OptionVersionNumber"),            # Version number of the Menu Option record. Required for updates.
            "OM_OM_ID": menu_details.get("PSSOffenceMenuID", ""),       # Foreign key linking the option to its parent Offence Menu (OM_ID).
            "CSH_CSH_ID": pss_change_set_header_pk,                     # Change Set Header reference under which the change is staged.
            "RelatedItemsIdentifier": related_identifier,               # Internal correlation identifier used when creating nested records within the same request.
            "OTEMenuOptionElements": option_elements_payload
        })

        related_identifier += 1

    # ---- Final payload structure ----
    payload = {
        "UpdateOffenceMenuFullType": {
            "OM_ID": menu_details.get("PSSOffenceMenuID", ""),              # Primary key of the Offence Menu. Leave blank when creating a new menu. Required when updating or deleting an existing menu.
            "OperationType": operation_type,                                # Indicates the operation to perform on the Offence Menu. Permitted values: C (Create), U (Update), D (Delete).
            "Name": menu_details.get("Name"),                               # The name/title of the Offence Menu.
            "VersionNumber": menu_details.get("VersionNumber"),             # Version number of the Offence Menu record. Required when updating to support optimistic locking.
            "CSH_CSH_ID": pss_change_set_header_pk,                         # Foreign key reference to the Change Set Header. All menu updates must be associated with an active Change Set.
            "HMCTSNotes": menu_details.get("Notes"),                        # Optional free-text notes recorded against the Offence Menu. Used for audit and display purposes.
            "EditType": map_edit_type(menu_details.get("VersionType")),     # Indicates the type of edit being performed (e.g., NEW, EDIT). Controls revision handling logic within PSS.

            "OTEMenuOptions": menu_options_payload
        },

        "AuditingInformation": {
            "ChangedBy": os.getenv("SYSTEM_USER"),                          # User or system identifier performing the change.
            "ChangedDate": menu_details.get("PublishDate", "")              # Timestamp of the change, in ISO‑8601 format.
        }
    }

    return payload


def build_soap_xml_update_offence_menu_full(json_obj: dict) -> str:
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
            "xmlns:ns17": "http://www.justice.gov.uk/magistrates/pss/UpdateChangeSetHeaderRequest",
            "xmlns:ns64": "http://www.justice.gov.uk/magistrates/pss/UpdateOffenceMenuFullRequest",
            "xmlns:wsa": "http://schemas.xmlsoap.org/ws/2004/08/addressing",

        }
    )

    # Header
    header = ET.SubElement(envelope, "soapenv:Header")

    ET.SubElement(header, "wsa:Action").text = "updateOffenceMenuFull"
    ET.SubElement(header, "wsa:MessageID").text = message_id
    ET.SubElement(header, "wsa:To").text = "pss_db"
    ET.SubElement(header, "wsa:RelatesTo").text = ""

    #reply_to = ET.SubElement(header, "wsa:ReplyTo")
    #ET.SubElement(reply_to, "wsa:Address").text = \
    #    "http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous"

    from_elem = ET.SubElement(header, "wsa:From")
    ET.SubElement(from_elem, "wsa:Address").text = "pss_sdui"

    # Body
    body = ET.SubElement(envelope, "soapenv:Body")
    payload = ET.SubElement(body, "ns64:UpdateOffenceMenuFullRequest")

    dict_to_xml(payload, json_obj)

    return ET.tostring(envelope, encoding="utf-8", xml_declaration=True, short_empty_elements=False).decode("utf-8")

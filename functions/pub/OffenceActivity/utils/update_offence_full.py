import os
import uuid
import xml.etree.ElementTree as ET

from utils.build_xml import dict_to_xml
from utils.format_date import convert_date_format
from utils.value_mappings import map_edit_type, map_version_type, map_y_n


def build_payload_update_offence_full(offence_details: dict, pss_change_set_header_pk: int) -> dict:
    """
    Builds the full UpdateOffenceFull payload, supporting multiple concurrent Offence
    Terminal Entries.
    """

    operation_type = map_version_type(offence_details.get("VersionType"))

    # Build OffenceTerminalEntries list
    ote_payload = []

    for ote in offence_details.get("OffenceTerminalEntries", []):

        ote_payload.append(
            {
                "OTE_ID": ote.get("PSSOffenceTerminalEntryID"),                                                     # Primary key of the terminal entry (blank for new entries).
                "EntryNumber": ote.get("EntryNumber"),                                                              # Sequential number of the terminal entry within the offence wording.
                "Min": ote.get("Minimum"),                                                                          # Minimum number of values allowed for this entry.
                "Max": ote.get("Maximum"),                                                                          # Maximum number of values allowed for this entry.
                "EntryFormat": ote.get("EntryFormat"),                                                              # Format of the entry (e.g. TXT, MNU).
                "EntryPrompt": ote.get("EntryPrompt"),                                                              # Prompt text displayed to users.
                "StandardEntryIdentifier": ote.get("StandardEntryIdentifier"),                                      # Standard identifier (e.g. OD for offence date, PT for place of offence).
                "OM_OM_ID": ote.get("PSSOffenceMenuID"),                                                            # Identifier of the associated menu option where EntryFormat is MNU.
                "MenuName": ote.get("OffenceMenuName"),                                                             # Name of the menu linked to this terminal entry.
                "CSH_CSH_ID": pss_change_set_header_pk,                                                             # Foreign key linking the terminal entry to the Change Set Header.
            },
        )

    # ---- Final payload structure ----
    payload = {
        "UpdateOffenceFullRequest": {
            "OffenceHeaderType": {
                "CJSCode": offence_details.get("CJSCode"),                                                          # Unique offence code (business identifier) for the offence.
                "Blocked": offence_details.get("Blocked"),                                                          # Indicates whether the offence is blocked from use (Y/N).
                "CSH_CSH_ID": pss_change_set_header_pk,                                                             # Foreign key linking this offence change to the owning Change Set Header.
            },          
            "OffenceRevisionsType": {           
                "EditType": map_edit_type(offence_details.get("VersionType")),                                      # Type of edit being performed (NEW, EDIT, NEW REVISION). Determines revision handling logic.
                "Recordable": map_y_n(offence_details.get("Recordable")),                                           # Indicates whether the offence is recordable (Y/N).
                "Reportable": map_y_n(offence_details.get("Reportable")),                                           # Indicates whether the offence is reportable (Y/N).
                "CJSTitle": offence_details.get("CJSTitle"),                                                        # English title of the offence as presented to users.
                "CustodialIndicator": map_y_n(offence_details.get("CustodialIndicator")),                           # Indicates whether the offence can result in custody (Y/N).
                "SOWReference": offence_details.get("SOWReference"),                                                # Reference to the Standard Offence Wording source (e.g. PNLD).
                "MISClass": offence_details.get("MISClassification"),                                               # Management Information System classification code.
                "OffenceType": offence_details.get("OffenceType"),                                                  # High‑level offence type (e.g. CI, CS, CM).
                "DVLACode": offence_details.get("DVLACode"),                                                        # DVLA offence code where applicable.
                "UseFrom": convert_date_format(offence_details.get("DateUsedFrom", "")),                            # Date from which this offence revision becomes effective.
                "UseTo": convert_date_format(offence_details.get("DateUsedTo", "")),                                # Date until which this offence revision is effective (blank if current).
                "Notes": offence_details.get("OffenceNotes"),                                                       # Free‑text notes relating to this revision.
                "StandardList": map_y_n(offence_details.get("StandardList")),                                       # Indicates whether the offence belongs to a standard list (Y/N).
                "MaxPenalty": offence_details.get("MaximumPenalty"),                                                # Textual representation of the maximum penalty.
                "Description": offence_details.get("Description"),                                                  # Additional descriptive text for the offence.
                "HOClass": offence_details.get("HOClass"),                                                          # Home Office offence class.
                "HOSubClass": offence_details.get("HOSubClass"),                                                    # Home Office offence subclass.
                "ProceedingsCode": offence_details.get("ProceedingsCode"),                                          # Code indicating applicable proceedings.
                "CJSTitleCY": offence_details.get("SecondLanguageCJSTitle"),                                        # Welsh language title of the offence.
                "CSH_CSH_ID": pss_change_set_header_pk,                                                             # Foreign key linking the revision to the Change Set Header.
            },
            "OffenceWordingsType": {
                "OffenceWording": offence_details.get("UserOffenceWording"),                                        # Structured offence wording text, including terminal entry placeholders (e.g. {1}, {2}).
                "SLOffenceWording": offence_details.get("SecondLanguageOffenceWordingText"),                        # Welsh language version of the offence wording.
                "CSH_CSH_ID": pss_change_set_header_pk,                                                             # Foreign key linking wording changes to the Change Set Header.
            },
            "ActAndSectionType": {
                "ActAndSection": offence_details.get("UserActsAndSection"),                                         # Legal Act and section reference in English.
                "SLActAndSection": offence_details.get("SecondLanguageOffenceActAndSectionText"),                   # Welsh language Act and section reference.
                "CSH_CSH_ID": pss_change_set_header_pk,                                                             # Foreign key linking the act/section to the Change Set Header.
            },
            "StatementOfFactsType": {
                "StatementOfFacts": offence_details.get("UserStatementOfFacts"),                                    # English statement of facts associated with the offence.
                "SLStatementOfFacts": offence_details.get("SecondLanguageOffenceStatementOfFactsText"),             # Welsh language statement of facts.
                "CSH_CSH_ID": pss_change_set_header_pk,                                                             # Foreign key linking the statement of facts to the Change Set Header.
            },
            "OffenceTerminalEntriesType": ote_payload,
            "CrownOffenceType": {
                "OffenceClass": offence_details.get("OffenceClass"),                                                # Crown Court offence classification.
                "ObsInd": offence_details.get("ObsoleteIndicator"),                                                 # Obsolescence indicator (Y/N).
                "CSH_CSH_ID": pss_change_set_header_pk,                                                             # Foreign key linking crown offence data to the Change Set Header.
            },
            "CppOffenceHeaderType": {
                "PNLDOffenceStartDate": offence_details.get("PNLDOffenceStartDate"),                                # Start date from PNLD source data.
                "PNLDOffenceEndDate": offence_details.get("PNLDOffenceEndDate"),                                    # End date from PNLD source data.
                "CSH_CSH_ID": pss_change_set_header_pk,                                                             # Foreign key linking PNLD header data to the Change Set Header.
            },
            "CppOffenceRevisionsType": {
                "DateOfLastUpdate": offence_details.get("PNLDDateOfLastUpdate"),                                    # Date the offence was last updated in the source system.
                "MaxFineTypeMagCtCode": offence_details.get("PNLDMaxFineTypeMagistratesCourt"),                     # Code representing the type of maximum fine.
                "MaxFineTypeMagCtDescription": offence_details.get("PNLDMaxFineTypeMagistratesCourtDescription"),   # Description of the maximum fine type.
                "PNLDStandardOffenceWording": offence_details.get("PNLDStandardOffenceWording"),                    # PNLD‑supplied standard offence wording (English).
                "SLPNLDStandardOffenceWording": offence_details.get("PNLDWelshStandardOffenceWording"),             # PNLD‑supplied standard offence wording (Welsh).
                "ProsecutionTimeLimit": offence_details.get("PNLDProsecutionTimeLimit"),                            # Time limit for prosecution.
                "ModeOfTrial": offence_details.get("PNLDModeOfTrial"),                                              # Mode of trial (e.g. Summary, Either Way, Indictable).
                "EndorsableFlag": offence_details.get("PNLDEndorsableFlag"),                                        # Indicates whether the offence is endorsable (Y/N).
                "LocationFlag": offence_details.get("PNLDLocationFlag"),                                            # Indicates whether location information is required (Y/N).
                "PrincipalOffenceCategory": offence_details.get("PNLDPrincipalOffenceCategory"),                    # Category used for Common Platform and CPS reporting.
                "CSH_CSH_ID": pss_change_set_header_pk,                                                             # Foreign key linking the revision to the Change Set Header.
            }
        },
        "AuditingInformation": {
            "ChangedBy": os.getenv("SYSTEM_USER"),                                                              # User or system identifier performing the change.
            "ChangedDate": offence_details.get("ReleasePackage", {}).get("PublishDate", "")                     # Timestamp of the change, in ISO‑8601 format.
        }
    }

    return payload


def build_soap_xml_update_offence_full(json_obj: dict) -> str:
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
            "xmlns:ns63": "http://www.justice.gov.uk/magistrates/pss/UpdateOffenceFullRequest",
            "xmlns:wsa": "http://schemas.xmlsoap.org/ws/2004/08/addressing",

        }
    )

    # Header
    header = ET.SubElement(envelope, "soapenv:Header")

    ET.SubElement(header, "wsa:Action").text = "updateOffenceFull"
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
    payload = ET.SubElement(body, "ns63:UpdateOffenceFullRequest")

    dict_to_xml(payload, json_obj)

    return ET.tostring(envelope, encoding="utf-8", xml_declaration=True, short_empty_elements=False).decode("utf-8")

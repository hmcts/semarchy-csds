import xml.etree.ElementTree as ET
from typing import Dict, Any


def _get_int(parent: ET.Element, tag: str):
    """Return int value of first direct child 'tag' (unprefixed) or None."""
    node = parent.find(tag)
    if node is None or node.text is None:
        return None
    text = node.text.strip()
    try:
        return int(text)
    except ValueError:
        return None

def extract_offence_revision_json_from_root(root: ET.Element, ns) -> Dict[str, Any]:
    """
    Parses the SOAP XML (root element) and returns:
    {
        "OffenceHeader": [
            {
                "OffenceHeaderID": 1,
                "PSSOffenceHeaderID": 2,
                "PSSPNLDOffenceHeaderID": 3
            }
        ],
        "OffenceRevision": [
            {
                "OffenceRevisionID": 10,
                "PSSOffenceRevisionID": 100,
                "PSSChangeSetHeaderID": 101,
                "PSSCivilApplicationDataID": 102,
                "PSSXHBReferenceOffenceID": 103,
                "PSSPNLDOffenceRevisionID": 104,
                "PSSApplicationsDataID": 105,
                "PSSOffenceActAndSectionID": 106,
                "PSSOffenceStatementOfFactID": 107,
                "PSSOffenceWordingID": 108,
            }
        ],
        "OffenceTerminalEntries": [
            {
                "OffenceTerminalEntryID": 1,
                "PSSOffenceTerminalEntryID": 5
            },
            {
                "OffenceTerminalEntryID": 2,
                "PSSOffenceTerminalEntryID": 6
            }
        ]
    }
    """

    if root is None or not isinstance(root, ET.Element):
        raise ValueError("root must be an xml.etree.ElementTree.Element (the SOAP Envelope)")

    # 1) Locate the response wrapper inside the SOAP Body
    resp = root.find(".//soap:Body/ns58:GetOffenceFullResponse", ns)
    if resp is None:
        raise ValueError("GetOffenceFullResponse not found under SOAP Body. "
                         "Check namespace prefix (ns58) and the element name.")

    # 2) Locate the inner payload
    payload = resp.find("GetOffenceFullResponseType")
    if payload is None:
        raise ValueError("GetOffenceFullResponseType not found under GetOffenceFullResponse")

    # 3) Build the result JSON
    result: Dict[str, Any] = {
        "OH_ID": _get_int(payload.find("OffenceHeaderType"), "OH_ID"),
        "POH_ID": _get_int(payload.find("CppOffenceHeaderType"), "POH_ID"),
        "OFR_ID": _get_int(payload.find("OffenceRevisionsType"), "OFR_ID")
        # Add the rest...
    }

    # 4) Iterate OffenceTerminalEntries (could be multiple)


    return result

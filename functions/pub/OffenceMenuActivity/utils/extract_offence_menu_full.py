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

def extract_offence_menu_json_from_root(root: ET.Element, ns) -> Dict[str, Any]:
    """
    Parses the SOAP XML (root element) and returns:
    {
        "OM_ID": 322176,
        "OTEMenuOptions":[
            {
                "OMO_ID": 813164,
                "OptionNumber": 1,
                "OTEMenuOptionElements": [
                    {
                        "OMOP_ID": 184134,
                        "ElementNumber": 1,
                        "OED_ID": 55736
                    }
                ]
            }
        ]
    }
    """

    if root is None or not isinstance(root, ET.Element):
        raise ValueError("root must be an xml.etree.ElementTree.Element (the SOAP Envelope)")

    # 1) Locate the response wrapper inside the SOAP Body
    resp = root.find(".//soap:Body/ns33:GetOffenceMenuFullResponse", ns)
    if resp is None:
        raise ValueError("GetOffenceMenuFullResponse not found under SOAP Body. "
                         "Check namespace prefix (ns33) and the element name.")

    # 2) In your sample, the inner payload is unprefixed
    payload = resp.find("GetOffenceMenuFullResponseType")
    if payload is None:
        raise ValueError("GetOffenceMenuFullResponseType not found under GetOffenceMenuFullResponse")

    # 3) Build the result JSON
    result: Dict[str, Any] = {
        "OM_ID": _get_int(payload, "OM_ID"),
        "OTEMenuOptions": []
    }

    # 4) Iterate OTEMenuOptions (could be multiple)
    for opt in payload.findall("OTEMenuOptions"):
        opt_obj: Dict[str, Any] = {
            "OMO_ID": _get_int(opt, "OMO_ID"),
            "OptionNumber": _get_int(opt, "OptionNumber"),
            "OTEMenuOptionElements": []
        }

        # 5) Iterate OTEMenuOptionElements (could be multiple)
        for elem_blk in opt.findall("OTEMenuOptionElements"):
            elem_obj: Dict[str, Any] = {
                "OMOP_ID": _get_int(elem_blk, "OMOP_ID"),
                "ElementNumber": _get_int(elem_blk, "ElementNumber"),
            }

            # OED_ID may appear either under OTEElementDefinitions/OED_ID or as OED_OED_ID
            oed_id = None
            defs = elem_blk.find("OTEElementDefinitions")
            if defs is not None:
                oed_id = _get_int(defs, "OED_ID")
            if oed_id is None:
                oed_id = _get_int(elem_blk, "OED_OED_ID")

            elem_obj["OED_ID"] = oed_id
            opt_obj["OTEMenuOptionElements"].append(elem_obj)

        result["OTEMenuOptions"].append(opt_obj)

    return result

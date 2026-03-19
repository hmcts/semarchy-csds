import xml.etree.ElementTree as ET

def dict_to_xml(parent, data: dict, nil_fields=None):
    """
    Recursively converts dict → XML.
    Supports:
    - dicts
    - lists (repeat elements)
    - Nil values (xsi:nil)
    - None values
    - empty strings (force <Tag></Tag>)
    """

    if nil_fields is None:
        nil_fields = set()

    for key, value in data.items():

        # ----- LIST CASE -----
        if isinstance(value, list):
            for item in value:
                # Each item becomes its own <key>...</key>
                if isinstance(item, dict):
                    elem = ET.SubElement(parent, key)
                    dict_to_xml(elem, item, nil_fields=nil_fields)

                elif item is None:
                    # Nil for list item only if the key is flagged; otherwise empty element
                    if key in nil_fields:
                        ET.SubElement(
                            parent,
                            key,
                            attrib={"xsi:nil": "true", "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance"}
                        )
                    else:
                        elem = ET.SubElement(parent, key)
                        elem.text = ""  # <key></key>
                else:
                    elem = ET.SubElement(parent, key)
                    # Empty string => <key></key>; otherwise normal scalar
                    elem.text = "" if item == "" else str(item)
            continue

        # ----- NIL (scalar) → only for keys in nil_fields -----
        if value is None and key in nil_fields:
            ET.SubElement(
                parent,
                key,
                attrib={"xsi:nil": "true", "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance"}
            )
            continue

        # ----- Normal None → empty element -----
        if value is None:
            elem = ET.SubElement(parent, key)
            elem.text = ""  # <key></key>
            continue

        # ----- Explicit empty string → empty element -----
        if value == "":
            elem = ET.SubElement(parent, key)
            elem.text = ""  # <key></key>
            continue

        # ----- Nested dict -----
        if isinstance(value, dict):
            elem = ET.SubElement(parent, key)
            dict_to_xml(elem, value, nil_fields=nil_fields)
            continue

        # ----- Normal scalar -----
        elem = ET.SubElement(parent, key)
        elem.text = str(value)

from lxml import etree
import pandas as pd
import os
import json
import hashlib


def calculate_hash(record, exclude_keys):
    """
    Compute a deterministic MD5 over the record dict, excluding specific keys.
    - None values are normalized to ''.
    - Uses JSON with sorted keys and compact separators to ensure stability.
    """
    exclude = set(exclude_keys or [])
    filtered = {
        k: ("" if v is None else v)
        for k, v in record.items()
        if k not in exclude
    }

    payload = json.dumps(
        filtered,
        sort_keys=True,
        separators=(",", ":"),   # compact, deterministic
        ensure_ascii=False       # preserve unicode
    )

    # Compute MD5 hash
    hash = hashlib.md5(payload.encode("utf-8")).hexdigest()

    return hash



def flatten_pnld(xml,
                config_location="config/flatten_pnld.json"):
    """
    Flatten XML into a single record by merging data from multiple parent paths.
    If a field in the config is missing in the XML, assign None.
    """
    join_delimiter="; "
    multivalue_strategy="first"

    # Load Config file
    config_path = os.path.join(os.getcwd(), config_location)

    with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)

    parents = config.get("parents", [])
    namespaces = config.get("namespaces")

    if not parents:
        raise ValueError("Config must include 'parents' list with element_path and columns.")

    # Initialize record with all expected columns set to None
    record = {}
    for parent_cfg in parents:
        for col_name in parent_cfg.get("columns", {}).keys():
            record[col_name] = None

    # Populate values from XML
    for parent_cfg in parents:
        element_path = parent_cfg.get("element_path")
        columns = parent_cfg.get("columns", {})

        if not element_path or not columns:
            continue

        row_elements = xml.xpath(element_path, namespaces=namespaces)
        elem = row_elements[0] if row_elements else None

        for col_name, rel_xpath in columns.items():
            if elem is None:
                continue  # Keep None if element not found

            result = elem.xpath(rel_xpath, namespaces=namespaces)

            normalized = []
            for item in result:
                if isinstance(item, etree._Element):
                    text = (item.text.strip() if item.text else "")
                    normalized.append(text)
                else:
                    normalized.append(str(item).strip())

            if not normalized:
                value = None
            elif multivalue_strategy == "first":
                value = normalized[0]
            elif multivalue_strategy == "join":
                value = join_delimiter.join(normalized)
            elif multivalue_strategy == "list":
                value = normalized
            else:
                raise ValueError("Invalid multivalue_strategy. Use 'first', 'join', or 'list'.")

            record[col_name] = value
    
    record["hashcol"] = calculate_hash(record, exclude_keys=['offenceenddate', 'dateoflastupdate'])

    return record
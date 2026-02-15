import os
import logging
import requests

def menu_id_lookup(menus):
    """
    Look up Semarchy menu IDs by sending MD5-hash searches in batches.

    Logging follows the PNLD convention:
      MENU HANDLING | menu_id_lookup | <step> - <status>
    """

    api_key = os.getenv("SemarchyAPIKey")
    base_url = os.getenv("SemarchyGetMenusNamedQueryURL")
    headers = {"API-Key": api_key}

    total_requested = len(menus)
    logging.info(
        f"MENU HANDLING | menu_id_lookup | START (total_requested={total_requested})"
    )

    final_md5_to_id = {}

    # Helper to chunk the list
    def chunked(seq, size=100):
        for i in range(0, len(seq), size):
            yield seq[i:i + size]

    # Process in chunks
    for idx, chunk in enumerate(chunked(menus, 100), start=1):
        logging.info(chunk)
        searched_count = len(chunk)

        # Build MD5 search string for this chunk
        md5_menu_to_search = "-" + "-".join(str(m["PNLDHashMD5"]) for m in chunk) + "-"
        url = f"{base_url}?MD5_HASH={md5_menu_to_search}"

        logging.info(
            f"MENU HANDLING | menu_id_lookup | Batch {idx} - START "
            f"(searched={searched_count})"
        )

        try:
            response = requests.get(url=url, headers=headers)
            response.raise_for_status()
            body = response.json()
            records = body.get("records", [])
        except Exception as e:
            logging.error(
                f"MENU HANDLING | menu_id_lookup | Batch {idx} - FAILED "
                f"(error={e})"
            )
            records = []

        # Extract mapping for this batch
        chunk_lookup = {
            item.get("PNLDHashMD5"): item.get("OTEMenuID")
            for item in records
            if item.get("PNLDHashMD5") is not None and item.get("OTEMenuID") is not None
        }

        returned_count = len(chunk_lookup)

        logging.info(
            f"MENU HANDLING | menu_id_lookup | Batch {idx} - COMPLETE "
            f"(searched={searched_count}, returned={returned_count})"
        )

        # Add to final merged map
        final_md5_to_id.update(chunk_lookup)

    total_found = len(final_md5_to_id)

    logging.info(
        f"MENU HANDLING | menu_id_lookup | DONE "
        f"(total_searched={total_requested}, total_returned={total_found})"
    )

    return final_md5_to_id

import logging

def extract_unique_menu_and_options(menus, menu_options):
    """
    Build:
      1) A unique, order-preserving list of menus:
            [{ "PNLDHashMD5": <md5>, "Name": <menu_name> }]
      2) A de-duplicated list of menu options.

    Logging follows PNLD standard:
        MENU HANDLING | extract_unique_menu_and_options | <Step> - <Status>
    """

    logging.info(
        f"MENU HANDLING | extract_unique_menu_and_options | START "
        f"(menus_in={len(menus)}, menu_options_in={len(menu_options)})"
    )

    seen = set()
    menus_output = []

    # Extract unique menus in original order
    for m in menus:
        md5 = m.get("menu_md5")
        name = m.get("name")

        if md5 is None:
            continue

        if md5 not in seen:
            seen.add(md5)
            menus_output.append({
                "PNLDHashMD5": md5,
                "Name": name
            })

    logging.info(
        f"MENU HANDLING | extract_unique_menu_and_options | Menu Deduplication - COMPLETE "
        f"(unique_menus={len(menus_output)})"
    )

    # Deduplicate menu options by full dictionary content
    deduped_options = [
        dict(t)
        for t in {tuple(sorted(d.items())) for d in menu_options}
    ]

    logging.info(
        f"MENU HANDLING | extract_unique_menu_and_options | Menu Options Deduplication - COMPLETE "
        f"(unique_options={len(deduped_options)})"
    )

    logging.info("MENU HANDLING | extract_unique_menu_and_options | DONE")

    return menus_output, deduped_options
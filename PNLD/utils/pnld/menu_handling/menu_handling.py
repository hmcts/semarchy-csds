from utils.pnld.menu_handling.helpers.menu_search import extract_unique_menu_and_options
from utils.pnld.menu_handling.helpers.menu_search import menu_id_lookup
from utils.pnld.menu_handling.helpers.post_menus import post_menus
from utils.pnld.menu_handling.helpers.handle_missing_menus import handle_missing_menus
import logging


def menu_handling(menus, menu_options, offences, source_files, messages, rp_id):
    """
    Handles menu extraction, lookup of menu IDs, posting new menus,
    updating offence revisions, and resolving any missing menu issues.

    Logging follows a consistent pattern across all PNLD processing components:
        "<Context> | <Step> - <Status> (counts/details)"
    """

    # ---------------------------------------------------------
    # STEP 1 — Extract unique menus and menu options
    # ---------------------------------------------------------
    logging.info("MENU HANDLING | Unique Menu Extraction - START")

    all_menus, all_menu_options = extract_unique_menu_and_options(menus, menu_options)

    logging.info(
        f"MENU HANDLING | Unique Menu Extraction - SUCCESS "
        f"(menus={len(all_menus)}, options={len(all_menu_options)})"
    )

    post_status = None  # Track Semarchy POST status for new menus

    # ---------------------------------------------------------
    # Only continue if any menus exist (excluding DATE menus)
    # ---------------------------------------------------------
    if len(all_menus) > 0:

        # ---------------------------------------------------------
        # STEP 2 — Lookup existing Semarchy menu IDs
        # ---------------------------------------------------------
        logging.info("MENU HANDLING | Menu ID Lookup (Existing) - START")

        existing_menu_mappings = menu_id_lookup(all_menus)
        menu_id_mapping = dict(existing_menu_mappings)

        logging.info(
            f"MENU HANDLING | Menu ID Lookup (Existing) - SUCCESS "
            f"(found={len(existing_menu_mappings)})"
        )

        # ---------------------------------------------------------
        # STEP 3 — Identify new (unknown) menus
        # ---------------------------------------------------------
        new_menus = [m for m in all_menus if m["PNLDHashMD5"] not in menu_id_mapping]
        new_menu_options = [
            mo for mo in all_menu_options if mo["PNLDMenuHashMD5"] not in menu_id_mapping
        ]

        logging.info(
            f"MENU HANDLING | New Menu Identification - COMPLETE "
            f"(new_menus={len(new_menus)}, new_options={len(new_menu_options)})"
        )

        # ---------------------------------------------------------
        # STEP 4 — POST new menus to Semarchy (if any)
        # STEP 5 — Lookup IDs for new menus (if applicable)
        # ---------------------------------------------------------
        if new_menus:
            logging.info("MENU HANDLING | New Menu POST - START")

            try:
                post_status = post_menus(new_menus, new_menu_options, rp_id)
                logging.info(
                    f"MENU HANDLING | New Menu POST - SUCCESS "
                    f"(status={post_status})"
                )
            except Exception as e:
                logging.error(f"MENU HANDLING | New Menu POST - FAILED ({e})")

            # If posted successfully, re‑lookup mappings for new menus
            if post_status in ["DONE", "WARNING"]:
                logging.info("MENU HANDLING | New Menu ID Lookup - START")

                new_menu_mappings = menu_id_lookup(new_menus)
                logging.info(
                    f"MENU HANDLING | New Menu ID Lookup - SUCCESS "
                    f"(found={len(new_menu_mappings)})"
                )

                menu_id_mapping.update(new_menu_mappings)

        else:
            logging.info("MENU HANDLING | New Menu POST - SKIPPED (no new menus)")

        logging.info(
            f"MENU HANDLING | Menu Mapping Consolidation - COMPLETE "
            f"(total_mappings={len(menu_id_mapping)})"
        )

        # ---------------------------------------------------------
        # STEP 6 — Update offence revisions with resolved menu IDs
        # ---------------------------------------------------------
        logging.info("MENU HANDLING | Offence Revision Update - START")

        unresolved_count = 0

        for rev in offences:
            if not isinstance(rev, dict):
                continue

            for key, value in list(rev.items()):
                if key.startswith("FID_Menu") and value is not None:

                    resolved = menu_id_mapping.get(value)

                    if resolved is not None:
                        rev[key] = resolved
                    else:
                        unresolved_count += 1
                        logging.warning(
                            f"MENU HANDLING | Offence Revision Update - "
                            f"UNRESOLVED menu hash '{value}' → setting None"
                        )
                        rev[key] = None

        logging.info(
            f"MENU HANDLING | Offence Revision Update - COMPLETE "
            f"(unresolved={unresolved_count})"
        )

    # ---------------------------------------------------------
    # STEP 7 — Handle missing menus (file/message/offence repair)
    # ---------------------------------------------------------
    if len(offences) > 0:
        logging.info("MENU HANDLING | Missing Menu Check - START")

        offences, source_files, messages = handle_missing_menus(
            offences, source_files, messages
        )

        logging.info("MENU HANDLING | Missing Menu Check - COMPLETE")


    return source_files, messages, offences
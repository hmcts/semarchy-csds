import logging


def handle_missing_menus(offence_revisions, source_files, messages):
    """
    Identify offence revisions that reference a Terminal Entry with EntryFormat='MNU'
    but have a missing/null corresponding FID_MenuXX, then:
      1) Collect their xml_file_id values.
      2) Remove those offences from the outgoing payload (and drop xml_file_id on kept ones).
      3) Mark related SourceFile records as Failure.
      4) Update related SourceFileMessage records to an error state.

    Logging uses PNLD standard format:
        MENU HANDLING | handle_missing_menus | <step> - <status> (details)
    """

    logging.info(
        "MENU HANDLING | handle_missing_menus | START "
        f"(offence_revisions_in={len(offence_revisions)})"
    )

    # ------------------------------------------------------------
    # STEP 1 — Find offences with EntryFormat='MNU' but missing FID_MenuXX
    # ------------------------------------------------------------
    missing_menu_xml = []

    for off in offence_revisions:
        for key, value in off.items():

            # Match TerminalEntryXX.EntryFormat
            if key.startswith("TerminalEntry") and key.endswith(".EntryFormat"):

                if value == "MNU":

                    xx = key[len("TerminalEntry"):key.index(".EntryFormat")]
                    fid_menu_key = f"FID_Menu{xx}"
                    menu_prompt_key = f"TerminalEntry{xx}.EntryPrompt"

                    # SPECIFY DATE → auto-assign default ID
                    if off.get(menu_prompt_key) == "SPECIFY DATE":
                        off[fid_menu_key] = 1

                    # Still missing? → capture XML file ID
                    if off.get(fid_menu_key) is None:
                        xml_id = off.get("xml_file_id")
                        missing_menu_xml.append(xml_id)

                        logging.warning(
                            "MENU HANDLING | handle_missing_menus | Missing Menu "
                            f"(xml_file_id={xml_id}, missing_key={fid_menu_key})"
                        )

    # Deduplicate list of failed xml_file_ids
    missing_menu_xml = set(missing_menu_xml)

    logging.info(
        "MENU HANDLING | handle_missing_menus | Missing Menu Detection - COMPLETE "
        f"(missing_menu_xml_count={len(missing_menu_xml)})"
    )

    # ------------------------------------------------------------
    # STEP 2 — Remove offences missing menus
    # ------------------------------------------------------------
    before_count = len(offence_revisions)

    offence_revisions = [
        {k: v for k, v in off.items()}
        for off in offence_revisions
        if off.get("xml_file_id") not in missing_menu_xml
    ]

    after_count = len(offence_revisions)
    removed_count = before_count - after_count

    logging.info(
        "MENU HANDLING | handle_missing_menus | Offence Revision Removal - COMPLETE "
        f"(removed={removed_count}, remaining={after_count})"
    )

    # ------------------------------------------------------------
    # STEP 3 — Mark source files as Failure
    # ------------------------------------------------------------
    updated_sf = 0

    for record in source_files:
        if record.get("SourceFileID") in missing_menu_xml:
            record["FID_SourceStatus"] = "Failure"
            updated_sf += 1

    logging.info(
        "MENU HANDLING | handle_missing_menus | SourceFile Update - COMPLETE "
        f"(source_files_marked_failed={updated_sf})"
    )

    # ------------------------------------------------------------
    # STEP 4 — Update SourceFileMessage records to ERROR
    # ------------------------------------------------------------
    updated_msgs = 0

    for rec in messages:
        src_id = rec.get("FID_SourceFile")
        msg_type = rec.get("SourceFileMessageType")

        if src_id in missing_menu_xml and msg_type == "COMPLETION":

            rec["SourceFileMessageCode"] = "MNU-ERR-001"
            rec["SourceFileMessageType"] = "ERROR"
            rec["SourceFileMessage"] = "Missing Menu"

            updated_msgs += 1

    logging.info(
        "MENU HANDLING | handle_missing_menus | SourceFileMessage Update - COMPLETE "
        f"(messages_updated={updated_msgs})"
    )

    # ------------------------------------------------------------
    # END
    # ------------------------------------------------------------
    logging.info("MENU HANDLING | handle_missing_menus | COMPLETE")

    return offence_revisions, source_files, messages
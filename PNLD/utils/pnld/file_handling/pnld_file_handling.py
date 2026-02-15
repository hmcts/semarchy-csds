import logging                    # Structured logging for diagnostics and auditing
from lxml import etree            # XML parsing/manipulation (used by helper modules)
from datetime import datetime     # Safe date parsing for baseline validation
import html                       # HTML entity unescaping (e.g., &amp;lt; → &lt;)
import traceback                  # Capture full stack traces for error reporting

# Domain-specific helper functions used in the PNLD processing pipeline.
# Notes:
# - These helpers are expected to be robust (no unhandled exceptions leaking here).
# - Each returns well-defined shapes as used below; if upstream changes, keep this file in sync.
from utils.pnld.file_handling.helpers.xsd_handling import pnld_xsd_validation
from utils.pnld.file_handling.helpers.pnld_flattening import flatten_pnld
from utils.pnld.file_handling.helpers.pnld_cleansing import cleanse_record
from utils.pnld.file_handling.helpers.pnld_validation import validate_text_pnld
from utils.pnld.file_handling.helpers.transformation.pnld_transform import extract_terminal_entries
from utils.pnld.file_handling.helpers.pnld_validation import compute_md5_hash
from utils.pnld.file_handling.helpers.pnld_validation import validate_pnld
from utils.pnld.file_handling.helpers.pnld_define_offence import define_offence


def pnld_file_handling(xml_record, xsd_encoded, rp_id, progress_tag):
    """
    Orchestrates end-to-end PNLD processing for a single XML record.

    Progress Tag:
      - `progress_tag` should be a progress marker in the form "[x/y]",
        where x = index of the current file being processed (1-based),
              y = total number of files to process.

    Pipeline overview (short-circuiting on failure):
      1) XSD validation
      2) XML flattening → dict-like record
      3) Baseline extraction + safe date parsing
      4) Cleansing & HTML unescaping
      5) Terminal entries / menus extraction (with md5→entry_number mapping)
      6) Text validation for SOW/SOF
      7) Final assembly of outputs (summary, messages, offence revision, menus/options)

    Input contract (xml_record expected keys):
      - 'SourceFileID'       : unique ID for the incoming XML file
      - 'SourceFileContent'  : the raw XML payload (bytes/str as expected by xsd handler)
      - 'BatchID'            : ingestion batch identifier (for correlation/ops)
      - 'SourceFileHeaderID' : header row ID (for downstream linkage)
      - 'UploadedBy'         : username/identifier (auditing)

    Output contract (dict):
      {
        'SourceFile'        : [ { 'SourceFileID', 'FID_SourceStatus', 'MessageCount' } ],
        'SourceFileMessage' : [ { 'FID_SourceFile', 'SourceFileMessageCode', 'SourceFileMessageType', 'SourceFileMessage' }, ... ],
        'SourceFileHeader'  : [ { 'SourceFileHeaderID' } ],
        'OffenceRevision'   : [ offence_record_if_success ],
        'Menu'              : [ menus extracted from text (with xml_file_id backfilled) ],
        'MenuOptions'       : [ unique menu options extracted from text ]
      }

    Logging:
      - All log lines include the prefix 'FILE HANDLING |', followed by progress tag, file ID, and batch ID.
      - Each step logs START and SUCCESS/FAILED with relevant counts.
      - Unexpected exceptions are caught, logged with traceback, and also emitted as a SourceFileMessage.
    """

    # ---------------------------
    # Extract request metadata
    # ---------------------------
    xml_file_id = xml_record.get("SourceFileID")
    xml_encoded = xml_record.get("SourceFileContent")
    batch_id = xml_record.get("BatchID")
    uploaded_by = xml_record.get("UploadedBy")

    # Standard log context suffix: "[x/y] | FileID=... | BatchID=..."
    ctx = f"{progress_tag} | FileID={xml_file_id} | BatchID={batch_id}"

    logging.info(f"FILE HANDLING | {ctx} | File Received - START")

    # --------------------------------------
    # Accumulators for outputs and messages
    # --------------------------------------
    messages = []             # All message dicts collected across stages
    offence_output = []       # Single-element list on success (define_offence output)
    menus_output = []         # Menus extracted from SOW/SOF (with xml_file_id attached)
    menu_options_output = []  # Menu options extracted and de-duplicated

    try:
        # ==============================================================
        # STEP 1 — XSD VALIDATION
        # ==============================================================
        logging.info(f"FILE HANDLING | {ctx} | XSD Validation - START")
        xsd_messages, xml_doc = pnld_xsd_validation(xml_encoded, xsd_encoded, xml_file_id)
        xsd_error_volume = len(xsd_messages)
        messages.extend(xsd_messages)

        if xsd_error_volume == 0:
            logging.info(f"FILE HANDLING | {ctx} | XSD Validation - SUCCESS")

            # ==============================================================
            # STEP 2 — FLATTEN
            # ==============================================================
            logging.info(f"FILE HANDLING | {ctx} | XML Flattening - START")
            record = flatten_pnld(xml_doc)
            logging.info(f"FILE HANDLING | {ctx} | XML Flattening - SUCCESS")

            # ==============================================================
            # STEP 3 — BASELINE PARSING (dates, ids)
            # ==============================================================
            def parse_date(date_str):
                """Parse 'YYYY-MM-DD' into date or return None if invalid."""
                if date_str:
                    try:
                        return datetime.strptime(date_str, "%Y-%m-%d").date()
                    except ValueError:
                        return None
                return None

            pnld_ref = record["pnldref"]
            cjs_code = record["cjsoffencecode"]
            title = record["title"]
            start_date = parse_date(record["offencestartdate"])
            end_date = parse_date(record["offenceenddate"])
            last_update = parse_date(record["dateoflastupdate"])

            # ==============================================================
            # INGESTION VALIDATION (PRE-CLEANSE)
            # ==============================================================
            logging.info(f"FILE HANDLING | {ctx} | Ingestion Validation - START")
            md5_hash = compute_md5_hash(
                record, ["offenceenddate", "dateoflastupdate", "sow_raw", "sof_raw"]
            )
            record["md5_hash"] = md5_hash

            ingestion_error_messages, ingestion_type = validate_pnld(
                pnld_ref, cjs_code, start_date, end_date, last_update, title, md5_hash, xml_file_id
            )
            ingestion_error_volume = len(ingestion_error_messages)
            messages.extend(ingestion_error_messages)

            if ingestion_error_volume == 0:
                logging.info(f"FILE HANDLING | {ctx} | Ingestion Validation - SUCCESS")

                # ==============================================================
                # STEP 4 — CLEANSE & HTML UNESCAPE
                # ==============================================================
                logging.info(f"FILE HANDLING | {ctx} | XML Cleanse - START")
                record["standardoffencewording"] = html.unescape(record["sow_raw"])

                if record["sof_raw"] is not None:
                    record["standardstatementoffacts"] = html.unescape(record["sof_raw"])
                else:
                    record["standardstatementoffacts"] = None

                record, cleanse_messages = cleanse_record(record, xml_file_id)
                cleanse_volume = len(cleanse_messages)
                messages.extend(cleanse_messages)
                logging.info(f"FILE HANDLING | {ctx} | XML Cleanse - SUCCESS (cleanses={cleanse_volume})")

                # ==============================================================
                # STEP 5 — TERMINAL ENTRIES & MENUS (EXTRACTION + INDEX RESOLUTION)
                # ==============================================================
                logging.info(f"FILE HANDLING | {ctx} | Terminal Entry Extraction - START")

                from utils.pnld.file_handling.helpers.pnld_collate_terminal_entries import (
                    keep_lowest_entry_per_md5,
                    replace_md5_placeholders,
                    replace_md5_with_entry_counter,
                    process_menus,
                )

                entry_counter = 0          # Running counter returned/updated by transform
                terminal_entries = []      # Combined SOW + SOF terminal entries
                menus = []                 # Menu definitions discovered in text
                menu_options = []          # Menu option definitions discovered in text
                entry_audit = []           # Links md5 → entry_counter/menu_counter

                # ---- Transform SOW
                (
                    record["standardoffencewording"],
                    te_subset,
                    menus_subset,
                    entry_counter,
                    audit,
                    menu_options_subset,
                ) = extract_terminal_entries(record["standardoffencewording_CLEANSED"], entry_counter)

                terminal_entries.extend(te_subset)
                menus.extend(menus_subset)
                menu_options.extend(menu_options_subset)
                entry_audit.extend(audit)

                # ---- Transform SOF (if present)
                if record["standardstatementoffacts_CLEANSED"] is not None:
                    (
                        record["standardstatementoffacts"],
                        te_subset,
                        menus_subset,
                        entry_counter,
                        audit,
                        menu_options_subset,
                    ) = extract_terminal_entries(record["standardstatementoffacts_CLEANSED"], entry_counter)

                    terminal_entries.extend(te_subset)
                    menus.extend(menus_subset)
                    menu_options.extend(menu_options_subset)
                    entry_audit.extend(audit)

                # Keep only the earliest instance per md5 (ensures stable numbering)
                entry_audit = keep_lowest_entry_per_md5(entry_audit)

                # Replace md5 placeholders in text with final entry numbers
                record["standardoffencewording"] = replace_md5_placeholders(
                    record["standardoffencewording"], entry_audit
                )
                if record["standardstatementoffacts"] is not None:
                    record["standardstatementoffacts"] = replace_md5_placeholders(
                        record["standardstatementoffacts"], entry_audit
                    )

                # Emit final terminal entries referencing entry_number instead of md5
                terminal_entries = replace_md5_with_entry_counter(terminal_entries, entry_audit)

                # Replace menu_md5 with the newly calculated md5 (includes Menu Name)
                menus, menu_options, terminal_entries = process_menus(
                    cjs_code, terminal_entries, menus, menu_options
                )

                # Flatten terminal entries into deterministic key/value pairs
                terminal_entries_formatted = {}
                for item in terminal_entries:
                    idx = str(item["entry_number"]).zfill(2)  # zero-pad for ordering: 01, 02, ...
                    terminal_entries_formatted[f"TerminalEntry{idx}.EntryNumber"] = item.get("entry_number")
                    terminal_entries_formatted[f"TerminalEntry{idx}.EntryFormat"] = item.get("format")
                    terminal_entries_formatted[f"TerminalEntry{idx}.EntryPrompt"] = item.get("prompt")
                    terminal_entries_formatted[f"TerminalEntry{idx}.Minimum"] = item.get("minimum")
                    terminal_entries_formatted[f"TerminalEntry{idx}.Maximum"] = item.get("maximum")
                    terminal_entries_formatted[f"TerminalEntry{idx}.StandardEntryIdentifier"] = item.get("sei")
                    # Downstream linkage (migrates to FID_Menu during menu handling)
                    terminal_entries_formatted[f"FID_Menu{idx}"] = item.get("menu_md5")

                logging.info(
                    f"FILE HANDLING | {ctx} | Terminal Entry Extraction - SUCCESS "
                    f"(entries={len(terminal_entries)}, menus={len(menus)}, options={len(menu_options)})"
                )

                # ==============================================================
                # STEP 6 — TEXT VALIDATION (human-readable rules)
                # ==============================================================
                logging.info(f"FILE HANDLING | {ctx} | Text Validation - START")
                text_error_messages = []

                # Validate SOW text
                text_error_messages.extend(
                    validate_text_pnld(
                        record["standardoffencewording"], "standardoffencewording", xml_file_id
                    )
                )

                # Validate SOF text (if provided)
                if record["standardstatementoffacts"] is not None:
                    text_error_messages.extend(
                        validate_text_pnld(
                            record["standardstatementoffacts"], "standardstatementoffacts", xml_file_id
                        )
                    )

                text_error_volume = len(text_error_messages)
                messages.extend(text_error_messages)

                if text_error_volume == 0:
                    logging.info(f"FILE HANDLING | {ctx} | Text Validation - SUCCESS")

                    # ==========================================================
                    # STEP 7 — OUTPUT ASSEMBLY (SUCCESS PATH)
                    # ==========================================================
                    output_record = define_offence(
                        record, terminal_entries_formatted, ingestion_type, uploaded_by, rp_id
                    )
                    output_record["xml_file_id"] = xml_file_id
                    offence_output = [output_record]

                    # Keep lineage: attach file id to each menu row
                    for menu in menus:
                        menu["xml_file_id"] = xml_file_id

                    menus_output = menus
                    menu_options_output = menu_options

                    xml_summary = [
                        {
                            "SourceFileID": xml_file_id,
                            "MessageCount": len(messages),
                        }
                    ]
                    logging.info(
                        f"FILE HANDLING | {ctx} | Output Assembly - SUCCESS "
                        f"(messages={len(messages)}, menus={len(menus_output)}, options={len(menu_options_output)})"
                    )

                else:
                    # ==========================================================
                    # STEP 7 — OUTPUT ASSEMBLY (FAILED TEXT VALIDATION)
                    # ==========================================================
                    logging.info(
                        f"FILE HANDLING | {ctx} | Text Validation - FAILED (errors={text_error_volume})"
                    )
                    xml_summary = [
                        {
                            "SourceFileID": xml_file_id,
                            "FID_SourceStatus": "Failed",
                            "MessageCount": len(messages),
                        }
                    ]

            else:
                # Ingestion validation failed — no offence output; report failure
                logging.info(
                    f"FILE HANDLING | {ctx} | Ingestion Validation - FAILED (errors={ingestion_error_volume})"
                )
                xml_summary = [
                    {
                        "SourceFileID": xml_file_id,
                        "FID_SourceStatus": "Failed",
                        "MessageCount": len(messages),
                    }
                ]

        else:
            # XSD validation failed — short-circuit with Failure summary
            logging.info(
                f"FILE HANDLING | {ctx} | XSD Validation - FAILED (errors={xsd_error_volume})"
            )
            xml_summary = [
                {
                    "SourceFileID": xml_file_id,
                    "FID_SourceStatus": "Failed",
                    "MessageCount": len(messages),
                }
            ]

    except Exception:
        # --------------------------------------------------------------
        # LAST-CHANCE HANDLER
        # --------------------------------------------------------------
        logging.exception(f"FILE HANDLING | {ctx} | UNHANDLED EXCEPTION")
        full_error = traceback.format_exc()

        messages.append(
            {
                "FID_SourceFile": xml_file_id,
                "SourceFileMessageCode": "XX-999",
                "SourceFileMessageType": "ERROR",
                "SourceFileMessage": full_error,
            }
        )

        xml_summary = [
            {
                "SourceFileID": xml_file_id,
                "FID_SourceStatus": "Failed",
                "MessageCount": len(messages),
            }
        ]

    # ---------------------------
    # Emit final counts & return
    # ---------------------------
    logging.info(f"FILE HANDLING | {ctx} | File Processing - COMPLETE (messages={len(messages)})")

    return {
        "SourceFile": xml_summary,
        "SourceFileMessage": messages,
        "OffenceRevision": offence_output,
        "Menu": menus_output,
        "MenuOptions": menu_options_output,
    }
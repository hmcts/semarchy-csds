import logging
import html
import traceback
from datetime import datetime
from lxml import etree

from pnld_process.utils.file_handling.helpers.xsd_handling import pnld_xsd_validation
from pnld_process.utils.file_handling.helpers.pnld_flattening import flatten_pnld
from pnld_process.utils.file_handling.helpers.pnld_cleansing import cleanse_record
from pnld_process.utils.file_handling.helpers.pnld_validation import (
    validate_text_pnld,
    compute_md5_hash,
    validate_pnld,
)
from pnld_process.utils.file_handling.helpers.sow_sof_transform.pnld_transform import (
    extract_terminal_entries,
)
from pnld_process.utils.file_handling.helpers.pnld_define_offence import define_offence
from pnld_process.utils.message_handling import add_message

from pnld_process.utils.file_handling.helpers.pnld_collate_terminal_entries import (
    keep_lowest_entry_per_md5,
    replace_md5_placeholders,
    replace_md5_with_entry_counter,
    process_menus,
)


# ----------------------------------------------------------------------
# Utility helpers
# ----------------------------------------------------------------------
def safe_date(date_str):
    """Safely convert YYYY-MM-DD to a date object."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return None


def log(ctx, message):
    logging.info(f"FILE HANDLING | {ctx} | {message}")


def unique_dicts(dict_list):
    """Remove duplicate dicts while preserving order."""
    seen = set()
    unique = []
    for d in dict_list:
        tup = tuple(d.items())
        if tup not in seen:
            seen.add(tup)
            unique.append(d)
    return unique


def format_terminal_entries(terminal_entries):
    """Format terminal entries into deterministic key/value pairs."""
    formatted = {}
    for item in terminal_entries:
        idx = str(item["entry_number"]).zfill(2)
        prefix = f"TerminalEntry{idx}"

        formatted[f"{prefix}.EntryNumber"] = item.get("entry_number")
        formatted[f"{prefix}.EntryFormat"] = item.get("format")
        formatted[f"{prefix}.EntryPrompt"] = item.get("prompt")
        formatted[f"{prefix}.Minimum"] = item.get("minimum")
        formatted[f"{prefix}.Maximum"] = item.get("maximum")
        formatted[f"{prefix}.StandardEntryIdentifier"] = item.get("sei")
        formatted[f"FID_Menu{idx}"] = item.get("menu_md5")

    return formatted


# ----------------------------------------------------------------------
# Main Pipeline
# ----------------------------------------------------------------------
def pnld_file_handling(xml_record, xsd_encoded, rp_id, progress_tag):
    """
    Orchestrates the full PNLD pipeline for a single XML file.
    See original function docstring for full contract.
    """

    xml_file_id = xml_record.get("SourceFileID")
    xml_raw = xml_record.get("SourceFileContent")
    batch_id = xml_record.get("BatchID")
    uploaded_by = xml_record.get("UploadedBy")

    ctx = f"{progress_tag} | FileID={xml_file_id} | BatchID={batch_id}"

    log(ctx, "File Received - START")

    messages = []

    try:
        # --------------------------------------------------------------
        # STEP 1 — XSD VALIDATION
        # --------------------------------------------------------------
        log(ctx, "XSD Validation - START")
        xsd_messages, xml_doc = pnld_xsd_validation(xml_raw, xsd_encoded, xml_file_id)
        messages.extend(xsd_messages)

        if xsd_messages:
            log(ctx, f"XSD Validation - FAILED (errors={len(xsd_messages)})")
            return {
                "SourceFile": [{
                    "SourceFileID": xml_file_id,
                    "FID_SourceStatus": "Failed",
                    "MessageCount": len(messages)
                }],
                "SourceFileMessage": messages,
                "OffenceRevision": [],
                "Menu": [],
                "MenuOptions": [],
            }

        log(ctx, "XSD Validation - SUCCESS")

        # --------------------------------------------------------------
        # STEP 2 — FLATTEN XML → DICT
        # --------------------------------------------------------------
        log(ctx, "XML Flattening - START")
        record = flatten_pnld(xml_doc)
        log(ctx, "XML Flattening - SUCCESS")

        # --------------------------------------------------------------
        # STEP 3 — BASELINE PARSING
        # --------------------------------------------------------------
        pnld_ref = record["pnldref"]
        cjs_code = record["cjsoffencecode"]
        title = record["title"]

        start_date = safe_date(record["offencestartdate"])
        end_date = safe_date(record["offenceenddate"])
        last_update = safe_date(record["dateoflastupdate"])

        # --------------------------------------------------------------
        # STEP 4 — INGESTION VALIDATION
        # --------------------------------------------------------------
        log(ctx, "Ingestion Validation - START")

        md5_hash = compute_md5_hash(
            record, ["offenceenddate", "dateoflastupdate", "sow_raw", "sof_raw"]
        )
        record["md5_hash"] = md5_hash

        ingestion_msgs, ingestion_type = validate_pnld(
            pnld_ref, cjs_code, start_date, end_date, last_update,
            title, md5_hash, xml_file_id
        )

        messages.extend(ingestion_msgs)

        if ingestion_msgs:
            log(ctx, f"Ingestion Validation - FAILED (errors={len(ingestion_msgs)})")

            # Detect special scheduling completion case
            has_completion = any(
                m.get("FID_SourceFile") == xml_file_id
                and m.get("MessageType") == "COMPLETION"
                for m in messages
            )

            status = (
                "Scheduling Complete - No Transformation"
                if has_completion and len(messages) == 1
                else "Failed"
            )

            return {
                "SourceFile": [{
                    "SourceFileID": xml_file_id,
                    "FID_SourceStatus": status,
                    "MessageCount": len(messages)
                }],
                "SourceFileMessage": messages,
                "OffenceRevision": [],
                "Menu": [],
                "MenuOptions": [],
            }

        log(ctx, "Ingestion Validation - SUCCESS")

        # --------------------------------------------------------------
        # STEP 5 — CLEANSE + HTML UNESCAPE
        # --------------------------------------------------------------
        log(ctx, "XML Cleanse - START")

        record["SOW"] = html.unescape(record["sow_raw"])
        record["SOF"] = (
            html.unescape(record["sof_raw"]) if record["sof_raw"] is not None else None
        )

        record, cleanse_msgs = cleanse_record(record, xml_file_id)
        messages.extend(cleanse_msgs)

        log(ctx, f"XML Cleanse - SUCCESS (cleanses={len(cleanse_msgs)})")

        # --------------------------------------------------------------
        # STEP 6 — TERMINAL ENTRIES + MENUS
        # --------------------------------------------------------------
        log(ctx, "Terminal Entry Extraction - START")

        entry_counter = 0
        all_entries = []
        menus = []
        menu_opts = []
        audit = []

        # ---- Process SOW
        (
            record["SOW"],
            te_sow,
            menus_sow,
            entry_counter,
            audit_sow,
            opts_sow,
        ) = extract_terminal_entries(record["SOW_CLEANSED"], entry_counter)

        all_entries.extend(te_sow)
        menus.extend(menus_sow)
        menu_opts.extend(opts_sow)
        audit.extend(audit_sow)

        # ---- Process SOF
        if record["SOF_CLEANSED"]:
            (
                record["SOF"],
                te_sof,
                menus_sof,
                entry_counter,
                audit_sof,
                opts_sof,
            ) = extract_terminal_entries(record["SOF_CLEANSED"], entry_counter)

            all_entries.extend(te_sof)
            menus.extend(menus_sof)
            menu_opts.extend(opts_sof)
            audit.extend(audit_sof)

        # Resolve md5 → entry number mapping
        audit = keep_lowest_entry_per_md5(audit)

        record["SOW"] = replace_md5_placeholders(record["SOW"], audit)
        if record["SOF"] is not None:
            record["SOF"] = replace_md5_placeholders(record["SOF"], audit)

        all_entries = replace_md5_with_entry_counter(all_entries, audit)

        menus, menu_opts, all_entries = process_menus(cjs_code, all_entries, menus, menu_opts)

        if len(all_entries) > 30:
            log(ctx, "Too Many Terminal Entries - FAILED")

            messages = add_message(
                messages,
                file_id=xml_file_id,
                code="ER-NSDT-TERMINALENTRY-001",
                msg_type="ERROR",
                issue="Invalid Offence details within File",
                cause=f"Maximum Terminal Entries is 30, found {len(all_entries)}",
                resolution="NSD to report back to PNLD to correct data at source.",
            )

            return {
                "SourceFile": [{
                    "SourceFileID": xml_file_id,
                    "FID_SourceStatus": "Failed",
                    "MessageCount": len(messages)
                }],
                "SourceFileMessage": messages,
                "OffenceRevision": [],
                "Menu": [],
                "MenuOptions": [],
            }

        formatted_entries = format_terminal_entries(all_entries)

        log(
            ctx,
            f"Terminal Entry Extraction - SUCCESS "
            f"(entries={len(all_entries)}, menus={len(menus)}, options={len(menu_opts)})"
        )

        # --------------------------------------------------------------
        # STEP 7 — TEXT VALIDATION
        # --------------------------------------------------------------
        log(ctx, "Text Validation - START")

        text_errors = []

        # Validate SOW + SOW menu options
        text_errors.extend(validate_text_pnld(record["SOW"], "SOW", xml_file_id))
        for opt in opts_sow:
            text_errors.extend(validate_text_pnld(opt["OptionText"], "SOWMENU", xml_file_id))

        # Validate SOF + SOF menu options
        if record["SOF"]:
            text_errors.extend(validate_text_pnld(record["SOF"], "SOF", xml_file_id))
            for opt in opts_sof:
                text_errors.extend(validate_text_pnld(opt["OptionText"], "SOFMENU", xml_file_id))

        text_errors = unique_dicts(text_errors)
        messages.extend(text_errors)

        if text_errors:
            log(ctx, f"Text Validation - FAILED (errors={len(text_errors)})")
            return {
                "SourceFile": [{
                    "SourceFileID": xml_file_id,
                    "FID_SourceStatus": "Failed",
                    "MessageCount": len(messages)
                }],
                "SourceFileMessage": messages,
                "OffenceRevision": [],
                "Menu": [],
                "MenuOptions": [],
            }

        log(ctx, "Text Validation - SUCCESS")

        # --------------------------------------------------------------
        # STEP 8 — SUCCESS OUTPUT ASSEMBLY
        # --------------------------------------------------------------
        offence_record = define_offence(
            record, formatted_entries, ingestion_type, uploaded_by, rp_id
        )
        offence_record["xml_file_id"] = xml_file_id

        for m in menus:
            m["xml_file_id"] = xml_file_id

        log(
            ctx,
            f"Output Assembly - SUCCESS "
            f"(messages={len(messages)}, menus={len(menus)}, options={len(menu_opts)})"
        )

        return {
            "SourceFile": [{
                "SourceFileID": xml_file_id,
                "MessageCount": len(messages),
            }],
            "SourceFileMessage": messages,
            "OffenceRevision": [offence_record],
            "Menu": menus,
            "MenuOptions": menu_opts,
        }

    # --------------------------------------------------------------
    # GLOBAL UNHANDLED EXCEPTION HANDLER
    # --------------------------------------------------------------
    except Exception:
        logging.exception(f"FILE HANDLING | {ctx} | UNHANDLED EXCEPTION")

        messages = add_message(
            messages,
            file_id=xml_file_id,
            code="ER-SUPP-UNEXPECTED-001",
            msg_type="ERROR",
            issue="Unexpected error when transforming file",
            cause=traceback.format_exc(),
            resolution="CONTACT SUPPORT TEAM",
        )

        return {
            "SourceFile": [{
                "SourceFileID": xml_file_id,
                "FID_SourceStatus": "Failed",
                "MessageCount": len(messages)
            }],
            "SourceFileMessage": messages,
            "OffenceRevision": [],
            "Menu": [],
            "MenuOptions": [],
        }
import logging
from lxml import etree
from datetime import datetime
import html
import traceback
import hashlib

# Domain-specific helpers (not used here but imported for consistency)
from utils.pnld.file_handling.helpers.xsd_handling import pnld_xsd_validation
from utils.pnld.file_handling.helpers.pnld_define_offence import define_offence


def pnld_file_handling_no_rp(xml_record, rp_message, logging_id):
    """
    Handles PNLD file processing when NO Release Package is available.

    Behaviour:
      - Marks the file as Failed
      - Emits a single error message describing the RP failure reason
      - Does NOT attempt XML/XSD processing or transforms

    Logging follows PNLD-wide standard:
        FILE HANDLING | <progress_tag> | <step> - <status>
    """

    # ---------------------------
    # Extract request metadata
    # ---------------------------
    xml_file_id = xml_record.get("SourceFileID")
    batch_id = xml_record.get("BatchID")

    ctx = f"{logging_id} | FileID={xml_file_id} | BatchID={batch_id}"

    logging.info(f"FILE HANDLING | {ctx} | No-RP Path - START")

    # ---------------------------
    # Build failed SourceFile block
    # ---------------------------
    source_file = [{
        "SourceFileID": xml_file_id,
        "FID_SourceStatus": "Failed",
        "MessageCount": 1,
    }]

    # ---------------------------
    # Add failure message
    # ---------------------------
    message = [{
        "FID_SourceFile": xml_file_id,
        "SourceFileMessageCode": "5-CO-CO-001",
        "SourceFileMessageType": "ERROR",
        "SourceFileMessage": rp_message,
    }]

    logging.error(
        f"FILE HANDLING | {ctx} | No-RP Path - FAILED "
        f"(reason='{rp_message}')"
    )

    # ---------------------------
    # No offence revision output
    # ---------------------------
    offence_revision = []

    logging.info(f"FILE HANDLING | {ctx} | No-RP Path - COMPLETE")

    # ---------------------------
    # Return consistent shape
    # ---------------------------
    return {
        "SourceFile": source_file,
        "SourceFileMessage": message,
        "OffenceRevision": offence_revision,
    }
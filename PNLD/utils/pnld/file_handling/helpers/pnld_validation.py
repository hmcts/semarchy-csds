import re
import os
import requests
from datetime import datetime






def validate_text_pnld(text,
                       attribute_name,
                       xml_file_id):
    """
    Scan `text` with a set of detection regex rules.
    For each match, append an error message with a 10-char context slice.

    Returns:
        List[dict]: messages containing detection results per match.
    """

    # -----------------------------------------------------------
    # Helper function to append a formatted message into `messages`
    # This creates a consistent message dictionary structure.
    # -----------------------------------------------------------
    def add_message(msg):
        messages.append({
            'FID_SourceFile': xml_file_id,
            'SourceFileMessageCode': 'CL-999',   # Code identifying which rule fired
            'SourceFileMessageType': 'ERROR',     # Always ERROR for validation failures
            'SourceFileMessage': msg              # Human-readable message text
        })
    
    ####################### REGEX DETECTION VALIDATION

    # ------------------------------------------------------------------------
    # Helper that extracts up to 10 characters before and after the match
    # This adds helpful context to error messages without dumping full text.
    # ------------------------------------------------------------------------
    def get_context_slice(s: str, start: int, end: int, pad: int = 10) -> str:
        left = max(0, start - pad)          # Ensure slice does not go below index 0
        right = min(len(s), end + pad)      # Ensure slice does not exceed text length
        return s[left:right]
    
    
    # This will be returned at the end containing all messages found in this run.
    messages = []

    # ---------------------------------------------------------------------
    # Configuration of rules:
    # Each rule contains:
    #   - a RuleID identifier
    #   - a regex used to detect forbidden or malformed patterns
    # ---------------------------------------------------------------------
    config = [
        # Matches either `)_[` or `]_`
        {'RuleID': 'MO-999', 'DetectionRegex': r'(?:\)_\[|\]_)'},

        # SPECIFY variants such as "**(..", "..)", or spaced-out "S P E C I F Y"
        {'RuleID': 'SP-999', 'DetectionRegex': r'(?:\*\*\(\.\.|\.\.\)|SPECIFY)'},

        # Matches HTML <br> fragments in escaped form such as "&lt;br" or "/&gt;"
        {'RuleID': 'BR-999', 'DetectionRegex': r'(?:\<br|\/\>)'}
    ]

    # -----------------------------------------------------
    # Apply each regex rule to the incoming text.
    # For every match, generate an error message.
    # -----------------------------------------------------
    for rule in config:
        rule_id = rule['RuleID']          # Retrieve rule identifier
        pattern = rule['DetectionRegex']  # Retrieve regex pattern

        # Compile the regex for matching
        regex = re.compile(pattern)

        # Find all matches for this pattern within the text
        matches = list(regex.finditer(text))
        if not matches:
            # No matches for this specific rule — skip to next rule
            continue

        # ---------------------------------------------------------
        # For EACH individual match generate a message showing:
        #   - rule ID
        #   - attribute name
        #   - matched substring
        #   - 10-character surrounding context
        # ---------------------------------------------------------
        for m in matches:
            detected = m.group(0)  # The actual matched text snippet
            context_slice = get_context_slice(text, m.start(), m.end(), pad=10)

            msg = (
                f"{rule_id} | Attribute: {attribute_name} | "
                f'Detected: "{detected}" | '
                f'Context: "{context_slice}"'
            )
            add_message(msg)



    ####################### MISSING PARENTHESIS VALIDATION
    
    # -------------------------------------------------------------------------
    # Simple helper that verifies opener and closer *counts* match.
    # NOTE: Does NOT verify ordering or nesting — only numeric equality.
    # -------------------------------------------------------------------------
    def missing_parenthesis(text, opener, closer):
            if text.count(opener) > text.count(closer):
                return f'{opener} without {closer}'
            elif text.count(opener) < text.count(closer):
                return f'{closer} without {opener}'
            else:
                return None
        

    # ---------------------------------------------------------------
    # List of bracket pairs to validate.
    # ---------------------------------------------------------------
    parenthesis = [
            {'Opener': '(', 'Closer': ')'}
            ,{'Opener': '[', 'Closer': ']'}
            ,{'Opener': '{', 'Closer': '}'}
    ]

    # ---------------------------------------------------------
    # Check each defined bracket pair for balanced counts.
    # If the number of opens ≠ closes, raise an error.
    # ---------------------------------------------------------
    for p in parenthesis:

        # Extract opener "(" and closer ")"
        opener = p['Opener']
        closer = p['Closer']

        output = missing_parenthesis(text, opener, closer)
        # Validate matching counts
        if output is not None:

            # Build error message
            # NOTE: As in original code, it prints the opener twice.
            msg = (
                f"MP-999 | Attribute: {attribute_name} | "
                f'Missing Parenthese: "{output}" | '
                f'Context: "{text}"'
                       )

            # Add the message for missing parentheses
            add_message(msg)


    # -------------------------------------
    # Return all validation messages found
    # -------------------------------------
    return messages






import hashlib
import json

def compute_md5_hash(record, exclude_keys):
    """
    Compute an MD5 hash for a dictionary, ignoring specified keys.
    - None values are normalized to empty string.
    - Uses JSON with sorted keys for deterministic hashing.
    """

    exclude = set(exclude_keys or [])
    
    # Filter out excluded keys and normalize values
    filtered = {
        k: ("" if v is None else v)
        for k, v in record.items()
        if k not in exclude
    }
    
    # Serialize deterministically
    payload = json.dumps(filtered, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    
    # Compute MD5 hash
    return hashlib.md5(payload.encode("utf-8")).hexdigest()




def validate_pnld(
    pnld_ref,
    cjs_code,
    start_date,
    end_date,
    last_update,
    title,
    md5_hash,
    xml_file_id
):
    """
    Validate PNLD baseline data against provided XML values.

    - Performs date consistency checks.
    - Calls the Semarchy Baseline Named Query to fetch existing records.
    - Determines ingestion type (NEW / UPDATE-<version> / INVALID).
    - Returns: (messages, ingestion_type)
    """

    messages = []
    ingestion_type = "TBC"

    # ----------------------------------------------------------------------
    # Helper functions
    # ----------------------------------------------------------------------
    def add_error(code, text):
        messages.append({
            "FID_SourceFile": xml_file_id,
            "SourceFileMessageCode": code,
            "SourceFileMessageType": "ERROR",
            "SourceFileMessage": text
        })

    def safe_parse_date(value, fmt="%Y-%m-%d"):
        if value is None:
            return None
        try:
            return datetime.strptime(value, fmt).date()
        except Exception:
            return None

    def norm_status(value):
        return (value or "").strip().title()

    # ----------------------------------------------------------------------
    # Date consistency checks
    # ----------------------------------------------------------------------
    if title is None:
        add_error("DC-003", 'Element "title" is missing')

    # ----------------------------------------------------------------------
    # Proceed only if no date errors
    # ----------------------------------------------------------------------
    if messages:
        return messages, ingestion_type
    

    if end_date and start_date and end_date < start_date:
        add_error(
            "DC-002",
            f"Offence End Date {end_date} is before Offence Start Date {start_date}"
        )

    if messages:
        return messages, ingestion_type

    # ----------------------------------------------------------------------
    # Fetch baseline
    # ----------------------------------------------------------------------
    baseline_url = (
        f"{os.getenv('SemarchyOffenceRevisionNamedQueryURL')}"
        f"?CJS_CODE={cjs_code}&PNLD_REF={pnld_ref}"
    )
    headers = {"API-Key": os.getenv("SemarchyAPIKey")}

    response = requests.get(baseline_url, headers=headers, timeout=10)
    response.raise_for_status()

    body = response.json()
    baseline_records = body.get("records", [])

    # ----------------------------------------------------------------------
    # Baseline relational validation
    # ----------------------------------------------------------------------
    for r in baseline_records:
        bl_cjs_code = r.get("CJSCode")
        bl_pnld_ref = r.get("SOWReference")

        if pnld_ref == bl_pnld_ref and cjs_code != bl_cjs_code:
            add_error(
                "BL-001",
                f"PNLD Ref {pnld_ref} is already associated to CJS Code {bl_cjs_code} within the Baseline"
            )

        if pnld_ref != bl_pnld_ref and cjs_code == bl_cjs_code:
            add_error(
                "BL-002",
                f"CJS Code {cjs_code} is already associated to PNLD Ref {bl_pnld_ref} within the Baseline"
            )

    if messages:
        return messages, ingestion_type

    # ----------------------------------------------------------------------
    # No baseline
    # ----------------------------------------------------------------------
    if len(baseline_records) == 0:
        ingestion_type = "Initial"
        return messages, ingestion_type

    # ----------------------------------------------------------------------
    # Unexpected >1 baseline rows
    # ----------------------------------------------------------------------
    if len(baseline_records) > 1:
        raise ValueError(
            f"Unexpected Combination of CJS Code + PnLD Ref for "
            f"CJS Code: {cjs_code} & PNLD Ref: {pnld_ref}"
        )

    # ----------------------------------------------------------------------
    # Exactly 1 baseline record
    # ----------------------------------------------------------------------
    r = baseline_records[0]

    bl_cjs_code = r.get("CJSCode")
    bl_pnld_ref = r.get("SOWReference")
    bl_authoring_status = norm_status(r.get("AuthoringStatus"))

    # ------------------------------------------
    # Authoring state validation
    # ------------------------------------------
    if bl_authoring_status in ["Draft", "Final"]:
        add_error(
            "BL-101",
            f"Offence with CJS Code {cjs_code} is currently in {bl_authoring_status} within Semarchy."
        )

    if messages:
        return messages, ingestion_type

    # ------------------------------------------
    # Date of Last Update checks
    # ------------------------------------------
    bl_dolu = safe_parse_date(r.get("PNLDDateOfLastUpdate"), "%Y-%m-%d")

    if bl_dolu:

        if last_update == bl_dolu:
            add_error(
                "BL-003",
                f"Date of Last Update {last_update} is identical to the Baseline"
            )

        if last_update < bl_dolu:
            add_error(
                "BL-004",
                f"Date of Last Update {last_update} is before the Baseline Date Of Last Update {bl_dolu}"
            )

    if messages:
        return messages, ingestion_type

    # ------------------------------------------
    # Offence update checks (hash + end date)
    # ------------------------------------------
    bl_md5 = r.get("PNLDHashMD5")
    bl_end_date = safe_parse_date(r.get("DateUsedTo"))

    same_hash = (bl_md5 == md5_hash)
    same_end_date = (bl_end_date == end_date)

    hashes_different = not same_hash
    dates_different = not same_end_date

    if same_hash and same_end_date:
        add_error(
            "BL-102",
            f"Upload has no updates when compared to Offence with CJS Code {cjs_code}"
        )

    if messages:
        return messages, ingestion_type

    # ------------------------------------------
    # Start/End Date structural checks
    # ------------------------------------------
    bl_start_date = safe_parse_date(r.get("DateUsedFrom"))

    if end_date and end_date < bl_start_date:
        add_error(
            "BL-111",
            f"File Offence End Date {end_date} is before CSDS Start Date {bl_start_date}"
        )

    if messages:
        return messages, ingestion_type

    # ------------------------------------------
    # Ingestion type resolution
    # ------------------------------------------
    publishing_status = r.get("PublishingStatus")

    if hashes_different:
        ingestion_type = "New"
    else:
        if dates_different:
            ingestion_type = "Edit" if publishing_status == "Active" else "New"
        else:
            ingestion_type = "New"

    return messages, ingestion_type
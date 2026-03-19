import re
import os
import requests
from datetime import datetime
from pnld_process.utils.message_handling import add_message





def validate_text_pnld(text,
                       attribute_name,
                       xml_file_id):
    """
    Scan `text` with a set of detection regex rules.
    For each match, append an error message with a 10-char context slice.

    Returns:
        List[dict]: messages containing detection results per match.
    """
    
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

        # SPECIFY variants such as "**(..", "..)", or spaced-out "S P E C I F Y"
        {
            'RuleID': '001',
            'DetectionRegex': r'(?:\*\*\(\.\.|\.\.\)|SPECIFY)',
            'Issue':f'Failed to transform Terminal Entries within {attribute_name}. The expected syntax is "**(..SPECIFY XXX..)"',
            'Cause':f'Indications of Terminal Entries have been detected within {attribute_name} after transformation.',
            'Resolution':f'Check for issues around Terminal Entry prompts, such as missing asterisks, leading/trailing dots, extra spaces () or SPECIFY in {attribute_name}. Examples "**(..SPECIFY DATE ", "SPECIFY DATE..)"'},

        # Matches either `)_[` or `]_`
        {
            'RuleID': '002',
            'DetectionRegex': r'(?:\)_\[|\]_)',
            'Issue':f'Failed to transform Menu Options within {attribute_name}. The expected syntax for each option is "(X)_[text]_", with each option separated by a single "<br />" break',
            'Cause':f'Indications of Terminal Entries have been detected within {attribute_name} after transformation.',
            'Resolution':f'Review {attribute_name} for Menu Option issues caused by missing or incorrect opening/closing parentheses. Examples "<br />(B)_[text<br />", "<br />)_[text]_<br />", "<b/>(B)_[text]_<br />"'},

        # Matches HTML <br> fragments in escaped form such as "&lt;br" or "/&gt;"
        {
            'RuleID': '003',
            'DetectionRegex': r'(?:\<br|\/\>)',
            'Issue':f'Failed to transform Breaks within {attribute_name}.  The expected syntax for each option is "<br />".',
            'Cause':f'Indications of malformed Breaks have been detected within {attribute_name} after transformation.',
            'Resolution':(f'Review {attribute_name} for incorrect or incomplete break tags. Common issues include partially formed tags such as "<br/" or "br />", or missing components around required breaks. '
                         'Ensure all breaks use the correct format: <br /> for single breaks and <br /><br /> for double breaks.')}
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
            context_slice = get_context_slice(text, m.start(), m.end(), pad=10)

            messages = add_message(
                    messages=messages,
                    file_id=xml_file_id,
                    code=f'ER-NSDT-{attribute_name}-{rule_id}',
                    msg_type='ERROR',
                    issue=rule['Issue'],
                    cause=f'{rule["Cause"]} Transformed Text: "{text}"',
                    resolution=rule['Resolution']
                )



    ####################### MISSING PARENTHESIS VALIDATION
    
    # -------------------------------------------------------------------------
    # Simple helper that verifies opener and closer *counts* match.
    # NOTE: Does NOT verify ordering or nesting — only numeric equality.
    # -------------------------------------------------------------------------
    def fn_missing_parenthesis(text, opener, closer):
            if text.count(opener) > text.count(closer):
                return opener, closer
            elif text.count(opener) < text.count(closer):
                return closer, opener
            else:
                return None, None
        

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

        present_parenthesis, missing_parenthesis = fn_missing_parenthesis(text, opener, closer)
        # Validate matching counts
        if missing_parenthesis is not None:

            # Build error message
            # NOTE: As in original code, it prints the opener twice.
            messages = add_message(
                    messages=messages,
                    file_id=xml_file_id,
                    code=f'ER-NSDT-{attribute_name}-004',
                    msg_type='ERROR',
                    issue=f'Failed to transform {attribute_name} as Missing Parenthesis has been detected',
                    cause=f'Indications of missing parenthesis "{missing_parenthesis}" have been detected within {attribute_name} after transformation. Transformed Text: "{text}"',
                    resolution=(f'Review {attribute_name} for missing parentheses. Identify any instance of "{present_parenthesis}" '
                                f'that does not have a matching "{missing_parenthesis}", and ensure all parentheses appear in complete pairs. '
                                'This could be outside of a terminal entry or menu (horse) with missing parenthesis or the terminal entry structure '
                                'is so badly formed in the xml that the system cannot perform a cleanse.'
                                )
 
                )

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
        messages = add_message(
                messages=messages,
                file_id=xml_file_id,
                code='ER-NSDT-XML-002',
                msg_type='ERROR',
                issue='XML does not meet the structure requirements of a PNLD XML',
                cause='Element "title" is missing',
                resolution='NSD to raise issue with PNLD once missing section identified.'
            )

    # ----------------------------------------------------------------------
    # Proceed only if no date errors
    # ----------------------------------------------------------------------
    if messages:
        return messages, ingestion_type
    

    if end_date and start_date and end_date < start_date:
        messages = add_message(
                messages=messages,
                file_id=xml_file_id,
                code='ER-NSDT-OFFENCE-001',
                msg_type='ERROR',
                issue='Invalid Offence Details within PNLD File',
                cause=f'Offence End Date "{end_date}" is before Offence Start Date "{start_date}" within PNLD File',
                resolution='Review XML date range within PNLD File - NSD to raise with PNLD'
            )

    if messages:
        return messages, ingestion_type

    # ----------------------------------------------------------------------
    # Fetch baseline
    # ----------------------------------------------------------------------
    baseline_url = (
        f'{os.getenv("SemarchyBaseURL")}/named-query/CSDS/GetOffenceRevisionPNLD/GD'
        f'?CJS_CODE={cjs_code}&PNLD_REF={pnld_ref}'
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
            messages = add_message(
                messages=messages,
                file_id=xml_file_id,
                code='ER-NSDT-SEARCH-001',
                msg_type='ERROR',
                issue='Invalid Offence Details within PNLD File',
                cause=f'PNLD Ref "{pnld_ref}" is already associated to CJS Code "{bl_cjs_code}" within CSDS',
                resolution='Raise issue with PNLD'
            )


        if pnld_ref != bl_pnld_ref and cjs_code == bl_cjs_code:
            messages = add_message(
                messages=messages,
                file_id=xml_file_id,
                code='ER-NSDT-SEARCH-002',
                msg_type='ERROR',
                issue='Invalid Offence Details within PNLD File',
                cause=f'CJS Code "{cjs_code}" is already associated to PNLD Ref "{bl_pnld_ref}" within CSDS',
                resolution='Raise issue with PNLD'
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
        messages = add_message(
                messages=messages,
                file_id=xml_file_id,
                code='ER-NSDT-STATUS-001',
                msg_type='ERROR',
                issue='Offence update not permitted',
                cause=f'Offence with CJS Code "{cjs_code}" cannot be updated because it is currently in "{bl_authoring_status}" within CSDS',
                resolution=(f'NSDT to investigate the {bl_authoring_status} offence already on CSDS. '
                            ' NSD to decide whether to publish the draft offence already in CSDS and then re-process the PNLD file or delete the draft in CSDS and re-process the PNLD file.')
            )

    if messages:
        return messages, ingestion_type

    # ------------------------------------------
    # Date of Last Update checks
    # ------------------------------------------
    bl_dolu = safe_parse_date(r.get("PNLDDateOfLastUpdate"), "%Y-%m-%d")

    if bl_dolu:

        if last_update == bl_dolu:
            messages = add_message(
                messages=messages,
                file_id=xml_file_id,
                code='ER-NSDT-COMPARISON-001',
                msg_type='ERROR',
                issue='Offence update not permitted',
                cause=f'PNLD File has Date of Last Update "{last_update}" which is identical to the Date Of Last Update for the Offence with CJS Code "{cjs_code}" within CSDS',
                resolution='NSD to check highest revision of the offence on CSDS to verify if the same file has been run since the last download. If not, contact PNLD.'
            )

        if last_update < bl_dolu:
            messages = add_message(
                messages=messages,
                file_id=xml_file_id,
                code='ER-NSDT-COMPARISON-002',
                msg_type='ERROR',
                issue='Offence update not permitted',
                cause=f'PNLD File has Date of Last Update "{last_update}" which is before the Date Of Last Update "{bl_dolu}" for the Offence with CJS Code "{cjs_code}" within CSDS',
                resolution=('NSD to check highest revision of the offence on CSDS to verify if the same file has been run since the last download (where date of last update may have been tweaked by NSD). '
                            'If no recent PNLD update then NSD contact PNLD.')
            )

    if messages:
        return messages, ingestion_type

    # ------------------------------------------
    # Offence update checks (hash + end date)
    # ------------------------------------------
    bl_md5 = r.get("SysPNLDDataHash")
    bl_end_date = safe_parse_date(r.get("DateUsedTo"))

    same_hash = (bl_md5 == md5_hash)
    same_end_date = (bl_end_date == end_date)

    hashes_different = not same_hash
    dates_different = not same_end_date

    if same_hash and same_end_date:
        messages = add_message(
                messages=messages,
                file_id=xml_file_id,
                code='CO-NSDT-COMPARISON-003',
                msg_type='COMPLETION',
                issue='Offence successfully processed, but no updates required',
                cause=f'PNLD File has no changes when compared to the Offence with CJS Code "{cjs_code}" within CSDS',
                resolution=('NSD to check the PNLD offence wording report to identify what the changes were supposed to be in the file.  If the changes were not of interest to NSD/CSDS then no further action required. '
                            'If the update was something we would expect a new revision to be created for, further checks required in the data and NSD to contact PNLD.')
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


    # ------------------------------------------
    # Start/End Date structural checks
    # ------------------------------------------
    bl_start_date = safe_parse_date(r.get("DateUsedFrom"))

    if ingestion_type=="New" and start_date and start_date <= bl_start_date:
        ingestion_type = "TBC"
        messages = add_message(
                messages=messages,
                file_id=xml_file_id,
                code='ER-NSDT-COMPARISON-004',
                msg_type='ERROR',
                issue='Offence update not permitted',
                cause=f'Offence update has been declared as a New Revision and the Offence Start Date "{start_date}"  within PNLD File is less than or equal to the Offence Start Date "{bl_start_date}" within CSDS',
                resolution=('Review XML date range within PNLD File against date range within CSDS. '
                            'NSD to tweak the xml start date as required to enable the file to be re-processed on CSDS.')
            )

    return messages, ingestion_type
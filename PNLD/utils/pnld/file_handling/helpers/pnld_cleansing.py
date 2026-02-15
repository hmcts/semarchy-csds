import re
import math
import json

# ---------- Validation helpers ----------

def validate_regex(pattern, group_index, pattern_id="Pattern"):
    """
    Ensure the compiled regex has >= 1 capturing group and the given group_index exists.
    Raises ValueError / TypeError on invalid inputs.
    """
    total_groups = pattern.groups
    if total_groups < 1:
        raise ValueError(f"{pattern_id} - regex must contain at least one capturing group; found {total_groups}.")

    if isinstance(group_index, int):
        if group_index < 1 or group_index > total_groups:
            raise ValueError(
                f"{pattern_id} - group_index {group_index} is out of range; "
                f"pattern has {total_groups} capturing group(s) (valid indices: 1..{total_groups})."
            )
    elif isinstance(group_index, str):
        names = pattern.groupindex.keys()
        if group_index not in names:
            available = ", ".join(sorted(names)) or "<none>"
            raise ValueError(
                f"{pattern_id} - group name {group_index!r} not found. "
                f"Available names: {available}."
            )
    else:
        raise TypeError(
            f"{pattern_id} - group_index must be int (positional) or str (named); got {type(group_index).__name__}."
        )

# ---------- Replacement engine (per value) ----------


def cleanse_text(
    text,
    pattern,
    group_index,
    replacement,
    *,
    rule_id,
    column,
    row_idx=0,
    context_chars=10,
    xml_file_id=None,
    msg_code="CODE",
    msg_type='INFORMATION'
):
    """
    Perform replacements on 'text' using 'pattern', replacing ONLY the specified capturing group
    for each match. Returns (updated_text, messages, match_count).

    Message 'Context' is a subset of the ORIGINAL text: +/- `context_chars` characters
    centered on the targeted group's span being replaced (safe-bounded to [0, len(text)]).
    """
    # Treat None/NaN as non-string and skip
    if text is None or (isinstance(text, float) and math.isnan(text)):
        return text, [], 0

    # Freeze the original text for context slices so messages always reflect pre-cleanse content
    original_text = str(text)
    n = len(original_text)

    messages = []
    match_count = 0

    def _group_span(m, grp):
        try:
            return m.start(grp), m.end(grp)
        except IndexError:
            raise ValueError(f"{rule_id} - group_index {grp!r} is out of range; pattern has {pattern.groups} group(s).")
        except KeyError:
            raise ValueError(f"{rule_id} - group name {grp!r} not found in pattern.")

    def _group_value(m, grp):
        try:
            return m.group(grp)
        except (IndexError, KeyError):
            return None

    def compute_replacement_value(m):
        return str(replacement(m)) if callable(replacement) else str(replacement)

    def _repl(m):
        nonlocal match_count
        match_count += 1

        # Full match boundaries (in original text)
        start0, end0 = m.start(0), m.end(0)

        # Targeted group boundaries and values
        start_g, end_g = _group_span(m, group_index)
        original_group = _group_value(m, group_index)
        replacement_group = compute_replacement_value(m)

        # Context window based on ORIGINAL text, +/- context_chars
        # If the group didn't capture (None), fall back to full match boundaries.
        if original_group is None:
            ctx_start = max(0, start0 - context_chars)
            ctx_end   = min(n, end0 + context_chars)
            context_slice = original_text[ctx_start:ctx_end]
        else:
            ctx_start = max(0, start_g - context_chars)
            ctx_end   = min(n, end_g + context_chars)
            context_slice = original_text[ctx_start:ctx_end]

        # Reconstruct the matched segment by replacing only the targeted group
        prefix = original_text[start0:start_g]
        suffix = original_text[end_g:end0]
        new_full = prefix + replacement_group + suffix

        # Standardized message payload (Cleanse per match)
        messages.append({
            'FID_SourceFile': xml_file_id,
            'SourceFileMessageCode': msg_code,
            'SourceFileMessageType': msg_type,
            'SourceFileMessage': (
                f"{rule_id} | Attribute: {column} | "
                f'Cleanse: "{original_group}" -> "{replacement_group}" | '
                f'Context: "{context_slice}"'
                # If you want, you can uncomment these for full before/after within the match:
                # f' | BeforeMatch: "{original_text[start0:end0]}" | AfterMatch: "{new_full}"'
            )
        })

        return new_full

    # Apply substitution across the entire text (pattern.sub calls _repl for matches on original_text)
    updated_text = pattern.sub(_repl, original_text)

    return updated_text, messages, match_count

# ---------- Orchestrator: apply a list of rules to a RECORD DICT (with chaining) ----------

def cleanse_record(
    record,
    xml_file_id,
    *,
    config_location='config/cleanse_pnld.json',
    regex_flags=0,   # e.g., re.MULTILINE | re.IGNORECASE
    context_chars=10,
    msg_code="CL-001",
    msg_type='INFORMATION'
):
    """
    Applies each rule to each scoped key in the single-record dictionary with CHAINING behavior:
      - First rule to touch a key creates {key}_CLEANSED from the ORIGINAL value (as str).
      - Subsequent rules continue cleansing the EXISTING {key}_CLEANSED (do NOT reset).
    If the source key does not exist, no cleansed key is created and a 'Skipped' message is logged.

    Mutates and returns (record, messages).
    """
    if not isinstance(record, dict):
        raise TypeError("record must be a dict representing a single record, e.g., {'col1': 'value'}")
    
    # Load in Detection Config
    with open(config_location, "r", encoding="utf-8") as f:
        rules = json.load(f)

    messages = []

    # Sort by SortOrder so the chained effect respects rule order
    rules_sorted = sorted(rules, key=lambda r: r.get("SortOrder", 0))

    for rule in rules_sorted:
        rule_id     = rule.get("RuleID", "UnknownRule")
        pattern_str = rule["DetectionRegex"]
        group_index = rule["RegexReplacementGroupIndex"]
        replace_val = rule.get("ReplaceValue", "")
        scope_cols  = rule.get("Scope", [])

        # Compile regex with proper error handling
        try:
            pattern = re.compile(pattern_str, flags=regex_flags)
        except re.error as ex:
            messages.append({
                'FID_SourceFile': xml_file_id,
                'SourceFileMessageCode': msg_code,
                'SourceFileMessageType': msg_type,
                'SourceFileMessage': f"{rule_id} - Skipped: invalid RegexPattern ({ex})"
            })
            continue

        # Validate group availability
        try:
            validate_regex(pattern, group_index, pattern_id=rule_id)
        except (ValueError, TypeError) as ex:
            messages.append({
                'FID_SourceFile': xml_file_id,
                'SourceFileMessageCode': msg_code,
                'SourceFileMessageType': msg_type,
                'SourceFileMessage': f"{rule_id} - Skipped: invalid group configuration ({ex})"
            })
            continue

        # Apply to each scoped key with chaining
        for key in scope_cols:
            if key not in record:
                # messages.append({
                #     'FID_SourceFile': xml_file_id,
                #     'SourceFileMessageCode': msg_code,
                #     'SourceFileMessageType': 'Cleanse',
                #     'SourceFileMessage': f"{rule_id} - Skipped: column '{key}' not found in record"
                # })
                continue

            cleansed_key = f"{key}_CLEANSED"

            # First rule touching this key: initialize from ORIGINAL
            if cleansed_key not in record:
                # Represent as string for regex operations; preserve None if value is None/NaN
                orig_val = record.get(key)
                if orig_val is None or (isinstance(orig_val, float) and math.isnan(orig_val)):
                    record[cleansed_key] = orig_val
                else:
                    record[cleansed_key] = str(orig_val)

            # Subsequent rules operate on EXISTING cleansed value (NO reset)
            current_val = record.get(cleansed_key)
            # Skip if None/NaN
            if current_val is None or (isinstance(current_val, float) and math.isnan(current_val)):
                continue

            current_str = str(current_val)
            # Quick detection pass
            if not pattern.search(current_str):
                continue

            updated, match_messages, count = cleanse_text(
                text=current_str,
                pattern=pattern,
                group_index=group_index,
                replacement=replace_val,
                rule_id=rule_id,
                column=key,
                row_idx=0,  # single-record dict
                context_chars=context_chars,
                xml_file_id=xml_file_id,
                msg_code=msg_code
            )

            if count > 0:
                record[cleansed_key] = updated
                messages.extend(match_messages)

    return record, messages
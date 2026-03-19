import re
import math
import json
from pnld_process.utils.message_handling import add_message

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


import math
import re

def cleanse_text(
    xml_file_id,
    attribute_name,
    rule_id,
    human_readable_message,
    text,
    pattern,
    group_index,
    replacement,
    *,
    context_chars=10,
):
    """
    Perform replacements on 'text' using 'pattern', replacing ONLY the specified capturing group
    for each match. Returns (updated_text, messages, match_count).

    context_slice_before:
        A subset of the ORIGINAL text around the targeted capturing group's span,
        bounded to +/- `context_chars` (safe-bounded within [0, len(text)]).

    context_slice_after:
        A subset representing how the text will look AFTER the replacement around
        that same context window:
            left_context (from original) + replacement_group + right_context (from original)

    Notes:
    - If 'text' is None or NaN, return (text, [], 0).
    - If the targeted capturing group didn't match for a given full match, that match is left unchanged
      and no message is logged for that match.
    - The 'replacement' can be a string or a callable that accepts the match and returns a string.
    """

    # Treat None/NaN as non-string and skip
    if text is None or (isinstance(text, float) and math.isnan(text)):
        return text, [], 0

    # Normalize to str and prepare
    original_text = str(text)
    n = len(original_text)

    # Basic validation
    if not isinstance(context_chars, int) or context_chars < 0:
        raise ValueError("context_chars must be a non-negative integer.")

    messages = []
    match_count = 0

    def _group_span(m, grp):
        try:
            return m.start(grp), m.end(grp)
        except IndexError:
            raise ValueError(
                f"{rule_id} - group_index {grp!r} is out of range; pattern has {pattern.groups} group(s)."
            )
        except KeyError:
            raise ValueError(f"{rule_id} - group name {grp!r} not found in pattern.")

    def _group_value(m, grp):
        try:
            return m.group(grp)
        except (IndexError, KeyError):
            return None

    def _compute_replacement_value(m):
        return str(replacement(m)) if callable(replacement) else str(replacement)

    def _repl(m):
        nonlocal match_count, messages

        # Full match boundaries (in original text)
        start0, end0 = m.start(0), m.end(0)

        # Targeted group value first to decide whether to act
        original_group = _group_value(m, group_index)

        # If the target group didn't capture, skip (no replacement, no log)
        if original_group is None:
            return m.group(0)

        # If the group captured, proceed
        start_g, end_g = _group_span(m, group_index)
        replacement_group = _compute_replacement_value(m)

        match_count += 1

        # Compute BEFORE context around the targeted group's span
        before_start = max(0, start_g - context_chars)
        before_end   = min(n, end_g + context_chars)
        context_slice_before = original_text[before_start:before_end]

        # Build the 'after' local view: left_context + replacement_group + right_context
        left_context  = original_text[before_start:start_g]
        right_context = original_text[end_g:before_end]
        context_slice_after = f"{left_context}{replacement_group}{right_context}"

        # Reconstruct the matched segment by replacing only the targeted group
        prefix  = original_text[start0:start_g]
        suffix  = original_text[end_g:end0]
        new_full = prefix + replacement_group + suffix

        # Build cause message with both context snapshots
        cause_message = (
            f'{human_readable_message} within {attribute_name} - '
            f'"{context_slice_before}" to "{context_slice_after}"'
        )

        # Keep add_message signature EXACTLY as you use it
        messages = add_message(
            messages=messages,
            file_id=xml_file_id,
            code=f'CL-NSD-{attribute_name.upper()}-{rule_id}',
            msg_type='CLEANSE',
            issue='Offence Transformed with Cleanses',
            cause=cause_message,
            resolution='NSD to report back to PNLD post processing to ensure they correct their data at source.'
        )

        return new_full

    # Apply substitution across the entire text
    updated_text = pattern.sub(_repl, original_text)

    return updated_text, messages, match_count

# ---------- Orchestrator: apply a list of rules to a RECORD DICT (with chaining) ----------

def cleanse_record(
    record,
    xml_file_id,
    *,
    config_location='config/cleanse_pnld.json',
    regex_flags=0,   # e.g., re.MULTILINE | re.IGNORECASE
    context_chars=10
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
        rule_id         = rule["RuleID"]
        human_message   = rule["HumanReadableText"]
        pattern_str     = rule["DetectionRegex"]
        group_index     = rule["RegexReplacementGroupIndex"]
        replace_val     = rule.get("ReplaceValue", "")
        scope_cols      = rule.get("Scope", [])

        # Compile regex with proper error handling
        try:
            pattern = re.compile(pattern_str, flags=regex_flags)
        except re.error as ex:
            raise ValueError(f'Invalid Regex Pattern within Config for Rule {rule_id} - {ex}')

        # Validate group availability
        try:
            validate_regex(pattern, group_index, pattern_id=rule_id)
        except (ValueError, TypeError) as ex:
            raise ValueError(f'Invalid Regex Group Configuration for Rule {rule_id} - {ex}')

        # Apply to each scoped key with chaining
        for key in scope_cols:

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
                xml_file_id=xml_file_id,
                attribute_name=key,
                rule_id=rule_id,
                human_readable_message=human_message,
                text=current_str,
                pattern=pattern,
                group_index=group_index,
                replacement=replace_val)

            if count > 0:
                record[cleansed_key] = updated
                messages.extend(match_messages)

    return record, messages
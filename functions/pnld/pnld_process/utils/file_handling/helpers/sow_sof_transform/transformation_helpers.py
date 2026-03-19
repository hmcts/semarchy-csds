
import re
import hashlib


def detect_split_type(text):
    """
    Returns the 'Type' for the first matching rule in config.

    Parameters
    ----------
    text : str
        The input text to evaluate.

    Returns
    -------
    str or None
        The 'Type' of the first matched rule, or None if no match is found.
    """
    # --- Load detection configuration ---
    # Example:
    # [
    #   {"Type": "Alpha", "Regex": "^[A-Z]+$", "SortOrder": 1},
    #   {"Type": "Numeric", "Regex": "^[0-9]+$", "SortOrder": 2}
    # ]
    config = [
        {"Type": "Menu", "Regex": r"\([A-Za-z0-9]+\)\_\[", "SortOrder": 1},
        {"Type": "Terminal Entry", "Regex": r"\*\*\(\.\.SPECIFY [A-Za-z0-9\s]+\.\.\)", "SortOrder": 2},
        {"Type": "Text", "Regex": r".+?", "SortOrder": 3}
    ]

    # --- Sort rules so lower SortOrder (higher priority) runs first ---
    rules = sorted(config, key=lambda r: r.get("SortOrder", 0))

    # --- Compile regex patterns once for efficiency ---
    compiled = [
        {"Type": r["Type"], "Pattern": re.compile(r["Regex"], flags=re.DOTALL)}
        for r in rules
    ]

    # --- Evaluate the text against each rule in priority order ---
    for r in compiled:
        if r["Pattern"].search(text or ""):
            return r["Type"]

    # --- No rule matched ---
    return None


def extract_menu_options(text):
    """
    Extract blocks of the form (X)_[ ... ]_ and replace the whole run
    with {menu_md5}. Also return a list of cleaned option texts and the MD5 value.

    Example
    -------
    Input:
        "were&lt;br /&gt;(F)_[loaded ...]_&lt;br /&gt;(G)_[retained ...]_&lt;br /&gt;(H)_[brought ...]_"

    Output:
        updated_text = "were {<md5>}"
        entries = ["loaded ...", "retained ...", "brought ..."]
        menu_md5 = "<md5>"
    """
    # --- Pattern: (X)_[ ... ]_ ---
    block_re = re.compile(r"\(([A-Za-z0-9]+)\)_\[(.*?)\]_", flags=re.DOTALL)

    source = text or ""
    matches = list(block_re.finditer(source))

    # --- If no blocks, return unchanged text and empty metadata ---
    if not matches:
        return text, [], None, None

    # --- Build MD5 from the concatenated raw blocks ---
    menu_joined = "-".join(m.group(0) for m in matches)
    menu_md5 = hashlib.md5(menu_joined.encode("utf-8")).hexdigest()

    # --- Collect cleaned option texts ---
    entries = []
    for opt_num, opt_match in enumerate(matches, start=1):
        raw = opt_match.group(2)
        cleaned = re.sub(r"\s+", " ", (raw or "").strip())
        entries.append({
            "option_number": opt_num,
            "option_text": cleaned
        })

    # --- Replace the entire sequence (from first match to last match) with {menu_md5} ---
    start = matches[0].start()
    end = matches[-1].end()
    updated = source[:start] + f"{{{menu_md5}}}" + source[end:]

    # --- Remove <br> artifacts and compress whitespace ---
    updated = re.sub(r"&lt;br\s*/?&gt;|<br\s*/?>", " ", updated, flags=re.IGNORECASE)
    updated = re.sub(r"\s+", " ", updated).strip()

    # --- Menu entry metadata (as in original) ---
    menu_entry = {
        "prompt": "SPECIFY VALUE",
        "format": "MNU",
        "minimum": 1,
        "maximum": 1,
        "md5": menu_md5
    }

    return updated, entries, menu_md5, menu_entry


def replace_specify_placeholders(text, entry_counter):
    """
    Replace occurrences of **(..SPECIFY X..)** with {<md5>} and collect entry metadata.

    Behavior
    --------
    - Each match increments `entry_counter`.
    - For each placeholder:
        - Replacement token: {<md5_of_LABEL>}
        - entries item: {
              'md5': <md5>,
              'prompt': <LABEL_UPPER>,
              'format': 'MNU' if LABEL == 'SPECIFY DATE' else 'TXT',
              'minimum': 1,
              'maximum': 1 (MNU) or 250 (TXT)
          }
        - entry_audit item: {'md5': <md5>, 'entry_counter': <n>}

    Returns
    -------
    tuple
        (updated_text, entries, updated_entry_counter, entry_audit)
    """
    # --- Regex (kept EXACTLY as provided) ---
    # r"\*\*\(\.\.(SPECIFY\s+[A-Z]+(?: [A-Z]+)*)*\.\.\)"
    pattern = re.compile(
        r"\*\*\(\.\.(SPECIFY\s+[A-Z]+(?: [A-Z]+)*)*\.\.\)",
        flags=re.IGNORECASE
    )

    entries = []
    entry_audit = []

    def _repl(m):
        nonlocal entry_counter
        entry_counter += 1

        raw_label = m.group(1)
        label = (raw_label or "").strip().upper()

        md5_label = hashlib.md5(label.encode("utf-8")).hexdigest()

        if label == 'SPECIFY DATE':
            entries.append({
                "md5": md5_label,
                "prompt": label,
                "format": 'MNU',
                "minimum": 1,
                "maximum": 1,
                "sei": "OD"
            })
        else:
            entries.append({
                "md5": md5_label,
                "prompt": label,
                "format": 'TXT',
                "minimum": 1,
                "maximum": 250
            })

        entry_audit.append({
            "md5": md5_label,
            "entry_counter": entry_counter
        })

        return f"{{{md5_label}}}"

    updated_text = pattern.sub(_repl, text or "")
    return updated_text, entries, entry_counter, entry_audit


def transform_specify_menu_option(text):
    """
    Replace occurrences of **(..SPECIFY X..)** with numbered placeholders {1}, {2}, ...
    and collect element metadata for each placeholder.

    Returns
    -------
    tuple
        (updated_text, elements)
    """
    # --- Regex (kept EXACTLY as provided) ---
    # r"\*\*\(\.\.(SPECIFY\s+[A-Z]+(?: [A-Z]+)*)*\.\.\)"
    pattern = re.compile(
        r"\*\*\(\.\.(SPECIFY\s+[A-Z]+(?: [A-Z]+)*)*\.\.\)",
        flags=re.IGNORECASE
    )

    counter = 0
    elements = []

    def _repl(m):
        nonlocal counter
        counter += 1

        raw_label = m.group(1)
        label = (raw_label or "").strip().upper()

        elements.append({
            "element_number": counter,
            "prompt": label,
            "format": "TXT",
            "minimum": 1,
            "maximum": 250
        })

        return f"[{counter}]"

    updated_text = pattern.sub(_repl, text or "")
    return updated_text, elements


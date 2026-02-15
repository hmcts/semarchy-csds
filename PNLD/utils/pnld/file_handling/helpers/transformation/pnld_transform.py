
import re
import logging

# External helpers (assumed available from your project)
from utils.pnld.file_handling.helpers.transformation.transformation_helpers import detect_split_type
from utils.pnld.file_handling.helpers.transformation.transformation_helpers import replace_specify_placeholders
from utils.pnld.file_handling.helpers.transformation.transformation_helpers import extract_menu_options
from utils.pnld.file_handling.helpers.transformation.transformation_helpers import transform_specify_menu_option

def extract_terminal_entries(text, entry_counter):
    """
    Transform 'Statement of Facts' (SoF) / 'Standard Offence Wording' (SOW) content by:
      - Splitting text into logical chunks on two consecutive <br> breaks.
      - Detecting the type of each chunk (Text, Terminal Entry, Menu).
      - For 'Terminal Entry': replacing **(..SPECIFY X..)** with numbered placeholders {n}
        and collecting entries as [{'entry_number': n, 'prompt': LABEL}, ...].
      - For 'Menu': collapsing (X)_[ ... ]_ sequences to {entry_number}, collecting menu
        options and any terminal entries inside each option.

    Returns
    -------
    transformed_text : str
        Concatenated and normalized text after all transformations.
    all_terminal_entries : list[dict]
        Items like {'entry_number': n, 'prompt': 'LABEL'} (plus 'MENU' rows for menu prompts).
    all_menu_options : list[dict]
        Items like {'entry_number': n, 'option_number': k, 'option_text': '...'}.
    all_menu_elements : list[dict]
        Terminal entries extracted from within menu options, each augmented with 'entry_number'.
    """

    # Running counter for terminal-entry placeholders across the whole document
    # entry_number = 0
    menu_counter=0

    # Accumulators
    transformed_splits = []   # list of {'id': i, 'text': chunk_text}
    all_terminal_entries = [] # terminal entry prompts, including a 'MENU' prompt
    all_menu_options = []     # collected menu options
    all_menus = []
    entry_audit = []

    # --- Split the incoming text on two consecutive breaks ---
    # Supports both literal <br> and HTML-encoded &lt;br&gt; variants:
    #   - <br>, <br/>, <br />
    #   - &lt;br&gt;, &lt;br/&gt;, &lt;br /&gt;
    # We split on TWO consecutive breaks to avoid over-segmentation.
    split_re = re.compile(r"(?:<br\s*/?>|&lt;br\s*/?&gt;){2}", flags=re.IGNORECASE)
    splits = re.split(split_re, text or "")

    # --- Process each split chunk ---
    for i, split_text in enumerate(splits):
        # Determine the split type using your helper
        split_type = detect_split_type(split_text)

        # CASE 1: Plain text chunk – keep as-is
        if split_type == 'Text':
            transformed_splits.append({
                "id": i,
                "text": split_text
            })

        # CASE 2: Terminal Entry – replace **(..SPECIFY X..)** with {n} and collect entries
        elif split_type == 'Terminal Entry':
            # Replace placeholders and collect entries, carrying the updated counter forward
            split_text, terminal_entries, entry_counter, audit = replace_specify_placeholders(
                text=split_text,
                entry_counter=entry_counter
            )

            transformed_splits.append({
                "id": i,
                "text": split_text
            })
            all_terminal_entries.extend(terminal_entries)
            entry_audit.extend(audit)

        # CASE 3: Menu – collapse (X)_[ ... ]_ blocks and collect options/elements
        elif split_type == 'Menu':
            # Allocate a new entry number for this menu and collapse its option block run to {entry_number}
            entry_counter += 1
            menu_counter += 1

            split_text, raw_options, menu_md5, terminal_entry = extract_menu_options(
                text=split_text
            )

            all_menus.append({
                'raw_md5': menu_md5
            })

            transformed_splits.append({
                "id": i,
                "text": split_text
            })

            entry_audit.append({
                'md5': menu_md5,
                'entry_counter': entry_counter,
                'menu_counter': menu_counter
                })

            # Record a prompt indicating a MENU entry for this entry_number
            all_terminal_entries.append(terminal_entry)

            # For each extracted menu option, optionally extract terminal entries inside it
            menu_options = []
            for option in raw_options:
                option_number = option.get('option_number')
                option_text = option.get('option_text')

                menu_option = {}
                # If the option contains terminal-entry content, extract those placeholders
                if detect_split_type(option_text) == 'Terminal Entry':
                    option_text, option_elements = transform_specify_menu_option(
                        text=option_text
                    )

                    # Tag each extracted element with the parent menu's entry_number
                    for element in option_elements:
                        idx = str(element["element_number"]).zfill(2)  # zero-pad to 2 digits
                        menu_option[f'ElementDefinition{idx}.ElementNumber']=element['element_number']
                        menu_option[f'ElementDefinition{idx}.EntryFormat']=element['format']
                        menu_option[f'ElementDefinition{idx}.EntryPrompt']=element['prompt']
                        menu_option[f'ElementDefinition{idx}.OTEElementMax']=element['maximum']
                        menu_option[f'ElementDefinition{idx}.OTEElementMin']=element['minimum']
                
                menu_option['raw_menu_md5']=menu_md5
                menu_option['OptionNumber']=option_number
                menu_option['OptionText']=option_text

                # Record the menu option (text may be updated if terminal entries were found)
                menu_options.append(menu_option)

            all_menu_options.extend(menu_options)

        # Fallback: if type is None/unknown, keep the chunk as-is (safe default)
        else:
            transformed_splits.append({
                "id": i,
                "text": split_text
            })

    # --- Final assembly ---

    # --- Collate transformed text
    # Sort chunks by their original order (id) and concatenate their text with single spaces
    transformed_splits = sorted(transformed_splits, key=lambda x: x["id"])
    transformed_text = " ".join(item["text"] for item in transformed_splits)

    # Normalize whitespace across the final text (collapse runs to single spaces; trim ends)
    transformed_text = re.sub(r"\s+", " ", transformed_text).strip()

    # Remove 'On ' from start of text if present
    transformed_text = re.sub(r'^On\s*\{', '{', transformed_text)


    return transformed_text, all_terminal_entries, all_menus, entry_counter, entry_audit, all_menu_options
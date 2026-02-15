import hashlib


def keep_lowest_entry_per_md5(items):
    """
    From multiple audit items per md5, keep the one with the lowest entry_counter.
    This ensures deterministic replacement when multiple identical fragments occur.
    """
    best = {}
    for item in items:
        md5 = item["md5"]
        if md5 not in best or item["entry_counter"] < best[md5]["entry_counter"]:
            best[md5] = item
    return list(best.values())


def replace_md5_placeholders(text, entries):
    """
    Replace placeholders like {<md5>} with {<entry_counter>} so text references
    align with the final numeric indices shown to users.
    """
    lookup = {e["md5"]: e["entry_counter"] for e in entries}
    for md5, counter in lookup.items():
        text = text.replace(f"{{{md5}}}", f"{{{counter}}}")
    return text



def replace_md5_with_entry_counter(items, lookup_list):
    """
    Convert 'md5' → 'entry_number' on terminal entries.
    Also, if prompt exactly equals 'SPECIFY VALUE', append menu_counter to clarify
    which repeated prompt instance is being referenced (SPECIFY VALUE {n}).
    """
    lookup = {
        e["md5"]: {
            "entry_counter": e.get("entry_counter"),
            "menu_counter": e.get("menu_counter"),
        }
        for e in lookup_list
    }

    updated = []
    for item in items:
        md5 = item.get("md5")
        if md5 in lookup:
            new_item = item.copy()

            # Final index users see in UIs/reports
            new_item["entry_number"] = lookup[md5]["entry_counter"]

            # Remove md5 after mapping — downstream should not rely on hashes
            # new_item.pop("md5", None)

            # Disambiguate repeated prompts (optional enhancement)
            if new_item.get("prompt") == "SPECIFY VALUE":
                menu_counter = lookup[md5].get("menu_counter")
                if menu_counter is not None:
                    new_item["prompt"] = f"SPECIFY VALUE {menu_counter}"

            updated.append(new_item)
        else:
            # If audit missing for an item, leave it unchanged (defensive)
            updated.append(item)

    return updated



def process_menus(cjs_code, terminal_entries, menus, menu_options):

    """
    Processes menus and menu options by generating new MD5 hashes, assigning
    menu names from terminal entries, updating menu options, and ensuring that
    terminal entries include a 'menu_md5' attribute whenever they map to a menu.
    """

    # Build lookup: md5 → terminal entry
    terminal_lookup = {t["md5"]: t for t in terminal_entries}

    # Build mapping: old_raw_md5 → new_menu_md5
    md5_mapping = {}

    updated_menus = []
    for menu in menus:
        raw_md5 = menu["raw_md5"]

        # Find matching terminal entry
        term = terminal_lookup.get(raw_md5)
        if not term:
            raise ValueError(f"Missing terminal entry for raw_md5={raw_md5}")

        prompt = term["prompt"]

        # Build new MD5: concat(raw_md5, cjs_code, prompt)
        new_md5_source = raw_md5 + cjs_code + prompt
        new_menu_md5 = hashlib.md5(new_md5_source.encode("utf-8")).hexdigest()

        # Store for menu options
        md5_mapping[raw_md5] = new_menu_md5

        # Add menu_md5 to terminal entry
        term["menu_md5"] = new_menu_md5

        # Produce updated menu record
        updated_menu = dict(menu)
        updated_menu["menu_md5"] = new_menu_md5
        updated_menu["name"] = prompt

        updated_menus.append(updated_menu)

    # Update menu options
    updated_menu_options = []
    for opt in menu_options:
        raw_menu_md5 = opt.get("raw_menu_md5")

        if raw_menu_md5 not in md5_mapping:
            raise ValueError(f"Missing mapped md5 for menu option raw_menu_md5={raw_menu_md5}")

        new_opt = dict(opt)
        new_opt.pop("raw_menu_md5", None)
        new_opt["PNLDMenuHashMD5"] = md5_mapping[raw_menu_md5]

        updated_menu_options.append(new_opt)
    
    # Deduplicate menu options
    updated_menu_options = [
        dict(t) for t in {tuple(sorted(d.items())) for d in updated_menu_options}
    ]

    return updated_menus, updated_menu_options, terminal_entries
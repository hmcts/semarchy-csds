def map_edit_type(edit_type: str) -> str:
    """
    Maps the Semarchy version type to the corresponding edit type for PSS API requests.
    """
    mapping = {
        "Initial": "NEW",
        "Edit": "EDIT",
        "Edit Revision": "EDIT",
        "New Revision": "NEW REVISION"
    }

    if edit_type not in mapping:
        raise ValueError(f"Invalid edit_type '{edit_type}'. Expected one of: {list(mapping.keys())}")

    return mapping.get(edit_type)


def map_version_type(version_type: str) -> str:
    """
    Maps the Semarchy version type to the corresponding operation type for PSS API requests.
    """
    mapping = {
        "Initial": "C",     # Create
        "Edit": "U"         # Update
    }

    if version_type not in mapping:
        raise ValueError(f"Invalid version_type '{version_type}'. Expected one of: {list(mapping.keys())}")

    return mapping.get(version_type)


def map_y_n(yes_no_str: str) -> str:
    """
    Maps the Semarchy version type to the corresponding edit type for PSS API requests.
    """
    mapping = {
        "Yes": "Y",
        "No": "N"
    }

    if yes_no_str not in mapping:
        raise ValueError(f"Invalid yes_no_str '{yes_no_str}'. Expected one of: {list(mapping.keys())}")

    return mapping.get(yes_no_str)

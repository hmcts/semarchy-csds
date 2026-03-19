def generate_csds_payload(arr):
    result = {
        "OffenceRevision": [],
        "OffenceTerminalEntries": []
    }

    for item in arr:
        if "OffenceRevision" in item:
            result["OffenceRevision"].extend(item["OffenceRevision"])

        if "OffenceTerminalEntries" in item:
            result["OffenceTerminalEntries"].extend(item["OffenceTerminalEntries"])

    return result
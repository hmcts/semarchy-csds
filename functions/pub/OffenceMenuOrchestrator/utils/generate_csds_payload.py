def generate_csds_payload(arr):
    result = {
        "OffenceMenu": [],
        "OffenceMenuOptions": [],
        "OffenceMenuElementDefinition": [],
        "ReleasePackageContent": []
    }

    for item in arr:
        if "OffenceMenu" in item:
            result["OffenceMenu"].extend(item["OffenceMenu"])

        if "OffenceMenuOptions" in item:
            result["OffenceMenuOptions"].extend(item["OffenceMenuOptions"])

        if "OffenceMenuElementDefinition" in item:
            result["OffenceMenuElementDefinition"].extend(item["OffenceMenuElementDefinition"])

        if "ReleasePackageContent" in item:
            result["ReleasePackageContent"].extend(item["ReleasePackageContent"])

    return result   # wrap in a list, as your output expects
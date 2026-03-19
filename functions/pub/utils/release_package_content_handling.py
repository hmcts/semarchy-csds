from utils.fail_handling import fail

def get_release_package_content_id(log_prefix, item_results, item_rp_contents, item_type, item_pk_name):

    results = []

    for item in item_results:
        status = item.get("ActivityStatus")
        
        # Copy original
        new_item = dict(item)

        if status == "FAILED":
            error_message = item.get("ActivityErrorMessage")

            # Get the 1st item (structure always has one)
            item_pk = item[item_type][0][item_pk_name]

            # Lookup in rp_content by ReleasePackageContentKey
            rp_content_match = next(
                (rpc for rpc in item_rp_contents if rpc["ReleasePackageContentKey"] == item_pk),
                None
            )

            if rp_content_match:
                new_item["ReleasePackageContent"] = [
                    {
                        "ReleasePackageContentID": rp_content_match["ReleasePackageContentID"],
                        "ReleasePackageContentErrorMessage": error_message
                    }
                ]
            else:
                fail(f"{log_prefix} Failed to retrieve Release Package Content Id for {item_type} Id {item_pk}")

        results.append(new_item)
    
    return results
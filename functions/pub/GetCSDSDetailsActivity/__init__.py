import os
import requests


def main(get_config):
    """
    Fetch records from the appropriate CSDS named query.
    Expected keys in get_config:
        - "ReleasePackageID"
        - "Type"
    """

    # Validate inputs
    rp_id = get_config.get("ReleasePackageID")
    get_type = get_config.get("Type")

    if not rp_id:
        raise ValueError("GET CSDS DETAILS | Missing required value: ReleasePackageID")

    if not get_type:
        raise ValueError("GET CSDS DETAILS | Missing required value: Type")

    # Mapping of allowed types
    query_paths = {
        "ReleasePackage": "named-query/CSDS/GetReleasePackageDetailsPSS/GD",
        "OffenceMenu": "named-query/CSDS/GetOffenceMenuDetailsPSS/GD",
        "ReleasePackageContents": "named-query/CSDS/GetReleasePackageContentsPSS/GD",
        "Offence": "named-query/CSDS/GetOffenceDetailsPSS/GD"
    }

    named_query_url = query_paths.get(get_type)
    if not named_query_url:
        raise ValueError(f"GET CSDS DETAILS | Unknown Type '{get_type}'. Valid: {list(query_paths.keys())}")

    # Environment variables
    base_url = os.getenv("SemarchyBaseURL")
    api_key = os.getenv("SemarchyAPIKey")

    if not base_url or not api_key:
        raise ValueError(f"GET CSDS DETAILS | Missing required environment variables (SemarchyBaseURL, SemarchyAPIKey)")
    
    url = f"{base_url.rstrip('/')}/{named_query_url}"
    headers = {"API-Key": api_key}

    try:
        response = requests.get(url, headers=headers, params={"RP_ID": rp_id}, timeout=10)
        response.raise_for_status()
        return response.json().get("records", [])
    except Exception as e:
        raise ValueError(f"GET CSDS DETAILS | Failed to retrieve data from CSDS: {e}") from e
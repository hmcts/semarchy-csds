from utils.pnld.release_package_handling.helpers.get_release_package import get_release_package
from utils.pnld.release_package_handling.helpers.create_release_package import create_release_package

def get_release_package_id():
    """
    Retrieves an open PNLD Release Package ID.
    If none exists, attempts to create one and retrieve it again.
    Raises ValueError if no Release Package can be obtained.
    """

    try:
        # First attempt to get an existing Release Package
        rp_id = get_release_package()
        if rp_id:
            return rp_id

        # None exists, so create one
        create_release_package()

        # Second attempt after creation
        rp_id = get_release_package()
        if rp_id:
            return rp_id

        # Still nothing – this is a real error
        raise ValueError("No open PNLD Release Package found after creation attempt.")

    except Exception as e:
        raise ValueError(f"Release Package retrieval failed: {e}") from e
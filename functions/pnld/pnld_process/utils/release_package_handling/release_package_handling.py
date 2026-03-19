from pnld_process.utils.release_package_handling.helpers.get_release_package import get_release_package
from pnld_process.utils.release_package_handling.helpers.create_release_package import create_release_package
from pnld_process.utils.message_handling import add_message


def get_release_package_id(messages):
    """
    Retrieves an open PNLD Release Package ID.
    If none exists, attempts to create one and then retrieve it again.
    Logs detailed error messages into `messages` on any failure.
    Returns:
        (rp_id, messages)
    """

    # ------------------------------------------------------------------
    # 1 - First attempt to retrieve an existing Release Package
    # ------------------------------------------------------------------
    try:
        rp_id, rp_status = get_release_package()
    except Exception as e:
        messages = add_message(
            messages=messages,
            file_id='TBC',
            code='ER-SUPP-RELEASEPACKAGE-001',
            msg_type='ERROR',
            issue='Failed to retrieve Open PNLD Release Package',
            cause=str(e),
            resolution='CONTACT SUPPORT TEAM'
        )
        return None, messages

    # ------------------------------------------------------------------
    # 2 - If none exists, try to create one, then retrieve again
    # ------------------------------------------------------------------
    if rp_id is None and rp_status is None:

        # Attempt creation
        try:
            create_release_package()
        except Exception as e:
            messages = add_message(
                messages=messages,
                file_id='TBC',
                code='ER-SUPP-RELEASEPACKAGE-002',
                msg_type='ERROR',
                issue='Failed to create Open PNLD Release Package',
                cause=str(e),
                resolution='CONTACT SUPPORT TEAM'
            )
            return None, messages

        # Attempt retrieval again
        try:
            rp_id, rp_status = get_release_package()
        except Exception as e:
            messages = add_message(
                messages=messages,
                file_id='TBC',
                code='ER-SUPP-RELEASEPACKAGE-001',
                msg_type='ERROR',
                issue='Failed to retrieve Open PNLD Release Package',
                cause=str(e),
                resolution='CONTACT SUPPORT TEAM'
            )
            return None, messages

    # ------------------------------------------------------------------
    # 3 - Validate the Release Package status
    # ------------------------------------------------------------------
    if rp_status == 'Open':
        return rp_id, messages

    # If status exists but is not Open → invalid
    messages = add_message(
        messages=messages,
        file_id='TBC',
        code='ER-NSDT-RELEASEPACKAGE-001',
        msg_type='ERROR',
        issue='Failed to retrieve Open PNLD Release Package',
        cause=f'An Unpublished PNLD Release Package exists but has a Status of "{rp_status}".',
        resolution='NSDT to publish unpublished PNLD Release Package or set it back to Open'
    )

    return rp_id, messages
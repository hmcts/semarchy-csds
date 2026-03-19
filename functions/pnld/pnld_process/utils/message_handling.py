def add_message(messages, file_id, code, msg_type, issue, cause, resolution):
    """
    Add a structured message entry to the message list.

    Args:
        messages (list): List to append the message dictionary into.
        file_id (int/str): ID of the source file.
        code (str): Message code identifier.
        msg_type (str): Message type/category.
        issue (str): Description of the issue.
        cause (str): The cause of the issue.
        resolution (str): Suggested resolution.

    Returns:
        list: Updated list with the new message included.
    """

    messages.append({
        "MessageCode": code,
        "MessageType": msg_type,
        "MessageIssue": issue,
        "MessageCause": cause,
        "MessageResolution": resolution,
        "FID_SourceFile": file_id
    })

    return messages
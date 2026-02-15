from utils.pnld.offence_handling.helpers.post_offence import post_offences
from utils.pnld.offence_handling.helpers.get_offence import get_offences
from utils.pnld.offence_handling.helpers.update_files import update_files

import logging

def offence_handling(source_files, messages, offences):
    """
    Handles POSTing offences to Semarchy, retrieving processed offences,
    and updating source file/message structures.

    Logging format is consistent with PNLD convention:
      OFFENCE HANDLING | <step> - <status> (details)
    """

    load_status = None
    error_message = None
    batch_id = None

    logging.info(
        f"OFFENCE HANDLING | START "
        f"(total_offences={len(offences)})"
    )

    # -------------------------------------------------------
    # STEP 1 — POST offences to Semarchy
    # -------------------------------------------------------
    logging.info("OFFENCE HANDLING | POST | START")

    try:
        load_status, batch_id = post_offences(offences)

        logging.info(
            f"OFFENCE HANDLING | POST | SUCCESS "
            f"(batch_id={batch_id}, load_status={load_status})"
        )

    except Exception as e:
        error_message = str(e)
        logging.error(
            f"OFFENCE HANDLING | POST | FAILED (error={error_message})"
        )

    # -------------------------------------------------------
    # STEP 2 — Process GET if POST succeeded
    # -------------------------------------------------------
    if load_status:

        # Acceptable terminal outcomes
        if load_status in ['DONE', 'WARNING']:
            logging.info(
                f"OFFENCE HANDLING | GET | START "
                f"(batch_id={batch_id}, load_status={load_status})"
            )

            try:
                loaded_offences = get_offences(batch_id)

                logging.info(
                    f"OFFENCE HANDLING | GET | SUCCESS "
                    f"(batch_id={batch_id}, returned={len(loaded_offences)})"
                )

            except Exception as e:
                error_message = (
                    f"GET failed for batch_id={batch_id}. Error: {e}"
                )
                logging.error(
                    f"OFFENCE HANDLING | GET | FAILED ({error_message})"
                )
                loaded_offences = []

            # Update files/messages
            source_files, messages = update_files(
                source_files, messages, loaded_offences, offences
            )

            logging.info("OFFENCE HANDLING | UPDATE FILES | COMPLETE")
            logging.info("OFFENCE HANDLING | COMPLETE")
            return source_files, messages

        # Unexpected terminal state
        else:
            logging.error(
                f"OFFENCE HANDLING | POST | UNEXPECTED STATUS "
                f"(batch_id={batch_id}, status={load_status})"
            )

            loaded_offences = []
            source_files, messages = update_files(
                source_files,
                messages,
                loaded_offences,
                offences,
                f"Unexpected load status '{load_status}' for Batch ID {batch_id}"
            )

            logging.info("OFFENCE HANDLING | COMPLETE (Unexpected Status Path)")
            return source_files, messages

    # -------------------------------------------------------
    # STEP 3 — POST failure path
    # -------------------------------------------------------
    if error_message:
        logging.error(
            f"OFFENCE HANDLING | ABORTED (POST FAILED) | {error_message}"
        )

        loaded_offences = []
        source_files, messages = update_files(
            source_files, messages, loaded_offences, offences, error_message
        )

        logging.info("OFFENCE HANDLING | COMPLETE (POST Failure Path)")
        return source_files, messages
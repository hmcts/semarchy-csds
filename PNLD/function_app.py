import azure.functions as func
import logging
import os
import requests
import asyncio
logging.basicConfig(level=logging.DEBUG)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


from utils.zip_decompression.helpers.zip_define_post_body import zip_define_post_body
from utils.zip_decompression.zip_decompression_batch_control import process_zip_batch

@app.function_name(name="zip_extract")
@app.route(route='zip_extract')

async def zip_extract(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Zip Extract has been activated.')

    try:

        # 1) ROCESS ZIP
        # Parse JSON body
        request_body = req.get_json()
        if not isinstance(request_body, dict):
            logging.error("Invalid JSON body: expected an object/dict.")
            return func.HttpResponse(
                "Invalid JSON body: expected a JSON object.",
                status_code=400
            )

        # Validate 'records' field
        input_records = request_body.get("records", [])
        if not isinstance(input_records, list):
            logging.error("Invalid 'records' field: expected a list.")
            return func.HttpResponse(
                "Invalid 'records' field: expected an array.",
                status_code=400
            )

        logging.info(f"Request parsing successful. Records count: {len(input_records)}")

        # Process each ZIP asynchronously
        processed_records = await process_zip_batch(input_records, max_concurrency=8)

        # 2) PREPARE & SEND Semarchy POST
        # Prepare Semarchy POST request
        post_url = os.getenv("SemarchyLoadURL")
        api_key = os.getenv("SemarchyAPIKey")
        if not post_url or not api_key:
            logging.error("Missing Semarchy configuration in environment variables.")
            return func.HttpResponse(
                "Server configuration error: Missing Semarchy URL or API key.",
                status_code=500
            )

        post_headers = {"API-Key": api_key}
        post_body = zip_define_post_body(processed_records)

        logging.info("Semarchy POST request generated.")
        logging.debug(f"POST Body: {post_body}")

        # Send POST request
        post_response = requests.post(
            url=post_url,
            json=post_body, 
            headers=post_headers, 
            timeout=(5, 60)
            )
        logging.info("POST request sent.")

        if post_response.status_code == 200:
            logging.info(f"Semarchy POST Response: {post_response.status_code}")
            return func.HttpResponse(
                f"ZIP Extract executed successfully. Data uploaded to Semarchy - {post_response.status_code}",
                status_code=200
            )
        else:
            logging.error(f"Semarchy POST failed: {post_response.status_code} - {post_response.text}")
            return func.HttpResponse(
                f"ZIP Extract executed successfully, but upload failed - {post_response.status_code} - {post_response.text}",
                status_code=500
            )

    # CAPTURE EXCEPTIONS 
    except requests.exceptions.HTTPError as http_err:
        # HTTP error with status and body
        status_code = getattr(http_err.response, "status_code", 500)
        body = getattr(http_err.response, "text", "")
        logging.error(f"Semarchy POST - HTTPError: {status_code} - {body}", status_code, body)
        return func.HttpResponse(
            f"ZIP Decompression Executed. Upload failed - {status_code} - {body}",
            status_code=500,
        )

    except requests.exceptions.RequestException as req_err:
        logging.error(f"Semarchy POST - RequestException: {req_err}", )
        return func.HttpResponse(
            f"ZIP Decompression Executed. Upload failed - network/client error: {req_err}",
            status_code=500,
        )

    except Exception as e:
        logging.exception("PNLD Process Failed")  # includes stack trace
        return func.HttpResponse(
            f"ZIP Decompression Failed - {e}",
            status_code=500,
        )

       


#################################################################################################
#################################################################################################
#################################################################################################
#################################################################################################
#################################################################################################
#################################################################################################

from utils.pnld.file_handling.helpers.xsd_handling import get_xsd
from utils.pnld.define_post_body import pnld_define_post_body, collate_records, extract_items
from utils.pnld.pnld_batch_control import process_pnld_batch, process_pnld_batch_no_rp
from utils.pnld.menu_handling.menu_handling import menu_handling
from utils.pnld.release_package_handling.release_package_handling import get_release_package_id
from utils.pnld.file_handling.duplicate_cjs import detect_duplicate_cjs
from utils.pnld.offence_handling.offence_handling import offence_handling

import logging
import os
import requests


@app.function_name(name="pnld_process")
@app.route(route="pnld_process")
async def pnld_process(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("PNLD PROCESS | START")

    try:
        # -------------------------------------------------------------
        # 1. Parse Incoming Request
        # -------------------------------------------------------------
        logging.info("REQUEST HANDLING | START")

        request_body = req.get_json()

        if not isinstance(request_body, dict):
            logging.error("REQUEST HANDLING | FAILURE (body_is_not_json_object)")
            return func.HttpResponse("Invalid JSON body: expected a JSON object.", status_code=400)

        input_records = request_body.get("records", [])
        if not isinstance(input_records, list):
            logging.error("REQUEST HANDLING | FAILURE ('records'_not_array)")
            return func.HttpResponse("Invalid 'records' field: expected an array.", status_code=400)

        logging.info(
            f"REQUEST HANDLING | SUCCESS (records_received={len(input_records)})"
        )

        # -------------------------------------------------------------
        # 2. Release Package Retrieval
        # -------------------------------------------------------------
        logging.info("RELEASE PACKAGE HANDLING | RETRIEVE | START")

        rp_id = None
        rp_error_message = None

        try:
            rp_id = get_release_package_id()
            logging.info(
                f"RELEASE PACKAGE HANDLING | RETRIEVE | SUCCESS (rp_id={rp_id})"
            )
        except Exception as e:
            rp_error_message = str(e)
            logging.warning(
                f"RELEASE PACKAGE HANDLING | RETRIEVE | FAILED (error={rp_error_message})"
            )

        # -------------------------------------------------------------
        # 3. File Handling (with or without Release Package)
        # -------------------------------------------------------------
        source_files = []
        source_file_messages = []
        offences = []

        if rp_id:
            logging.info("FILE HANDLING | START (with_release_package)")

            menus = []
            menu_options = []

            # Duplicate detection
            logging.info("DUPLICATE MANAGEMENT | START")
            duplicate_records, non_duplicate_records = detect_duplicate_cjs(input_records)
            logging.info(
                f"DUPLICATE MANAGEMENT | COMPLETE "
                f"(duplicates={len(duplicate_records)}, non_duplicates={len(non_duplicate_records)})"
            )

            if non_duplicate_records:
                # XSD retrieval
                logging.info("FILE HANDLING | XSD Retrieval - START")
                xsd_encoded = get_xsd()
                logging.info("FILE HANDLING | XSD Retrieval - SUCCESS")

                # Process XML files
                logging.info("FILE HANDLING | Batch Processing - START")
                processed_records = await process_pnld_batch(
                    non_duplicate_records,
                    xsd_encoded,
                    rp_id,
                    max_concurrency=8,
                )
                logging.info("FILE HANDLING | Batch Processing - COMPLETE")

                # Extract structured components
                source_files.extend(extract_items(processed_records, 'SourceFile'))
                source_file_messages.extend(extract_items(processed_records, 'SourceFileMessage'))
                offences.extend(extract_items(processed_records, 'OffenceRevision'))
                menus.extend(extract_items(processed_records, 'Menu'))
                menu_options.extend(extract_items(processed_records, 'MenuOptions'))

                logging.info("FILE HANDLING | COMPLETE (with_release_package)")

                # ---------------------------------------------------------
                # MENU HANDLING
                # ---------------------------------------------------------
                logging.info("MENU HANDLING | START")
                source_files, source_file_messages, offences = menu_handling(
                    menus, menu_options, offences, source_files, source_file_messages, rp_id
                )
                logging.info("MENU HANDLING | COMPLETE")

                # ---------------------------------------------------------
                # OFFENCE HANDLING
                # ---------------------------------------------------------
                logging.info("OFFENCE HANDLING | START")
                if offences:
                    source_files, source_file_messages = offence_handling(
                        source_files, source_file_messages, offences
                    )
                else:
                    logging.info("OFFENCE HANDLING | No Offences")
                logging.info("OFFENCE HANDLING | COMPLETE")

            # Process duplicates
            if duplicate_records:
                logging.info("FILE HANDLING | Duplicate Records - START")
                source_files.extend(extract_items(duplicate_records, 'SourceFile'))
                source_file_messages.extend(extract_items(duplicate_records, 'SourceFileMessage'))
                logging.info("FILE HANDLING | Duplicate Records - COMPLETE")

        else:
            # No RP available - run No‑RP workflow
            logging.info("FILE HANDLING | START (no_release_package)")

            processed_records = await process_pnld_batch_no_rp(
                input_records,
                rp_error_message
            )

            source_files.extend(extract_items(processed_records, 'SourceFile'))
            source_file_messages.extend(extract_items(processed_records, 'SourceFileMessage'))

            logging.info("FILE HANDLING | COMPLETE (no_release_package)")

        # -------------------------------------------------------------
        # 4. Submit Semarchy POST
        # -------------------------------------------------------------
        logging.info("SEMARCHY POST | PREPARE")

        post_url = os.getenv("SemarchyLoadURL")
        api_key = os.getenv("SemarchyAPIKey")

        if not post_url or not api_key:
            logging.error("SEMARCHY POST | FAILURE (missing_url_or_api_key)")
            return func.HttpResponse(
                "Server configuration error: Missing Semarchy URL or API key.",
                status_code=500
            )

        post_headers = {"API-Key": api_key}
        post_body = pnld_define_post_body(source_files, source_file_messages)

        logging.info(f"SEMARCHY POST | SEND (url={post_url})")
        with requests.Session() as session:
            response = session.post(
                url=post_url,
                json=post_body,
                headers=post_headers,
                timeout=(5, 60),
            )
            response.raise_for_status()

        logging.info(f"SEMARCHY POST | SUCCESS (status={response.status_code})")

        # -------------------------------------------------------------
        # END — Successful Process
        # -------------------------------------------------------------
        logging.info("PNLD PROCESS | COMPLETE (SUCCESS)")

        return func.HttpResponse(
            f"PNLD Process Executed Successfully. Semarchy responded with {response.status_code}.",
            status_code=200,
        )

    # -------------------------------------------------------------
    # Exception Handling
    # -------------------------------------------------------------
    except requests.exceptions.HTTPError as http_err:
        status = getattr(http_err.response, "status_code", 500)
        body = getattr(http_err.response, "text", "")
        logging.error(
            f"SEMARCHY POST | HTTP ERROR (status={status}, body={body})"
        )

        return func.HttpResponse(
            f"PNLD Process executed but upload failed — HTTP {status}: {body}",
            status_code=500,
        )

    except requests.exceptions.RequestException as req_err:
        logging.error(f"SEMARCHY POST | NETWORK FAILURE ({req_err})")

        return func.HttpResponse(
            f"PNLD Process executed but network error occurred: {req_err}",
            status_code=500,
        )

    except Exception as e:
        logging.exception(f"PNLD PROCESS | UNHANDLED FAILURE ({e})")
        return func.HttpResponse(
            f"PNLD Process failed: {e}",
            status_code=500,
        )
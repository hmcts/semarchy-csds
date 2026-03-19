import azure.functions as func
import logging
import os
import requests
import asyncio
logging.basicConfig(level=logging.DEBUG)

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

from zip_extract.utils.helpers.zip_define_post_body import zip_define_post_body
from zip_extract.utils.zip_decompression_batch_control import process_zip_batch


async def main(req: func.HttpRequest) -> func.HttpResponse:
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
        post_url = f'{os.getenv("SemarchyBaseURL")}/loads/CSDS'
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
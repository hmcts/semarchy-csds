import json
import azure.functions as func
from azure.durable_functions import DurableOrchestrationClient
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = DurableOrchestrationClient(starter)

    orchestrator_name = req.route_params.get("orchestratorName")

    try:
        payload = req.get_json()
        logging.info(payload)
    except:
        payload = None

    if isinstance(payload, list):
        rp_records = payload
    elif isinstance(payload, dict) and "records" in payload:
        rp_records = payload["records"]
    elif isinstance(payload, dict) and "ReleasePackageIDs" in payload:
        rp_records = payload["ReleasePackageIDs"]
    else:
        raise ValueError("Unsupported payload format")
    logging.info(rp_records)


    started_instances = []

    for rp_record in rp_records:
        instance_id = await client.start_new(
            orchestration_function_name=orchestrator_name,
            client_input=rp_record
        )
        started_instances.append(instance_id)

    return func.HttpResponse(
        json.dumps({"started": started_instances}),
        mimetype="application/json",
        status_code=202
    )
import logging
from collections import Counter

def detect_duplicate_cjs(records):
    """
    Detects duplicate CJS codes and returns:
      - duplicate_records: records that must be marked Failed
      - non_duplicates:    records that proceed to normal processing

    Logging follows PNLD standard:
        DUPLICATE MANAGEMENT | <step> - <status> (details)
    """

    logging.info(
        f"DUPLICATE MANAGEMENT | START (total_records={len(records)})"
    )

    # Count occurrences of each CJSCode
    counts = Counter(item["CJSCode"] for item in records)

    # Categorise records
    duplicates = [item for item in records if counts[item["CJSCode"]] > 1]
    non_duplicates = [item for item in records if counts[item["CJSCode"]] == 1]

    logging.info(
        f"DUPLICATE MANAGEMENT | Detection - COMPLETE "
        f"(duplicates={len(duplicates)}, non_duplicates={len(non_duplicates)})"
    )

    duplicate_records = []

    # Build duplicate error records
    for item in duplicates:
        xml_file_id = item["SourceFileID"]
        cjs_code = item["CJSCode"]

        logging.warning(
            f"DUPLICATE MANAGEMENT | Duplicate Found "
            f"(file_id={xml_file_id}, cjs_code={cjs_code})"
        )

        source_file = {
            "SourceFileID": xml_file_id,
            "FID_SourceStatus": "Failed",
            "MessageCount": 1,
        }

        source_file_message = {
            "FID_SourceFile": xml_file_id,
            "SourceFileMessageCode": "DUP-CJS-001",
            "SourceFileMessageType": "ERROR",
            "SourceFileMessage": f"Duplicate CJS Code detected: {cjs_code}",
        }

        duplicate_output = {
            "SourceFile": [source_file],
            "SourceFileMessage": [source_file_message],
        }

        duplicate_records.append(duplicate_output)

    logging.info(
        f"DUPLICATE MANAGEMENT | COMPLETE "
        f"(duplicate_records_built={len(duplicate_records)})"
    )

    return duplicate_records, non_duplicates
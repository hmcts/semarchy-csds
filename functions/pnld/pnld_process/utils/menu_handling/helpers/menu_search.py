import os
import logging
import requests
import asyncio

def menu_id_lookup(menu, progress_tag):
        
    md5_hash = menu.get("SysPNLDDataHash")
    
    api_key = os.getenv("SemarchyAPIKey")
    base_url = f'{os.getenv("SemarchyBaseURL")}/named-query/CSDS/GetMenusPNLD/GD'
    headers = {"API-Key": api_key}

    url = f"{base_url}?MD5_HASH={md5_hash}"

    try:
        response = requests.get(url=url, headers=headers)
        response.raise_for_status()
        body = response.json()
        records = body.get("records", [])
    except Exception as e:
        logging.error(
            f"MENU HANDLING | menu_id_lookup | {progress_tag} | MD5 {md5_hash} - FAILED "
            f"(error={e})"
        )
        records = []
    
    return records

async def batch_menu_lookup(menus, max_concurrency=8):

    semaphore = asyncio.Semaphore(max_concurrency)
    queue = asyncio.Queue()
    
    async def _worker(idx, menu):
        async with semaphore:
            try:
                logging.info(f"[{idx}/{len(menus)}] - Menu Lookup - START")
                menu_id = await asyncio.to_thread(menu_id_lookup, menu, f'[{idx}/{len(menus)}]')
                logging.info(f"[{idx}/{len(menus)}] - Menu Lookup - COMPLETE")
                await queue.put(menu_id)
            except Exception as e:
                logging.exception(f"Error Menu Lookup [{idx}]: {e}")

    # Create tasks
    tasks = [asyncio.create_task(_worker(idx, menu))
             for idx, menu in enumerate(menus, start=1)]

    # Wait for all workers to finish
    await asyncio.gather(*tasks)

    # Collect results from queue
    menu_ids = []
    while not queue.empty():
        menu_ids.append(await queue.get())

    menu_ids = [item for sub in menu_ids for item in sub]
    
    return menu_ids


async def create_menu_id_mapping(menus):

    total_requested = len(menus)
    logging.info(
        f"MENU HANDLING | menu_id_lookup | START (total_requested={total_requested})"
    )

    logging.info(menus)
    menu_id_records = await batch_menu_lookup(menus)
    logging.info(menu_id_records)
    # Extract mapping for this batch
    md5_to_id = {
        item.get("SysPNLDDataHash"): item.get("OffenceMenuID")
        for item in menu_id_records
        if item.get("SysPNLDDataHash") is not None and item.get("OffenceMenuID") is not None
    }

    total_found = len(md5_to_id)

    logging.info(
        f"MENU HANDLING | menu_id_lookup | DONE "
        f"(total_searched={total_requested}, total_returned={total_found})"
    )

    return md5_to_id



def extract_unique_menu_and_options(menus, menu_options):
    """
    Build:
      1) A unique, order-preserving list of menus:
            [{ "PNLDHashMD5": <md5>, "Name": <menu_name> }]
      2) A de-duplicated list of menu options.

    Logging follows PNLD standard:
        MENU HANDLING | extract_unique_menu_and_options | <Step> - <Status>
    """

    logging.info(
        f"MENU HANDLING | extract_unique_menu_and_options | START "
        f"(menus_in={len(menus)}, menu_options_in={len(menu_options)})"
    )

    seen = set()
    menus_output = []

    # Extract unique menus in original order
    for m in menus:
        md5 = m.get("menu_md5")
        name = m.get("name")

        if md5 is None:
            continue

        if md5 not in seen:
            seen.add(md5)
            menus_output.append({
                "SysPNLDDataHash": md5,
                "Name": name
            })

    logging.info(
        f"MENU HANDLING | extract_unique_menu_and_options | Menu Deduplication - COMPLETE "
        f"(unique_menus={len(menus_output)})"
    )

    # Deduplicate menu options by full dictionary content
    deduped_options = [
        dict(t)
        for t in {tuple(sorted(d.items())) for d in menu_options}
    ]

    logging.info(
        f"MENU HANDLING | extract_unique_menu_and_options | Menu Options Deduplication - COMPLETE "
        f"(unique_options={len(deduped_options)})"
    )

    logging.info("MENU HANDLING | extract_unique_menu_and_options | DONE")

    return menus_output, deduped_options
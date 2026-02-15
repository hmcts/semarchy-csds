import logging
import asyncio

from utils.pnld.file_handling.pnld_file_handling import pnld_file_handling 


async def process_pnld_batch(input_records, xsd_encoded, rp_id, max_concurrency=8):
    """
    Process each XML record concurrently using asyncio.
    Calls process_pnld(xml_file, xsd) in a thread pool for non-blocking execution.
    """
    semaphore = asyncio.Semaphore(max_concurrency)
    queue = asyncio.Queue()
    
    async def _worker(idx, xml_file):
        async with semaphore:
            try:
                logging.info(f"FILE HANDLING | [{idx}/{len(input_records)}] - START")
                processed_record = await asyncio.to_thread(pnld_file_handling, xml_file, xsd_encoded, rp_id, f'[{idx}/{len(input_records)}]')
                logging.info(f"FILE HANDLING | [{idx}/{len(input_records)}] - COMPLETE")
                await queue.put(processed_record)
            except Exception as e:
                logging.exception((f"FILE HANDLING | [{idx}/{len(input_records)}] - ERROR - {e}"))

    # Create tasks
    tasks = [asyncio.create_task(_worker(idx, xml_file))
             for idx, xml_file in enumerate(input_records, start=1)]

    # Wait for all workers to finish
    await asyncio.gather(*tasks)

    # Collect results from queue
    processed_records = []
    while not queue.empty():
        processed_records.append(await queue.get())
    
    return processed_records










from utils.pnld.file_handling.pnld_file_handling_no_rp import pnld_file_handling_no_rp

async def process_pnld_batch_no_rp(input_records, rp_message, max_concurrency=8):
    """
    Process each XML record concurrently using asyncio.
    Calls process_pnld(xml_file, xsd) in a thread pool for non-blocking execution.
    """
    semaphore = asyncio.Semaphore(max_concurrency)
    queue = asyncio.Queue()
    
    async def _worker(idx, xml_file):
        async with semaphore:
            try:
                logging.info(f"[{idx}/{len(input_records)}] - Processing XML - START")
                processed_record = await asyncio.to_thread(pnld_file_handling_no_rp, xml_file, rp_message, f'[{idx}/{len(input_records)}]')
                logging.info(f"[{idx}/{len(input_records)}] - Processing XML - COMPLETE")
                await queue.put(processed_record)
            except Exception as e:
                logging.exception(f"Error processing XML [{idx}]: {e}")

    # Create tasks
    tasks = [asyncio.create_task(_worker(idx, xml_file))
             for idx, xml_file in enumerate(input_records, start=1)]

    # Wait for all workers to finish
    await asyncio.gather(*tasks)

    # Collect results from queue
    processed_records = []
    while not queue.empty():
        processed_records.append(await queue.get())
    
    return processed_records
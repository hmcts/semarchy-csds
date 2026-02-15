import logging
import asyncio

from utils.zip_decompression.zip_decompression_functions import process_zip

# # Process each ZIP record
# result_files = []
# for idx, record in enumerate(input_records, start=1):
#     logging.info(f"Processing ZIP [{idx}/{len(input_records)}]")
#     result = process_zip(record)
#     result_files.extend(result)


async def process_zip_batch(input_records, 
                             max_concurrency: int = 8):
    """
    Process each XML record concurrently using asyncio.
    Calls process_zip(zip_file) in a thread pool for non-blocking execution.
    """

    semaphore = asyncio.Semaphore(max_concurrency)
    queue = asyncio.Queue()

    async def _worker(idx, zip_file):
        async with semaphore:
            try:
                logging.info(f"Processing ZIP [{idx}/{len(input_records)}] - START")
                # Run synchronous process_xml in a thread
                processed_record = await asyncio.to_thread(process_zip, zip_file)
                logging.info(f"Processing ZIP [{idx}/{len(input_records)}] - SUCCESS")
                await queue.put(processed_record)
            except Exception as e:
                logging.exception(f"Error processing ZIP [{idx}]: {e}")

    # Create tasks
    tasks = [asyncio.create_task(_worker(idx, zip_file))
             for idx, zip_file in enumerate(input_records, start=1)]

    # Wait for all workers to finish
    await asyncio.gather(*tasks)

    # Collect results from queue
    processed_records = []
    while not queue.empty():
        processed_records.append(await queue.get())

    return processed_records
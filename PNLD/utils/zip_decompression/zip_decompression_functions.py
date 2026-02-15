import logging
import base64
import io

from utils.zip_decompression.helpers.zip_decompress import decompress_zip
from utils.zip_decompression.helpers.zip_validation import is_true_zip

def process_zip(zip_record):

    # Declare parameters for ZIP Files
    zip_file_id = zip_record.get('SourceZIPID')
    zip_encoded_file = zip_record.get('ZIPFileContent')
    batch_id = zip_record.get('BatchID')
    file_uploaded_by = zip_record.get('UploadedBy')
    logging.info(f'ZIP File Received - File ID: {zip_file_id} - Batch ID: {batch_id}')

    # Decode the Base64 ZIP file
    zip_file = base64.b64decode(zip_encoded_file)
    zip_file = io.BytesIO(zip_file)

    if is_true_zip(zip_file):
        
        child_files_output = decompress_zip(zip_file_id, zip_file, file_uploaded_by)
        zip_length = len(child_files_output)

        # Success JSON for this ZIP
        zip_output = [{
            'SourceZIPID': zip_file_id,
            #'ZIPFileName': zip_file_name,
            'FID_SourceStatus': 'Success',
            'FileCount': zip_length,
            #'Notes': zip_notes,
            #'ZIPFileContent': zip_encoded_file
            }]

        logging.info(f'ZIP File ID: {zip_file_id} - All Files Successfully Processed ({zip_length} Files)')

    else:
        # Failure JSON for this ZIP
        zip_output = [{
            'SourceZIPID': zip_file_id,
            #'ZIPFileName': zip_file_name,
            'FID_SourceStatus': 'Failure',
            #'Notes': zip_notes,
            #'ZIPFileContent': zip_encoded_file
            }]

        child_files_output = []

        logging.info(f'ZIP File ID: {zip_file_id} - File is Not a ZIP')

    result = {
            'SourceFile': child_files_output, 
            'SourceZIP': zip_output
        }

    return result

import zipfile
import logging
import base64


def decompress_zip(zip_file_id, zip_file, file_uploaded_by):

    child_files = []

    # Open ZIP File
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        # Iterate over each file in the ZIP
        for file_name in zip_ref.namelist():
            logging.info(f'ZIP File ID: {zip_file_id} - Child File: {file_name} - Detected')

            if file_name.endswith('.xml'):
                logging.info(f'ZIP File ID: {zip_file_id} - Child File: {file_name} - File is XML')
                
                # Open and read the file
                with zip_ref.open(file_name) as file:
                    file_bytes = base64.b64encode(file.read()).decode('utf-8')

                    # Prepare JSON for the POST
                    json_output = {
                        'SourceFileName': file_name,
                        'SourceFileContent': file_bytes,
                        'FID_SourceZIP': zip_file_id,
                        'FID_SourceStatus': 'To Be Processed',
                        'UploadedBy': file_uploaded_by
                    }

                    child_files.append(json_output)
                    logging.info(f'ZIP File ID: {zip_file_id} - Child File: {file_name} - Successfully Processed')

            else:
                logging.info(f'ZIP File ID: {zip_file_id} - Child File: {file_name} - File is Not XML')

    return child_files

from lxml import etree
import base64
import requests
import os

def get_xsd():

    xsd_get_url = os.getenv('SemarchyXSDURL')
    api_key = os.getenv('SemarchyAPIKey')
    get_headers = {'API-Key': api_key}

    # Make the GET request with Basic Authentication
    response = requests.get(url=xsd_get_url, headers=get_headers)
    api_body = response.json()

    xsd_details = api_body.get('records', [])[0]
    xsd_encoded = xsd_details.get('XSDFileContent')

    return xsd_encoded

from lxml import etree




def pnld_xsd_validation(xml_encoded, xsd_encoded, xml_file_id):
    """
    Validates an XML document against an XSD and returns (valid_flag, messages).

    - Does NOT raise on invalid XML or schema violations.
    - Returns False and a populated 'messages' list when mandatory elements are missing.
    """

    messages = []
    valid_flag = False

    # Decode the Base64 XSD file
    xsd_bytes = base64.b64decode(xsd_encoded)
    xml_bytes = base64.b64decode(xml_encoded)

    # 1) Parse XSD safely
    try:
        xsd_doc = etree.XML(xsd_bytes)  # parse XSD bytes into an Element
        schema = etree.XMLSchema(xsd_doc)
    except (etree.XMLSyntaxError, etree.XMLSchemaParseError) as e:
        messages.append({
            'FID_SourceFile': xml_file_id,
            'SourceFileMessageCode': 'XSD-999',
            'SourceFileMessageType': 'ERROR',
            'SourceFileMessage': str(e)
        })
        return False, messages, None

    # 2) Parse XML safely
    try:
        parser = etree.XMLParser(ns_clean=True, remove_blank_text=True)
        xml_root = etree.fromstring(xml_bytes, parser)
        xml_tree = etree.ElementTree(xml_root)  # validate the full document
    except etree.XMLSyntaxError as e:
        # XML is not even well-formed; capture all syntax errors
        # e.error_log may contain multiple entries; fall back to str(e) if empty
        if e.error_log:
            for err in e.error_log:
                messages.append({
                    'FID_SourceFile': xml_file_id,
                    'SourceFileMessageCode': 'XML-999',
                    'SourceFileMessageType': 'ERROR',
                    'SourceFileMessage': f'Line {err.line} - {err.message}'
                })
        else:
            messages.append({
                'FID_SourceFile': xml_file_id,
                'SourceFileMessageCode': 'XML-999',
                'SourceFileMessageType': 'ERROR',
                'SourceFileMessage': str(e)
            })
        return False, messages, None

    # 3) Validate (non-throwing) and read the CURRENT error_log
    valid_flag = schema.validate(xml_tree)  # returns True/False, does not raise

    if not valid_flag:
        # This will include "Missing child element(s)" and similar mandatory-field errors
        for error in schema.error_log:
            messages.append({
                'FID_SourceFile': xml_file_id,
                'SourceFileMessageCode': 'XML-999',
                'SourceFileMessageType': 'ERROR',
                'SourceFileMessage': f'Line {error.line} - {error.message}'
            })

    return messages, xml_tree
   


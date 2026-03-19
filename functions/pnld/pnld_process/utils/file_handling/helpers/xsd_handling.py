from lxml import etree
import base64
import requests
import os
from pnld_process.utils.message_handling import add_message

def get_xsd():

    xsd_get_url = f'{os.getenv("SemarchyBaseURL")}/named-query/CSDS/ExportSourceXSD/GD'
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
        messages = add_message(
                    messages=messages,
                    file_id=xml_file_id,
                    code='ER-SUPP-XSD-001',
                    msg_type='ERROR',
                    issue='Invalid PNLD XSD identified',
                    cause=str(e),
                    resolution='CONTACT SUPPORT TEAM'
                )
        return messages, None

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
                messages = add_message(
                            messages=messages,
                            file_id=xml_file_id,
                            code='ER-NSDT-XML-001',
                            msg_type='ERROR',
                            issue='XML does not meet the structure requirements of a PNLD XML',
                            cause=f'{err.message}',
                            resolution='NSD to check for missing data in the xml or empty data tags. Once issue identified NSD to raise with PNLD.'
                        )
        else:
            messages = add_message(
                        messages=messages,
                        file_id=xml_file_id,
                        code='ER-NSDT-XML-001',
                        msg_type='ERROR',
                        issue='XML does not meet the structure requirements of a PNLD XML',
                        cause=str(e),
                        resolution='NSD to check for missing data in the xml or empty data tags. Once issue identified NSD to raise with PNLD.'
                    )
        return messages, None

    # 3) Validate (non-throwing) and read the CURRENT error_log
    valid_flag = schema.validate(xml_tree)  # returns True/False, does not raise

    if not valid_flag:
        # This will include "Missing child element(s)" and similar mandatory-field errors
        for err in schema.error_log:
           messages = add_message(
                    messages=messages,
                    file_id=xml_file_id,
                    code='ER-NSDT-XML-001',
                    msg_type='ERROR',
                    issue='XML does not meet the structure requirements of a PNLD XML',
                    cause=f'{err.message}',
                    resolution='NSD to check for missing data in the xml or empty data tags. Once issue identified NSD to raise with PNLD.'
                )

    return messages, xml_tree
   


import logging 

def collate_records(records):

    result = {
        "SourceFile": []
        , "SourceFileMessage": []
        , "OffenceRevision": []
        , "Menu": []
        , "MenuOptions": []
        }
    
    for item in records or []:
        result["SourceFile"].extend(item.get("SourceFile", []))
        result["SourceFileMessage"].extend(item.get("SourceFileMessage", []))
        result["OffenceRevision"].extend(item.get("OffenceRevision", []))
        result["Menu"].extend(item.get("Menu", []))
        result["MenuOptions"].extend(item.get("MenuOptions", []))

    logging.info(
        "Collation - SUCCESS; SourceFile: %d, SourceFileMessage: %d,  OffenceRevision: %d,  Menu: %d,   MenuOptions: %d",
        len(result["SourceFile"]),
        len(result["SourceFileMessage"]),
        len(result["OffenceRevision"]),
        len(result["Menu"]),
        len(result["MenuOptions"])
    )
    logging.debug(f"Collated result: {result}")

    return result


def extract_items(data, key):
    """
    Extract and return all values from the array of dicts for the given key.
    """
    results = []
    for item in data:
        if key in item:
            results.extend(item[key])
    return results



def pnld_define_post_body(source_files, messages):
   
    post_body = {
              'action': 'CREATE_LOAD_AND_SUBMIT',
              'programName': 'UPDATE_DATA_REST_API',
              'loadDescription': 'Process PNLD XML Files',
              'jobName': 'SourceFileIntegrationLoad',
              'persistOptions': {
                  'defaultPublisherId': 'PNLD',
                  'optionsPerEntity': {},
                  'missingIdBehavior': 'GENERATE',
                  'persistMode': 'IF_NO_ERROR_OR_MATCH'
              },
              'persistRecords': {
                  'SourceFile': source_files,
                  'SourceFileMessage': messages
              }
          }
    logging.debug(post_body)
    return post_body
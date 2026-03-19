import logging 

def zip_define_post_body(records):

    result = {
        "SourceZIP": []
        , "SourceFile": []
        }

    for item in records or []:
        result["SourceZIP"].extend(item.get("SourceZIP", []))
        result["SourceFile"].extend(item.get("SourceFile", []))

    
    logging.info(
        "Collation - SUCCESS; SourceZIP: %d, SourceFile: %d",
        len(result["SourceZIP"]),
        len(result["SourceFile"]),
    )
    logging.debug(f"Collated result: {result}")

    post_body = {
              'action': 'CREATE_LOAD_AND_SUBMIT',
              'programName': 'UPDATE_DATA_REST_API',
              'loadDescription': 'Decompress ZIP files',
              'jobName': 'SourceFileIntegrationLoad',
              'persistOptions': {
                  'defaultPublisherId': 'PNLD',
                  'optionsPerEntity': {
                      'SourceFile': {
                          'enrichers': ['SourceFileExtract', 'SourceFileName']
                      }
                  },
                  'missingIdBehavior': 'GENERATE',
                  'persistMode': 'IF_NO_ERROR_OR_MATCH'
              },
              'persistRecords': result
          }

    return post_body


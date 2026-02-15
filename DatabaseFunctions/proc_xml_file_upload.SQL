-- ======================================================================================================================================================================
-- Author: Sachin Monteiro
-- Date: 19-Nov-2025
-- Purpose:
-- The procedure proc_xml_file_upload updates records in the CSDS.sa_test_upload table for a specified LoadID. 
-- It reads XML data stored in each record, extracting the file name from the <pnldref> element and the last updated date from the <dateoflastupdate> element. 
-- These extracted values are then used to update the file_name (with ".xml" appended) and file_updated_date attributes in the table.
-- This allows automatic updating of metadata fields based on the XML content within each record, enabling efficient bulk updates tied to a particular batch of data.
-- The procedure uses PostgreSQL's XML processing functions combined with a filtered update for precise data enrichment inside the database.
-- ======================================================================================================================================================================
--
CREATE OR REPLACE PROCEDURE CSDS.proc_xml_file_upload(LoadID BIGINT)
LANGUAGE plpgsql
AS $procedure$
--
BEGIN
--
  UPDATE CSDS.sa_test_upload 		S
    SET file_name         = CONCAT(F.file_name,'.xml')
       ,file_updated_date = F.file_updated_date
    FROM (SELECT test_upload_id                                                                               AS test_upload_id
                ,COALESCE((xpath('//pnldref/text()', convert_from(file, 'UTF8')::xml))[1]::text, '')          AS file_name
                ,COALESCE((xpath('//dateoflastupdate/text()', convert_from(file, 'UTF8')::xml))[1]::text, '') AS file_updated_date
            FROM CSDS.sa_test_upload
           WHERE b_loadid = LoadID
          ) F
    WHERE S.test_upload_id = F.test_upload_id;
--
END;
--
$procedure$
;

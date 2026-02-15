-- DROP FUNCTION csds.fn_get_source_file_name(numeric, numeric);

CREATE OR REPLACE FUNCTION csds.fn_get_source_file_name(v_currentloadid numeric, v_source_file_id numeric)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_file_name text;
BEGIN
    -- Pick one row for this load; if file name or zip id are NULL, skip those filters.
    SELECT
        NULLIF(
            (xpath(
                'string(//document/codes/cjsoffencecode)',
                xmlparse(document convert_from(sf.source_file_content, 'UTF8'))
            ))[1]::text,
            '')
		|| '_'
		|| NULLIF(
            (xpath(
                'string(//document/pnldref)',
                xmlparse(document convert_from(sf.source_file_content, 'UTF8'))
            ))[1]::text,
            '')
		 || '.xml'
    INTO v_file_name
    FROM csds.sa_source_file AS sf
    WHERE sf.b_loadid = v_currentloadid
		AND sf.source_file_id = v_source_file_id
    ORDER BY sf.b_credate DESC NULLS LAST
    LIMIT 1;

    -- If no rows found or extracted element is NULL/empty, return a timestamp-based default.
    IF v_file_name IS NULL THEN
        v_file_name := to_char(current_timestamp, 'YYYYMMDD_HH24MISS') || '.xml';
    END IF;

    RETURN v_file_name;
END;
$function$
;

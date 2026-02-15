-- DROP FUNCTION csds.fn_get_source_file_dolu(numeric, numeric);

CREATE OR REPLACE FUNCTION csds.fn_get_source_file_dolu(v_currentloadid numeric, v_source_file_id numeric)
 RETURNS date
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_date_of_last_update   date;
    v_date_text             text;
BEGIN
    -- Extract //document/ancillary/dateoflastupdate as trimmed text (empty -> NULL)
    SELECT NULLIF(
               btrim( (xpath(
                   'string(//document/ancillary/dateoflastupdate)',
                   xmlparse(document convert_from(sf.source_file_content, 'UTF8'))
               ))[1]::text ),
               ''
           )
    INTO v_date_text
    FROM csds.sa_source_file AS sf
    WHERE sf.b_loadid       = v_currentloadid
      AND sf.source_file_id = v_source_file_id;

    -- If no row or empty value, return NULL
    IF v_date_text IS NULL THEN
        RETURN NULL;
    END IF;

    -- Try to cast to date; if invalid, return NULL instead of failing
    BEGIN
        v_date_of_last_update := v_date_text::date;
    EXCEPTION WHEN others THEN
        v_date_of_last_update := NULL;
    END;

    RETURN v_date_of_last_update;
END;
$function$
;

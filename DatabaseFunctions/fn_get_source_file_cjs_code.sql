-- DROP FUNCTION csds.fn_get_source_file_cjs_code(numeric, numeric);

CREATE OR REPLACE FUNCTION csds.fn_get_source_file_cjs_code(v_currentloadid numeric, v_source_file_id numeric)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    -- Raw value extracted from XML
    v_cjs_code_raw  text;

    -- Validated/truncated output value
    v_cjs_code      varchar(8);
BEGIN
    ----------------------------------------------------------------------
    -- Extract CJS Offence Code from XML
    ----------------------------------------------------------------------
    BEGIN
        SELECT NULLIF(
                   (xpath(
                        'string(//document/codes/cjsoffencecode)',
                        xmlparse(document convert_from(sf.source_file_content, 'UTF8'))
                    ))[1]::text,
                   ''
               )
        INTO STRICT v_cjs_code_raw
        FROM csds.sa_source_file AS sf
        WHERE sf.b_loadid       = v_currentloadid
          AND sf.source_file_id = v_source_file_id;

    EXCEPTION
        WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION
                'Multiple csds.sa_source_file rows found for (loadid=%, source_file_id=%); refine selection.',
                v_currentloadid, v_source_file_id;
    END;

    ----------------------------------------------------------------------
    -- Enforce length rules:
    --   - NULL if value exceeds 8 characters
    ----------------------------------------------------------------------
    IF v_cjs_code_raw IS NOT NULL
       AND char_length(v_cjs_code_raw) > 8 THEN
        v_cjs_code := NULL;
    ELSE
        v_cjs_code := v_cjs_code_raw::varchar(8);
    END IF;

    RETURN v_cjs_code;
END;
$function$
;

-- DROP FUNCTION csds.fn_get_current_cjscode_version(text);

CREATE OR REPLACE FUNCTION csds.fn_get_current_cjscode_version(v_cjs_code text)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_version bigint;
BEGIN
    SELECT MAX(version_number)
      INTO v_version
    FROM csds.gd_offence_revision
    WHERE cjs_code = v_cjs_code;

    -- Will return NULL if no rows or all version_number are NULL
    RETURN v_version;
END;
$function$
;

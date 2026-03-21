-- DROP FUNCTION csds.fn_get_offence_next_version_type(numeric);

CREATE OR REPLACE FUNCTION csds.fn_get_offence_next_version_type(v_ofr_id numeric)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_cjs_code TEXT;
    v_next_version_type TEXT;
BEGIN
    -- Get the cjs_code for the provided ofr_id
    SELECT cjs_code
    INTO v_cjs_code
    FROM csds.gd_offence_revision
    WHERE ofr_id = v_ofr_id;

    -- If not found, return None
    IF v_cjs_code IS NULL THEN
        RETURN 'None';
    END IF;

    -- Find the next version (next highest ofr_id for this cjs_code)
    SELECT version_type
    INTO v_next_version_type
    FROM csds.gd_offence_revision
    WHERE cjs_code = v_cjs_code
      AND ofr_id > v_ofr_id       -- strictly higher = next version
    ORDER BY ofr_id
    LIMIT 1;

    -- If no next version exists, return 'None'
    IF v_next_version_type IS NULL THEN
        RETURN 'None';
    END IF;

    RETURN v_next_version_type;
END;
$function$
;

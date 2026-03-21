-- DROP FUNCTION csds.fn_get_offence_next_date_from(numeric);

CREATE OR REPLACE FUNCTION csds.fn_get_offence_next_date_from(v_ofr_id numeric)
 RETURNS date
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_cjs_code TEXT;
    v_next_date DATE;
BEGIN
    -- Get the cjs_code for the provided ofr_id
    SELECT cjs_code
    INTO v_cjs_code
    FROM csds.gd_offence_revision
    WHERE ofr_id = v_ofr_id;

    -- If not found, return NULL
    IF v_cjs_code IS NULL THEN
        RETURN NULL;
    END IF;

    -- Get the next version's date_used_from
    SELECT date_used_from
    INTO v_next_date
    FROM csds.gd_offence_revision
    WHERE cjs_code = v_cjs_code
      AND ofr_id > v_ofr_id
    ORDER BY ofr_id
    LIMIT 1;

    -- Return the date (NULL if none found)
    RETURN v_next_date;
END;
$function$
;

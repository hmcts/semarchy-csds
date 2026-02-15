-- DROP FUNCTION csds.fn_get_source_file_offence_revision(text, text, date, numeric);

CREATE OR REPLACE FUNCTION csds.fn_get_source_file_offence_revision(v_cjs_code text, v_pnld_ref text, v_date_of_last_update date, v_loadid numeric)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_offence_revision_id NUMERIC;
BEGIN
    SELECT MAX(ofr_id)
    INTO v_offence_revision_id
    FROM csds.sa_offence_revision
    WHERE cjs_code = v_cjs_code
      AND sow_ref = v_pnld_ref
      AND pnld_date_of_last_update = v_date_of_last_update
	  AND b_loadid = v_loadid;

    RETURN v_offence_revision_id;
END;
$function$
;

-- DROP FUNCTION csds.fn_get_current_offence_revision_pnld(text, text);

CREATE OR REPLACE FUNCTION csds.fn_get_current_offence_revision_pnld(v_code text, v_type text)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_offence_revision bigint;
    v_cjs_code text;
BEGIN

    IF v_type = 'cjs_code' THEN

        SELECT MAX(ofr_id)
          INTO v_offence_revision
        FROM csds.gd_offence_revision
        WHERE cjs_code = v_code;

        RETURN v_offence_revision;

    ELSIF v_type = 'sow_ref' THEN

        SELECT cjs_code
          INTO v_cjs_code
        FROM csds.gd_offence_revision
        WHERE sow_ref = v_code
        ORDER BY ofr_id DESC
        LIMIT 1;

        SELECT MAX(ofr_id)
          INTO v_offence_revision
        FROM csds.gd_offence_revision
        WHERE cjs_code = v_cjs_code;

        RETURN v_offence_revision;

    ELSE
        RAISE EXCEPTION 'Invalid v_type: %, expected cjs_code or sow_ref', v_type;
    END IF;

END;
$function$
;

-- DROP FUNCTION csds.fn_create_offence_header(varchar, numeric, varchar);

CREATE OR REPLACE FUNCTION csds.fn_create_offence_header(v_username character varying, v_loadid numeric, v_offence_status character varying)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_oh_id numeric;
BEGIN
    -- Generate next sequence value for OH_ID
    v_oh_id := nextval('csds.seq_offence_header');

    -- Insert new offence header record
    INSERT INTO csds.gd_offence_header (
        oh_id,
        b_classname,
        b_batchid,
        b_credate,
        b_upddate,
        b_creator,
        b_updator,
        f_offence_header_status
    )
    VALUES (
        v_oh_id,
        'OffenceHeader',
        v_loadid,
        now(),
        now(),
        v_username,
        v_username,
        v_offence_status
       );

    -- Return the OH_ID that was inserted
    RETURN v_oh_id;
END;
$function$
;

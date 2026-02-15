-- DROP FUNCTION csds.fn_create_offence_header_pnld(text, numeric, text);

CREATE OR REPLACE FUNCTION csds.fn_create_offence_header_pnld(v_cjs_code text, v_loadid numeric DEFAULT NULL::numeric, v_username text DEFAULT 'PNLD'::text)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_oh_id numeric;
BEGIN
    -- Try to find an existing Offence Header for this CJS code
    SELECT DISTINCT oh.oh_id
    INTO v_oh_id
    FROM csds.gd_offence_header oh
    LEFT JOIN csds.gd_offence_revision ore 
        ON ore.f_offence_header = oh.oh_id
    WHERE ore.cjs_code = v_cjs_code;

    -- If not found, create a new one
    IF v_oh_id IS NULL THEN

        v_oh_id := nextval('csds.seq_offence_header');

        INSERT INTO csds.gd_offence_header (
            oh_id,
            b_classname,
            b_batchid,
            b_credate,
            b_upddate,
            b_creator,
            b_updator
            -- f_offence_header_status   -- uncomment if required
        )
        VALUES (
            v_oh_id,
            'OffenceHeader',
            v_loadid,
            now(),
            now(),
            v_username,
            v_username
            -- v_offence_status
        );

        RETURN v_oh_id;

    ELSE
        -- Already exists → return the existing OH_ID
        RETURN v_oh_id;
    END IF;

END;
$function$
;

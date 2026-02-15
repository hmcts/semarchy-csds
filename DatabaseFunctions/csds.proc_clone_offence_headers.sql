-- PROCEDURE: csds.proc_clone_offence_headers(numeric, character varying)

-- DROP PROCEDURE IF EXISTS csds.proc_clone_offence_headers(numeric, character varying);

CREATE OR REPLACE PROCEDURE csds.proc_clone_offence_headers(
	v_currentloadid numeric,
	v_username character varying)
LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
    rec             record;
    v_rows          integer;
    v_new_header_id numeric;
BEGIN

-- Create new headers based on the original headers for all revisions in sa context
    RAISE NOTICE 'Cloning headers for load id % by %', v_currentloadid, v_username;	
    FOR rec IN
        SELECT
            rev.ofr_id,
            rev.b_loadid,
            head.oh_id                AS old_header_id,
            head.b_classname,
            head.b_batchid,
            head.f_change_set_header,
            head.f_offence_header_status,
            head.f_offnc_hdrs_ref_offnc,
            head.pnld_start_date,
            head.pnld_end_date
        FROM csds.sa_offence_revision rev
        JOIN csds.gd_offence_header head
            ON head.oh_id = rev.f_offence_header
        WHERE rev.b_loadid = v_currentloadid
    LOOP
        -- Insert cloned header
        INSERT INTO csds.gd_offence_header (
            oh_id,
            b_classname,
            b_batchid,
            b_credate,
            b_upddate,
            b_creator,
            b_updator,
            f_change_set_header,
            f_offence_header_status,
            f_offnc_hdrs_ref_offnc,
            pnld_start_date,
            pnld_end_date
        )
        VALUES (
            nextval('csds.seq_offence_header'),
            rec.b_classname,
            rec.b_loadid,        -- or rec.b_batchid if that is what you want
            now(),
            now(),
            v_username,
            v_username,
            rec.f_change_set_header,
            rec.f_offence_header_status,
            rec.f_offnc_hdrs_ref_offnc,
            rec.pnld_start_date,
            rec.pnld_end_date
        )
        RETURNING oh_id INTO v_new_header_id;

        -- Point revision to new header
		-- and clear the cloned from field
        UPDATE csds.sa_offence_revision
        SET f_offence_header = v_new_header_id
        WHERE ofr_id = rec.ofr_id;

        RAISE NOTICE 'Revision %: old header %, new header %',
            rec.ofr_id, rec.old_header_id, v_new_header_id;
    END LOOP;
	
	/*Clear cloned_by*/
    UPDATE csds.sa_offence_revision
    SET    cloned_from = NULL
    WHERE b_loadid = v_currentloadid;
	
	
END;
$BODY$;

ALTER PROCEDURE csds.proc_clone_offence_headers(numeric, character varying)
    OWNER TO postgres;

GRANT EXECUTE ON PROCEDURE csds.proc_clone_offence_headers(numeric, character varying) TO PUBLIC;

GRANT EXECUTE ON PROCEDURE csds.proc_clone_offence_headers(numeric, character varying) TO csds;

GRANT EXECUTE ON PROCEDURE csds.proc_clone_offence_headers(numeric, character varying) TO postgres;


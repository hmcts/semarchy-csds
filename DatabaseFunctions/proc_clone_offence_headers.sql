-- DROP PROCEDURE csds.proc_clone_offence_headers(numeric, varchar);

CREATE OR REPLACE PROCEDURE csds.proc_clone_offence_headers(v_currentloadid numeric, v_username character varying)
 LANGUAGE plpgsql
AS $procedure$

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
	
	/*Clear cloned_by - commented out by SM on 06/03/2026 */
    -- UPDATE csds.sa_offence_revision
    -- SET    cloned_from = NULL
    -- WHERE b_loadid = v_currentloadid;
	
	
END;
$procedure$
;

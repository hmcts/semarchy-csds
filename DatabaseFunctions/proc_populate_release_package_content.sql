-- DROP PROCEDURE csds.proc_populate_release_package_content(numeric, text, text, text, text, numeric, numeric, text, text);

CREATE OR REPLACE PROCEDURE csds.proc_populate_release_package_content(v_batchid numeric, v_user text, v_content_type text, v_content_name text, v_content_url text, v_content_key numeric, v_release_package_id numeric, v_content_notes text, v_content_status text)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    cnt integer;
BEGIN
    -- Get existing count for the content key/type
    SELECT COUNT(1)
      INTO cnt
      FROM csds.gd_release_package_content
     WHERE rp_content_type = v_content_type
       AND rp_content_key  = v_content_key;

    IF cnt = 0 THEN
        INSERT INTO csds.gd_release_package_content (
            release_package_content_i,
            b_classname,
            b_batchid,
            b_credate,
            b_upddate,
            b_creator,
            b_updator,
            rp_content_type,
            rp_content_name,
            rp_content_url,
            rp_content_key,
			rp_content_auth_status,
			hmcts_notes,
            f_release_package
        )
        VALUES (
            nextval('csds.seq_release_package_content'),
            'ReleasePackageContent',
            v_batchid,
            CURRENT_DATE,
            CURRENT_DATE,
            v_user,
            v_user,
            v_content_type,
            v_content_name,
            v_content_url,
            v_content_key,
			v_content_status,
			v_content_notes,
            v_release_package_id
        );
    ELSE
        UPDATE csds.gd_release_package_content
           SET b_batchid          = v_batchid,
               b_upddate          = CURRENT_DATE,
               b_updator          = v_user,
               rp_content_name    = v_content_name,
			   rp_content_auth_status  = v_content_status,
			   hmcts_notes        = v_content_notes,
               f_release_package  = v_release_package_id
         WHERE rp_content_type = v_content_type
           AND rp_content_key  = v_content_key;
    END IF;
END;
$procedure$
;

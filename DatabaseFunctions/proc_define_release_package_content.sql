-- DROP PROCEDURE csds.proc_define_release_package_content(numeric, text, text);

CREATE OR REPLACE PROCEDURE csds.proc_define_release_package_content(
    v_batchid      numeric,
    v_user         text,
    v_content_type text
)
LANGUAGE plpgsql
AS $procedure$
DECLARE
    cnt                  integer;
    v_content_name       text;
    v_content_url        text;
    v_content_key        numeric;
    v_release_package_id numeric;
    v_content_notes      text;
    v_content_status     text;
BEGIN
    IF v_content_type = 'Offence' THEN
        SELECT 
            cjs_code,
            concat('http://localhost:8088/semarchy/mdm-app/CSDS/CrimeStandingDataService/browsing/AllOffences/', ofr_id),
            ofr_id,
            f_release_package,
            offence_notes,
            authoring_status
        INTO 
            v_content_name, v_content_url, v_content_key, v_release_package_id, v_content_notes, v_content_status
        FROM csds.gd_offence_revision
        WHERE b_batchid = v_batchid
        LIMIT 1;

    ELSIF v_content_type = 'Menu' THEN
        SELECT 
            name,
            concat('http://localhost:8088/semarchy/mdm-app/CSDS/CrimeStandingDataService/browsing/AllMenus/', om_id),
            om_id,
            f_release_package,
            hmcts_notes,
            authoring_status
        INTO 
            v_content_name, v_content_url, v_content_key, v_release_package_id, v_content_notes, v_content_status
        FROM csds.gd_ote_menu
        WHERE b_batchid = v_batchid
        LIMIT 1;

    ELSE
        RAISE EXCEPTION 'Unsupported v_content_type: %', v_content_type;
    END IF;

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
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
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
           SET b_batchid             = v_batchid,
               b_upddate             = CURRENT_TIMESTAMP,
               b_updator             = v_user,
               rp_content_name       = v_content_name,
               rp_content_auth_status= v_content_status,
               hmcts_notes           = v_content_notes,
               f_release_package     = v_release_package_id
         WHERE rp_content_type = v_content_type
           AND rp_content_key  = v_content_key;
    END IF;
END;
$procedure$;
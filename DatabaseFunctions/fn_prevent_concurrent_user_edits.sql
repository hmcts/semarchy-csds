/* DROP FUNCTION csds.fn_prevent_concurrent_user_edits(
	numeric, varchar, varchar
	);
	*/

CREATE OR REPLACE FUNCTION csds.fn_prevent_concurrent_user_edits(
    v_parentid numeric, v_entityname character varying, v_mode character varying
)
RETURNS text
LANGUAGE plpgsql
AS $function$
--
DECLARE
--
--row_count NUMERIC;
user 		VARCHAR(128);
--
BEGIN
-- Browsing Collection configuration:
IF v_entityName = 'OffenceRevision' AND v_mode = 'Browse' THEN
--
	SELECT STRING_AGG (b_loadcreator,',' ORDER BY b_loadcreator) INTO user
	  FROM
		(SELECT DISTINCT B.b_loadcreator
           FROM csds.sa_offence_revision		S
           JOIN csds.dl_batch  				   	B
            ON B.b_loadid  = S.b_loadid
         WHERE v_parentID  = S.ofr_id
           AND B.b_status  = 'RUNNING'
		   ) AS dist;
--
END IF;
--
IF v_entityName = 'OffenceMenu' AND v_mode = 'Browse' THEN
--
	SELECT STRING_AGG (b_loadcreator,',' ORDER BY b_loadcreator) INTO user
	  FROM
		(SELECT DISTINCT B.b_loadcreator
           FROM csds.sa_ote_menu				S
           JOIN csds.dl_batch  				   	B
            ON B.b_loadid  = S.b_loadid
         WHERE v_parentID  = S.om_id
           AND B.b_status  = 'RUNNING'
		   ) AS dist;
--
END IF;
--
RETURN user;
--
END;
--
$function$;

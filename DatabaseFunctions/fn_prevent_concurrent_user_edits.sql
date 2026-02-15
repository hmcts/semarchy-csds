-- DROP FUNCTION csds.fn_prevent_concurrent_user_edits(numeric, varchar);

CREATE OR REPLACE FUNCTION csds.fn_prevent_concurrent_user_edits(v_parentid numeric, v_entityname character varying)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

--
DECLARE
--
--row_count NUMERIC;
user VARCHAR(128);
--
BEGIN
--
IF v_entityName = 'OffenceRevision' THEN
--
SELECT MAX(B.b_loadcreator) INTO user
  FROM CSDS.sa_offence_revision			S
  JOIN CSDS.dl_batch  				   	B
    ON B.b_loadid  = S.b_loadid
 WHERE v_parentID  = S.ofr_id
   AND B.b_status  = 'RUNNING';
--
END IF;
--
RETURN user;
--
END
--
$function$
;

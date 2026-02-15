-- DROP FUNCTION csds.fn_get_menu_id_for_options(numeric, text);

CREATE OR REPLACE FUNCTION csds.fn_get_menu_id_for_options(v_currentloadid numeric, v_menu_md5 text)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_menu_id numeric;
BEGIN
    SELECT om_id
    INTO v_menu_id
    FROM csds.sa_ote_menu
    WHERE b_loadid = v_currentloadid
      AND pnld_hash_md5 = v_menu_md5;

    RETURN v_menu_id;
END;
$function$
;

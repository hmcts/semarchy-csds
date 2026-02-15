-- DROP FUNCTION csds.fn_check_option_number_unique(numeric, numeric);

CREATE OR REPLACE FUNCTION csds.fn_check_option_number_unique(v_option_number numeric, v_currentloadid numeric)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_count INTEGER;
BEGIN
    -- Count matching rows
    SELECT COUNT(1)
    INTO v_count
    FROM csds.sa_ote_menu_options
    WHERE option_number = v_option_number
      AND b_loadid = v_currentloadid
	  AND v_option_number <> 0;
 
    -- If more than one match exists, uniqueness fails
    IF v_count > 1 THEN
        RETURN 'FALSE';
    ELSE
        RETURN 'TRUE';
    END IF;
END;
$function$
;
-- DROP FUNCTION csds.fn_terminal_entry_number_calc(varchar, varchar, numeric, numeric, numeric, varchar);

CREATE OR REPLACE FUNCTION csds.fn_terminal_entry_number_calc(v_schemaname character varying, v_authortype character varying, v_currentloadid numeric, v_copiedfrom numeric, v_parentid numeric, v_username character varying)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
--
DECLARE
--
  v_LoadStatus 			VARCHAR(30) := 'RUNNING';
  v_GD_MaxOTEEntryNo 	NUMERIC(10);
  v_SA_MaxOTEEntryNo 	NUMERIC(10);
  v_MaxOTEEntryNo 		NUMERIC(10);
--
BEGIN
--
  IF v_authorType = 'OTE_COPY' THEN
-- Get maximum entry numbers
    SELECT COALESCE(MAX(entry_number),0) INTO v_GD_MaxOTEEntryNo
      FROM CSDS.GD_OFFENCE_TERMINAL_ENTRY
     WHERE f_offence_revision = v_parentID
       AND COALESCE(delete_indicator,FALSE) = FALSE;
--
    SELECT COALESCE(MAX(entry_number),0) INTO v_SA_MaxOTEEntryNo
      FROM CSDS.SA_OFFENCE_TERMINAL_ENTRY
     WHERE f_offence_revision = v_parentID
       AND b_loadid           = v_currentLoadID;
--
    v_MaxOTEEntryNo := GREATEST(v_GD_MaxOTEEntryNo, v_SA_MaxOTEEntryNo) + 1;
--
  END IF;
--
  RETURN v_MaxOTEEntryNo;
--
END;
$function$
;

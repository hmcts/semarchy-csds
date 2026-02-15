-- PROCEDURE: csds.proc_default_terminal_entries(character varying, character varying, numeric, numeric, numeric, character varying)

-- DROP PROCEDURE IF EXISTS csds.proc_default_terminal_entries(character varying, character varying, numeric, numeric, numeric, character varying);

CREATE OR REPLACE PROCEDURE csds.proc_default_terminal_entries(
	v_schemaname character varying,
	v_authortype character varying,
	v_currentloadid numeric,
	v_copiedfrom numeric,
	v_parentid numeric,
	v_username character varying)
LANGUAGE 'plpgsql'
AS $BODY$

--
DECLARE
--
v_LoadStatus  		VARCHAR(30) := 'RUNNING';
v_OTECloneCount 	NUMERIC(10);
v_GD_MaxOTEEntryNo	NUMERIC(10);
v_SA_MaxOTEEntryNo  NUMERIC(10);
v_MaxOTEEntryNo		NUMERIC(10);
v_OTEEntryNo		NUMERIC(10);
--
BEGIN
--
IF v_authorType = 'CREATE' THEN
--
INSERT INTO CSDS.SA_OFFENCE_TERMINAL_ENTRY
	  (b_loadid
	  ,ote_id
	  ,b_classname
      ,b_credate
      ,b_upddate
      ,b_creator
      ,b_updator
	  ,entry_number
	  ,minimum
	  ,maximum
	  ,f_reference_offence --"Format"
	  --,entry_format
	  ,entry_prompt
	  ,f_sei
	  --,standard_entry_identifier
	  ,version_number
	  ,changed_by
	  ,changed_date
	  ,f_offence_revision
	  ,delete_indicator
	  ,url
	  ,f_offnc_trmnl_entry_mnu
	  --,om_om_id
	   )
SELECT v_currentLoadId
      ,NEXTVAL('csds.seq_offence_terminal_entry')
	  ,'OffenceTerminalEntry'
      ,CURRENT_TIMESTAMP
      ,CURRENT_TIMESTAMP
      ,v_userName
      ,v_userName
      ,1
      ,1
      ,1
      ,'MNU'
      ,'Specify Date'
      ,'OD'
      ,0
      ,1
      ,CURRENT_TIMESTAMP
      ,v_parentID						-- f_offence_revision: This should be the v_parent_id, which inturn references the offence revision ID
	  ,FALSE
	  ,NULL
	  ,106
      --,om_om_id
 WHERE NOT EXISTS (
				   SELECT 1 FROM CSDS.SA_OFFENCE_TERMINAL_ENTRY
					WHERE f_offence_revision = v_parentID
					  AND entry_number IN (0,1) -- Entry Number gets set to zero when delete indicator is set against a terminal entry.
					  AND b_loadid           = v_currentLoadId
		              --AND delete_indicator   = FALSE
				   )
UNION ALL
SELECT v_currentLoadId
      ,NEXTVAL('csds.seq_offence_terminal_entry')
	  ,'OffenceTerminalEntry'
      ,CURRENT_TIMESTAMP
      ,CURRENT_TIMESTAMP
      ,v_userName
      ,v_userName
      ,2
      ,1
      ,250
      ,'TXT'
      ,'Place of Offence'
      ,'PT'
      ,0
      ,1
      ,CURRENT_TIMESTAMP
      ,v_parentID						-- f_offence_revision: This should be the v_parent_id, which inturn references the offence revision ID
	  ,FALSE
	  ,NULL
	  ,NULL
      --,om_om_id
 WHERE NOT EXISTS (
				   SELECT 1 FROM CSDS.SA_OFFENCE_TERMINAL_ENTRY
					WHERE f_offence_revision = v_parentID
					  AND entry_number IN (0,2) -- Entry Number gets set to zero when delete indicator is set against a terminal entry.
					  AND b_loadid           = v_currentLoadId
		              --AND delete_indicator   = FALSE
				   )
UNION ALL
SELECT v_currentLoadId
      ,NEXTVAL('csds.seq_offence_terminal_entry')
	  ,'OffenceTerminalEntry'
      ,CURRENT_TIMESTAMP
      ,CURRENT_TIMESTAMP
      ,v_userName
      ,v_userName
      ,3
      ,1
      ,1000
      ,'TXT'
      ,'Details of Offence'
      ,NULL
      ,0
      ,1
      ,CURRENT_TIMESTAMP
      ,v_parentID						-- f_offence_revision: This should be the v_parent_id, which inturn references the offence revision ID
	  ,FALSE
	  ,NULL
	  ,NULL
      --,om_om_id
 WHERE NOT EXISTS (
				   SELECT 1 FROM CSDS.SA_OFFENCE_TERMINAL_ENTRY
					WHERE f_offence_revision = v_parentID
					  AND entry_number IN (0,3) -- Entry Number gets set to zero when delete indicator is set against a terminal entry.
					  AND b_loadid           = v_currentLoadId
		              --AND delete_indicator   = FALSE
				   );	  
--
END IF;
--
END;
--
$BODY$;

ALTER PROCEDURE csds.proc_default_terminal_entries(character varying, character varying, numeric, numeric, numeric, character varying)
    OWNER TO postgres;

GRANT EXECUTE ON PROCEDURE csds.proc_default_terminal_entries(character varying, character varying, numeric, numeric, numeric, character varying) TO PUBLIC;

GRANT EXECUTE ON PROCEDURE csds.proc_default_terminal_entries(character varying, character varying, numeric, numeric, numeric, character varying) TO csds;

GRANT EXECUTE ON PROCEDURE csds.proc_default_terminal_entries(character varying, character varying, numeric, numeric, numeric, character varying) TO postgres;


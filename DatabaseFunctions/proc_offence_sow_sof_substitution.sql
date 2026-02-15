-- DROP PROCEDURE csds.proc_offence_sow_sof_substitution(varchar, varchar, numeric, numeric, numeric, numeric, varchar);

CREATE OR REPLACE PROCEDURE csds.proc_offence_sow_sof_substitution(v_schemaname character varying, v_entrypromptsubstitution character varying, v_currentloadid numeric, v_currentbatchid numeric, v_copiedfrom numeric, v_parentid numeric, v_username character varying)
 LANGUAGE plpgsql
AS $procedure$
--
DECLARE
--
v_entryPromptSubstitutionUpd	VARCHAR(4000);
v_rowCount                    	INTEGER;
v_offenceWordingID				INTEGER;
--
BEGIN
--
SELECT STRING_AGG('{' || entry_number || '}', ',' ORDER BY entry_number)	INTO v_entryPromptSubstitutionUpd
  FROM CSDS.sa_offence_terminal_entry
 WHERE f_offence_revision = v_parentID
   AND b_loadid           = v_currentLoadID
   AND delete_indicator   = FALSE;
--
SELECT COUNT(*)	INTO v_rowCount
  FROM CSDS.gd_offence_wording
 WHERE offence_wording_text = v_entryPromptSubstitutionUpd;
--
IF v_rowCount = 0 THEN
-- Insert a new record if not found
	INSERT INTO CSDS.gd_offence_wording 
			(offence_wording_id, b_classname, b_batchid, b_credate, b_upddate, b_creator, b_updator, offence_wording_text, sl_offence_wording_text
			,version_number, changed_by, changed_date, f_sow_status)
     VALUES (NEXTVAL('csds.seq_offence_wording'), 'OffenceWording', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, v_userName, v_userName, v_entryPromptSubstitutionUpd, NULL
	        ,1, v_userName, CURRENT_TIMESTAMP, 'Draft');
--
ELSE
-- Update the existing record if found
    UPDATE CSDS.gd_offence_wording
       SET offence_wording_text    = v_entryPromptSubstitutionUpd
	      ,sl_offence_wording_text = NULL
		  ,b_upddate			   = CURRENT_TIMESTAMP
		  ,b_updator               = v_userName
		  ,changed_by              = v_userName
		  ,changed_date            = CURRENT_TIMESTAMP
		  ,f_sow_status            = 'Draft'
     WHERE offence_wording_text = v_entryPromptSubstitutionUpd;
--
	SELECT COALESCE(S.offence_wording_id, G.offence_wording_id)		INTO v_offenceWordingID
	  FROM            CSDS.sa_offence_wording S
      FULL OUTER JOIN CSDS.gd_offence_wording G ON S.offence_wording_text = G.offence_wording_text
     WHERE COALESCE(S.offence_wording_text, G.offence_wording_text) = v_entryPromptSubstitutionUpd;
--
	UPDATE CSDS.sa_offence_revision
	   SET f_offence_wording = v_offenceWordingID
	 WHERE b_loadid          = v_currentLoadID
	   AND ofr_id            = v_parentID;
--
END IF;
--
END;
--
$procedure$
;

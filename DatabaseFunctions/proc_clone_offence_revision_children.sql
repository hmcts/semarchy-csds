-- DROP PROCEDURE csds.proc_clone_offence_revision_children(numeric, varchar, numeric, varchar);

CREATE OR REPLACE PROCEDURE csds.proc_clone_offence_revision_children(v_currentloadid numeric, v_username character varying, v_parentid numeric, v_authoringtype character varying)
 LANGUAGE plpgsql
AS $procedure$

--
DECLARE
--
v_copiedFrom	NUMERIC( 38);
--
BEGIN
	--
	--INSERT INTO CSDS._tmp (message) VALUES ('proc_clone_offence_revision_children');
	SELECT b_copiedfrom	INTO v_copiedFrom
	  FROM CSDS.sa_offence_revision
	 WHERE ofr_id   = v_parentID
	   AND b_loadid = v_currentLoadID
	   AND b_copiedfrom IS NOT NULL;

	-- ======================================================================================================================================================================
	-- Populate OTE_ID with new primary key from sequence SEQ_OFFENCE_TERMINAL_ENTRY.
	-- Populate B_COPIEDFROM with the OTE_ID (offence terminal entry ID) of the copied offence revision.
	-- Populate F_OFFENCE_REVISION with the new parent offence revision available via the v_parentID variable.
	-- Populate metadata (creator, timestamps, etc.) from v_userName and CURRENT_TIMESTAMP.
	-- ======================================================================================================================================================================
	
		INSERT INTO CSDS.sa_offence_terminal_entry
			(b_loadid,
			 ote_id,
			 b_classname,
			 b_copiedfrom,
			 b_credate,
			 b_upddate,
			 b_creator,
			 b_updator,
			 entry_number,
			 minimum,
			 maximum,
			 entry_format,
			 entry_prompt,
			 standard_entry_identifier,
			 version_number,
			 changed_by,
			 changed_date,
			 f_offnc_trmnl_entry_mnu,
			 f_offnc_trmnl_entry_mnu_opt,
			 delete_indicator,
			 f_offence_revision,
			 url,
			 f_entry_format2,
			 f_reference_offence,
			 f_sei)
		SELECT v_currentLoadID,
			   NEXTVAL('csds.seq_offence_terminal_entry'),
			   'OffenceTerminalEntry',
			   ote_id,
			   CURRENT_TIMESTAMP,
			   CURRENT_TIMESTAMP,
			   v_userName,
			   v_userName,
			   entry_number,
			   minimum,
			   maximum,
			   entry_format,
			   entry_prompt,
			   standard_entry_identifier,
			   version_number,
			   changed_by,
			   changed_date,
			   f_offnc_trmnl_entry_mnu,
			   f_offnc_trmnl_entry_mnu_opt,
			   delete_indicator,
			   v_parentID,
			   url,
			   f_entry_format2,
			   f_reference_offence,
			   f_sei			   
	     FROM  CSDS.gd_offence_terminal_entry
	     WHERE f_offence_revision = v_copiedFrom
		 AND   delete_indicator   = false
		 AND NOT EXISTS (SELECT 1  /*Ensure we havent already copied the children*/
						 FROM CSDS.sa_offence_terminal_entry 
						 WHERE b_loadid = v_currentLoadID
						 AND f_offence_revision = v_parentID
						  );
	
END;

$procedure$
;

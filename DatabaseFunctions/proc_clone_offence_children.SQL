-- DROP PROCEDURE csds.proc_clone_offence_children(numeric, numeric, varchar, numeric, varchar);

CREATE OR REPLACE PROCEDURE csds.proc_clone_offence_children(v_currentloadid numeric, v_copiedfrom numeric, v_username character varying, v_parentid numeric, v_authoringtype character varying)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
INSERT INTO CSDS._tmp (message) VALUES ('proc_clone_offence_children');
--
IF v_copiedFrom IS NOT NULL THEN
--
	IF v_authoringType = 'CloneOffence' THEN
--
-- ======================================================================================================================================================================
-- The insertion of an OffenceRevision record should occur only when a clone of an OffenceHeader has been initiated.
-- ======================================================================================================================================================================
-- ======================================================================================================================================================================
-- Populate OFR_ID with new primary key from sequence SEQ_OFFENCE_REVISION.
-- Populate B_COPIEDFROM with the OFR_ID (offence revision ID) of the copied offence revision.
-- Populate F_OFFENCE_HEADER with the new parent offence header available via v_parentID.
-- Populate metadata (creator, timestamps, etc.) from v_userName and CURRENT_TIMESTAMP.
-- ======================================================================================================================================================================
--
	INSERT INTO CSDS.sa_offence_revision 
		(b_loadid,ofr_id,b_authoringtype,b_classname,b_copiedfrom,b_credate,b_upddate,b_creator,b_updator,recordable
		,reportable,cjs_title,custodial_indicator,sow_reference,mis_classification,offence_type,dvla_code,date_used_from,date_used_to,offence_notes,standard_list,traffic_control
		,maximum_penalty,version_number,changed_by,changed_date,description,derived_from_cjs_code,ho_class,ho_subclass,proceedings_code,sl_cjs_title,f_offence_act_and_section
		,f_offence_statement_of_fact,f_offence_wording,f_offence_header)
	SELECT v_currentLoadID,NEXTVAL('csds.seq_offence_revision'),'DATA_ENTRY','OffenceRevision',ofr_id,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,v_userName,v_userName,recordable
		  ,reportable,cjs_title,custodial_indicator,sow_reference,mis_classification,offence_type,dvla_code,date_used_from,date_used_to,offence_notes,standard_list,traffic_control
		  ,maximum_penalty,version_number,changed_by,changed_date,description,derived_from_cjs_code,ho_class,ho_subclass,proceedings_code,sl_cjs_title,f_offence_act_and_section
		  ,f_offence_statement_of_fact,f_offence_wording,v_parentID
      FROM CSDS.gd_offence_revision 
     WHERE f_offence_header = v_copiedFrom;
--
-- ======================================================================================================================================================================
-- Populate OTE_ID with new primary key from sequence SEQ_OFFENCE_TERMINAL_ENTRY.
-- Populate B_COPIEDFROM with the OTE_ID (offence terminal entry IDs) of the respective copied offence terminal entries.
-- Populate F_OFFENCE_REVISION with the new parent offence revision available via the OFR_ID in the SA_OFFENCE_REVISION table (created in previous step).
-- Populate metadata (creator, timestamps, etc.) from v_userName and CURRENT_TIMESTAMP.
-- Use B_COPIEDFROM from SA_OFFENCE_REVISION to identify terminal entries associated to the "original" offence revision.
-- ======================================================================================================================================================================
--
	INSERT INTO CSDS.sa_offence_terminal_entry
		(b_loadid,ote_id,b_classname,b_copiedfrom,b_credate,b_upddate,b_creator,b_updator
		,entry_number,minimum,maximum,entry_format,entry_prompt,standard_entry_identifier,version_number,changed_by
		,changed_date,f_offnc_trmnl_entry_mnu,f_offnc_trmnl_entry_mnu_opt,delete_indicator
		,f_offence_revision)
	SELECT v_currentLoadID,NEXTVAL('csds.seq_offence_terminal_entry'),'OffenceTerminalEntry',GOTE.ote_id,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,v_userName,v_userName
		  ,GOTE.entry_number,GOTE.minimum,GOTE.maximum,GOTE.entry_format,GOTE.entry_prompt,GOTE.standard_entry_identifier,GOTE.version_number,GOTE.changed_by
		  ,GOTE.changed_date,GOTE.f_offnc_trmnl_entry_mnu,GOTE.f_offnc_trmnl_entry_mnu_opt,GOTE.delete_indicator
		  ,SOFR.ofr_id
      FROM CSDS.gd_offence_terminal_entry	GOTE
      JOIN CSDS.sa_offence_revision 		SOFR ON GOTE.f_offence_revision = SOFR.b_copiedfrom
     WHERE SOFR.f_offence_header = v_parentID;
--
	ELSIF v_authoringType = 'AttemptCloneRevision' THEN

	INSERT INTO CSDS._tmp (message) VALUES ('AttemptCloneRevision');
--
-- ======================================================================================================================================================================
-- The clone of an OffenceRevision record should result in cloning the OffenceTerminalEntries.
-- ======================================================================================================================================================================
-- ======================================================================================================================================================================
-- Populate OTE_ID with new primary key from sequence SEQ_OFFENCE_TERMINAL_ENTRY.
-- Populate B_COPIEDFROM with the OTE_ID (offence terminal entry ID) of the copied offence revision.
-- Populate F_OFFENCE_REVISION with the new parent offence revision available via the v_parentID variable.
-- Populate metadata (creator, timestamps, etc.) from v_userName and CURRENT_TIMESTAMP.
-- ======================================================================================================================================================================
--
	INSERT INTO CSDS.sa_offence_terminal_entry
		(b_loadid,ote_id,b_classname,b_copiedfrom,b_credate,b_upddate,b_creator,b_updator
		,entry_number,minimum,maximum,entry_format,entry_prompt,standard_entry_identifier,version_number,changed_by
		,changed_date,f_offnc_trmnl_entry_mnu,f_offnc_trmnl_entry_mnu_opt,delete_indicator,f_offence_revision)
	SELECT v_currentLoadID,NEXTVAL('csds.seq_offence_terminal_entry'),'OffenceTerminalEntry',ote_id,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,v_userName,v_userName
          ,entry_number,minimum,maximum,entry_format,entry_prompt,standard_entry_identifier,version_number,changed_by
          ,changed_date,f_offnc_trmnl_entry_mnu,f_offnc_trmnl_entry_mnu_opt,delete_indicator,v_parentID
      FROM CSDS.gd_offence_terminal_entry
     WHERE f_offence_revision = v_copiedFrom;
--
	END IF;
--
END IF;
--
END;
--
$procedure$
;
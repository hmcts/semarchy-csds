-- DROP PROCEDURE csds.proc_set_draft_final_exists_indicator(bigint, varchar, varchar);
CREATE OR REPLACE PROCEDURE csds.proc_set_draft_final_exists_indicator(v_loadid bigint, v_entityname character varying, v_mode character varying)
 LANGUAGE plpgsql
AS $procedure$
--
DECLARE
--
--row_count NUMERIC;
v_clonedfrom	TEXT;
v_cjscode		TEXT;
--
BEGIN
-- ===============================================================================================================================
-- When a new revision (including an edit revision) is created for a published offence record, the system shall set the DraftFinalExistIndicator flag on the corresponding published record.
-- This flag shall be used by the Offence Revision Action set to determine which actions are enabled or disabled.
-- When the revision is discarded, the system shall clear the DraftFinalExistIndicator flag on the published record.
-- ===============================================================================================================================
--
IF v_entityName = 'OffenceRevision' AND v_mode = 'Edit' THEN
--
-- ==============================================================================================================================
-- From the offence revision ID, identify the CJS Code for which the DraftFinalExistIndicator has to be set 
-- or unset if it has been discarded.
-- ==============================================================================================================================
	SELECT ofr.cloned_from
	      ,ofr.cjs_code 
      INTO v_clonedfrom, v_cjscode
	  FROM csds.sa_offence_revision	ofr
      JOIN csds.dl_batch			dbt ON ofr.b_loadid = dbt.b_loadid
     WHERE dbt.b_loadid = v_loadid
     LIMIT 1;
--
-- ==============================================================================================================================
-- Once the CJS Code has been identified, identify the published Offence Revision for the CJS Code.
-- 1. If the user clicks on finish on xDM, then set the DraftFinalExistIndicator to true on the published Offence revision.
-- 2. If the user deletes the created draft, then unset DraftFinalExistIndicator on the published Offence revision.
-- ==============================================================================================================================
	-- If it is an edit revision, new revision or an incohate clone - revision / clone created for first time, set DraftFinalExistIndicator.
	UPDATE csds.gd_offence_revision
       SET draft_final_exists_ind = true
	 WHERE cjs_code                 = COALESCE(v_clonedfrom,v_cjscode)
	   AND authoring_status         = 'Published'
	   AND current_record_indicator = true;
	-- If the edit revision, new revision or an incohate clone - revision / clone created is being deleted - then check set DraftFinalExistIndicator to false if no draft exists for the published Offence Revision.
		
	UPDATE csds.gd_offence_revision gor
	   SET draft_final_exists_ind = false
	 WHERE gor.cjs_code               = COALESCE(v_clonedfrom,v_cjscode)
	   AND gor.authoring_status       = 'Published'
	   AND gor.draft_final_exists_ind = true
	   AND NOT EXISTS (
						SELECT 1
						  FROM csds.gd_offence_revision gfr
						 WHERE (gfr.cloned_from = COALESCE(v_clonedfrom,v_cjscode) OR gfr.cjs_code = COALESCE(v_clonedfrom,v_cjscode))
						   AND gfr.authoring_status IN ('Draft','Final')
						);
--
END IF;
--
END;
$procedure$
;

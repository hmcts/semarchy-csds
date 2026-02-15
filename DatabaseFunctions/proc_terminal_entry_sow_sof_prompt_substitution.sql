-- DROP PROCEDURE csds.proc_terminal_entry_sow_sof_prompt_substitution(varchar, numeric, numeric, varchar);

CREATE OR REPLACE PROCEDURE csds.proc_terminal_entry_sow_sof_prompt_substitution(v_schemaname character varying, v_currentloadid numeric, v_parentid numeric, v_username character varying)
 LANGUAGE plpgsql
AS $procedure$
--
DECLARE
  v_SOWPromptSubstitution VARCHAR(4000);
  v_SOFPromptSubstitution VARCHAR(4000);
--
BEGIN
--
  WITH combined AS (
    SELECT entry_number, entry_prompt, 1 AS priority
      FROM csds.sa_offence_terminal_entry
     WHERE f_offence_revision = v_parentID
       AND b_loadid           = v_currentLoadID
       AND COALESCE(delete_indicator,false) = false			-- Newly created OffenceTerminalEntries are set to a NULL.
    UNION ALL
    SELECT entry_number, entry_prompt, 2 AS priority
      FROM csds.gd_offence_terminal_entry
     WHERE f_offence_revision = v_parentID
       AND delete_indicator   = false
    UNION ALL
    SELECT S.entry_number, S.entry_prompt, 1 AS priority
      FROM csds.sa_offence_terminal_entry   S
      JOIN csds.gd_offence_terminal_entry   G
	    ON S.f_offence_revision = G.f_offence_revision
	   AND S.entry_number = G.entry_number
     WHERE G.delete_indicator   = false
       AND S.delete_indicator   = true
	   AND S.b_loadid           = v_currentLoadID
	   AND S.f_offence_revision = v_parentID
  )
  ,ranked AS (
    SELECT DISTINCT
           FIRST_VALUE(entry_number) OVER (PARTITION BY entry_number ORDER BY priority ASC) AS entry_number
          ,FIRST_VALUE(entry_prompt) OVER (PARTITION BY entry_number ORDER BY priority ASC) AS entry_prompt
      FROM combined
  )
  SELECT string_agg(format('{%s}', entry_prompt), ', ' ORDER BY entry_number) INTO v_SOWPromptSubstitution
    FROM ranked;
--
  UPDATE csds.sa_offence_revision
     SET entry_prmpt_sub_sow = v_SOWPromptSubstitution
        ,entry_prmpt_sub_sof = v_SOWPromptSubstitution
   WHERE b_loadid = v_currentLoadID
     AND ofr_id   = v_parentID;
--
  UPDATE csds.gd_offence_revision
     SET entry_prmpt_sub_sow = v_SOWPromptSubstitution
        ,entry_prmpt_sub_sof = v_SOWPromptSubstitution
   WHERE ofr_id = v_parentID;
--
END;
--
$procedure$
;

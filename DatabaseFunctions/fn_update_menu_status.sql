-- DROP FUNCTION csds.fn_update_menu_status(numeric, numeric);

CREATE OR REPLACE FUNCTION csds.fn_update_menu_status(v_currentloadid numeric, v_menuid numeric)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
/* Return menu status for a given load/menu:
   - If any linked Offence Revision is 'Active' → return sm.f_ote_menu_status.
   - Else → return 'Inactive'. */
DECLARE
    v_active_count numeric := 0;
    v_status       text;
BEGIN
    -- Count active Offence Revisions linked to this menu in this load
    SELECT COUNT(*)
      INTO v_active_count
      FROM csds.sa_ote_menu sm
      LEFT JOIN csds.gd_offence_terminal_entry ote
             ON ote.f_offnc_trmnl_entry_mnu = sm.om_id
      LEFT JOIN csds.gd_offence_revision gor
             ON gor.ofr_id = ote.f_offence_revision
     WHERE sm.b_loadid = v_currentloadid
       AND sm.om_id    = v_menuid
       AND (gor.f_status <> 'Inactive' OR gor.f_status IS NULL);

    IF v_active_count > 0 THEN
        -- Return stored menu status
        SELECT sm.f_ote_menu_status
          INTO v_status
          FROM csds.sa_ote_menu sm
         WHERE sm.b_loadid = v_currentloadid
           AND sm.om_id    = v_menuid
         LIMIT 1;

        RETURN v_status;
    ELSE
        RETURN 'Inactive';
    END IF;
END;
$function$
;

-- DROP FUNCTION csds.fn_get_offence_menu_status(numeric, numeric, text);

CREATE OR REPLACE FUNCTION csds.fn_get_offence_menu_status(p_menu_id numeric, p_load_id numeric, p_current_status text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_menu_status text;
BEGIN

    WITH menu_match_sa AS (
        SELECT sa.publishing_status
        FROM csds.sa_offence_revision sa
        JOIN csds.sa_release_package rp
          ON rp.rp_id = sa.f_release_package
        CROSS JOIN LATERAL unnest(ARRAY[
            sa.f_menu_01, sa.f_menu_02, sa.f_menu_03, sa.f_menu_04, sa.f_menu_05,
            sa.f_menu_06, sa.f_menu_07, sa.f_menu_08, sa.f_menu_09, sa.f_menu_10,
            sa.f_menu_11, sa.f_menu_12, sa.f_menu_13, sa.f_menu_14, sa.f_menu_15,
            sa.f_menu_16, sa.f_menu_17, sa.f_menu_18, sa.f_menu_19, sa.f_menu_20,
            sa.f_menu_21, sa.f_menu_22, sa.f_menu_23, sa.f_menu_24, sa.f_menu_25,
            sa.f_menu_26, sa.f_menu_27, sa.f_menu_28, sa.f_menu_29, sa.f_menu_30
        ]) AS menu_id
        WHERE sa.b_loadid = p_load_id
          AND rp.status = 'Published'
          AND menu_id = p_menu_id
    ),

    menu_match_gd AS (
        SELECT gor.publishing_status, gor.ofr_id
        FROM csds.gd_offence_revision gor
        JOIN csds.gd_release_package rp
          ON rp.rp_id = gor.f_release_package
        CROSS JOIN LATERAL unnest(ARRAY[
            gor.f_menu_01, gor.f_menu_02, gor.f_menu_03, gor.f_menu_04, gor.f_menu_05,
            gor.f_menu_06, gor.f_menu_07, gor.f_menu_08, gor.f_menu_09, gor.f_menu_10,
            gor.f_menu_11, gor.f_menu_12, gor.f_menu_13, gor.f_menu_14, gor.f_menu_15,
            gor.f_menu_16, gor.f_menu_17, gor.f_menu_18, gor.f_menu_19, gor.f_menu_20,
            gor.f_menu_21, gor.f_menu_22, gor.f_menu_23, gor.f_menu_24, gor.f_menu_25,
            gor.f_menu_26, gor.f_menu_27, gor.f_menu_28, gor.f_menu_29, gor.f_menu_30
        ]) AS menu_id
        WHERE rp.status = 'Published'
          AND menu_id = p_menu_id
          AND NOT EXISTS (
              SELECT 1
              FROM csds.sa_offence_revision sa2
              WHERE sa2.b_loadid = p_load_id
                AND sa2.ofr_id = gor.ofr_id
          )
    )

    SELECT CASE
        WHEN EXISTS (
                SELECT 1
                FROM menu_match_sa
                WHERE publishing_status = 'Active'
            )
          OR EXISTS (
                SELECT 1
                FROM menu_match_gd
                WHERE publishing_status = 'Active'
            )
        THEN 'Active'

        WHEN EXISTS (
                SELECT 1
                FROM menu_match_sa
                WHERE publishing_status = 'Future'
            )
          OR EXISTS (
                SELECT 1
                FROM menu_match_gd
                WHERE publishing_status = 'Future'
            )
        THEN 'Future'

        ELSE 'Inactive'
    END
    INTO v_menu_status;

    RETURN v_menu_status;
END;
$function$
;

-- DROP FUNCTION csds.fn_menu_status_from_revisions(numeric);

CREATE OR REPLACE FUNCTION csds.fn_menu_status_from_revisions(p_menu_id numeric)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_menu_status text;
BEGIN
    SELECT
        CASE
            /* 1) At least one ACTIVE offence revision */
            WHEN EXISTS (
                SELECT 1
                FROM csds.gd_offence_revision gor
				JOIN csds.gd_release_package rp
                  ON rp.rp_id = gor.f_release_package
                WHERE rp.status = 'Published'
                  AND p_menu_id IN (
                    gor.f_menu_01, gor.f_menu_02, gor.f_menu_03, gor.f_menu_04, gor.f_menu_05,
                    gor.f_menu_06, gor.f_menu_07, gor.f_menu_08, gor.f_menu_09, gor.f_menu_10,
                    gor.f_menu_11, gor.f_menu_12, gor.f_menu_13, gor.f_menu_14, gor.f_menu_15,
                    gor.f_menu_16, gor.f_menu_17, gor.f_menu_18, gor.f_menu_19, gor.f_menu_20,
                    gor.f_menu_21, gor.f_menu_22, gor.f_menu_23, gor.f_menu_24, gor.f_menu_25,
                    gor.f_menu_26, gor.f_menu_27, gor.f_menu_28, gor.f_menu_29, gor.f_menu_30,
                    gor.f_menu_31, gor.f_menu_32, gor.f_menu_33, gor.f_menu_34, gor.f_menu_35,
                    gor.f_menu_36, gor.f_menu_37, gor.f_menu_38, gor.f_menu_39, gor.f_menu_40,
                    gor.f_menu_41, gor.f_menu_42, gor.f_menu_43, gor.f_menu_44, gor.f_menu_45,
                    gor.f_menu_46, gor.f_menu_47, gor.f_menu_48, gor.f_menu_49, gor.f_menu_50
                )
                AND gor.publishing_status = 'Active'
            )
            THEN 'Active'

            /* 2) At least one FUTURE offence revision and no Active */
            WHEN EXISTS (
                SELECT 1
                FROM csds.gd_offence_revision gor
				JOIN csds.gd_release_package rp
                  ON rp.rp_id = gor.f_release_package
                WHERE rp.status = 'Published'
                  AND p_menu_id IN (
                    gor.f_menu_01, gor.f_menu_02, gor.f_menu_03, gor.f_menu_04, gor.f_menu_05,
                    gor.f_menu_06, gor.f_menu_07, gor.f_menu_08, gor.f_menu_09, gor.f_menu_10,
                    gor.f_menu_11, gor.f_menu_12, gor.f_menu_13, gor.f_menu_14, gor.f_menu_15,
                    gor.f_menu_16, gor.f_menu_17, gor.f_menu_18, gor.f_menu_19, gor.f_menu_20,
                    gor.f_menu_21, gor.f_menu_22, gor.f_menu_23, gor.f_menu_24, gor.f_menu_25,
                    gor.f_menu_26, gor.f_menu_27, gor.f_menu_28, gor.f_menu_29, gor.f_menu_30,
                    gor.f_menu_31, gor.f_menu_32, gor.f_menu_33, gor.f_menu_34, gor.f_menu_35,
                    gor.f_menu_36, gor.f_menu_37, gor.f_menu_38, gor.f_menu_39, gor.f_menu_40,
                    gor.f_menu_41, gor.f_menu_42, gor.f_menu_43, gor.f_menu_44, gor.f_menu_45,
                    gor.f_menu_46, gor.f_menu_47, gor.f_menu_48, gor.f_menu_49, gor.f_menu_50
                )
                AND gor.publishing_status = 'Future'
            )
            THEN 'Future'

            /* 3) Only Inactive or no linked revisions */
            WHEN NOT EXISTS (
                SELECT 1
                FROM csds.gd_offence_revision gor
				JOIN csds.gd_release_package rp
                  ON rp.rp_id = gor.f_release_package
                WHERE rp.status = 'Published'
                  AND p_menu_id IN (
                    gor.f_menu_01, gor.f_menu_02, gor.f_menu_03, gor.f_menu_04, gor.f_menu_05,
                    gor.f_menu_06, gor.f_menu_07, gor.f_menu_08, gor.f_menu_09, gor.f_menu_10,
                    gor.f_menu_11, gor.f_menu_12, gor.f_menu_13, gor.f_menu_14, gor.f_menu_15,
                    gor.f_menu_16, gor.f_menu_17, gor.f_menu_18, gor.f_menu_19, gor.f_menu_20,
                    gor.f_menu_21, gor.f_menu_22, gor.f_menu_23, gor.f_menu_24, gor.f_menu_25,
                    gor.f_menu_26, gor.f_menu_27, gor.f_menu_28, gor.f_menu_29, gor.f_menu_30,
                    gor.f_menu_31, gor.f_menu_32, gor.f_menu_33, gor.f_menu_34, gor.f_menu_35,
                    gor.f_menu_36, gor.f_menu_37, gor.f_menu_38, gor.f_menu_39, gor.f_menu_40,
                    gor.f_menu_41, gor.f_menu_42, gor.f_menu_43, gor.f_menu_44, gor.f_menu_45,
                    gor.f_menu_46, gor.f_menu_47, gor.f_menu_48, gor.f_menu_49, gor.f_menu_50
                )
                AND gor.publishing_status IN ('Active', 'Future')
            )
            THEN 'Inactive'

            /* 4) Otherwise leave menu status unchanged */
            ELSE sm.publishing_status
        END
    INTO v_menu_status
    FROM csds.sa_ote_menu sm
    WHERE sm.om_id = p_menu_id;

    RETURN v_menu_status;
END;
$function$
;

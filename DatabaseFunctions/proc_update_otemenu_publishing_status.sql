-- DROP PROCEDURE csds.proc_update_otemenu_publishing_status(numeric, text);

CREATE OR REPLACE PROCEDURE csds.proc_update_otemenu_publishing_status(IN v_batchid numeric, IN v_username text)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_updates integer;
BEGIN

/*
    Unpivot the 30 possible menu FK columns from gd_offence_revision
    into a single stream of (menu_id, publishing_status)
*/

WITH offence_menu_links AS (

    SELECT f_menu_01 AS om_id, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_01 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_02, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_02 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_03, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_03 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_04, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_04 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_05, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_05 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_06, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_06 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_07, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_07 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_08, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_08 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_09, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_09 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_10, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_10 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_11, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_11 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_12, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_12 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_13, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_13 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_14, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_14 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_15, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_15 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_16, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_16 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_17, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_17 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_18, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_18 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_19, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_19 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_20, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_20 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_21, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_21 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_22, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_22 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_23, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_23 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_24, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_24 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_25, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_25 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_26, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_26 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_27, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_27 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_28, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_28 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_29, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_29 IS NOT NULL
      AND b_batchid = v_batchid

    UNION ALL
    SELECT f_menu_30, publishing_status
    FROM csds.gd_offence_revision
    WHERE f_menu_30 IS NOT NULL
      AND b_batchid = v_batchid

),

menu_status_calc AS (

    SELECT
        om_id,

        CASE
            WHEN SUM(CASE WHEN publishing_status = 'Active' THEN 1 ELSE 0 END) > 0
                THEN 'Active'

            WHEN SUM(CASE WHEN publishing_status = 'Future' THEN 1 ELSE 0 END) > 0
                 AND SUM(CASE WHEN publishing_status = 'Active' THEN 1 ELSE 0 END) = 0
                THEN 'Future'

            ELSE 'Inactive'
        END AS derived_status

    FROM offence_menu_links
    GROUP BY om_id
),

upd AS (

    UPDATE csds.gd_ote_menu m
       SET publishing_status = s.derived_status,
           b_batchid         = v_batchid,
           b_upddate         = now(),
           b_updator         = v_username
    FROM menu_status_calc s
    WHERE m.om_id = s.om_id
      AND COALESCE(m.publishing_status,'###') <> s.derived_status

    RETURNING 1
)

SELECT COUNT(*) INTO v_updates FROM upd;

RAISE NOTICE 'OTEMenu Publishing Status Updated: %', v_updates;

END;
$procedure$
;

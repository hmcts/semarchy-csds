-- DROP PROCEDURE csds.proc_unpivot_terminal_entries(varchar, numeric, varchar);

CREATE OR REPLACE PROCEDURE csds.proc_unpivot_terminal_entries(v_schemaname character varying, v_currentloadid numeric, v_username character varying)
 LANGUAGE plpgsql
AS $procedure$

DECLARE
    v_batchid  numeric(38);
    v_parentid numeric(38);
BEGIN
    -- Resolve batch id for this load
    SELECT b_batchid INTO v_batchid
    FROM csds.dl_batch
    WHERE b_loadid = v_currentloadid;

    RAISE NOTICE 'BatchId %', v_batchid;

    -- Resolve the single offence revision id for this load
    SELECT ofr_id INTO v_parentid
    FROM csds.sa_offence_revision
    WHERE b_loadid = v_currentloadid;

    RAISE NOTICE 'ParentId %', v_parentid;

    /*
       Single statement using CTEs: build source set -> delete flagged -> upsert remaining
       NOTE: Requires a UNIQUE index on (f_offence_revision, entry_number) in target.
    */
    WITH src AS (
        SELECT
            r.ofr_id AS f_offence_revision,
            CAST(v.entry_number AS numeric(5))          AS entry_number,
            v.entry_format,
            v.standard_entry_identifier,
            v.minimum,
            v.maximum,
            v.entry_prompt,
            v.menu                                       AS f_offnc_trmnl_entry_mnu,
            v.delete_indicator,
            r.changed_by,
            r.changed_date
        FROM csds.sa_offence_revision r
        CROSS JOIN LATERAL (
            VALUES
              (r.t01entry_number, r.t01entry_format, r.t01standard_entry_identifier, r.t01minimum, r.t01maximum, r.t01entry_prompt, r.f_menu_01, r.t01delete_indicator),
              (r.t02entry_number, r.t02entry_format, r.t02standard_entry_identifier, r.t02minimum, r.t02maximum, r.t02entry_prompt, r.f_menu_02, r.t02delete_indicator),
              (r.t03entry_number, r.t03entry_format, r.t03standard_entry_identifier, r.t03minimum, r.t03maximum, r.t03entry_prompt, r.f_menu_03, r.t03delete_indicator),
              (r.t04entry_number, r.t04entry_format, r.t04standard_entry_identifier, r.t04minimum, r.t04maximum, r.t04entry_prompt, r.f_menu_04, r.t04delete_indicator),
              (r.t05entry_number, r.t05entry_format, r.t05standard_entry_identifier, r.t05minimum, r.t05maximum, r.t05entry_prompt, r.f_menu_05, r.t05delete_indicator),
              (r.t06entry_number, r.t06entry_format, r.t06standard_entry_identifier, r.t06minimum, r.t06maximum, r.t06entry_prompt, r.f_menu_06, r.t06delete_indicator),
              (r.t07entry_number, r.t07entry_format, r.t07standard_entry_identifier, r.t07minimum, r.t07maximum, r.t07entry_prompt, r.f_menu_07, r.t07delete_indicator),
              (r.t08entry_number, r.t08entry_format, r.t08standard_entry_identifier, r.t08minimum, r.t08maximum, r.t08entry_prompt, r.f_menu_08, r.t08delete_indicator),
              (r.t09entry_number, r.t09entry_format, r.t09standard_entry_identifier, r.t09minimum, r.t09maximum, r.t09entry_prompt, r.f_menu_09, r.t09delete_indicator),
              (r.t10entry_number, r.t10entry_format, r.t10standard_entry_identifier, r.t10minimum, r.t10maximum, r.t10entry_prompt, r.f_menu_10, r.t10delete_indicator),
              (r.t11entry_number, r.t11entry_format, r.t11standard_entry_identifier, r.t11minimum, r.t11maximum, r.t11entry_prompt, r.f_menu_11, r.t11delete_indicator),
              (r.t12entry_number, r.t12entry_format, r.t12standard_entry_identifier, r.t12minimum, r.t12maximum, r.t12entry_prompt, r.f_menu_12, r.t12delete_indicator),
              (r.t13entry_number, r.t13entry_format, r.t13standard_entry_identifier, r.t13minimum, r.t13maximum, r.t13entry_prompt, r.f_menu_13, r.t13delete_indicator),
              (r.t14entry_number, r.t14entry_format, r.t14standard_entry_identifier, r.t14minimum, r.t14maximum, r.t14entry_prompt, r.f_menu_14, r.t14delete_indicator),
              (r.t15entry_number, r.t15entry_format, r.t15standard_entry_identifier, r.t15minimum, r.t15maximum, r.t15entry_prompt, r.f_menu_15, r.t15delete_indicator),
              (r.t16entry_number, r.t16entry_format, r.t16standard_entry_identifier, r.t16minimum, r.t16maximum, r.t16entry_prompt, r.f_menu_16, r.t16delete_indicator),
              (r.t17entry_number, r.t17entry_format, r.t17standard_entry_identifier, r.t17minimum, r.t17maximum, r.t17entry_prompt, r.f_menu_17, r.t17delete_indicator),
              (r.t18entry_number, r.t18entry_format, r.t18standard_entry_identifier, r.t18minimum, r.t18maximum, r.t18entry_prompt, r.f_menu_18, r.t18delete_indicator),
              (r.t19entry_number, r.t19entry_format, r.t19standard_entry_identifier, r.t19minimum, r.t19maximum, r.t19entry_prompt, r.f_menu_19, r.t19delete_indicator),
              (r.t20entry_number, r.t20entry_format, r.t20standard_entry_identifier, r.t20minimum, r.t20maximum, r.t20entry_prompt, r.f_menu_20, r.t20delete_indicator),
              (r.t21entry_number, r.t21entry_format, r.t21standard_entry_identifier, r.t21minimum, r.t21maximum, r.t21entry_prompt, r.f_menu_21, r.t21delete_indicator),
              (r.t22entry_number, r.t22entry_format, r.t22standard_entry_identifier, r.t22minimum, r.t22maximum, r.t22entry_prompt, r.f_menu_22, r.t22delete_indicator),
              (r.t23entry_number, r.t23entry_format, r.t23standard_entry_identifier, r.t23minimum, r.t23maximum, r.t23entry_prompt, r.f_menu_23, r.t23delete_indicator),
              (r.t24entry_number, r.t24entry_format, r.t24standard_entry_identifier, r.t24minimum, r.t24maximum, r.t24entry_prompt, r.f_menu_24, r.t24delete_indicator),
              (r.t25entry_number, r.t25entry_format, r.t25standard_entry_identifier, r.t25minimum, r.t25maximum, r.t25entry_prompt, r.f_menu_25, r.t25delete_indicator),
              (r.t26entry_number, r.t26entry_format, r.t26standard_entry_identifier, r.t26minimum, r.t26maximum, r.t26entry_prompt, r.f_menu_26, r.t26delete_indicator),
              (r.t27entry_number, r.t27entry_format, r.t27standard_entry_identifier, r.t27minimum, r.t27maximum, r.t27entry_prompt, r.f_menu_27, r.t27delete_indicator),
              (r.t28entry_number, r.t28entry_format, r.t28standard_entry_identifier, r.t28minimum, r.t28maximum, r.t28entry_prompt, r.f_menu_28, r.t28delete_indicator),
              (r.t29entry_number, r.t29entry_format, r.t29standard_entry_identifier, r.t29minimum, r.t29maximum, r.t29entry_prompt, r.f_menu_29, r.t29delete_indicator),
              (r.t30entry_number, r.t30entry_format, r.t30standard_entry_identifier, r.t30minimum, r.t30maximum, r.t30entry_prompt, r.f_menu_30, r.t30delete_indicator),
              (r.t31entry_number, r.t31entry_format, r.t31standard_entry_identifier, r.t31minimum, r.t31maximum, r.t31entry_prompt, r.f_menu_31, r.t31delete_indicator),
              (r.t32entry_number, r.t32entry_format, r.t32standard_entry_identifier, r.t32minimum, r.t32maximum, r.t32entry_prompt, r.f_menu_32, r.t32delete_indicator),
              (r.t33entry_number, r.t33entry_format, r.t33standard_entry_identifier, r.t33minimum, r.t33maximum, r.t33entry_prompt, r.f_menu_33, r.t33delete_indicator),
              (r.t34entry_number, r.t34entry_format, r.t34standard_entry_identifier, r.t34minimum, r.t34maximum, r.t34entry_prompt, r.f_menu_34, r.t34delete_indicator),
              (r.t35entry_number, r.t35entry_format, r.t35standard_entry_identifier, r.t35minimum, r.t35maximum, r.t35entry_prompt, r.f_menu_35, r.t35delete_indicator),
              (r.t36entry_number, r.t36entry_format, r.t36standard_entry_identifier, r.t36minimum, r.t36maximum, r.t36entry_prompt, r.f_menu_36, r.t36delete_indicator),
              (r.t37entry_number, r.t37entry_format, r.t37standard_entry_identifier, r.t37minimum, r.t37maximum, r.t37entry_prompt, r.f_menu_37, r.t37delete_indicator),
              (r.t38entry_number, r.t38entry_format, r.t38standard_entry_identifier, r.t38minimum, r.t38maximum, r.t38entry_prompt, r.f_menu_38, r.t38delete_indicator),
              (r.t39entry_number, r.t39entry_format, r.t39standard_entry_identifier, r.t39minimum, r.t39maximum, r.t39entry_prompt, r.f_menu_39, r.t39delete_indicator),
              (r.t40entry_number, r.t40entry_format, r.t40standard_entry_identifier, r.t40minimum, r.t40maximum, r.t40entry_prompt, r.f_menu_40, r.t40delete_indicator),
              (r.t41entry_number, r.t41entry_format, r.t41standard_entry_identifier, r.t41minimum, r.t41maximum, r.t41entry_prompt, r.f_menu_41, r.t41delete_indicator),
              (r.t42entry_number, r.t42entry_format, r.t42standard_entry_identifier, r.t42minimum, r.t42maximum, r.t42entry_prompt, r.f_menu_42, r.t42delete_indicator),
              (r.t43entry_number, r.t43entry_format, r.t43standard_entry_identifier, r.t43minimum, r.t43maximum, r.t43entry_prompt, r.f_menu_43, r.t43delete_indicator),
              (r.t44entry_number, r.t44entry_format, r.t44standard_entry_identifier, r.t44minimum, r.t44maximum, r.t44entry_prompt, r.f_menu_44, r.t44delete_indicator),
              (r.t45entry_number, r.t45entry_format, r.t45standard_entry_identifier, r.t45minimum, r.t45maximum, r.t45entry_prompt, r.f_menu_45, r.t45delete_indicator),
              (r.t46entry_number, r.t46entry_format, r.t46standard_entry_identifier, r.t46minimum, r.t46maximum, r.t46entry_prompt, r.f_menu_46, r.t46delete_indicator),
              (r.t47entry_number, r.t47entry_format, r.t47standard_entry_identifier, r.t47minimum, r.t47maximum, r.t47entry_prompt, r.f_menu_47, r.t47delete_indicator),
              (r.t48entry_number, r.t48entry_format, r.t48standard_entry_identifier, r.t48minimum, r.t48maximum, r.t48entry_prompt, r.f_menu_48, r.t48delete_indicator),
              (r.t49entry_number, r.t49entry_format, r.t49standard_entry_identifier, r.t49minimum, r.t49maximum, r.t49entry_prompt, r.f_menu_49, r.t49delete_indicator),
              (r.t50entry_number, r.t50entry_format, r.t50standard_entry_identifier, r.t50minimum, r.t50maximum, r.t50entry_prompt, r.f_menu_50, r.t50delete_indicator)
        ) AS v(entry_number, entry_format, standard_entry_identifier, minimum, maximum, entry_prompt, menu, delete_indicator)
        WHERE r.b_loadid = v_currentloadid
          AND r.ofr_id   = v_parentid
    ), del AS (
        DELETE FROM csds.gd_offence_terminal_entry t
        USING src s
        WHERE t.f_offence_revision = s.f_offence_revision
          AND t.entry_number       = s.entry_number
          AND s.entry_number IS NOT NULL
          AND s.delete_indicator IS TRUE
        RETURNING t.f_offence_revision, t.entry_number
    )
    INSERT INTO csds.gd_offence_terminal_entry (
        ote_id,
        b_classname,
        b_batchid,
        b_credate,
        b_upddate,
        b_creator,
        b_updator,
        f_offence_revision,
        entry_number,
        entry_format,
        standard_entry_identifier,
        minimum,
        maximum,
        entry_prompt,
        version_number,
        changed_by,
        changed_date,
        f_offnc_trmnl_entry_mnu
    )
    SELECT
        nextval('csds.seq_offence_terminal_entry'),
        'OffenceTerminalEntry',
        v_batchid,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        v_username,
        v_username,
        s.f_offence_revision,
        s.entry_number,
        s.entry_format,
        s.standard_entry_identifier,
        s.minimum,
        s.maximum,
        s.entry_prompt,
        1,
        s.changed_by,
        s.changed_date,
        s.f_offnc_trmnl_entry_mnu
    FROM src s
    WHERE s.entry_number IS NOT NULL
      AND COALESCE(s.delete_indicator, false) = false
    ON CONFLICT (f_offence_revision, entry_number)
    DO UPDATE SET
        b_classname               = EXCLUDED.b_classname,
        b_batchid                 = EXCLUDED.b_batchid,
        b_upddate                 = EXCLUDED.b_upddate,
        b_updator                 = EXCLUDED.b_updator,
        entry_format              = EXCLUDED.entry_format,
        standard_entry_identifier = EXCLUDED.standard_entry_identifier,
        minimum                   = EXCLUDED.minimum,
        maximum                   = EXCLUDED.maximum,
        entry_prompt              = EXCLUDED.entry_prompt,
        version_number            = EXCLUDED.version_number,
        changed_by                = EXCLUDED.changed_by,
        changed_date              = EXCLUDED.changed_date,
        f_offnc_trmnl_entry_mnu   = EXCLUDED.f_offnc_trmnl_entry_mnu;

    RAISE NOTICE 'Complete %', v_batchid;
END;

$procedure$
;
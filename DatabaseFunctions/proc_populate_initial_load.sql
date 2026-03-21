-- DROP PROCEDURE csds.proc_populate_initial_load(text);

CREATE OR REPLACE PROCEDURE csds.proc_populate_initial_load(IN p_user_name text DEFAULT NULL::text)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_userName TEXT := p_user_name;
    v_load_id  NUMERIC;
    v_header_count INT;
    v_revision_count INT;
BEGIN
    -- ============================================
    -- 1️⃣ Generate new Load ID
    -- ============================================
    v_load_id := semarchy_repository.get_new_loadid(
        'CSDS',
        'PSSOffencesMigrationLoad',
        'Load Offence Revision',
        v_userName
    );
    RAISE NOTICE 'Load ID: %', v_load_id;

   DROP TABLE IF EXISTS tmp_otemenu_map;

    CREATE TEMP TABLE tmp_otemenu_map AS
    SELECT
    m.om_id as old_om_id,
    nextval('csds.seq_ote_menu') AS new_om_id,
    b_credate,
    b_upddate,
    b_creator,
    b_updator,
    mcc_mcc_id,
    name,
    notes,
    version_number,
    changed_by,
    changed_date,
    authoring_status,
    publishing_status,
    version_type,
    f_release_package,
    PSSOTEMenuID
    FROM csds."migratedOTEMenus" m;


-- Insert data from migratedOTEMenus into sa_ote_menu
INSERT INTO csds.sa_ote_menu (
    b_loadid,
    b_classname,
    om_id,
    b_credate,
    b_upddate,
    b_creator,
    b_updator,
    mcc_mcc_id,
    name,
    notes,
    version_number,
    changed_by,
    changed_date,
    authoring_status,
    publishing_status,
    version_type,
    f_release_package,
    pss_om_id
)
SELECT
    v_load_id,
    'OffenceMenu',
    new_om_id,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    p_user_name,
    p_user_name,
    mcc_mcc_id,
    name,
    notes,
    version_number,
    changed_by,
    changed_date,
    'Published',
    publishing_status,
    version_type,
    f_release_package,
     PSSOTEMenuID
FROM tmp_otemenu_map;

-- Insert data from migratedMenuOptions into sa_ote_menu_options
INSERT INTO csds.sa_ote_menu_options (
    b_loadid,
    b_classname,
    omo_id,
b_credate,
b_upddate,
    b_creator,
    b_updator,
    mcc_mcc_id,
    option_number,
    option_text,
    version_number,
    changed_by,
    changed_date,
    delete_indicator,
    e01element_number,
    e01entry_format,
    e01minimum,
    e01maximum,
    e01entry_prompt,
    e01delete_indicator,
    e02element_number,
    e02entry_format,
    e02minimum,
    e02maximum,
    e02entry_prompt,
    e02delete_indicator,
    e03element_number,
    e03entry_format,
    e03minimum,
    e03maximum,
    e03entry_prompt,
    e03delete_indicator,
    e04element_number,
    e04entry_format,
    e04minimum,
    e04maximum,
    e04entry_prompt,
    e04delete_indicator,
    e05element_number,
    e05entry_format,
    e05minimum,
    e05maximum,
    e05entry_prompt,
    e05delete_indicator,
    e06element_number,
    e06entry_format,
    e06minimum,
    e06maximum,
    e06entry_prompt,
    e06delete_indicator,
    e07element_number,
    e07entry_format,
    e07minimum,
    e07maximum,
    e07entry_prompt,
    e07delete_indicator,
    e08element_number,
    e08entry_format,
    e08minimum,
    e08maximum,
    e08entry_prompt,
    e08delete_indicator,
    e09element_number,
    e09entry_format,
    e09minimum,
    e09maximum,
    e09entry_prompt,
    e09delete_indicator,
    e10element_number,
    e10entry_format,
    e10minimum,
    e10maximum,
    e10entry_prompt,
    e10delete_indicator,
    f_ote_optn_menu,
    pss_omo_id
)
SELECT
    v_load_id,
    'OffenceMenuOptions',
    nextval('csds.seq_ote_menu_options'),
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    p_user_name,
    p_user_name,
    o.mcc_mcc_id,
    o.option_number,
    o.option_text,
    o.version_number,
    o.changed_by,
    o.changed_date,
    delete_indicator,
    e01element_number,
    e01entry_format,
    e01minimum,
    e01maximum,
    e01entry_prompt,
    e01delete_indicator,
    e02element_number,
    e02entry_format,
    e02minimum,
    e02maximum,
    e02entry_prompt,
    e02delete_indicator,
    e03element_number,
    e03entry_format,
    e03minimum,
    e03maximum,
    e03entry_prompt,
    e03delete_indicator,
    e04element_number,
    e04entry_format,
    e04minimum,
    e04maximum,
    e04entry_prompt,
    e04delete_indicator,
    e05element_number,
    e05entry_format,
    e05minimum,
    e05maximum,
    e05entry_prompt,
    e05delete_indicator,
    e06element_number,
    e06entry_format,
    e06minimum,
    e06maximum,
    e06entry_prompt,
    e06delete_indicator,
    e07element_number,
    e07entry_format,
    e07minimum,
    e07maximum,
    e07entry_prompt,
    e07delete_indicator,
    e08element_number,
    e08entry_format,
    e08minimum,
    e08maximum,
    e08entry_prompt,
    e08delete_indicator,
    e09element_number,
    e09entry_format,
    e09minimum,
    e09maximum,
    e09entry_prompt,
    e09delete_indicator,
    e10element_number,
    e10entry_format,
    e10minimum,
    e10maximum,
    e10entry_prompt,
    e10delete_indicator,
    new_om_id,
    omo_id
FROM csds."migratedMenuOptions" o
    JOIN tmp_otemenu_map t ON o.f_ote_optn_menu = t.old_om_id;

   -- ============================================
    -- 2️⃣ Build header mapping in a temporary table
    -- ============================================

    DROP TABLE IF EXISTS tmp_header_map;

    CREATE TEMP TABLE tmp_header_map AS
    SELECT
        h.oh_id            AS old_oh_id,
        nextval('csds.seq_offence_header') AS new_oh_id,
        h.b_classname,
        h.b_credate,
        h.b_upddate,
        h.b_creator,
        h.b_updator,
        h.pnld_start_date,
        h.pnld_end_date,
        h.pssoffenceheaderid,
        h.psspnldoffenceheaderid
    FROM csds.migratedoffenceheaders h;

     -- Insert headers
    INSERT INTO csds.SA_OFFENCE_HEADER (
       b_loadid, oh_id, b_classname, b_credate, b_upddate,
       b_creator, b_updator, pnld_start_date, pnld_end_date,
       pss_oh_id, pss_poh_id
    )
    SELECT
      v_load_id, new_oh_id, b_classname, CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    p_user_name,
    p_user_name, pnld_start_date, pnld_end_date, pssoffenceheaderid,
    psspnldoffenceheaderid
    FROM tmp_header_map;

    GET DIAGNOSTICS v_header_count = ROW_COUNT;
    RAISE NOTICE 'Inserted % header rows', v_header_count;

    -- ============================================
    -- 3️⃣ Insert Offence Revisions
    -- ============================================
    INSERT INTO csds.SA_OFFENCE_REVISION (
        b_loadid, ofr_id, b_classname, b_credate, b_upddate,
        b_creator, b_updator, recordable, reportable, cjs_title, custodial_indicator,
        date_used_from, date_used_to, standard_list, traffic_control, version_number,
        dvla_code, offence_notes, maximum_penalty, description,
        derived_from_cjs_code, ho_class, ho_subclass, proceedings_code, sl_cjs_title,
        pnld_stndrd_offnc_wording, sl_pnld_standard_off_word, pnld_date_of_last_update,
        prosecution_time_limit, max_fine_type_magct_code, max_fine_type_magct_desc,
        mode_of_trial, endorsable_flag, location_flag, principal_offnc_category,
        user_offence_wording, user_statement_of_facts, user_acts_and_section,
        sl_off_statmnt_fct_txt, sl_offence_wording_txt, sl_offence_act_sec_txt,
        can_be_bulk, initial_fee, contested_fee, application_synonym,
        exparte, jurisdiction, appeal_flag, summons_template_type, link_type, hearing_code,
        applicant_appellant_flag, plea_applicable_flag, active_offence_order,
        commissioner_of_oath, breach_type, court_of_appeal_flag, court_extract_available,
        listing_ntfctn_tmplt, boxwork_ntfctn_tmplt, prosecutor_as_third_party,
        resentence_activation_cde, prefix, obsolete_indicator, 
        cjs_code, area, blocked, offence_code,
        pnld_offence_start_date, pnld_offence_end_date, authoring_status, publishing_status, offence_type, mis_classification,
        offence_class, pss_changed_by, pss_changed_date, pss_ofr_id, 
pss_cca_id,
pss_reference_offence_id,
pss_por_id,
pss_cad_id,
pss_oas_id,
pss_ow_id,
pss_osf_id,
offence_source,
version_type,
current_record_indicator,
        -- terminal entries t01–t30
t01entry_number, t01minimum, t01maximum, t01entry_format, t01entry_prompt,
t01standard_entry_identifier, t01delete_indicator, t01sys_terminal_entry_short,
t02entry_number, t02minimum, t02maximum, t02entry_format, t02entry_prompt,
t02standard_entry_identifier, t02delete_indicator, t02sys_terminal_entry_short,
t03entry_number, t03minimum, t03maximum, t03entry_format, t03entry_prompt,
t03standard_entry_identifier, t03delete_indicator, t03sys_terminal_entry_short,
t04entry_number, t04minimum, t04maximum, t04entry_format, t04entry_prompt,
t04standard_entry_identifier, t04delete_indicator, t04sys_terminal_entry_short,
t05entry_number, t05minimum, t05maximum, t05entry_format, t05entry_prompt,
t05standard_entry_identifier, t05delete_indicator, t05sys_terminal_entry_short,
t06entry_number, t06minimum, t06maximum, t06entry_format, t06entry_prompt,
t06standard_entry_identifier, t06delete_indicator, t06sys_terminal_entry_short,
t07entry_number, t07minimum, t07maximum, t07entry_format, t07entry_prompt,
t07standard_entry_identifier, t07delete_indicator, t07sys_terminal_entry_short,
t08entry_number, t08minimum, t08maximum, t08entry_format, t08entry_prompt,
t08standard_entry_identifier, t08delete_indicator, t08sys_terminal_entry_short,
t09entry_number, t09minimum, t09maximum, t09entry_format, t09entry_prompt,
t09standard_entry_identifier, t09delete_indicator, t09sys_terminal_entry_short,
t10entry_number, t10minimum, t10maximum, t10entry_format, t10entry_prompt,
t10standard_entry_identifier, t10delete_indicator, t10sys_terminal_entry_short,
t11entry_number, t11minimum, t11maximum, t11entry_format, t11entry_prompt,
t11standard_entry_identifier, t11delete_indicator, t11sys_terminal_entry_short,
t12entry_number, t12minimum, t12maximum, t12entry_format, t12entry_prompt,
t12standard_entry_identifier, t12delete_indicator, t12sys_terminal_entry_short,
t13entry_number, t13minimum, t13maximum, t13entry_format, t13entry_prompt,
t13standard_entry_identifier, t13delete_indicator, t13sys_terminal_entry_short,
t14entry_number, t14minimum, t14maximum, t14entry_format, t14entry_prompt,
t14standard_entry_identifier, t14delete_indicator, t14sys_terminal_entry_short,
t15entry_number, t15minimum, t15maximum, t15entry_format, t15entry_prompt,
t15standard_entry_identifier, t15delete_indicator, t15sys_terminal_entry_short,
t16entry_number, t16minimum, t16maximum, t16entry_format, t16entry_prompt,
t16standard_entry_identifier, t16delete_indicator, t16sys_terminal_entry_short,
t17entry_number, t17minimum, t17maximum, t17entry_format, t17entry_prompt,
t17standard_entry_identifier, t17delete_indicator, t17sys_terminal_entry_short,
t18entry_number, t18minimum, t18maximum, t18entry_format, t18entry_prompt,
t18standard_entry_identifier, t18delete_indicator, t18sys_terminal_entry_short,
t19entry_number, t19minimum, t19maximum, t19entry_format, t19entry_prompt,
t19standard_entry_identifier, t19delete_indicator, t19sys_terminal_entry_short,
t20entry_number, t20minimum, t20maximum, t20entry_format, t20entry_prompt,
t20standard_entry_identifier, t20delete_indicator, t20sys_terminal_entry_short,
t21entry_number, t21minimum, t21maximum, t21entry_format, t21entry_prompt,
t21standard_entry_identifier, t21delete_indicator, t21sys_terminal_entry_short,
t22entry_number, t22minimum, t22maximum, t22entry_format, t22entry_prompt,
t22standard_entry_identifier, t22delete_indicator, t22sys_terminal_entry_short,
t23entry_number, t23minimum, t23maximum, t23entry_format, t23entry_prompt,
t23standard_entry_identifier, t23delete_indicator, t23sys_terminal_entry_short,
t24entry_number, t24minimum, t24maximum, t24entry_format, t24entry_prompt,
t24standard_entry_identifier, t24delete_indicator, t24sys_terminal_entry_short,
t25entry_number, t25minimum, t25maximum, t25entry_format, t25entry_prompt,
t25standard_entry_identifier, t25delete_indicator, t25sys_terminal_entry_short,
t26entry_number, t26minimum, t26maximum, t26entry_format, t26entry_prompt,
t26standard_entry_identifier, t26delete_indicator, t26sys_terminal_entry_short,
t27entry_number, t27minimum, t27maximum, t27entry_format, t27entry_prompt,
t27standard_entry_identifier, t27delete_indicator, t27sys_terminal_entry_short,
t28entry_number, t28minimum, t28maximum, t28entry_format, t28entry_prompt,
t28standard_entry_identifier, t28delete_indicator, t28sys_terminal_entry_short,
t29entry_number, t29minimum, t29maximum, t29entry_format, t29entry_prompt,
t29standard_entry_identifier, t29delete_indicator, t29sys_terminal_entry_short,
t30entry_number, t30minimum, t30maximum, t30entry_format, t30entry_prompt,
t30standard_entry_identifier, t30delete_indicator, t30sys_terminal_entry_short,
f_menu_01, f_menu_02, f_menu_03, f_menu_04, f_menu_05,
f_menu_06, f_menu_07, f_menu_08, f_menu_09, f_menu_10,
f_menu_11, f_menu_12, f_menu_13, f_menu_14, f_menu_15,
f_menu_16, f_menu_17, f_menu_18, f_menu_19, f_menu_20,
f_menu_21, f_menu_22, f_menu_23, f_menu_24, f_menu_25,
f_menu_26, f_menu_27, f_menu_28, f_menu_29, f_menu_30,
        f_offence_header
    )
SELECT
        v_load_id,
        nextval('csds.seq_offence_revision'),
        r.b_classname,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    p_user_name,
    p_user_name,
        r.recordable,
        r.reportable,
        r.cjs_title,
        r.custodial_indicator,
        r.date_used_from,
        r.date_used_to,
        r.standard_list,
        r.traffic_control,
        r.version_number,
        r.dvla_code,
        r.offence_notes,
        r.maximum_penalty,
        r.description,
        r.derived_from_cjs_code,
        r.ho_class,
        r.ho_subclass,
        r.proceedings_code,
        r.sl_cjs_title,
        r.pnld_stndrd_offnc_wording,
        r.sl_pnld_standard_off_word,
        r.pnld_date_of_last_update,
        r.prosecution_time_limit,
        r.max_fine_type_magct_code,
        r.max_fine_type_magct_desc,
        r.mode_of_trial,
        r.endorsable_flag,
        r.location_flag,
        r.principal_offnc_category,
        r.user_offence_wording,
        r.user_statement_of_facts,
        r.user_acts_and_section,
        r.sl_off_statmnt_fct_txt,
        r.sl_offence_wording_text,
        r.sl_offence_act_sec_txt,
        r.can_be_bulk,
        r.initial_fee_applicable as initial_fee,
        r.contested_fee_applicable as contested_fee,
        r.application_synonym,
        r.exparte,
        r.jurisdiction,
        r.appeal_flag,
        r.summons_template_type,
        r.link_type,
        r.hearing_code,
        r.applicant_appellant_flag,
        r.plea_applicable_flag,
        r.active_offence_order,
        r.commissioner_of_oath,
        r.breach_type,
        r.court_of_appeal_flag,
        r.court_extract_available,
        r.listing_notification_temp,
        r.boxwork_notification_temp,
        r.prosecutor_as_third_party,
        r.resentencing_activation_c,
        r.prefix,
        r.obsolete_indicator,
        r.cjs_code,
        r.area,
        r.blocked,
        r.offence_code,
        r.pnld_offence_start_date,
        r.pnld_offence_end_date,
	   'Published',
        CASE
          WHEN r.date_used_from > CURRENT_DATE THEN 'Future'
          WHEN r.date_used_from <= CURRENT_DATE
             AND (r.date_used_to >= CURRENT_DATE OR r.date_used_to IS NULL) THEN 'Active'
          ELSE 'Inactive'
        END,
        r.offence_type,
        r.mis_classification,
	   r.offence_class,
        r.pss_changed_by,
        r.pss_changed_date,
        r.pss_ofr_id as pss_offence_revision_id,
r.pss_cad_id,
r.pss_xhb_ref_id,
r.pss_pnld_ofr_id,
r.pss_ad_id,
r.pss_aas_id,
r.pss_ow_id,
r.pss_osf_id,
r.offence_source,
r.version_type,
r.currentrecordindicator,
        -- t01–t30
r.t01entry_number, r.t01minimum, r.t01maximum, r.t01entry_format, r.t01entry_prompt,
r.t01standard_entry_identifier, r.t01delete_indicator, r.t01sys_terminal_entry_short,
r.t02entry_number, r.t02minimum, r.t02maximum, r.t02entry_format, r.t02entry_prompt,
r.t02standard_entry_identifier, r.t02delete_indicator, r.t02sys_terminal_entry_short,
r.t03entry_number, r.t03minimum, r.t03maximum, r.t03entry_format, r.t03entry_prompt,
r.t03standard_entry_identifier, r.t03delete_indicator, r.t03sys_terminal_entry_short,
r.t04entry_number, r.t04minimum, r.t04maximum, r.t04entry_format, r.t04entry_prompt,
r.t04standard_entry_identifier, r.t04delete_indicator, r.t04sys_terminal_entry_short,
r.t05entry_number, r.t05minimum, r.t05maximum, r.t05entry_format, r.t05entry_prompt,
r.t05standard_entry_identifier, r.t05delete_indicator, r.t05sys_terminal_entry_short,
r.t06entry_number, r.t06minimum, r.t06maximum, r.t06entry_format, r.t06entry_prompt,
r.t06standard_entry_identifier, r.t06delete_indicator, r.t06sys_terminal_entry_short,
r.t07entry_number, r.t07minimum, r.t07maximum, r.t07entry_format, r.t07entry_prompt,
r.t07standard_entry_identifier, r.t07delete_indicator, r.t07sys_terminal_entry_short,
r.t08entry_number, r.t08minimum, r.t08maximum, r.t08entry_format, r.t08entry_prompt,
r.t08standard_entry_identifier, r.t08delete_indicator, r.t08sys_terminal_entry_short,
r.t09entry_number, r.t09minimum, r.t09maximum, r.t09entry_format, r.t09entry_prompt,
r.t09standard_entry_identifier, r.t09delete_indicator, r.t09sys_terminal_entry_short,
r.t10entry_number, r.t10minimum, r.t10maximum, r.t10entry_format, r.t10entry_prompt,
r.t10standard_entry_identifier, r.t10delete_indicator, r.t10sys_terminal_entry_short,
r.t11entry_number, r.t11minimum, r.t11maximum, r.t11entry_format, r.t11entry_prompt,
r.t11standard_entry_identifier, r.t11delete_indicator, r.t11sys_terminal_entry_short,
r.t12entry_number, r.t12minimum, r.t12maximum, r.t12entry_format, r.t12entry_prompt,
r.t12standard_entry_identifier, r.t12delete_indicator, r.t12sys_terminal_entry_short,
r.t13entry_number, r.t13minimum, r.t13maximum, r.t13entry_format, r.t13entry_prompt,
r.t13standard_entry_identifier, r.t13delete_indicator, r.t13sys_terminal_entry_short,
r.t14entry_number, r.t14minimum, r.t14maximum, r.t14entry_format, r.t14entry_prompt,
r.t14standard_entry_identifier, r.t14delete_indicator, r.t14sys_terminal_entry_short,
r.t15entry_number, r.t15minimum, r.t15maximum, r.t15entry_format, r.t15entry_prompt,
r.t15standard_entry_identifier, r.t15delete_indicator, r.t15sys_terminal_entry_short,
r.t16entry_number, r.t16minimum, r.t16maximum, r.t16entry_format, r.t16entry_prompt,
r.t16standard_entry_identifier, r.t16delete_indicator, r.t16sys_terminal_entry_short,
r.t17entry_number, r.t17minimum, r.t17maximum, r.t17entry_format, r.t17entry_prompt,
r.t17standard_entry_identifier, r.t17delete_indicator, r.t17sys_terminal_entry_short,
r.t18entry_number, r.t18minimum, r.t18maximum, r.t18entry_format, r.t18entry_prompt,
r.t18standard_entry_identifier, r.t18delete_indicator, r.t18sys_terminal_entry_short,
r.t19entry_number, r.t19minimum, r.t19maximum, r.t19entry_format, r.t19entry_prompt,
r.t19standard_entry_identifier, r.t19delete_indicator, r.t19sys_terminal_entry_short,
r.t20entry_number, r.t20minimum, r.t20maximum, r.t20entry_format, r.t20entry_prompt,
r.t20standard_entry_identifier, r.t20delete_indicator, r.t20sys_terminal_entry_short,
r.t21entry_number, r.t21minimum, r.t21maximum, r.t21entry_format, r.t21entry_prompt,
r.t21standard_entry_identifier, r.t21delete_indicator, r.t21sys_terminal_entry_short,
r.t22entry_number, r.t22minimum, r.t22maximum, r.t22entry_format, r.t22entry_prompt,
r.t22standard_entry_identifier, r.t22delete_indicator, r.t22sys_terminal_entry_short,
r.t23entry_number, r.t23minimum, r.t23maximum, r.t23entry_format, r.t23entry_prompt,
r.t23standard_entry_identifier, r.t23delete_indicator, r.t23sys_terminal_entry_short,
r.t24entry_number, r.t24minimum, r.t24maximum, r.t24entry_format, r.t24entry_prompt,
r.t24standard_entry_identifier, r.t24delete_indicator, r.t24sys_terminal_entry_short,
r.t25entry_number, r.t25minimum, r.t25maximum, r.t25entry_format, r.t25entry_prompt,
r.t25standard_entry_identifier, r.t25delete_indicator, r.t25sys_terminal_entry_short,
r.t26entry_number, r.t26minimum, r.t26maximum, r.t26entry_format, r.t26entry_prompt,
r.t26standard_entry_identifier, r.t26delete_indicator, r.t26sys_terminal_entry_short,
r.t27entry_number, r.t27minimum, r.t27maximum, r.t27entry_format, r.t27entry_prompt,
r.t27standard_entry_identifier, r.t27delete_indicator, r.t27sys_terminal_entry_short,
r.t28entry_number, r.t28minimum, r.t28maximum, r.t28entry_format, r.t28entry_prompt,
r.t28standard_entry_identifier, r.t28delete_indicator, r.t28sys_terminal_entry_short,
r.t29entry_number, r.t29minimum, r.t29maximum, r.t29entry_format, r.t29entry_prompt,
r.t29standard_entry_identifier, r.t29delete_indicator, r.t29sys_terminal_entry_short,
r.t30entry_number, r.t30minimum, r.t30maximum, r.t30entry_format, r.t30entry_prompt,
r.t30standard_entry_identifier, r.t30delete_indicator, r.t30sys_terminal_entry_short,
m01.new_om_id, m02.new_om_id, m03.new_om_id, m04.new_om_id, m05.new_om_id,
m06.new_om_id, m07.new_om_id, m08.new_om_id, m09.new_om_id, m10.new_om_id,
m11.new_om_id, m12.new_om_id, m13.new_om_id, m14.new_om_id, m15.new_om_id,
m16.new_om_id, m17.new_om_id, m18.new_om_id, m19.new_om_id, m20.new_om_id,
m21.new_om_id, m22.new_om_id, m23.new_om_id, m24.new_om_id, m25.new_om_id,
m26.new_om_id, m27.new_om_id, m28.new_om_id, m29.new_om_id, m30.new_om_id,
        t.new_oh_id
    FROM csds.migratedoffencerevisions r
JOIN tmp_header_map t ON r.oh_oh_id = t.old_oh_id
LEFT JOIN tmp_otemenu_map m01 ON r.f_menu_01 = m01.old_om_id
LEFT JOIN tmp_otemenu_map m02 ON r.f_menu_02 = m02.old_om_id
LEFT JOIN tmp_otemenu_map m03 ON r.f_menu_03 = m03.old_om_id
LEFT JOIN tmp_otemenu_map m04 ON r.f_menu_04 = m04.old_om_id
LEFT JOIN tmp_otemenu_map m05 ON r.f_menu_05 = m05.old_om_id
LEFT JOIN tmp_otemenu_map m06 ON r.f_menu_06 = m06.old_om_id
LEFT JOIN tmp_otemenu_map m07 ON r.f_menu_07 = m07.old_om_id
LEFT JOIN tmp_otemenu_map m08 ON r.f_menu_08 = m08.old_om_id
LEFT JOIN tmp_otemenu_map m09 ON r.f_menu_09 = m09.old_om_id
LEFT JOIN tmp_otemenu_map m10 ON r.f_menu_10 = m10.old_om_id
LEFT JOIN tmp_otemenu_map m11 ON r.f_menu_11 = m11.old_om_id
LEFT JOIN tmp_otemenu_map m12 ON r.f_menu_12 = m12.old_om_id
LEFT JOIN tmp_otemenu_map m13 ON r.f_menu_13 = m13.old_om_id
LEFT JOIN tmp_otemenu_map m14 ON r.f_menu_14 = m14.old_om_id
LEFT JOIN tmp_otemenu_map m15 ON r.f_menu_15 = m15.old_om_id
LEFT JOIN tmp_otemenu_map m16 ON r.f_menu_16 = m16.old_om_id
LEFT JOIN tmp_otemenu_map m17 ON r.f_menu_17 = m17.old_om_id
LEFT JOIN tmp_otemenu_map m18 ON r.f_menu_18 = m18.old_om_id
LEFT JOIN tmp_otemenu_map m19 ON r.f_menu_19 = m19.old_om_id
LEFT JOIN tmp_otemenu_map m20 ON r.f_menu_20 = m20.old_om_id
LEFT JOIN tmp_otemenu_map m21 ON r.f_menu_21 = m21.old_om_id
LEFT JOIN tmp_otemenu_map m22 ON r.f_menu_22 = m22.old_om_id
LEFT JOIN tmp_otemenu_map m23 ON r.f_menu_23 = m23.old_om_id
LEFT JOIN tmp_otemenu_map m24 ON r.f_menu_24 = m24.old_om_id
LEFT JOIN tmp_otemenu_map m25 ON r.f_menu_25 = m25.old_om_id
LEFT JOIN tmp_otemenu_map m26 ON r.f_menu_26 = m26.old_om_id
LEFT JOIN tmp_otemenu_map m27 ON r.f_menu_27 = m27.old_om_id
LEFT JOIN tmp_otemenu_map m28 ON r.f_menu_28 = m28.old_om_id
LEFT JOIN tmp_otemenu_map m29 ON r.f_menu_29 = m29.old_om_id
LEFT JOIN tmp_otemenu_map m30 ON r.f_menu_30 = m30.old_om_id;

    GET DIAGNOSTICS v_revision_count = ROW_COUNT;

     RAISE NOTICE 'Inserted % offence revision rows', v_revision_count; 


    -- ============================================
    -- 4️⃣ Submit Load
    -- ============================================
    PERFORM semarchy_repository.submit_load(v_load_id, 'PSSOffencesMigrationLoad', v_userName);

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred. Rolling back load %', v_load_id;
        RAISE;
END;
$procedure$
;

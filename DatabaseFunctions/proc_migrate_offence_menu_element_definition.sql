-- DROP PROCEDURE csds.proc_migrate_offence_menu_element_definition(numeric, text);

CREATE OR REPLACE PROCEDURE csds.proc_migrate_offence_menu_element_definition(IN v_batchid numeric, IN v_username text)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $procedure$
DECLARE
    v_inserts integer;
BEGIN

    WITH source_data AS (
        SELECT 
            g.omo_id,
            g.e01element_number, g.e01entry_format, g.e01minimum, g.e01maximum, g.e01entry_prompt,
            g.e02element_number, g.e02entry_format, g.e02minimum, g.e02maximum, g.e02entry_prompt,
            g.e03element_number, g.e03entry_format, g.e03minimum, g.e03maximum, g.e03entry_prompt,
            g.e04element_number, g.e04entry_format, g.e04minimum, g.e04maximum, g.e04entry_prompt,
            g.e05element_number, g.e05entry_format, g.e05minimum, g.e05maximum, g.e05entry_prompt,
            g.e06element_number, g.e06entry_format, g.e06minimum, g.e06maximum, g.e06entry_prompt,
            g.e07element_number, g.e07entry_format, g.e07minimum, g.e07maximum, g.e07entry_prompt,
            g.e08element_number, g.e08entry_format, g.e08minimum, g.e08maximum, g.e08entry_prompt,
            g.e09element_number, g.e09entry_format, g.e09minimum, g.e09maximum, g.e09entry_prompt,
            g.e10element_number, g.e10entry_format, g.e10minimum, g.e10maximum, g.e10entry_prompt,

            -- PSS values come from migratedMenuOptions
            m.e01pss_oed_id,  m.e01pss_omop_id,
            m.e02pss_oed_id,  m.e02pss_omop_id,
            m.e03pss_oed_id,  m.e03pss_omop_id,
            m.e04pss_oed_id,  m.e04pss_omop_id,
            m.e05pss_oed_id,  m.e05pss_omop_id,
            m.e06pss_oed_id,  m.e06pss_omop_id,
            m.e07pss_oed_id,  m.e07pss_omop_id,
            m.e08pss_oed_id,  m.e08pss_omop_id,
            m.e09pss_oed_id,  m.e09pss_omop_id,
            m.e10pss_oed_id,  m.e10pss_omop_id

        FROM csds.gd_ote_menu_options g
        LEFT JOIN csds."migratedMenuOptions" m
            ON g.pss_omo_id = m.omo_id
        WHERE g.b_batchid = v_batchid
    ),
    expanded AS (
        SELECT 
            s.omo_id AS f_offence_menu_options,
            x.element_number,
            x.entry_format,
            x.oed_min,
            x.oed_max,
            x.entry_prompt,
            x.pss_oed_id,
            x.pss_omop_id
        FROM source_data s
        CROSS JOIN LATERAL (
            VALUES
            (s.e01element_number, s.e01entry_format, s.e01minimum, s.e01maximum, s.e01entry_prompt, s.e01pss_oed_id, s.e01pss_omop_id),
            (s.e02element_number, s.e02entry_format, s.e02minimum, s.e02maximum, s.e02entry_prompt, s.e02pss_oed_id, s.e02pss_omop_id),
            (s.e03element_number, s.e03entry_format, s.e03minimum, s.e03maximum, s.e03entry_prompt, s.e03pss_oed_id, s.e03pss_omop_id),
            (s.e04element_number, s.e04entry_format, s.e04minimum, s.e04maximum, s.e04entry_prompt, s.e04pss_oed_id, s.e04pss_omop_id),
            (s.e05element_number, s.e05entry_format, s.e05minimum, s.e05maximum, s.e05entry_prompt, s.e05pss_oed_id, s.e05pss_omop_id),
            (s.e06element_number, s.e06entry_format, s.e06minimum, s.e06maximum, s.e06entry_prompt, s.e06pss_oed_id, s.e06pss_omop_id),
            (s.e07element_number, s.e07entry_format, s.e07minimum, s.e07maximum, s.e07entry_prompt, s.e07pss_oed_id, s.e07pss_omop_id),
            (s.e08element_number, s.e08entry_format, s.e08minimum, s.e08maximum, s.e08entry_prompt, s.e08pss_oed_id, s.e08pss_omop_id),
            (s.e09element_number, s.e09entry_format, s.e09minimum, s.e09maximum, s.e09entry_prompt, s.e09pss_oed_id, s.e09pss_omop_id),
            (s.e10element_number, s.e10entry_format, s.e10minimum, s.e10maximum, s.e10entry_prompt, s.e10pss_oed_id, s.e10pss_omop_id)
        ) AS x(
            element_number,
            entry_format,
            oed_min,
            oed_max,
            entry_prompt,
            pss_oed_id,
            pss_omop_id
        )
        WHERE x.element_number IS NOT NULL
    ),
    ins AS (
        INSERT INTO csds.gd_ote_element_definitions
            (oed_id, b_classname, b_batchid, b_credate, b_upddate,
             b_creator, b_updator,
             element_number, entry_format, oed_min, oed_max, entry_prompt,
             f_offence_menu_options, pss_oed_id, pss_omop_id)
        SELECT 
            nextval('csds.seq_release_package_content'),
            'OffenceMenuElementDefinition',
            v_batchid,
            now(),
            now(),
            v_username,
            v_username,
            element_number,
            entry_format,
            oed_min,
            oed_max,
            entry_prompt,
            f_offence_menu_options,
            pss_oed_id,
            pss_omop_id
        FROM expanded
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_inserts FROM ins;

    RAISE NOTICE 'Inserted: %', v_inserts;

END;
$procedure$
;

-- SQL Script to extract out the DB functions & stored procedures:
-- SELECT n.nspname 				 AS schema_name
--       ,p.proname 				 AS routine_name
-- 	  ,pg_get_functiondef(p.oid) AS definition
--  FROM pg_proc 		p
--  JOIN pg_namespace 	n
--    ON n.oid = p.pronamespace
-- WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
--   AND n.nspname = 'csds'
--   AND p.proname NOT LIKE 'sem_%'
-- ORDER BY schema_name, routine_name;
-- ============================================
-- Objects that have been dropped from the DB:
-- ============================================
-- fn_sort_terminal_entries
-- fn_terminal_entry_number_calc
-- proc_default_terminal_entries
-- proc_default_terminal_entries_p
-- proc_define_release_package_content
-- proc_offence_sow_sof_substitution
-- proc_populate_release_package_content
-- proc_terminal_entry_sow_sof_prompt_substitution
-- proc_clone_offence_master_data_insert
-- ============================================
--
SELECT pg_get_functiondef(p.oid)||';' AS definition
  FROM pg_proc 		p
  JOIN pg_namespace 	n
    ON n.oid = p.pronamespace
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
   AND n.nspname = 'csds'
   AND p.proname NOT LIKE 'sem_%';
--
CREATE OR REPLACE FUNCTION csds.fn_check_option_number_unique(v_option_number numeric, v_currentloadid numeric)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_count INTEGER;
BEGIN
    -- Count matching rows
    SELECT COUNT(1)
    INTO v_count
    FROM csds.sa_ote_menu_options
    WHERE option_number = v_option_number
      AND b_loadid = v_currentloadid
	  AND v_option_number <> 0;
 
    -- If more than one match exists, uniqueness fails
    IF v_count > 1 THEN
        RETURN 'FALSE';
    ELSE
        RETURN 'TRUE';
    END IF;
END;
$function$
;
--
CREATE OR REPLACE FUNCTION csds.fn_create_offence_header(v_username character varying, v_loadid numeric, v_offence_status character varying)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_oh_id numeric;
BEGIN
    -- Generate next sequence value for OH_ID
    v_oh_id := nextval('csds.seq_offence_header');

    -- Insert new offence header record
    INSERT INTO csds.gd_offence_header (
        oh_id,
        b_classname,
        b_batchid,
        b_credate,
        b_upddate,
        b_creator,
        b_updator--,
        --f_offence_header_status
    )
    VALUES (
        v_oh_id,
        'OffenceHeader',
        v_loadid,
        now(),
        now(),
        v_username,
        v_username--,
        --v_offence_status
       );

    -- Return the OH_ID that was inserted
    RETURN v_oh_id;
END;
$function$
;
--
CREATE OR REPLACE FUNCTION csds.fn_get_current_cjscode_version(v_cjs_code text)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_version bigint;
BEGIN
    SELECT MAX(version_number)
      INTO v_version
    FROM csds.gd_offence_revision
    WHERE cjs_code = v_cjs_code;

    -- Will return NULL if no rows or all version_number are NULL
    RETURN v_version;
END;
$function$
;
--
CREATE OR REPLACE FUNCTION csds.fn_get_current_offence_revision_pnld(v_code text, v_type text)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_offence_revision bigint;
    v_cjs_code text;
BEGIN

    IF v_type = 'cjs_code' THEN

        SELECT MAX(ofr_id)
          INTO v_offence_revision
        FROM csds.gd_offence_revision
        WHERE cjs_code = v_code;

        RETURN v_offence_revision;

    ELSIF v_type = 'sow_ref' THEN

        SELECT cjs_code
          INTO v_cjs_code
        FROM csds.gd_offence_revision
        WHERE sow_ref = v_code
        ORDER BY ofr_id DESC
        LIMIT 1;

        SELECT MAX(ofr_id)
          INTO v_offence_revision
        FROM csds.gd_offence_revision
        WHERE cjs_code = v_cjs_code;

        RETURN v_offence_revision;

    ELSE
        RAISE EXCEPTION 'Invalid v_type: %, expected cjs_code or sow_ref', v_type;
    END IF;

END;
$function$
;
--
CREATE OR REPLACE FUNCTION csds.fn_get_menu_id_for_options(v_currentloadid numeric, v_menu_md5 text)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_menu_id numeric;
BEGIN
    SELECT om_id
    INTO v_menu_id
    FROM csds.sa_ote_menu
    WHERE b_loadid = v_currentloadid
      AND pnld_hash_md5 = v_menu_md5;

    RETURN v_menu_id;
END;
$function$
;
--
CREATE OR REPLACE FUNCTION csds.fn_get_or_create_source_file_header_id(v_currentloadid numeric, v_username text, v_source_file_id numeric)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    -- raw values from XML (unbounded to allow validation before casting)
    v_pnld_ref_raw    text;
    v_cjs_code_raw    text;

    -- validated/truncated variables matching business constraints
    v_pnld_ref        varchar(10);
    v_cjs_code        varchar(8);

    v_file_header_id  integer;

    -- advisory lock keys (two-key variant)
    lock_key1         int;
    lock_key2         int;
BEGIN
    --------------------------------------------------------------------------
    -- 1) Extract required values from XML for this specific file id
    --    Read as text first, then validate length & assign NULL if exceeded.
    --------------------------------------------------------------------------
    BEGIN
        SELECT
            NULLIF(
                (xpath('string(//document/pnldref)',
                       xmlparse(document convert_from(sf.source_file_content, 'UTF8'))
                ))[1]::text, ''
            ),
            NULLIF(
                (xpath('string(//document/codes/cjsoffencecode)',
                       xmlparse(document convert_from(sf.source_file_content, 'UTF8'))
                ))[1]::text, ''
            )
        INTO STRICT v_pnld_ref_raw, v_cjs_code_raw
        FROM csds.sa_source_file AS sf
        WHERE sf.b_loadid       = v_currentloadid
          AND sf.source_file_id = v_source_file_id;
    EXCEPTION
        WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION
                'Multiple csds.sa_source_file rows found for (loadid=%, source_file_id=%); refine selection.',
                v_currentloadid, v_source_file_id;
    END;

    --------------------------------------------------------------------------
    -- 1a) Enforce length rules:
    --      - pnld_ref: NULL if > 10 chars
    --      - cjs_code: NULL if > 8 chars
    --------------------------------------------------------------------------
    IF v_pnld_ref_raw IS NOT NULL AND char_length(v_pnld_ref_raw) > 10 THEN
        v_pnld_ref := NULL;
    ELSE
        -- cast to varchar(10) only if within limit
        v_pnld_ref := v_pnld_ref_raw::varchar(10);
    END IF;

    IF v_cjs_code_raw IS NOT NULL AND char_length(v_cjs_code_raw) > 8 THEN
        v_cjs_code := NULL;
    ELSE
        -- cast to varchar(8) only if within limit
        v_cjs_code := v_cjs_code_raw::varchar(8);
    END IF;

    --------------------------------------------------------------------------
    -- 2) Advisory lock guards concurrent header creation for this pair
    --    Use COALESCE to avoid NULL in lock arguments.
    --------------------------------------------------------------------------
    lock_key1 := hashtext(COALESCE(v_pnld_ref, ''));
    lock_key2 := hashtext(COALESCE(v_cjs_code, ''));

    PERFORM pg_advisory_lock(lock_key1, lock_key2);

    BEGIN
        ----------------------------------------------------------------------
        -- 3) Find or create header (csds.gd_source_file_header)
        --    Use IS NOT DISTINCT FROM so NULLs compare equal.
        ----------------------------------------------------------------------
        SELECT sfh.source_file_header_id
          INTO v_file_header_id
        FROM csds.gd_source_file_header AS sfh
        WHERE sfh.source_file_reference IS NOT DISTINCT FROM v_pnld_ref
          AND sfh.cjs_code              IS NOT DISTINCT FROM v_cjs_code;

        IF NOT FOUND THEN
            INSERT INTO csds.gd_source_file_header (
                source_file_header_id,
                b_classname,
                b_batchid,
                b_credate,
                b_upddate,
                b_creator,
                b_updator,
                source_file_reference,
                cjs_code
            )
            VALUES (
                nextval('csds.seq_source_file_header'),
                'SourceFileHeader',
                1,              -- TODO: pass actual batch id if required
                now(),
                now(),
                v_username,
                v_username,
                v_pnld_ref,
                v_cjs_code
            )
            RETURNING source_file_header_id
            INTO v_file_header_id;
        END IF;

        -- Always release the lock on success
        PERFORM pg_advisory_unlock(lock_key1, lock_key2);

    EXCEPTION
        WHEN OTHERS THEN
            -- Ensure the lock is released on any error, then re-raise
            PERFORM pg_advisory_unlock(lock_key1, lock_key2);
            RAISE;
    END;

    --------------------------------------------------------------------------
    -- 4) Return the id we found/created
    --------------------------------------------------------------------------
       RETURN v_file_header_id;
END;
$function$
;
--
CREATE OR REPLACE FUNCTION csds.fn_get_source_file_cjs_code(v_currentloadid numeric, v_source_file_id numeric)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    -- Raw value extracted from XML
    v_cjs_code_raw  text;

    -- Validated/truncated output value
    v_cjs_code      varchar(8);
BEGIN
    ----------------------------------------------------------------------
    -- Extract CJS Offence Code from XML
    ----------------------------------------------------------------------
    BEGIN
        SELECT NULLIF(
                   (xpath(
                        'string(//document/codes/cjsoffencecode)',
                        xmlparse(document convert_from(sf.source_file_content, 'UTF8'))
                    ))[1]::text,
                   ''
               )
        INTO STRICT v_cjs_code_raw
        FROM csds.sa_source_file AS sf
        WHERE sf.b_loadid       = v_currentloadid
          AND sf.source_file_id = v_source_file_id;

    EXCEPTION
        WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION
                'Multiple csds.sa_source_file rows found for (loadid=%, source_file_id=%); refine selection.',
                v_currentloadid, v_source_file_id;
    END;

    ----------------------------------------------------------------------
    -- Enforce length rules:
    --   - NULL if value exceeds 8 characters
    ----------------------------------------------------------------------
    IF v_cjs_code_raw IS NOT NULL
       AND char_length(v_cjs_code_raw) > 8 THEN
        v_cjs_code := NULL;
    ELSE
        v_cjs_code := v_cjs_code_raw::varchar(8);
    END IF;

    RETURN v_cjs_code;
END;
$function$
;
--
CREATE OR REPLACE FUNCTION csds.fn_get_source_file_dolu(v_currentloadid numeric, v_source_file_id numeric)
 RETURNS date
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_date_of_last_update   date;
    v_date_text             text;
BEGIN
    -- Extract //document/ancillary/dateoflastupdate as trimmed text (empty -> NULL)
    SELECT NULLIF(
               btrim( (xpath(
                   'string(//document/ancillary/dateoflastupdate)',
                   xmlparse(document convert_from(sf.source_file_content, 'UTF8'))
               ))[1]::text ),
               ''
           )
    INTO v_date_text
    FROM csds.sa_source_file AS sf
    WHERE sf.b_loadid       = v_currentloadid
      AND sf.source_file_id = v_source_file_id;

    -- If no row or empty value, return NULL
    IF v_date_text IS NULL THEN
        RETURN NULL;
    END IF;

    -- Try to cast to date; if invalid, return NULL instead of failing
    BEGIN
        v_date_of_last_update := v_date_text::date;
    EXCEPTION WHEN others THEN
        v_date_of_last_update := NULL;
    END;

    RETURN v_date_of_last_update;
END;
$function$
;
--
CREATE OR REPLACE FUNCTION csds.fn_get_source_file_name(v_currentloadid numeric, v_source_file_id numeric)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_file_name text;
BEGIN
    -- Pick one row for this load; if file name or zip id are NULL, skip those filters.
    SELECT
        NULLIF(
            (xpath(
                'string(//document/codes/cjsoffencecode)',
                xmlparse(document convert_from(sf.source_file_content, 'UTF8'))
            ))[1]::text,
            '')
		|| '_'
		|| NULLIF(
            (xpath(
                'string(//document/pnldref)',
                xmlparse(document convert_from(sf.source_file_content, 'UTF8'))
            ))[1]::text,
            '')
		 || '.xml'
    INTO v_file_name
    FROM csds.sa_source_file AS sf
    WHERE sf.b_loadid = v_currentloadid
		AND sf.source_file_id = v_source_file_id
    ORDER BY sf.b_credate DESC NULLS LAST
    LIMIT 1;

    -- If no rows found or extracted element is NULL/empty, return a timestamp-based default.
    IF v_file_name IS NULL THEN
        v_file_name := to_char(current_timestamp, 'YYYYMMDD_HH24MISS') || '.xml';
    END IF;

    RETURN v_file_name;
END;
$function$
;
--
CREATE OR REPLACE FUNCTION csds.fn_get_source_file_offence_revision(v_cjs_code text, v_pnld_ref text, v_date_of_last_update date, v_loadid numeric)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_offence_revision_id NUMERIC;
BEGIN
    SELECT MAX(ofr_id)
    INTO v_offence_revision_id
    FROM csds.sa_offence_revision
    WHERE cjs_code = v_cjs_code
      AND sow_ref = v_pnld_ref
      AND pnld_date_of_last_update = v_date_of_last_update
	  AND b_loadid = v_loadid;

    RETURN v_offence_revision_id;
END;
$function$
;
--
CREATE OR REPLACE FUNCTION csds.fn_get_source_file_pnld_ref(v_currentloadid numeric, v_source_file_id numeric)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    -- Raw value extracted from XML
    v_pnld_ref_raw  text;

    -- Validated/truncated output value
    v_pnld_ref      varchar(8);
BEGIN
    ----------------------------------------------------------------------
    -- Extract PNLD Ref from XML
    ----------------------------------------------------------------------
    BEGIN
        SELECT NULLIF(
                   (xpath(
                        'string(//document/pnldref)',
                        xmlparse(document convert_from(sf.source_file_content, 'UTF8'))
                    ))[1]::text,
                   ''
               )
        INTO STRICT v_pnld_ref_raw
        FROM csds.sa_source_file AS sf
        WHERE sf.b_loadid       = v_currentloadid
          AND sf.source_file_id = v_source_file_id;

    EXCEPTION
        WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION
                'Multiple csds.sa_source_file rows found for (loadid=%, source_file_id=%); refine selection.',
                v_currentloadid, v_source_file_id;
    END;

    ----------------------------------------------------------------------
    -- Enforce length rules:
    --   - NULL if value exceeds 8 characters
    ----------------------------------------------------------------------
    IF v_pnld_ref_raw IS NOT NULL
       AND char_length(v_pnld_ref_raw) > 8 THEN
        v_pnld_ref := NULL;
    ELSE
        v_pnld_ref := v_pnld_ref_raw::varchar(8);
    END IF;

    RETURN v_pnld_ref;
END;
$function$
;
--
CREATE OR REPLACE FUNCTION csds.fn_menu_status_from_revisions(p_menu_id numeric, p_load_id numeric, p_current_status text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_menu_status text;
BEGIN
    -- Early guard: if there are no published Active/Future revisions at all, return existing status
    IF NOT EXISTS (
        SELECT 1
        FROM csds.sa_offence_revision sa
        JOIN csds.gd_release_package rp
          ON rp.rp_id = sa.f_release_package
        WHERE sa.b_loadid = p_load_id
          AND sa.publishing_status IN ('Active','Future')
          AND rp.status = 'Published'
          AND p_menu_id IN (
            sa.f_menu_01, sa.f_menu_02, sa.f_menu_03, sa.f_menu_04, sa.f_menu_05,
            sa.f_menu_06, sa.f_menu_07, sa.f_menu_08, sa.f_menu_09, sa.f_menu_10,
            sa.f_menu_11, sa.f_menu_12, sa.f_menu_13, sa.f_menu_14, sa.f_menu_15,
            sa.f_menu_16, sa.f_menu_17, sa.f_menu_18, sa.f_menu_19, sa.f_menu_20,
            sa.f_menu_21, sa.f_menu_22, sa.f_menu_23, sa.f_menu_24, sa.f_menu_25,
            sa.f_menu_26, sa.f_menu_27, sa.f_menu_28, sa.f_menu_29, sa.f_menu_30
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM csds.gd_offence_revision gor
        JOIN csds.gd_release_package rp
          ON rp.rp_id = gor.f_release_package
        WHERE rp.status = 'Published'
          AND gor.publishing_status IN ('Active','Future')
          AND p_menu_id IN (
            gor.f_menu_01, gor.f_menu_02, gor.f_menu_03, gor.f_menu_04, gor.f_menu_05,
            gor.f_menu_06, gor.f_menu_07, gor.f_menu_08, gor.f_menu_09, gor.f_menu_10,
            gor.f_menu_11, gor.f_menu_12, gor.f_menu_13, gor.f_menu_14, gor.f_menu_15,
            gor.f_menu_16, gor.f_menu_17, gor.f_menu_18, gor.f_menu_19, gor.f_menu_20,
            gor.f_menu_21, gor.f_menu_22, gor.f_menu_23, gor.f_menu_24, gor.f_menu_25,
            gor.f_menu_26, gor.f_menu_27, gor.f_menu_28, gor.f_menu_29, gor.f_menu_30
          )
    ) THEN
        -- Return existing status instead of NULL
        RETURN p_current_status;
    END IF;

    -- Main CASE block for Active / Future / Inactive
    SELECT
        CASE
            -- 1) At least one Active revision
            WHEN
            (
                EXISTS (
                    SELECT 1
                    FROM csds.sa_offence_revision sa
                    JOIN csds.gd_release_package rp
                      ON rp.rp_id = sa.f_release_package
                    WHERE sa.b_loadid = p_load_id
                      AND sa.publishing_status = 'Active'
                      AND rp.status = 'Published'
                      AND p_menu_id IN (
                        sa.f_menu_01, sa.f_menu_02, sa.f_menu_03, sa.f_menu_04, sa.f_menu_05,
                        sa.f_menu_06, sa.f_menu_07, sa.f_menu_08, sa.f_menu_09, sa.f_menu_10,
                        sa.f_menu_11, sa.f_menu_12, sa.f_menu_13, sa.f_menu_14, sa.f_menu_15,
                        sa.f_menu_16, sa.f_menu_17, sa.f_menu_18, sa.f_menu_19, sa.f_menu_20,
                        sa.f_menu_21, sa.f_menu_22, sa.f_menu_23, sa.f_menu_24, sa.f_menu_25,
                        sa.f_menu_26, sa.f_menu_27, sa.f_menu_28, sa.f_menu_29, sa.f_menu_30
                      )
                )
                OR
                EXISTS (
                    SELECT 1
                    FROM csds.gd_offence_revision gor
                    JOIN csds.gd_release_package rp
                      ON rp.rp_id = gor.f_release_package
                    WHERE rp.status = 'Published'
                      AND gor.publishing_status = 'Active'
                      AND p_menu_id IN (
                        gor.f_menu_01, gor.f_menu_02, gor.f_menu_03, gor.f_menu_04, gor.f_menu_05,
                        gor.f_menu_06, gor.f_menu_07, gor.f_menu_08, gor.f_menu_09, gor.f_menu_10,
                        gor.f_menu_11, gor.f_menu_12, gor.f_menu_13, gor.f_menu_14, gor.f_menu_15,
                        gor.f_menu_16, gor.f_menu_17, gor.f_menu_18, gor.f_menu_19, gor.f_menu_20,
                        gor.f_menu_21, gor.f_menu_22, gor.f_menu_23, gor.f_menu_24, gor.f_menu_25,
                        gor.f_menu_26, gor.f_menu_27, gor.f_menu_28, gor.f_menu_29, gor.f_menu_30
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM csds.sa_offence_revision sa2
                          WHERE sa2.b_loadid = p_load_id
                            AND sa2.ofr_id = gor.ofr_id
                      )
                )
            )
            THEN 'Active'

            -- 2) At least one Future revision and no Active
            WHEN
            (
                EXISTS (
                    SELECT 1
                    FROM csds.sa_offence_revision sa
                    JOIN csds.gd_release_package rp
                      ON rp.rp_id = sa.f_release_package
                    WHERE sa.b_loadid = p_load_id
                      AND sa.publishing_status = 'Future'
                      AND rp.status = 'Published'
                      AND p_menu_id IN (
                        sa.f_menu_01, sa.f_menu_02, sa.f_menu_03, sa.f_menu_04, sa.f_menu_05,
                        sa.f_menu_06, sa.f_menu_07, sa.f_menu_08, sa.f_menu_09, sa.f_menu_10,
                        sa.f_menu_11, sa.f_menu_12, sa.f_menu_13, sa.f_menu_14, sa.f_menu_15,
                        sa.f_menu_16, sa.f_menu_17, sa.f_menu_18, sa.f_menu_19, sa.f_menu_20,
                        sa.f_menu_21, sa.f_menu_22, sa.f_menu_23, sa.f_menu_24, sa.f_menu_25,
                        sa.f_menu_26, sa.f_menu_27, sa.f_menu_28, sa.f_menu_29, sa.f_menu_30
                      )
                )
                OR
                EXISTS (
                    SELECT 1
                    FROM csds.gd_offence_revision gor
                    JOIN csds.gd_release_package rp
                      ON rp.rp_id = gor.f_release_package
                    WHERE rp.status = 'Published'
                      AND gor.publishing_status = 'Future'
                      AND p_menu_id IN (
                        gor.f_menu_01, gor.f_menu_02, gor.f_menu_03, gor.f_menu_04, gor.f_menu_05,
                        gor.f_menu_06, gor.f_menu_07, gor.f_menu_08, gor.f_menu_09, gor.f_menu_10,
                        gor.f_menu_11, gor.f_menu_12, gor.f_menu_13, gor.f_menu_14, gor.f_menu_15,
                        gor.f_menu_16, gor.f_menu_17, gor.f_menu_18, gor.f_menu_19, gor.f_menu_20,
                        gor.f_menu_21, gor.f_menu_22, gor.f_menu_23, gor.f_menu_24, gor.f_menu_25,
                        gor.f_menu_26, gor.f_menu_27, gor.f_menu_28, gor.f_menu_29, gor.f_menu_30
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM csds.sa_offence_revision sa2
                          WHERE sa2.b_loadid = p_load_id
                            AND sa2.ofr_id = gor.ofr_id
                      )
                )
            )
            THEN 'Future'

            -- 3) Only Inactive revisions remain
            ELSE 'Inactive'
        END
    INTO v_menu_status
    FROM csds.sa_ote_menu sm
    WHERE sm.om_id = p_menu_id;

    RETURN v_menu_status;
END;
$function$
;
--
CREATE OR REPLACE FUNCTION csds.fn_prevent_concurrent_user_edits(v_parentid numeric, v_entityname character varying)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

--
DECLARE
--
--row_count NUMERIC;
user VARCHAR(128);
--
BEGIN
--
IF v_entityName = 'OffenceRevision' THEN
--

 
SELECT STRING_AGG (b_loadcreator,',' ORDER BY b_loadcreator) INTO user
  FROM
  (SELECT DISTINCT B.b_loadcreator
  FROM csds.sa_offence_revision			S
  JOIN csds.dl_batch  				   	B
    ON B.b_loadid  = S.b_loadid
 WHERE v_parentID  = S.ofr_id
   AND B.b_status  = 'RUNNING') AS dist;
--
END IF;

IF v_entityName = 'OTEMenu' THEN
 
SELECT STRING_AGG (b_loadcreator,',' ORDER BY b_loadcreator) INTO user
  FROM
  (SELECT DISTINCT B.b_loadcreator
  FROM csds.sa_ote_menu			S
  JOIN csds.dl_batch  				   	B
    ON B.b_loadid  = S.b_loadid
 WHERE v_parentID  = S.om_id
   AND B.b_status  = 'RUNNING') AS dist;
   
END IF;


--
RETURN user;
--
END
--
$function$
;
--
CREATE OR REPLACE PROCEDURE csds.proc_clone_offence_headers(v_currentloadid numeric, v_username character varying)
 LANGUAGE plpgsql
AS $procedure$

DECLARE
    rec             record;
    v_rows          integer;
    v_new_header_id numeric;
BEGIN

-- Create new headers based on the original headers for all revisions in sa context
    RAISE NOTICE 'Cloning headers for load id % by %', v_currentloadid, v_username;	
    FOR rec IN
        SELECT
            rev.ofr_id,
            rev.b_loadid,
            head.oh_id                AS old_header_id,
            head.b_classname,
            head.b_batchid,
            --head.f_change_set_header,
            --head.f_offence_header_status,
            --head.f_offnc_hdrs_ref_offnc,
            head.pnld_start_date,
            head.pnld_end_date
        FROM csds.sa_offence_revision rev
        JOIN csds.gd_offence_header head
            ON head.oh_id = rev.f_offence_header
        WHERE rev.b_loadid = v_currentloadid
    LOOP
        -- Insert cloned header
        INSERT INTO csds.gd_offence_header (
            oh_id,
            b_classname,
            b_batchid,
            b_credate,
            b_upddate,
            b_creator,
            b_updator,
            --f_change_set_header,
            --f_offence_header_status,
            --f_offnc_hdrs_ref_offnc,
            pnld_start_date,
            pnld_end_date
        )
        VALUES (
            nextval('csds.seq_offence_header'),
            rec.b_classname,
            rec.b_loadid,        -- or rec.b_batchid if that is what you want
            now(),
            now(),
            v_username,
            v_username,
            --rec.f_change_set_header,
            --rec.f_offence_header_status,
            --rec.f_offnc_hdrs_ref_offnc,
            rec.pnld_start_date,
            rec.pnld_end_date
        )
        RETURNING oh_id INTO v_new_header_id;

        -- Point revision to new header
		-- and clear the cloned from field
        UPDATE csds.sa_offence_revision
        SET f_offence_header = v_new_header_id
        WHERE ofr_id = rec.ofr_id;

        RAISE NOTICE 'Revision %: old header %, new header %',
            rec.ofr_id, rec.old_header_id, v_new_header_id;
    END LOOP;
	
	/*Clear cloned_by*/
    UPDATE csds.sa_offence_revision
    SET    cloned_from = NULL
    WHERE b_loadid = v_currentloadid;
	
	
END;
$procedure$
;
--
CREATE OR REPLACE PROCEDURE csds.proc_insert_release_package_content(v_batchid numeric, v_username text, v_serverbaseurl text)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_URLConcat					text := '/mdm-app/CSDS/CrimeStandingDataService/browsing';
	v_RlsePckageStatus			text := 'Open';
	id_RlsePckageOpen			int[];
	id_RlsePckageFinal			int[];
BEGIN
	-- Add all the release packages from respective entities into the array
	SELECT array_agg(sQry.f_release_package) INTO id_RlsePckageFinal FROM
	(
	 SELECT f_release_package FROM csds.gd_offence_revision WHERE authoring_status = 'Final' AND b_batchid = v_batchID UNION
	 SELECT f_release_package FROM csds.gd_ote_menu 	    WHERE authoring_status = 'Final' AND b_batchid = v_batchID
	) sQry;
	--
	-- Add all the open release packages into the array
	SELECT array_agg(rp_id) INTO id_RlsePckageOpen
	  FROM csds.gd_release_package
	 WHERE status = v_RlsePckageStatus;
	--
	-- Identify release package content to derive deletes, insert and update on gd_release_package_content.
	WITH source AS (
    SELECT 'Offence'													AS rp_content_type
	      ,oRvsn.cjs_code												AS rp_content_name
	      ,v_serverBaseURL||v_URLConcat||'/AllOffences/'||oRvsn.ofr_id	AS rp_content_url
		  ,oRvsn.ofr_id													AS rp_content_key
		  ,oRvsn.f_release_package										AS f_release_package
		  ,oRvsn.offence_notes											AS notes
		  ,oRvsn.authoring_status										AS rp_content_auth_status
      FROM csds.gd_offence_revision			oRvsn
	 WHERE oRvsn.b_batchid = v_batchID
	UNION ALL
    SELECT 'Offence Menu'												AS rp_content_type
	      ,oMenu.name													AS rp_content_name
	      ,v_serverBaseURL||v_URLConcat||'/AllOffenceMenus/'||oMenu.om_id		AS rp_content_url
		  ,oMenu.om_id													AS rp_content_key
		  ,oMenu.f_release_package										AS f_release_package
		  ,oMenu.hmcts_notes											AS notes
		  ,oMenu.authoring_status										AS rp_content_auth_status
      FROM csds.gd_ote_menu					oMenu
	 WHERE oMenu.b_batchid = v_batchID 
	)
	-- delete release package content from gd_release_package_content that no longer tagged to the respective release package.
	,del AS (
	DELETE FROM csds.gd_release_package_content trgt
	 USING source srce1
     WHERE trgt.f_release_package = ANY (id_RlsePckageOpen)
       AND (trgt.rp_content_type, trgt.rp_content_key) IN 
			(
			 -- record is removed from a release package. 
			 SELECT rp_content_type, rp_content_key
			   FROM source
			  WHERE f_release_package IS NULL
			--
			UNION
			--  record is removed from a release package and assigned to a different release package.
			SELECT src1.rp_content_type, src1.rp_content_key
			   FROM csds.gd_release_package_content trg1
			   JOIN source 						 	src1
			     ON trg1.rp_content_type    = src1.rp_content_type
			    AND trg1.rp_content_key     = src1.rp_content_key
			  WHERE trg1.f_release_package != src1.f_release_package
			)
	RETURNING trgt.f_release_package, trgt.rp_content_type, trgt.rp_content_key
	)
	INSERT INTO csds.gd_release_package_content (
		release_package_cntnt_id
	   ,b_classname
	   ,b_batchid
	   ,b_credate
	   ,b_upddate
	   ,b_creator
	   ,b_updator
	   ,rp_content_type
	   ,rp_content_name
	   ,rp_content_url
	   ,rp_content_key
	   ,f_release_package
	   ,notes
	   ,rp_content_auth_status
	)
	SELECT
		nextval('csds.seq_release_package_content')
	   ,'ReleasePackageContent'
	   ,v_batchID
	   ,CURRENT_TIMESTAMP
	   ,CURRENT_TIMESTAMP
	   ,v_userName
	   ,v_userName
	   ,src2.rp_content_type
	   ,src2.rp_content_name
	   ,src2.rp_content_url
	   ,src2.rp_content_key
	   ,src2.f_release_package
	   ,src2.notes
	   ,src2.rp_content_auth_status
	 FROM source	src2
	-- update release package content on gd_release_package_content if any differences for unique key mentioned below.
    ON CONFLICT (f_release_package, rp_content_type, rp_content_key)
    DO UPDATE SET
		b_batchid              = v_batchID
	   ,b_upddate              = CURRENT_TIMESTAMP
	   ,b_updator              = v_userName
	   ,rp_content_name        = EXCLUDED.rp_content_name
	   ,rp_content_url         = EXCLUDED.rp_content_url
	   ,notes            	   = EXCLUDED.notes
	   ,rp_content_auth_status = EXCLUDED.rp_content_auth_status;
	--
	-- update release package content count on gd_release_package
	UPDATE csds.gd_release_package AS trgt
	   SET content_count = COALESCE(cnt.content_count, 0)
	  FROM (
			SELECT grp.rp_id								AS rp_id
			      ,COUNT(grc.release_package_cntnt_id) 		AS content_count
             FROM 	   csds.gd_release_package 				grp 
             LEFT JOIN csds.gd_release_package_content 		grc ON grp.rp_id = grc.f_release_package 
            WHERE grp.rp_id = ANY (id_RlsePckageOpen)
            GROUP BY grp.rp_id 
           ) AS cnt
     WHERE trgt.rp_id = cnt.rp_id;
	--
END;
--
$procedure$
;
--
CREATE OR REPLACE PROCEDURE csds.proc_perf_test_load_offence(v_recordcount integer, v_username character varying, v_cjsprefix character varying)
 LANGUAGE plpgsql
AS $procedure$

DECLARE v_load_id INTEGER;

v_counter INTEGER;
v_headerkey INTEGER;

BEGIN
	-- ============================================
	-- Generate the new LoadID
	-- ============================================
	v_load_id := semarchy_repository.get_new_loadid('CSDS','OffenceRevisionDataAuthoring','Load Offence Revision',v_userName);
	--
	RAISE NOTICE 'New Load ID: %',v_load_id;
	--
	-- Create Header
	INSERT INTO csds.SA_OFFENCE_HEADER (
	     b_loadid
		,oh_id
		,b_classname
		,b_credate
		,b_upddate
		,b_creator
		,b_updator

			)
		VALUES (
			-- Semarchy technical
			v_load_id
			,nextval('csds.seq_offence_header')
			,'OffenceHeader'
			,CURRENT_TIMESTAMP
			,CURRENT_TIMESTAMP
			,v_userName
			,v_userName
			);
			
	--- Keep Header Key for Child Foreign Key Insert
	
	SELECT oh_id
		INTO v_headerkey
		FROM csds.sa_offence_header soh
		WHERE soh.b_loadid = v_load_id;
			
	-- ============================================
	-- Insert loop
	-- ============================================
	v_counter := 1;

	WHILE v_counter <= v_recordCount LOOP
		INSERT INTO csds.SA_OFFENCE_REVISION (
			-- Semarchy technical
			 b_loadid,ofr_id,b_classname,b_credate,b_upddate,b_creator,b_updator
			-- Business attributes
			,recordable,reportable,cjs_title,custodial_indicator,date_used_from,date_used_to,standard_list,traffic_control,version_number,changed_by
			,changed_date,dvla_code,offence_notes,maximum_penalty,description,derived_from_cjs_code,ho_class,ho_subclass,proceedings_code,sl_cjs_title
			,pnld_stndrd_offnc_wording,sl_pnld_standard_off_word,pnld_date_of_last_update,prosecution_time_limit,max_fine_type_magct_code,mode_of_trial
			,endorsable_flag,location_flag,principal_offnc_category,user_offence_wording,user_statement_of_facts,user_acts_and_section,entry_prmpt_sub_sow
			,entry_prmpt_sub_sof,entry_prompt_subs_ans,current_editor,cjs_code,area,blocked,sl_off_statmnt_fct_txt,sl_offence_wording_text,sl_offence_act_sec_txt
			,offence_code,pnld_offence_start_date,pnld_offence_end_date,sow_ref,cloned_from,clone_type_code,sys_cloned_to,sys_show_deleted,authoring_status
			,publishing_status,offence_type,offence_source,mis_classification,offence_class
			-- Terminal entries t01–t30
			,t01entry_number,t01minimum,t01maximum,t01entry_format,t01entry_prompt,t01standard_entry_identifier,t01delete_indicator,t02entry_number,t02minimum,t02maximum,t02entry_format,t02entry_prompt,t02standard_entry_identifier,t02delete_indicator
            ,t03entry_number,t03minimum,t03maximum,t03entry_format,t03entry_prompt,t03standard_entry_identifier,t03delete_indicator,t04entry_number,t04minimum,t04maximum,t04entry_format,t04entry_prompt,t04standard_entry_identifier,t04delete_indicator
            ,t05entry_number,t05minimum,t05maximum,t05entry_format,t05entry_prompt,t05standard_entry_identifier,t05delete_indicator,t06entry_number,t06minimum,t06maximum,t06entry_format,t06entry_prompt,t06standard_entry_identifier,t06delete_indicator
            ,t07entry_number,t07minimum,t07maximum,t07entry_format,t07entry_prompt,t07standard_entry_identifier,t07delete_indicator,t08entry_number,t08minimum,t08maximum,t08entry_format,t08entry_prompt,t08standard_entry_identifier,t08delete_indicator
            ,t09entry_number,t09minimum,t09maximum,t09entry_format,t09entry_prompt,t09standard_entry_identifier,t09delete_indicator,t10entry_number,t10minimum,t10maximum,t10entry_format,t10entry_prompt,t10standard_entry_identifier,t10delete_indicator
            ,t11entry_number,t11minimum,t11maximum,t11entry_format,t11entry_prompt,t11standard_entry_identifier,t11delete_indicator,t12entry_number,t12minimum,t12maximum,t12entry_format,t12entry_prompt,t12standard_entry_identifier,t12delete_indicator
            ,t13entry_number,t13minimum,t13maximum,t13entry_format,t13entry_prompt,t13standard_entry_identifier,t13delete_indicator,t14entry_number,t14minimum,t14maximum,t14entry_format,t14entry_prompt,t14standard_entry_identifier,t14delete_indicator
            ,t15entry_number,t15minimum,t15maximum,t15entry_format,t15entry_prompt,t15standard_entry_identifier,t15delete_indicator,t16entry_number,t16minimum,t16maximum,t16entry_format,t16entry_prompt,t16standard_entry_identifier,t16delete_indicator
            ,t17entry_number,t17minimum,t17maximum,t17entry_format,t17entry_prompt,t17standard_entry_identifier,t17delete_indicator,t18entry_number,t18minimum,t18maximum,t18entry_format,t18entry_prompt,t18standard_entry_identifier,t18delete_indicator
            ,t19entry_number,t19minimum,t19maximum,t19entry_format,t19entry_prompt,t19standard_entry_identifier,t19delete_indicator,t20entry_number,t20minimum,t20maximum,t20entry_format,t20entry_prompt,t20standard_entry_identifier,t20delete_indicator
            ,t21entry_number,t21minimum,t21maximum,t21entry_format,t21entry_prompt,t21standard_entry_identifier,t21delete_indicator,t22entry_number,t22minimum,t22maximum,t22entry_format,t22entry_prompt,t22standard_entry_identifier,t22delete_indicator
            ,t23entry_number,t23minimum,t23maximum,t23entry_format,t23entry_prompt,t23standard_entry_identifier,t23delete_indicator,t24entry_number,t24minimum,t24maximum,t24entry_format,t24entry_prompt,t24standard_entry_identifier,t24delete_indicator
            ,t25entry_number,t25minimum,t25maximum,t25entry_format,t25entry_prompt,t25standard_entry_identifier,t25delete_indicator,t26entry_number,t26minimum,t26maximum,t26entry_format,t26entry_prompt,t26standard_entry_identifier,t26delete_indicator
            ,t27entry_number,t27minimum,t27maximum,t27entry_format,t27entry_prompt,t27standard_entry_identifier,t27delete_indicator,t28entry_number,t28minimum,t28maximum,t28entry_format,t28entry_prompt,t28standard_entry_identifier,t28delete_indicator
            ,t29entry_number,t29minimum,t29maximum,t29entry_format,t29entry_prompt,t29standard_entry_identifier,t29delete_indicator,t30entry_number,t30minimum,t30maximum,t30entry_format,t30entry_prompt,t30standard_entry_identifier,t30delete_indicator
			-- Reference fields
			,f_offence_header
			,f_menu_01,f_menu_02,f_menu_03,f_menu_04,f_menu_05,f_menu_06,f_menu_07,f_menu_08,f_menu_09,f_menu_10,f_menu_11,f_menu_12,f_menu_13,f_menu_14,f_menu_15,f_menu_16,f_menu_17,f_menu_18,f_menu_19,f_menu_20,f_menu_21,f_menu_22,f_menu_23,f_menu_24,f_menu_25,f_menu_26,f_menu_27,f_menu_28,f_menu_29,f_menu_30
			,can_be_bulk,initial_fee,contested_fee,application_synonym,exparte,jurisdiction,appeal_flag,summons_template_type,link_type,hearing_code
            ,applicant_appellant_flag,plea_applicable_flag,active_offence_order,commissioner_of_oath,breach_type,court_of_appeal_flag,court_extract_available
		    ,listing_notification_temp,boxwork_notification_temp,prosecutor_as_third_party,resentencing_activation_c,prefix,obsolete_indicator
			)
		VALUES (
			-- Semarchy technical
			v_load_id,nextval('csds.seq_offence_revision'),'OffenceRevision','2026-01-14 22:41:33.544',CURRENT_TIMESTAMP,v_userName,v_userName
			-- Business attributes
			,'Yes','Yes','Speeding over statutory limit','No',DATE '2024-01-01',NULL,'Yes',TRUE,1,'admin',CURRENT_TIMESTAMP,'A123','Test offence notes','Fine and penalty points'
			,'Driving a motor vehicle above the legal speed limit',NULL,10,2,12345,'Speeding offence','Exceeding speed limit on public road','Standard wording for speeding offence',DATE '2024-06-01'
			,'6 months','F','Summary','Y','N','Road Traffic','User wording','User statement of facts','Road Traffic Act 1988 s.89','Prompt SOW','Prompt SOF','Prompt Answer','editor1'
			,v_cjsprefix||v_counter,1,'N','Statement of facts','Offence wording','Act and section',123,CURRENT_TIMESTAMP,NULL,'SOW001',NULL,NULL,NULL,FALSE,'Draft','Not Published','CR','MOJ','DAM','1'
			-- Terminal entries (same pattern values)
            ,1,1,1,'MNU','Terminal Entry 01','CO',FALSE,2,1,1,'MNU','Terminal Entry 02','CO',FALSE,3,1,1,'MNU','Terminal Entry 03','CO',FALSE,4,1,1,'MNU','Terminal Entry 04','CO',FALSE,5,1,1,'MNU','Terminal Entry 05','CO',FALSE,6,1,1,'MNU','Terminal Entry 06','CO',FALSE,7,1,1,'MNU','Terminal Entry 07','CO',FALSE,8,1,1,'MNU','Terminal Entry 08','CO',FALSE
            ,9,1,1,'MNU','Terminal Entry 09','CO',FALSE,10,1,1,'MNU','Terminal Entry 10','CO',FALSE,11,1,1,'MNU','Terminal Entry 11','CO',FALSE,12,0,1,'MNU','Terminal Entry 12','CO',FALSE,13,1,1,'MNU','Terminal Entry 13','CO',FALSE,14,1,1,'MNU','Terminal Entry 14','CO',FALSE,15,1,1,'MNU','Terminal Entry 15','CO',FALSE,16,1,1,'MNU','Terminal Entry 16','CO',FALSE
			,17,1,1,'MNU','Terminal Entry 17','CO',FALSE,18,1,1,'MNU','Terminal Entry 18','CO',FALSE,19,1,1,'MNU','Terminal Entry 19','CO',FALSE,20,1,1,'MNU','Terminal Entry 20','CO',FALSE,21,1,1,'MNU','Terminal Entry 21','CO',FALSE,22,1,1,'MNU','Terminal Entry 22','CO',FALSE,23,1,1,'MNU','Terminal Entry 23','CO',FALSE,24,1,1,'MNU','Terminal Entry 24','CO',FALSE
            ,25,1,1,'MNU','Terminal Entry 25','CO',FALSE,26,1,1,'MNU','Terminal Entry 26','CO',FALSE,27,1,1,'MNU','Terminal Entry 27','CO',FALSE,28,1,1,'MNU','Terminal Entry 28','CO',FALSE,29,1,1,'MNU','Terminal Entry 29','CO',FALSE,30,1,1,'MNU','Terminal Entry 30','CO',FALSE
			-- Reference fields 
			,v_headerkey
			,'2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2'
			,TRUE,TRUE,TRUE,'Synonym',TRUE,'Crown',TRUE,'BREACH','STANDALONE','Appeal','Appellant',TRUE,'OFFENCE',TRUE,'NOT_APPLICABLE',TRUE,TRUE,'NOT_APPLICABLE','NOTIF',TRUE,'N','N','N'
			);

	v_counter := v_counter + 1;
END

LOOP;
--
-- ============================================
-- Submit Load
-- ============================================
PERFORM semarchy_repository.submit_load(v_load_id,'OffenceRevisionDataAuthoring',v_userName);END;
--
$procedure$
;
--
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
               (r.t01entry_number, r.t01entry_format, r.t01standard_entry_identifier, r.t01minimum, r.t01maximum, r.t01entry_prompt, r.f_menu_01, r.t01delete_indicator)
              ,(r.t02entry_number, r.t02entry_format, r.t02standard_entry_identifier, r.t02minimum, r.t02maximum, r.t02entry_prompt, r.f_menu_02, r.t02delete_indicator)
              ,(r.t03entry_number, r.t03entry_format, r.t03standard_entry_identifier, r.t03minimum, r.t03maximum, r.t03entry_prompt, r.f_menu_03, r.t03delete_indicator)
              ,(r.t04entry_number, r.t04entry_format, r.t04standard_entry_identifier, r.t04minimum, r.t04maximum, r.t04entry_prompt, r.f_menu_04, r.t04delete_indicator)
              ,(r.t05entry_number, r.t05entry_format, r.t05standard_entry_identifier, r.t05minimum, r.t05maximum, r.t05entry_prompt, r.f_menu_05, r.t05delete_indicator)
              ,(r.t06entry_number, r.t06entry_format, r.t06standard_entry_identifier, r.t06minimum, r.t06maximum, r.t06entry_prompt, r.f_menu_06, r.t06delete_indicator)
              ,(r.t07entry_number, r.t07entry_format, r.t07standard_entry_identifier, r.t07minimum, r.t07maximum, r.t07entry_prompt, r.f_menu_07, r.t07delete_indicator)
              ,(r.t08entry_number, r.t08entry_format, r.t08standard_entry_identifier, r.t08minimum, r.t08maximum, r.t08entry_prompt, r.f_menu_08, r.t08delete_indicator)
              ,(r.t09entry_number, r.t09entry_format, r.t09standard_entry_identifier, r.t09minimum, r.t09maximum, r.t09entry_prompt, r.f_menu_09, r.t09delete_indicator)
              ,(r.t10entry_number, r.t10entry_format, r.t10standard_entry_identifier, r.t10minimum, r.t10maximum, r.t10entry_prompt, r.f_menu_10, r.t10delete_indicator)
              ,(r.t11entry_number, r.t11entry_format, r.t11standard_entry_identifier, r.t11minimum, r.t11maximum, r.t11entry_prompt, r.f_menu_11, r.t11delete_indicator)
              ,(r.t12entry_number, r.t12entry_format, r.t12standard_entry_identifier, r.t12minimum, r.t12maximum, r.t12entry_prompt, r.f_menu_12, r.t12delete_indicator)
              ,(r.t13entry_number, r.t13entry_format, r.t13standard_entry_identifier, r.t13minimum, r.t13maximum, r.t13entry_prompt, r.f_menu_13, r.t13delete_indicator)
              ,(r.t14entry_number, r.t14entry_format, r.t14standard_entry_identifier, r.t14minimum, r.t14maximum, r.t14entry_prompt, r.f_menu_14, r.t14delete_indicator)
              ,(r.t15entry_number, r.t15entry_format, r.t15standard_entry_identifier, r.t15minimum, r.t15maximum, r.t15entry_prompt, r.f_menu_15, r.t15delete_indicator)
              ,(r.t16entry_number, r.t16entry_format, r.t16standard_entry_identifier, r.t16minimum, r.t16maximum, r.t16entry_prompt, r.f_menu_16, r.t16delete_indicator)
              ,(r.t17entry_number, r.t17entry_format, r.t17standard_entry_identifier, r.t17minimum, r.t17maximum, r.t17entry_prompt, r.f_menu_17, r.t17delete_indicator)
              ,(r.t18entry_number, r.t18entry_format, r.t18standard_entry_identifier, r.t18minimum, r.t18maximum, r.t18entry_prompt, r.f_menu_18, r.t18delete_indicator)
              ,(r.t19entry_number, r.t19entry_format, r.t19standard_entry_identifier, r.t19minimum, r.t19maximum, r.t19entry_prompt, r.f_menu_19, r.t19delete_indicator)
              ,(r.t20entry_number, r.t20entry_format, r.t20standard_entry_identifier, r.t20minimum, r.t20maximum, r.t20entry_prompt, r.f_menu_20, r.t20delete_indicator)
              ,(r.t21entry_number, r.t21entry_format, r.t21standard_entry_identifier, r.t21minimum, r.t21maximum, r.t21entry_prompt, r.f_menu_21, r.t21delete_indicator)
              ,(r.t22entry_number, r.t22entry_format, r.t22standard_entry_identifier, r.t22minimum, r.t22maximum, r.t22entry_prompt, r.f_menu_22, r.t22delete_indicator)
              ,(r.t23entry_number, r.t23entry_format, r.t23standard_entry_identifier, r.t23minimum, r.t23maximum, r.t23entry_prompt, r.f_menu_23, r.t23delete_indicator)
              ,(r.t24entry_number, r.t24entry_format, r.t24standard_entry_identifier, r.t24minimum, r.t24maximum, r.t24entry_prompt, r.f_menu_24, r.t24delete_indicator)
              ,(r.t25entry_number, r.t25entry_format, r.t25standard_entry_identifier, r.t25minimum, r.t25maximum, r.t25entry_prompt, r.f_menu_25, r.t25delete_indicator)
              ,(r.t26entry_number, r.t26entry_format, r.t26standard_entry_identifier, r.t26minimum, r.t26maximum, r.t26entry_prompt, r.f_menu_26, r.t26delete_indicator)
              ,(r.t27entry_number, r.t27entry_format, r.t27standard_entry_identifier, r.t27minimum, r.t27maximum, r.t27entry_prompt, r.f_menu_27, r.t27delete_indicator)
              ,(r.t28entry_number, r.t28entry_format, r.t28standard_entry_identifier, r.t28minimum, r.t28maximum, r.t28entry_prompt, r.f_menu_28, r.t28delete_indicator)
              ,(r.t29entry_number, r.t29entry_format, r.t29standard_entry_identifier, r.t29minimum, r.t29maximum, r.t29entry_prompt, r.f_menu_29, r.t29delete_indicator)
              ,(r.t30entry_number, r.t30entry_format, r.t30standard_entry_identifier, r.t30minimum, r.t30maximum, r.t30entry_prompt, r.f_menu_30, r.t30delete_indicator)
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
--
;
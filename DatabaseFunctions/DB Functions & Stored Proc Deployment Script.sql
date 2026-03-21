/*
-- SQL Script to extract out the DB functions & stored procedures:
SELECT n.nspname 				 		AS schema_name
      ,p.proname 				 		AS routine_name
      ,pg_get_functiondef(p.oid)||';' 	AS definition
  FROM pg_proc 		p
  JOIN pg_namespace 	n
    ON n.oid = p.pronamespace
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
   AND n.nspname = 'csds'
   AND p.proname NOT LIKE 'sem_%'
 ORDER BY schema_name, routine_name;
*/
-- ===========================================================
-- For reference: Objects that have been dropped from the DB:
-- ===========================================================
-- fn_sort_terminal_entries
-- fn_terminal_entry_number_calc
-- proc_default_terminal_entries
-- proc_default_terminal_entries_p
-- proc_define_release_package_content
-- proc_offence_sow_sof_substitution
-- proc_populate_release_package_content
-- proc_terminal_entry_sow_sof_prompt_substitution
-- proc_clone_offence_master_data_insert
-- ===========================================================
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
CREATE OR REPLACE FUNCTION csds.fn_create_offence_header(v_cjs_code text, v_batchid bigint, v_username text)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
	
	v_oh_id integer;		 -- Variable to store the offence header ID

BEGIN

	-- Attempt to retrieve an existing offence header ID associated with the provided CJS code
    SELECT DISTINCT goh.oh_id INTO v_oh_id
      FROM      csds.sa_offence_revision  sor			-- Source Offence revision table
      LEFT JOIN csds.gd_offence_revision  gor			-- Golden Offence revision table
	         ON sor.cjs_code = gor.cjs_code
	  LEFT JOIN csds.gd_offence_header 	  goh			-- Golden Offence header table
	         ON gor.f_offence_header = goh.oh_id
	 WHERE sor.cjs_code = v_cjs_code;					-- Filter by the input CJS code
    
	
	-- If no Offence header ID was found, create a new one
	IF v_oh_id IS NULL THEN
		
		-- Insert a new Offence header record
        INSERT INTO csds.gd_offence_header (
            oh_id,
            b_classname,
            b_batchid,
            b_credate,
            b_upddate,
            b_creator,
            b_updator
        )
        VALUES (
            nextval('csds.seq_offence_header'),
            'OffenceHeader',
            v_batchid,
            now(),
            now(),
            v_username,
            v_username
        )
        RETURNING oh_id INTO v_oh_id;					-- Store the generated OH_ID into variable
		
	END IF;
-- Return the Offence header ID (existing or newly created)
RETURN v_oh_id;

END;
$function$
;
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
        WHERE sow_reference = v_code
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
      AND sys_pnld_data_hash = v_menu_md5;

    RETURN v_menu_id;
END;
$function$
;
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
CREATE OR REPLACE FUNCTION csds.fn_get_offence_next_date_from(v_ofr_id numeric)
 RETURNS date
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_cjs_code TEXT;
    v_next_date DATE;
BEGIN
    -- Get the cjs_code for the provided ofr_id
    SELECT cjs_code
    INTO v_cjs_code
    FROM csds.gd_offence_revision
    WHERE ofr_id = v_ofr_id;

    -- If not found, return NULL
    IF v_cjs_code IS NULL THEN
        RETURN NULL;
    END IF;

    -- Get the next version's date_used_from
    SELECT date_used_from
    INTO v_next_date
    FROM csds.gd_offence_revision
    WHERE cjs_code = v_cjs_code
      AND ofr_id > v_ofr_id
    ORDER BY ofr_id
    LIMIT 1;

    -- Return the date (NULL if none found)
    RETURN v_next_date;
END;
$function$
;
CREATE OR REPLACE FUNCTION csds.fn_get_offence_next_version_type(v_ofr_id numeric)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_cjs_code TEXT;
    v_next_version_type TEXT;
BEGIN
    -- Get the cjs_code for the provided ofr_id
    SELECT cjs_code
    INTO v_cjs_code
    FROM csds.gd_offence_revision
    WHERE ofr_id = v_ofr_id;

    -- If not found, return None
    IF v_cjs_code IS NULL THEN
        RETURN 'None';
    END IF;

    -- Find the next version (next highest ofr_id for this cjs_code)
    SELECT version_type
    INTO v_next_version_type
    FROM csds.gd_offence_revision
    WHERE cjs_code = v_cjs_code
      AND ofr_id > v_ofr_id       -- strictly higher = next version
    ORDER BY ofr_id
    LIMIT 1;

    -- If no next version exists, return 'None'
    IF v_next_version_type IS NULL THEN
        RETURN 'None';
    END IF;

    RETURN v_next_version_type;
END;
$function$
;
CREATE OR REPLACE FUNCTION csds.fn_get_offence_revision_info(v_cjs_code text, v_loadid bigint, v_type text)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
    
	v_return bigint;	-- Variable used to store the result returned by the function

BEGIN

	-- If the request type is 'Version'
	-- Retrieve the latest version number for the given CJS code
	IF v_type = 'Version' THEN
	
		-- Will return NULL if no rows exist or if all version_number values are NULL
 	    SELECT MAX(version_number) INTO v_return
          FROM csds.gd_offence_revision
		 WHERE cjs_code = v_cjs_code;

/*	

	-- *** This functionality is no longer required as the User have requested that we do not truncate the CJS Title, 
    -- *** instead we clone it as-is and then have a validation if it is longer than 120 characters.
	--
	-- If the request type is 'CJSTitle'
	-- Perform validation on the CJS title length depending on its prefix
	ELSIF v_type = 'CJSTitle' THEN
		
		-- CJS Code is passed as NULL in this case.
		-- If it were provided, validation might incorrectly set an error message against the CJS Code as well.
		SELECT CASE WHEN SUBSTR(sor.cjs_title,1,10) = 'Attempt - '      AND LENGTH(gor.cjs_title) > 110 THEN 1	-- If title starts with 'Attempt - ' and exceeds 110 characters, return 1
					WHEN SUBSTR(sor.cjs_title,1,13) = 'Conspiracy - '   AND LENGTH(gor.cjs_title) > 107 THEN 1	-- If title starts with 'Conspiracy - ' and exceeds 107 characters, return 1
					WHEN SUBSTR(sor.cjs_title,1,15) = 'Aid and Abet - ' AND LENGTH(gor.cjs_title) > 105 THEN 1	-- If title starts with 'Aid and Abet - ' and exceeds 105 characters, return 1
					ELSE 0																						-- If none of the above conditions are met, return 0
			   END 				   INTO v_return
	  FROM csds.sa_offence_revision	sor
	  JOIN csds.gd_offence_revision	gor ON sor.b_copiedfrom = gor.ofr_id
	 WHERE sor.b_loadid  = v_loadid;
*/

	END IF;

    RETURN v_return;	-- Return the calculated value
	
END;
$function$
;
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
      AND sow_reference = v_pnld_ref
      AND pnld_date_of_last_update = v_date_of_last_update
	  AND b_loadid = v_loadid;

    RETURN v_offence_revision_id;
END;
$function$
;
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
CREATE OR REPLACE FUNCTION csds.fn_new_revision_date_validation(v_cjs_code text)
 RETURNS date
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_result DATE;

BEGIN

SELECT date_used_from
INTO v_result
FROM  csds.gd_offence_revision
WHERE  cjs_code = v_cjs_code
and current_record_indicator = true   
LIMIT 1;

    RETURN v_result;
END;
$function$
;
CREATE OR REPLACE FUNCTION csds.fn_prevent_concurrent_user_edits(v_parentid numeric, v_entityname character varying, v_mode character varying)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
--
DECLARE
--
--row_count NUMERIC;
user 		VARCHAR(128);
--
BEGIN
-- Browsing Collection configuration:
IF v_entityName = 'OffenceRevision' AND v_mode = 'Browse' THEN
--
	SELECT STRING_AGG (b_loadcreator,',' ORDER BY b_loadcreator) INTO user
	  FROM
		(SELECT DISTINCT B.b_loadcreator
           FROM csds.sa_offence_revision		S
           JOIN csds.dl_batch  				   	B
            ON B.b_loadid  = S.b_loadid
         WHERE v_parentID  = S.ofr_id
           AND B.b_status  = 'RUNNING'
		   ) AS dist;
--
END IF;
--
IF v_entityName = 'OffenceMenu' AND v_mode = 'Browse' THEN
--
	SELECT STRING_AGG (b_loadcreator,',' ORDER BY b_loadcreator) INTO user
	  FROM
		(SELECT DISTINCT B.b_loadcreator
           FROM csds.sa_ote_menu				S
           JOIN csds.dl_batch  				   	B
            ON B.b_loadid  = S.b_loadid
         WHERE v_parentID  = S.om_id
           AND B.b_status  = 'RUNNING'
		   ) AS dist;
--
END IF;
--
RETURN user;
--
END;
--
$function$
;
CREATE OR REPLACE PROCEDURE csds.proc_clone_offence_menu_options(v_currentloadid numeric, v_userid text)
 LANGUAGE plpgsql
AS $procedure$
--
DECLARE
v_copiedFrom numeric(38) DEFAULT (SELECT b_copiedfrom FROM csds.sa_ote_menu WHERE b_loadid = v_currentLoadId);
v_parentID	 numeric(38) DEFAULT (SELECT om_id        FROM csds.sa_ote_menu WHERE b_loadid = v_currentLoadId);
--
BEGIN
--
INSERT INTO csds.sa_ote_menu_options 
(b_loadid,omo_id,b_classname,b_copiedfrom,b_credate,b_upddate,b_creator,b_updator
,option_number,option_text,version_number,changed_by,changed_date,f_ote_optn_menu,sys_show_deleted,sys_pnld_menu_data_hash,delete_indicator
,e01element_number,e01entry_format,e01minimum,e01maximum,e01entry_prompt,e01delete_indicator
,e02element_number,e02entry_format,e02minimum,e02maximum,e02entry_prompt,e02delete_indicator
,e03element_number,e03entry_format,e03minimum,e03maximum,e03entry_prompt,e03delete_indicator
,e04element_number,e04entry_format,e04minimum,e04maximum,e04entry_prompt,e04delete_indicator
,e05element_number,e05entry_format,e05minimum,e05maximum,e05entry_prompt,e05delete_indicator
,e06element_number,e06entry_format,e06minimum,e06maximum,e06entry_prompt,e06delete_indicator
,e07element_number,e07entry_format,e07minimum,e07maximum,e07entry_prompt,e07delete_indicator
,e08element_number,e08entry_format,e08minimum,e08maximum,e08entry_prompt,e08delete_indicator
,e09element_number,e09entry_format,e09minimum,e09maximum,e09entry_prompt,e09delete_indicator
,e10element_number,e10entry_format,e10minimum,e10maximum,e10entry_prompt,e10delete_indicator
,sys_optn_txt_elmnt_valid
)
SELECT 
 v_currentLoadId,nextval('csds.seq_ote_menu_options'),gdo.b_classname,gdo.omo_id,current_timestamp,current_timestamp,v_userID,v_userID
,gdo.option_number,gdo.option_text,gdo.version_number,gdo.changed_by,gdo.changed_date,v_parentID,gdo.sys_show_deleted,gdo.sys_pnld_menu_data_hash,gdo.delete_indicator
,gdo.e01element_number,gdo.e01entry_format,gdo.e01minimum,gdo.e01maximum,gdo.e01entry_prompt,gdo.e01delete_indicator
,gdo.e02element_number,gdo.e02entry_format,gdo.e02minimum,gdo.e02maximum,gdo.e02entry_prompt,gdo.e02delete_indicator
,gdo.e03element_number,gdo.e03entry_format,gdo.e03minimum,gdo.e03maximum,gdo.e03entry_prompt,gdo.e03delete_indicator
,gdo.e04element_number,gdo.e04entry_format,gdo.e04minimum,gdo.e04maximum,gdo.e04entry_prompt,gdo.e04delete_indicator
,gdo.e05element_number,gdo.e05entry_format,gdo.e05minimum,gdo.e05maximum,gdo.e05entry_prompt,gdo.e05delete_indicator
,gdo.e06element_number,gdo.e06entry_format,gdo.e06minimum,gdo.e06maximum,gdo.e06entry_prompt,gdo.e06delete_indicator
,gdo.e07element_number,gdo.e07entry_format,gdo.e07minimum,gdo.e07maximum,gdo.e07entry_prompt,gdo.e07delete_indicator
,gdo.e08element_number,gdo.e08entry_format,gdo.e08minimum,gdo.e08maximum,gdo.e08entry_prompt,gdo.e08delete_indicator
,gdo.e09element_number,gdo.e09entry_format,gdo.e09minimum,gdo.e09maximum,gdo.e09entry_prompt,gdo.e09delete_indicator
,gdo.e10element_number,gdo.e10entry_format,gdo.e10minimum,gdo.e10maximum,gdo.e10entry_prompt,gdo.e10delete_indicator
,gdo.sys_optn_txt_elmnt_valid
  FROM      csds.gd_ote_menu_options gdo
       JOIN csds.sa_ote_menu 		 som ON gdo.f_ote_optn_menu = som.b_copiedfrom
  LEFT JOIN csds.sa_ote_menu_options tgt ON tgt.b_loadid        = v_currentloadid  AND tgt.f_ote_optn_menu = som.om_id
 WHERE som.b_loadid = v_currentLoadID
   AND tgt.omo_id IS NULL;
 
-- WHERE f_ote_optn_menu  = v_copiedFrom
--   AND NOT EXISTS (SELECT 1 
--                     FROM csds.sa_ote_menu_options
-- 					WHERE b_loadid        = v_currentLoadID
-- 					  AND f_ote_optn_menu = v_parentID);
--
END;
--
$procedure$
;
CREATE OR REPLACE PROCEDURE csds.proc_get_offence_and_menu_status_changes(v_loadid numeric, v_user text)
 LANGUAGE plpgsql
AS $procedure$
BEGIN

    /*
     * ===============================================================
     *  CTE 1: Get the latest published offence revision per CJS code
     * ===============================================================
     */
    WITH cte_latest_published AS (
        SELECT 
            gor.cjs_code,
            gor.ofr_id AS latest_ofr_id
        FROM csds.sa_release_package srp
        INNER JOIN csds.gd_offence_revision gor 
            ON gor.f_release_package = srp.rp_id
        WHERE srp.b_loadid = v_loadid
          AND srp.status = 'Published'
    ),

    /*
     * ===============================================================
     *  CTE 2: Get the most recent previous (older) offence revision
     *         for each latest published offence
     * ===============================================================
     */
    cte_previous AS (
        SELECT 
            lp.cjs_code,
            MAX(prev.ofr_id) AS previous_ofr_id
        FROM cte_latest_published lp
        JOIN csds.gd_offence_revision prev
          ON prev.cjs_code = lp.cjs_code
         AND prev.ofr_id < lp.latest_ofr_id
        GROUP BY lp.cjs_code
    )


    /* ===============================================================
     * INSERT 1: Insert offence revisions related to the latest 
     *           published release package
     * =============================================================== */

    -- 1.1 Insert newly published offences
    INSERT INTO csds.sa_offence_revision (
        b_loadid,
		b_classname,
		b_credate,
		b_upddate,
		b_creator,
		b_updator,
        current_record_indicator, 
		t30sys_unq_entry_num_ind, t30clone_indicator, t30delete_indicator, t29sys_unq_entry_num_ind, t29clone_indicator, t29delete_indicator, t28sys_unq_entry_num_ind, t28clone_indicator, t28delete_indicator, t27sys_unq_entry_num_ind, t27clone_indicator, t27delete_indicator, t26sys_unq_entry_num_ind, t26clone_indicator, t26delete_indicator, t25sys_unq_entry_num_ind, t25clone_indicator, t25delete_indicator, t24sys_unq_entry_num_ind, t24clone_indicator, t24delete_indicator, t23sys_unq_entry_num_ind, t23clone_indicator, t23delete_indicator, t22sys_unq_entry_num_ind, t22clone_indicator, t22delete_indicator, t21sys_unq_entry_num_ind, t21clone_indicator, t21delete_indicator, t20sys_unq_entry_num_ind, t20clone_indicator, t20delete_indicator, t19sys_unq_entry_num_ind, t19clone_indicator, t19delete_indicator, t18sys_unq_entry_num_ind, t18clone_indicator, t18delete_indicator, t17sys_unq_entry_num_ind, t17clone_indicator, t17delete_indicator, t16sys_unq_entry_num_ind, t16clone_indicator, t16delete_indicator, t15sys_unq_entry_num_ind, t15clone_indicator, t15delete_indicator, t14sys_unq_entry_num_ind, t14clone_indicator, t14delete_indicator, t13sys_unq_entry_num_ind, t13clone_indicator, t13delete_indicator, t12sys_unq_entry_num_ind, t12clone_indicator, t12delete_indicator, t11sys_unq_entry_num_ind, t11clone_indicator, t11delete_indicator, t10sys_unq_entry_num_ind, t10clone_indicator, t10delete_indicator, t09sys_unq_entry_num_ind, t09clone_indicator, t09delete_indicator, t08sys_unq_entry_num_ind, t08clone_indicator, t08delete_indicator, t07sys_unq_entry_num_ind, t07clone_indicator, t07delete_indicator, t06sys_unq_entry_num_ind, t06clone_indicator, t06delete_indicator, t05sys_unq_entry_num_ind, t05clone_indicator, t05delete_indicator, t04sys_unq_entry_num_ind, t04clone_indicator, t04delete_indicator, t03sys_unq_entry_num_ind, t03clone_indicator, t03delete_indicator, t02sys_unq_entry_num_ind, t02clone_indicator, t02delete_indicator, t01sys_unq_entry_num_ind, t01clone_indicator, t01delete_indicator, prosecutor_as_third_party, court_extract_available, court_of_appeal_flag, commissioner_of_oath, plea_applicable_flag, appeal_flag, exparte, contested_fee, initial_fee, can_be_bulk, sys_sort_terminal_entry, sys_show_deleted, traffic_control, t30sys_terminal_entry_short, t30standard_entry_identifier, t30entry_prompt, t30entry_format, t29sys_terminal_entry_short, t29standard_entry_identifier, t29entry_prompt, t29entry_format, t28sys_terminal_entry_short, t28standard_entry_identifier, t28entry_prompt, t28entry_format, t27sys_terminal_entry_short, t27standard_entry_identifier, t27entry_prompt, t27entry_format, t26sys_terminal_entry_short, t26standard_entry_identifier, t26entry_prompt, t26entry_format, t25sys_terminal_entry_short, t25standard_entry_identifier, t25entry_prompt, t25entry_format, t24sys_terminal_entry_short, t24standard_entry_identifier, t24entry_prompt, t24entry_format, t23sys_terminal_entry_short, t23standard_entry_identifier, t23entry_prompt, t23entry_format, t22sys_terminal_entry_short, t22standard_entry_identifier, t22entry_prompt, t22entry_format, t21sys_terminal_entry_short, t21standard_entry_identifier, t21entry_prompt, t21entry_format, t20sys_terminal_entry_short, t20standard_entry_identifier, t20entry_prompt, t20entry_format, t19sys_terminal_entry_short, t19standard_entry_identifier, t19entry_prompt, t19entry_format, t18sys_terminal_entry_short, t18standard_entry_identifier, t18entry_prompt, t18entry_format, t17sys_terminal_entry_short, t17standard_entry_identifier, t17entry_prompt, t17entry_format, t16sys_terminal_entry_short, t16standard_entry_identifier, t16entry_prompt, t16entry_format, t15sys_terminal_entry_short, t15standard_entry_identifier, t15entry_prompt, t15entry_format, t14sys_terminal_entry_short, t14standard_entry_identifier, t14entry_prompt, t14entry_format, t13sys_terminal_entry_short, t13standard_entry_identifier, t13entry_prompt, t13entry_format, t12sys_terminal_entry_short, t12standard_entry_identifier, t12entry_prompt, t12entry_format, t11sys_terminal_entry_short, t11standard_entry_identifier, t11entry_prompt, t11entry_format, t10sys_terminal_entry_short, t10standard_entry_identifier, t10entry_prompt, t10entry_format, t09sys_terminal_entry_short, t09standard_entry_identifier, t09entry_prompt, t09entry_format, t08sys_terminal_entry_short, t08standard_entry_identifier, t08entry_prompt, t08entry_format, t07sys_terminal_entry_short, t07standard_entry_identifier, t07entry_prompt, t07entry_format, t06sys_terminal_entry_short, t06standard_entry_identifier, t06entry_prompt, t06entry_format, t05sys_terminal_entry_short, t05standard_entry_identifier, t05entry_prompt, t05entry_format, t04sys_terminal_entry_short, t04standard_entry_identifier, t04entry_prompt, t04entry_format, t03sys_terminal_entry_short, t03standard_entry_identifier, t03entry_prompt, t03entry_format, t02sys_terminal_entry_short, t02standard_entry_identifier, t02entry_prompt, t02entry_format, t01sys_terminal_entry_short, t01standard_entry_identifier, t01entry_prompt, t01entry_format, version_type, sys_terminal_entry_clone, sys_entry_number_sequence, sys_terminal_entry_long, sys_pnld_data_hash, obsolete_indicator, prefix, resentence_activation_cde, boxwork_ntfctn_tmplt, listing_ntfctn_tmplt, breach_type, active_offence_order, applicant_appellant_flag, hearing_code, link_type, summons_template_type, jurisdiction, application_synonym, offence_class, mis_classification, offence_source, offence_type, publishing_status, authoring_status, clone_type_code, cloned_from, sow_reference, sl_offence_act_sec_txt, sl_offence_wording_txt, sl_off_statmnt_fct_txt, blocked, cjs_code, current_editor, entry_prompt_substitution, entry_prmpt_sub_sof, entry_prmpt_sub_sow, user_acts_and_section, user_statement_of_facts, user_offence_wording, principal_offnc_category, location_flag, endorsable_flag, mode_of_trial, max_fine_type_magct_desc, max_fine_type_magct_code, prosecution_time_limit, sl_pnld_standard_off_word, pnld_stndrd_offnc_wording, sl_cjs_title, derived_from_cjs_code, description, maximum_penalty, offence_notes, dvla_code, standard_list, custodial_indicator, cjs_title, reportable, recordable, pss_changed_date, pnld_offence_end_date, pnld_offence_start_date, pnld_date_of_last_update, date_used_to, date_used_from, f_offence_header, f_menu_30, f_menu_29, f_menu_28, f_menu_27, f_menu_26, f_menu_25, f_menu_24, f_menu_23, f_menu_22, f_menu_21, f_menu_20, f_menu_19, f_menu_18, f_menu_17, f_menu_16, f_menu_15, f_menu_14, f_menu_13, f_menu_12, f_menu_11, f_menu_10, f_menu_09, f_menu_08, f_menu_07, f_menu_06, f_menu_05, f_menu_04, f_menu_03, f_menu_02, f_menu_01, f_release_package, t30maximum, t30minimum, t30entry_number, t29maximum, t29minimum, t29entry_number, t28maximum, t28minimum, t28entry_number, t27maximum, t27minimum, t27entry_number, t26maximum, t26minimum, t26entry_number, t25maximum, t25minimum, t25entry_number, t24maximum, t24minimum, t24entry_number, t23maximum, t23minimum, t23entry_number, t22maximum, t22minimum, t22entry_number, t21maximum, t21minimum, t21entry_number, t20maximum, t20minimum, t20entry_number, t19maximum, t19minimum, t19entry_number, t18maximum, t18minimum, t18entry_number, t17maximum, t17minimum, t17entry_number, t16maximum, t16minimum, t16entry_number, t15maximum, t15minimum, t15entry_number, t14maximum, t14minimum, t14entry_number, t13maximum, t13minimum, t13entry_number, t12maximum, t12minimum, t12entry_number, t11maximum, t11minimum, t11entry_number, t10maximum, t10minimum, t10entry_number, t09maximum, t09minimum, t09entry_number, t08maximum, t08minimum, t08entry_number, t07maximum, t07minimum, t07entry_number, t06maximum, t06minimum, t06entry_number, t05maximum, t05minimum, t05entry_number, t04maximum, t04minimum, t04entry_number, t03maximum, t03minimum, t03entry_number, t02maximum, t02minimum, t02entry_number, t01maximum, t01minimum, t01entry_number, pss_csh_csh_id, pss_ofr_id, pss_changed_by, sys_max_entry_number, sys_cloned_to, offence_code, area, proceedings_code, ho_subclass, ho_class, version_number, ofr_id
		)
    SELECT 
        v_loadid,
		gor.b_classname,
		gor.b_credate,
		CURRENT_TIMESTAMP,
		gor.b_creator,
		v_user,
        TRUE,
		gor.t30sys_unq_entry_num_ind, gor.t30clone_indicator, gor.t30delete_indicator, gor.t29sys_unq_entry_num_ind, gor.t29clone_indicator, gor.t29delete_indicator, gor.t28sys_unq_entry_num_ind, gor.t28clone_indicator, gor.t28delete_indicator, gor.t27sys_unq_entry_num_ind, gor.t27clone_indicator, gor.t27delete_indicator, gor.t26sys_unq_entry_num_ind, gor.t26clone_indicator, gor.t26delete_indicator, gor.t25sys_unq_entry_num_ind, gor.t25clone_indicator, gor.t25delete_indicator, gor.t24sys_unq_entry_num_ind, gor.t24clone_indicator, gor.t24delete_indicator, gor.t23sys_unq_entry_num_ind, gor.t23clone_indicator, gor.t23delete_indicator, gor.t22sys_unq_entry_num_ind, gor.t22clone_indicator, gor.t22delete_indicator, gor.t21sys_unq_entry_num_ind, gor.t21clone_indicator, gor.t21delete_indicator, gor.t20sys_unq_entry_num_ind, gor.t20clone_indicator, gor.t20delete_indicator, gor.t19sys_unq_entry_num_ind, gor.t19clone_indicator, gor.t19delete_indicator, gor.t18sys_unq_entry_num_ind, gor.t18clone_indicator, gor.t18delete_indicator, gor.t17sys_unq_entry_num_ind, gor.t17clone_indicator, gor.t17delete_indicator, gor.t16sys_unq_entry_num_ind, gor.t16clone_indicator, gor.t16delete_indicator, gor.t15sys_unq_entry_num_ind, gor.t15clone_indicator, gor.t15delete_indicator, gor.t14sys_unq_entry_num_ind, gor.t14clone_indicator, gor.t14delete_indicator, gor.t13sys_unq_entry_num_ind, gor.t13clone_indicator, gor.t13delete_indicator, gor.t12sys_unq_entry_num_ind, gor.t12clone_indicator, gor.t12delete_indicator, gor.t11sys_unq_entry_num_ind, gor.t11clone_indicator, gor.t11delete_indicator, gor.t10sys_unq_entry_num_ind, gor.t10clone_indicator, gor.t10delete_indicator, gor.t09sys_unq_entry_num_ind, gor.t09clone_indicator, gor.t09delete_indicator, gor.t08sys_unq_entry_num_ind, gor.t08clone_indicator, gor.t08delete_indicator, gor.t07sys_unq_entry_num_ind, gor.t07clone_indicator, gor.t07delete_indicator, gor.t06sys_unq_entry_num_ind, gor.t06clone_indicator, gor.t06delete_indicator, gor.t05sys_unq_entry_num_ind, gor.t05clone_indicator, gor.t05delete_indicator, gor.t04sys_unq_entry_num_ind, gor.t04clone_indicator, gor.t04delete_indicator, gor.t03sys_unq_entry_num_ind, gor.t03clone_indicator, gor.t03delete_indicator, gor.t02sys_unq_entry_num_ind, gor.t02clone_indicator, gor.t02delete_indicator, gor.t01sys_unq_entry_num_ind, gor.t01clone_indicator, gor.t01delete_indicator, gor.prosecutor_as_third_party, gor.court_extract_available, gor.court_of_appeal_flag, gor.commissioner_of_oath, gor.plea_applicable_flag, gor.appeal_flag, gor.exparte, gor.contested_fee, gor.initial_fee, gor.can_be_bulk, gor.sys_sort_terminal_entry, gor.sys_show_deleted, gor.traffic_control, gor.t30sys_terminal_entry_short, gor.t30standard_entry_identifier, gor.t30entry_prompt, gor.t30entry_format, gor.t29sys_terminal_entry_short, gor.t29standard_entry_identifier, gor.t29entry_prompt, gor.t29entry_format, gor.t28sys_terminal_entry_short, gor.t28standard_entry_identifier, gor.t28entry_prompt, gor.t28entry_format, gor.t27sys_terminal_entry_short, gor.t27standard_entry_identifier, gor.t27entry_prompt, gor.t27entry_format, gor.t26sys_terminal_entry_short, gor.t26standard_entry_identifier, gor.t26entry_prompt, gor.t26entry_format, gor.t25sys_terminal_entry_short, gor.t25standard_entry_identifier, gor.t25entry_prompt, gor.t25entry_format, gor.t24sys_terminal_entry_short, gor.t24standard_entry_identifier, gor.t24entry_prompt, gor.t24entry_format, gor.t23sys_terminal_entry_short, gor.t23standard_entry_identifier, gor.t23entry_prompt, gor.t23entry_format, gor.t22sys_terminal_entry_short, gor.t22standard_entry_identifier, gor.t22entry_prompt, gor.t22entry_format, gor.t21sys_terminal_entry_short, gor.t21standard_entry_identifier, gor.t21entry_prompt, gor.t21entry_format, gor.t20sys_terminal_entry_short, gor.t20standard_entry_identifier, gor.t20entry_prompt, gor.t20entry_format, gor.t19sys_terminal_entry_short, gor.t19standard_entry_identifier, gor.t19entry_prompt, gor.t19entry_format, gor.t18sys_terminal_entry_short, gor.t18standard_entry_identifier, gor.t18entry_prompt, gor.t18entry_format, gor.t17sys_terminal_entry_short, gor.t17standard_entry_identifier, gor.t17entry_prompt, gor.t17entry_format, gor.t16sys_terminal_entry_short, gor.t16standard_entry_identifier, gor.t16entry_prompt, gor.t16entry_format, gor.t15sys_terminal_entry_short, gor.t15standard_entry_identifier, gor.t15entry_prompt, gor.t15entry_format, gor.t14sys_terminal_entry_short, gor.t14standard_entry_identifier, gor.t14entry_prompt, gor.t14entry_format, gor.t13sys_terminal_entry_short, gor.t13standard_entry_identifier, gor.t13entry_prompt, gor.t13entry_format, gor.t12sys_terminal_entry_short, gor.t12standard_entry_identifier, gor.t12entry_prompt, gor.t12entry_format, gor.t11sys_terminal_entry_short, gor.t11standard_entry_identifier, gor.t11entry_prompt, gor.t11entry_format, gor.t10sys_terminal_entry_short, gor.t10standard_entry_identifier, gor.t10entry_prompt, gor.t10entry_format, gor.t09sys_terminal_entry_short, gor.t09standard_entry_identifier, gor.t09entry_prompt, gor.t09entry_format, gor.t08sys_terminal_entry_short, gor.t08standard_entry_identifier, gor.t08entry_prompt, gor.t08entry_format, gor.t07sys_terminal_entry_short, gor.t07standard_entry_identifier, gor.t07entry_prompt, gor.t07entry_format, gor.t06sys_terminal_entry_short, gor.t06standard_entry_identifier, gor.t06entry_prompt, gor.t06entry_format, gor.t05sys_terminal_entry_short, gor.t05standard_entry_identifier, gor.t05entry_prompt, gor.t05entry_format, gor.t04sys_terminal_entry_short, gor.t04standard_entry_identifier, gor.t04entry_prompt, gor.t04entry_format, gor.t03sys_terminal_entry_short, gor.t03standard_entry_identifier, gor.t03entry_prompt, gor.t03entry_format, gor.t02sys_terminal_entry_short, gor.t02standard_entry_identifier, gor.t02entry_prompt, gor.t02entry_format, gor.t01sys_terminal_entry_short, gor.t01standard_entry_identifier, gor.t01entry_prompt, gor.t01entry_format, gor.version_type, gor.sys_terminal_entry_clone, gor.sys_entry_number_sequence, gor.sys_terminal_entry_long, gor.sys_pnld_data_hash, gor.obsolete_indicator, gor.prefix, gor.resentence_activation_cde, gor.boxwork_ntfctn_tmplt, gor.listing_ntfctn_tmplt, gor.breach_type, gor.active_offence_order, gor.applicant_appellant_flag, gor.hearing_code, gor.link_type, gor.summons_template_type, gor.jurisdiction, gor.application_synonym, gor.offence_class, gor.mis_classification, gor.offence_source, gor.offence_type, gor.publishing_status, gor.authoring_status, gor.clone_type_code, gor.cloned_from, gor.sow_reference, gor.sl_offence_act_sec_txt, gor.sl_offence_wording_txt, gor.sl_off_statmnt_fct_txt, gor.blocked, gor.cjs_code, gor.current_editor, gor.entry_prompt_substitution, gor.entry_prmpt_sub_sof, gor.entry_prmpt_sub_sow, gor.user_acts_and_section, gor.user_statement_of_facts, gor.user_offence_wording, gor.principal_offnc_category, gor.location_flag, gor.endorsable_flag, gor.mode_of_trial, gor.max_fine_type_magct_desc, gor.max_fine_type_magct_code, gor.prosecution_time_limit, gor.sl_pnld_standard_off_word, gor.pnld_stndrd_offnc_wording, gor.sl_cjs_title, gor.derived_from_cjs_code, gor.description, gor.maximum_penalty, gor.offence_notes, gor.dvla_code, gor.standard_list, gor.custodial_indicator, gor.cjs_title, gor.reportable, gor.recordable, gor.pss_changed_date, gor.pnld_offence_end_date, gor.pnld_offence_start_date, gor.pnld_date_of_last_update, gor.date_used_to, gor.date_used_from, gor.f_offence_header, gor.f_menu_30, gor.f_menu_29, gor.f_menu_28, gor.f_menu_27, gor.f_menu_26, gor.f_menu_25, gor.f_menu_24, gor.f_menu_23, gor.f_menu_22, gor.f_menu_21, gor.f_menu_20, gor.f_menu_19, gor.f_menu_18, gor.f_menu_17, gor.f_menu_16, gor.f_menu_15, gor.f_menu_14, gor.f_menu_13, gor.f_menu_12, gor.f_menu_11, gor.f_menu_10, gor.f_menu_09, gor.f_menu_08, gor.f_menu_07, gor.f_menu_06, gor.f_menu_05, gor.f_menu_04, gor.f_menu_03, gor.f_menu_02, gor.f_menu_01, gor.f_release_package, gor.t30maximum, gor.t30minimum, gor.t30entry_number, gor.t29maximum, gor.t29minimum, gor.t29entry_number, gor.t28maximum, gor.t28minimum, gor.t28entry_number, gor.t27maximum, gor.t27minimum, gor.t27entry_number, gor.t26maximum, gor.t26minimum, gor.t26entry_number, gor.t25maximum, gor.t25minimum, gor.t25entry_number, gor.t24maximum, gor.t24minimum, gor.t24entry_number, gor.t23maximum, gor.t23minimum, gor.t23entry_number, gor.t22maximum, gor.t22minimum, gor.t22entry_number, gor.t21maximum, gor.t21minimum, gor.t21entry_number, gor.t20maximum, gor.t20minimum, gor.t20entry_number, gor.t19maximum, gor.t19minimum, gor.t19entry_number, gor.t18maximum, gor.t18minimum, gor.t18entry_number, gor.t17maximum, gor.t17minimum, gor.t17entry_number, gor.t16maximum, gor.t16minimum, gor.t16entry_number, gor.t15maximum, gor.t15minimum, gor.t15entry_number, gor.t14maximum, gor.t14minimum, gor.t14entry_number, gor.t13maximum, gor.t13minimum, gor.t13entry_number, gor.t12maximum, gor.t12minimum, gor.t12entry_number, gor.t11maximum, gor.t11minimum, gor.t11entry_number, gor.t10maximum, gor.t10minimum, gor.t10entry_number, gor.t09maximum, gor.t09minimum, gor.t09entry_number, gor.t08maximum, gor.t08minimum, gor.t08entry_number, gor.t07maximum, gor.t07minimum, gor.t07entry_number, gor.t06maximum, gor.t06minimum, gor.t06entry_number, gor.t05maximum, gor.t05minimum, gor.t05entry_number, gor.t04maximum, gor.t04minimum, gor.t04entry_number, gor.t03maximum, gor.t03minimum, gor.t03entry_number, gor.t02maximum, gor.t02minimum, gor.t02entry_number, gor.t01maximum, gor.t01minimum, gor.t01entry_number, gor.pss_csh_csh_id, gor.pss_ofr_id, gor.pss_changed_by, gor.sys_max_entry_number, gor.sys_cloned_to, gor.offence_code, gor.area, gor.proceedings_code, gor.ho_subclass, gor.ho_class, gor.version_number, gor.ofr_id
    FROM csds.sa_release_package srp
    INNER JOIN csds.gd_offence_revision gor 
        ON gor.f_release_package = srp.rp_id
    WHERE srp.b_loadid = v_loadid
      AND srp.status = 'Published'

    UNION

    -- 1.2 Insert previous version of each published offence
    SELECT  
        v_loadid,
		gor.b_classname,
		gor.b_credate,
		CURRENT_TIMESTAMP,
		gor.b_creator,
		v_user,
        FALSE,
		gor.t30sys_unq_entry_num_ind, gor.t30clone_indicator, gor.t30delete_indicator, gor.t29sys_unq_entry_num_ind, gor.t29clone_indicator, gor.t29delete_indicator, gor.t28sys_unq_entry_num_ind, gor.t28clone_indicator, gor.t28delete_indicator, gor.t27sys_unq_entry_num_ind, gor.t27clone_indicator, gor.t27delete_indicator, gor.t26sys_unq_entry_num_ind, gor.t26clone_indicator, gor.t26delete_indicator, gor.t25sys_unq_entry_num_ind, gor.t25clone_indicator, gor.t25delete_indicator, gor.t24sys_unq_entry_num_ind, gor.t24clone_indicator, gor.t24delete_indicator, gor.t23sys_unq_entry_num_ind, gor.t23clone_indicator, gor.t23delete_indicator, gor.t22sys_unq_entry_num_ind, gor.t22clone_indicator, gor.t22delete_indicator, gor.t21sys_unq_entry_num_ind, gor.t21clone_indicator, gor.t21delete_indicator, gor.t20sys_unq_entry_num_ind, gor.t20clone_indicator, gor.t20delete_indicator, gor.t19sys_unq_entry_num_ind, gor.t19clone_indicator, gor.t19delete_indicator, gor.t18sys_unq_entry_num_ind, gor.t18clone_indicator, gor.t18delete_indicator, gor.t17sys_unq_entry_num_ind, gor.t17clone_indicator, gor.t17delete_indicator, gor.t16sys_unq_entry_num_ind, gor.t16clone_indicator, gor.t16delete_indicator, gor.t15sys_unq_entry_num_ind, gor.t15clone_indicator, gor.t15delete_indicator, gor.t14sys_unq_entry_num_ind, gor.t14clone_indicator, gor.t14delete_indicator, gor.t13sys_unq_entry_num_ind, gor.t13clone_indicator, gor.t13delete_indicator, gor.t12sys_unq_entry_num_ind, gor.t12clone_indicator, gor.t12delete_indicator, gor.t11sys_unq_entry_num_ind, gor.t11clone_indicator, gor.t11delete_indicator, gor.t10sys_unq_entry_num_ind, gor.t10clone_indicator, gor.t10delete_indicator, gor.t09sys_unq_entry_num_ind, gor.t09clone_indicator, gor.t09delete_indicator, gor.t08sys_unq_entry_num_ind, gor.t08clone_indicator, gor.t08delete_indicator, gor.t07sys_unq_entry_num_ind, gor.t07clone_indicator, gor.t07delete_indicator, gor.t06sys_unq_entry_num_ind, gor.t06clone_indicator, gor.t06delete_indicator, gor.t05sys_unq_entry_num_ind, gor.t05clone_indicator, gor.t05delete_indicator, gor.t04sys_unq_entry_num_ind, gor.t04clone_indicator, gor.t04delete_indicator, gor.t03sys_unq_entry_num_ind, gor.t03clone_indicator, gor.t03delete_indicator, gor.t02sys_unq_entry_num_ind, gor.t02clone_indicator, gor.t02delete_indicator, gor.t01sys_unq_entry_num_ind, gor.t01clone_indicator, gor.t01delete_indicator, gor.prosecutor_as_third_party, gor.court_extract_available, gor.court_of_appeal_flag, gor.commissioner_of_oath, gor.plea_applicable_flag, gor.appeal_flag, gor.exparte, gor.contested_fee, gor.initial_fee, gor.can_be_bulk, gor.sys_sort_terminal_entry, gor.sys_show_deleted, gor.traffic_control, gor.t30sys_terminal_entry_short, gor.t30standard_entry_identifier, gor.t30entry_prompt, gor.t30entry_format, gor.t29sys_terminal_entry_short, gor.t29standard_entry_identifier, gor.t29entry_prompt, gor.t29entry_format, gor.t28sys_terminal_entry_short, gor.t28standard_entry_identifier, gor.t28entry_prompt, gor.t28entry_format, gor.t27sys_terminal_entry_short, gor.t27standard_entry_identifier, gor.t27entry_prompt, gor.t27entry_format, gor.t26sys_terminal_entry_short, gor.t26standard_entry_identifier, gor.t26entry_prompt, gor.t26entry_format, gor.t25sys_terminal_entry_short, gor.t25standard_entry_identifier, gor.t25entry_prompt, gor.t25entry_format, gor.t24sys_terminal_entry_short, gor.t24standard_entry_identifier, gor.t24entry_prompt, gor.t24entry_format, gor.t23sys_terminal_entry_short, gor.t23standard_entry_identifier, gor.t23entry_prompt, gor.t23entry_format, gor.t22sys_terminal_entry_short, gor.t22standard_entry_identifier, gor.t22entry_prompt, gor.t22entry_format, gor.t21sys_terminal_entry_short, gor.t21standard_entry_identifier, gor.t21entry_prompt, gor.t21entry_format, gor.t20sys_terminal_entry_short, gor.t20standard_entry_identifier, gor.t20entry_prompt, gor.t20entry_format, gor.t19sys_terminal_entry_short, gor.t19standard_entry_identifier, gor.t19entry_prompt, gor.t19entry_format, gor.t18sys_terminal_entry_short, gor.t18standard_entry_identifier, gor.t18entry_prompt, gor.t18entry_format, gor.t17sys_terminal_entry_short, gor.t17standard_entry_identifier, gor.t17entry_prompt, gor.t17entry_format, gor.t16sys_terminal_entry_short, gor.t16standard_entry_identifier, gor.t16entry_prompt, gor.t16entry_format, gor.t15sys_terminal_entry_short, gor.t15standard_entry_identifier, gor.t15entry_prompt, gor.t15entry_format, gor.t14sys_terminal_entry_short, gor.t14standard_entry_identifier, gor.t14entry_prompt, gor.t14entry_format, gor.t13sys_terminal_entry_short, gor.t13standard_entry_identifier, gor.t13entry_prompt, gor.t13entry_format, gor.t12sys_terminal_entry_short, gor.t12standard_entry_identifier, gor.t12entry_prompt, gor.t12entry_format, gor.t11sys_terminal_entry_short, gor.t11standard_entry_identifier, gor.t11entry_prompt, gor.t11entry_format, gor.t10sys_terminal_entry_short, gor.t10standard_entry_identifier, gor.t10entry_prompt, gor.t10entry_format, gor.t09sys_terminal_entry_short, gor.t09standard_entry_identifier, gor.t09entry_prompt, gor.t09entry_format, gor.t08sys_terminal_entry_short, gor.t08standard_entry_identifier, gor.t08entry_prompt, gor.t08entry_format, gor.t07sys_terminal_entry_short, gor.t07standard_entry_identifier, gor.t07entry_prompt, gor.t07entry_format, gor.t06sys_terminal_entry_short, gor.t06standard_entry_identifier, gor.t06entry_prompt, gor.t06entry_format, gor.t05sys_terminal_entry_short, gor.t05standard_entry_identifier, gor.t05entry_prompt, gor.t05entry_format, gor.t04sys_terminal_entry_short, gor.t04standard_entry_identifier, gor.t04entry_prompt, gor.t04entry_format, gor.t03sys_terminal_entry_short, gor.t03standard_entry_identifier, gor.t03entry_prompt, gor.t03entry_format, gor.t02sys_terminal_entry_short, gor.t02standard_entry_identifier, gor.t02entry_prompt, gor.t02entry_format, gor.t01sys_terminal_entry_short, gor.t01standard_entry_identifier, gor.t01entry_prompt, gor.t01entry_format, gor.version_type, gor.sys_terminal_entry_clone, gor.sys_entry_number_sequence, gor.sys_terminal_entry_long, gor.sys_pnld_data_hash, gor.obsolete_indicator, gor.prefix, gor.resentence_activation_cde, gor.boxwork_ntfctn_tmplt, gor.listing_ntfctn_tmplt, gor.breach_type, gor.active_offence_order, gor.applicant_appellant_flag, gor.hearing_code, gor.link_type, gor.summons_template_type, gor.jurisdiction, gor.application_synonym, gor.offence_class, gor.mis_classification, gor.offence_source, gor.offence_type, gor.publishing_status, gor.authoring_status, gor.clone_type_code, gor.cloned_from, gor.sow_reference, gor.sl_offence_act_sec_txt, gor.sl_offence_wording_txt, gor.sl_off_statmnt_fct_txt, gor.blocked, gor.cjs_code, gor.current_editor, gor.entry_prompt_substitution, gor.entry_prmpt_sub_sof, gor.entry_prmpt_sub_sow, gor.user_acts_and_section, gor.user_statement_of_facts, gor.user_offence_wording, gor.principal_offnc_category, gor.location_flag, gor.endorsable_flag, gor.mode_of_trial, gor.max_fine_type_magct_desc, gor.max_fine_type_magct_code, gor.prosecution_time_limit, gor.sl_pnld_standard_off_word, gor.pnld_stndrd_offnc_wording, gor.sl_cjs_title, gor.derived_from_cjs_code, gor.description, gor.maximum_penalty, gor.offence_notes, gor.dvla_code, gor.standard_list, gor.custodial_indicator, gor.cjs_title, gor.reportable, gor.recordable, gor.pss_changed_date, gor.pnld_offence_end_date, gor.pnld_offence_start_date, gor.pnld_date_of_last_update, gor.date_used_to, gor.date_used_from, gor.f_offence_header, gor.f_menu_30, gor.f_menu_29, gor.f_menu_28, gor.f_menu_27, gor.f_menu_26, gor.f_menu_25, gor.f_menu_24, gor.f_menu_23, gor.f_menu_22, gor.f_menu_21, gor.f_menu_20, gor.f_menu_19, gor.f_menu_18, gor.f_menu_17, gor.f_menu_16, gor.f_menu_15, gor.f_menu_14, gor.f_menu_13, gor.f_menu_12, gor.f_menu_11, gor.f_menu_10, gor.f_menu_09, gor.f_menu_08, gor.f_menu_07, gor.f_menu_06, gor.f_menu_05, gor.f_menu_04, gor.f_menu_03, gor.f_menu_02, gor.f_menu_01, gor.f_release_package, gor.t30maximum, gor.t30minimum, gor.t30entry_number, gor.t29maximum, gor.t29minimum, gor.t29entry_number, gor.t28maximum, gor.t28minimum, gor.t28entry_number, gor.t27maximum, gor.t27minimum, gor.t27entry_number, gor.t26maximum, gor.t26minimum, gor.t26entry_number, gor.t25maximum, gor.t25minimum, gor.t25entry_number, gor.t24maximum, gor.t24minimum, gor.t24entry_number, gor.t23maximum, gor.t23minimum, gor.t23entry_number, gor.t22maximum, gor.t22minimum, gor.t22entry_number, gor.t21maximum, gor.t21minimum, gor.t21entry_number, gor.t20maximum, gor.t20minimum, gor.t20entry_number, gor.t19maximum, gor.t19minimum, gor.t19entry_number, gor.t18maximum, gor.t18minimum, gor.t18entry_number, gor.t17maximum, gor.t17minimum, gor.t17entry_number, gor.t16maximum, gor.t16minimum, gor.t16entry_number, gor.t15maximum, gor.t15minimum, gor.t15entry_number, gor.t14maximum, gor.t14minimum, gor.t14entry_number, gor.t13maximum, gor.t13minimum, gor.t13entry_number, gor.t12maximum, gor.t12minimum, gor.t12entry_number, gor.t11maximum, gor.t11minimum, gor.t11entry_number, gor.t10maximum, gor.t10minimum, gor.t10entry_number, gor.t09maximum, gor.t09minimum, gor.t09entry_number, gor.t08maximum, gor.t08minimum, gor.t08entry_number, gor.t07maximum, gor.t07minimum, gor.t07entry_number, gor.t06maximum, gor.t06minimum, gor.t06entry_number, gor.t05maximum, gor.t05minimum, gor.t05entry_number, gor.t04maximum, gor.t04minimum, gor.t04entry_number, gor.t03maximum, gor.t03minimum, gor.t03entry_number, gor.t02maximum, gor.t02minimum, gor.t02entry_number, gor.t01maximum, gor.t01minimum, gor.t01entry_number, gor.pss_csh_csh_id, gor.pss_ofr_id, gor.pss_changed_by, gor.sys_max_entry_number, gor.sys_cloned_to, gor.offence_code, gor.area, gor.proceedings_code, gor.ho_subclass, gor.ho_class, gor.version_number, gor.ofr_id
    FROM csds.gd_offence_revision gor
    JOIN cte_previous p
      ON gor.cjs_code = p.cjs_code
     AND gor.ofr_id   = p.previous_ofr_id;


    /* ===============================================================
     * INSERT 2: Insert previously published offences whose 
     *           publishing statuses need updating
     * =============================================================== */
    INSERT INTO csds.sa_offence_revision (
        b_loadid,
		b_classname,
		b_credate,
		b_upddate,
		b_creator,
		b_updator,
        current_record_indicator, 
		t30sys_unq_entry_num_ind, t30clone_indicator, t30delete_indicator, t29sys_unq_entry_num_ind, t29clone_indicator, t29delete_indicator, t28sys_unq_entry_num_ind, t28clone_indicator, t28delete_indicator, t27sys_unq_entry_num_ind, t27clone_indicator, t27delete_indicator, t26sys_unq_entry_num_ind, t26clone_indicator, t26delete_indicator, t25sys_unq_entry_num_ind, t25clone_indicator, t25delete_indicator, t24sys_unq_entry_num_ind, t24clone_indicator, t24delete_indicator, t23sys_unq_entry_num_ind, t23clone_indicator, t23delete_indicator, t22sys_unq_entry_num_ind, t22clone_indicator, t22delete_indicator, t21sys_unq_entry_num_ind, t21clone_indicator, t21delete_indicator, t20sys_unq_entry_num_ind, t20clone_indicator, t20delete_indicator, t19sys_unq_entry_num_ind, t19clone_indicator, t19delete_indicator, t18sys_unq_entry_num_ind, t18clone_indicator, t18delete_indicator, t17sys_unq_entry_num_ind, t17clone_indicator, t17delete_indicator, t16sys_unq_entry_num_ind, t16clone_indicator, t16delete_indicator, t15sys_unq_entry_num_ind, t15clone_indicator, t15delete_indicator, t14sys_unq_entry_num_ind, t14clone_indicator, t14delete_indicator, t13sys_unq_entry_num_ind, t13clone_indicator, t13delete_indicator, t12sys_unq_entry_num_ind, t12clone_indicator, t12delete_indicator, t11sys_unq_entry_num_ind, t11clone_indicator, t11delete_indicator, t10sys_unq_entry_num_ind, t10clone_indicator, t10delete_indicator, t09sys_unq_entry_num_ind, t09clone_indicator, t09delete_indicator, t08sys_unq_entry_num_ind, t08clone_indicator, t08delete_indicator, t07sys_unq_entry_num_ind, t07clone_indicator, t07delete_indicator, t06sys_unq_entry_num_ind, t06clone_indicator, t06delete_indicator, t05sys_unq_entry_num_ind, t05clone_indicator, t05delete_indicator, t04sys_unq_entry_num_ind, t04clone_indicator, t04delete_indicator, t03sys_unq_entry_num_ind, t03clone_indicator, t03delete_indicator, t02sys_unq_entry_num_ind, t02clone_indicator, t02delete_indicator, t01sys_unq_entry_num_ind, t01clone_indicator, t01delete_indicator, prosecutor_as_third_party, court_extract_available, court_of_appeal_flag, commissioner_of_oath, plea_applicable_flag, appeal_flag, exparte, contested_fee, initial_fee, can_be_bulk, sys_sort_terminal_entry, sys_show_deleted, traffic_control, t30sys_terminal_entry_short, t30standard_entry_identifier, t30entry_prompt, t30entry_format, t29sys_terminal_entry_short, t29standard_entry_identifier, t29entry_prompt, t29entry_format, t28sys_terminal_entry_short, t28standard_entry_identifier, t28entry_prompt, t28entry_format, t27sys_terminal_entry_short, t27standard_entry_identifier, t27entry_prompt, t27entry_format, t26sys_terminal_entry_short, t26standard_entry_identifier, t26entry_prompt, t26entry_format, t25sys_terminal_entry_short, t25standard_entry_identifier, t25entry_prompt, t25entry_format, t24sys_terminal_entry_short, t24standard_entry_identifier, t24entry_prompt, t24entry_format, t23sys_terminal_entry_short, t23standard_entry_identifier, t23entry_prompt, t23entry_format, t22sys_terminal_entry_short, t22standard_entry_identifier, t22entry_prompt, t22entry_format, t21sys_terminal_entry_short, t21standard_entry_identifier, t21entry_prompt, t21entry_format, t20sys_terminal_entry_short, t20standard_entry_identifier, t20entry_prompt, t20entry_format, t19sys_terminal_entry_short, t19standard_entry_identifier, t19entry_prompt, t19entry_format, t18sys_terminal_entry_short, t18standard_entry_identifier, t18entry_prompt, t18entry_format, t17sys_terminal_entry_short, t17standard_entry_identifier, t17entry_prompt, t17entry_format, t16sys_terminal_entry_short, t16standard_entry_identifier, t16entry_prompt, t16entry_format, t15sys_terminal_entry_short, t15standard_entry_identifier, t15entry_prompt, t15entry_format, t14sys_terminal_entry_short, t14standard_entry_identifier, t14entry_prompt, t14entry_format, t13sys_terminal_entry_short, t13standard_entry_identifier, t13entry_prompt, t13entry_format, t12sys_terminal_entry_short, t12standard_entry_identifier, t12entry_prompt, t12entry_format, t11sys_terminal_entry_short, t11standard_entry_identifier, t11entry_prompt, t11entry_format, t10sys_terminal_entry_short, t10standard_entry_identifier, t10entry_prompt, t10entry_format, t09sys_terminal_entry_short, t09standard_entry_identifier, t09entry_prompt, t09entry_format, t08sys_terminal_entry_short, t08standard_entry_identifier, t08entry_prompt, t08entry_format, t07sys_terminal_entry_short, t07standard_entry_identifier, t07entry_prompt, t07entry_format, t06sys_terminal_entry_short, t06standard_entry_identifier, t06entry_prompt, t06entry_format, t05sys_terminal_entry_short, t05standard_entry_identifier, t05entry_prompt, t05entry_format, t04sys_terminal_entry_short, t04standard_entry_identifier, t04entry_prompt, t04entry_format, t03sys_terminal_entry_short, t03standard_entry_identifier, t03entry_prompt, t03entry_format, t02sys_terminal_entry_short, t02standard_entry_identifier, t02entry_prompt, t02entry_format, t01sys_terminal_entry_short, t01standard_entry_identifier, t01entry_prompt, t01entry_format, version_type, sys_terminal_entry_clone, sys_entry_number_sequence, sys_terminal_entry_long, sys_pnld_data_hash, obsolete_indicator, prefix, resentence_activation_cde, boxwork_ntfctn_tmplt, listing_ntfctn_tmplt, breach_type, active_offence_order, applicant_appellant_flag, hearing_code, link_type, summons_template_type, jurisdiction, application_synonym, offence_class, mis_classification, offence_source, offence_type, publishing_status, authoring_status, clone_type_code, cloned_from, sow_reference, sl_offence_act_sec_txt, sl_offence_wording_txt, sl_off_statmnt_fct_txt, blocked, cjs_code, current_editor, entry_prompt_substitution, entry_prmpt_sub_sof, entry_prmpt_sub_sow, user_acts_and_section, user_statement_of_facts, user_offence_wording, principal_offnc_category, location_flag, endorsable_flag, mode_of_trial, max_fine_type_magct_desc, max_fine_type_magct_code, prosecution_time_limit, sl_pnld_standard_off_word, pnld_stndrd_offnc_wording, sl_cjs_title, derived_from_cjs_code, description, maximum_penalty, offence_notes, dvla_code, standard_list, custodial_indicator, cjs_title, reportable, recordable, pss_changed_date, pnld_offence_end_date, pnld_offence_start_date, pnld_date_of_last_update, date_used_to, date_used_from, f_offence_header, f_menu_30, f_menu_29, f_menu_28, f_menu_27, f_menu_26, f_menu_25, f_menu_24, f_menu_23, f_menu_22, f_menu_21, f_menu_20, f_menu_19, f_menu_18, f_menu_17, f_menu_16, f_menu_15, f_menu_14, f_menu_13, f_menu_12, f_menu_11, f_menu_10, f_menu_09, f_menu_08, f_menu_07, f_menu_06, f_menu_05, f_menu_04, f_menu_03, f_menu_02, f_menu_01, f_release_package, t30maximum, t30minimum, t30entry_number, t29maximum, t29minimum, t29entry_number, t28maximum, t28minimum, t28entry_number, t27maximum, t27minimum, t27entry_number, t26maximum, t26minimum, t26entry_number, t25maximum, t25minimum, t25entry_number, t24maximum, t24minimum, t24entry_number, t23maximum, t23minimum, t23entry_number, t22maximum, t22minimum, t22entry_number, t21maximum, t21minimum, t21entry_number, t20maximum, t20minimum, t20entry_number, t19maximum, t19minimum, t19entry_number, t18maximum, t18minimum, t18entry_number, t17maximum, t17minimum, t17entry_number, t16maximum, t16minimum, t16entry_number, t15maximum, t15minimum, t15entry_number, t14maximum, t14minimum, t14entry_number, t13maximum, t13minimum, t13entry_number, t12maximum, t12minimum, t12entry_number, t11maximum, t11minimum, t11entry_number, t10maximum, t10minimum, t10entry_number, t09maximum, t09minimum, t09entry_number, t08maximum, t08minimum, t08entry_number, t07maximum, t07minimum, t07entry_number, t06maximum, t06minimum, t06entry_number, t05maximum, t05minimum, t05entry_number, t04maximum, t04minimum, t04entry_number, t03maximum, t03minimum, t03entry_number, t02maximum, t02minimum, t02entry_number, t01maximum, t01minimum, t01entry_number, pss_csh_csh_id, pss_ofr_id, pss_changed_by, sys_max_entry_number, sys_cloned_to, offence_code, area, proceedings_code, ho_subclass, ho_class, version_number, ofr_id
		)
    SELECT  
        v_loadid,
		gor.b_classname,
		gor.b_credate,
		CURRENT_TIMESTAMP,
		gor.b_creator,
		v_user,
        gor.current_record_indicator,
		gor.t30sys_unq_entry_num_ind, gor.t30clone_indicator, gor.t30delete_indicator, gor.t29sys_unq_entry_num_ind, gor.t29clone_indicator, gor.t29delete_indicator, gor.t28sys_unq_entry_num_ind, gor.t28clone_indicator, gor.t28delete_indicator, gor.t27sys_unq_entry_num_ind, gor.t27clone_indicator, gor.t27delete_indicator, gor.t26sys_unq_entry_num_ind, gor.t26clone_indicator, gor.t26delete_indicator, gor.t25sys_unq_entry_num_ind, gor.t25clone_indicator, gor.t25delete_indicator, gor.t24sys_unq_entry_num_ind, gor.t24clone_indicator, gor.t24delete_indicator, gor.t23sys_unq_entry_num_ind, gor.t23clone_indicator, gor.t23delete_indicator, gor.t22sys_unq_entry_num_ind, gor.t22clone_indicator, gor.t22delete_indicator, gor.t21sys_unq_entry_num_ind, gor.t21clone_indicator, gor.t21delete_indicator, gor.t20sys_unq_entry_num_ind, gor.t20clone_indicator, gor.t20delete_indicator, gor.t19sys_unq_entry_num_ind, gor.t19clone_indicator, gor.t19delete_indicator, gor.t18sys_unq_entry_num_ind, gor.t18clone_indicator, gor.t18delete_indicator, gor.t17sys_unq_entry_num_ind, gor.t17clone_indicator, gor.t17delete_indicator, gor.t16sys_unq_entry_num_ind, gor.t16clone_indicator, gor.t16delete_indicator, gor.t15sys_unq_entry_num_ind, gor.t15clone_indicator, gor.t15delete_indicator, gor.t14sys_unq_entry_num_ind, gor.t14clone_indicator, gor.t14delete_indicator, gor.t13sys_unq_entry_num_ind, gor.t13clone_indicator, gor.t13delete_indicator, gor.t12sys_unq_entry_num_ind, gor.t12clone_indicator, gor.t12delete_indicator, gor.t11sys_unq_entry_num_ind, gor.t11clone_indicator, gor.t11delete_indicator, gor.t10sys_unq_entry_num_ind, gor.t10clone_indicator, gor.t10delete_indicator, gor.t09sys_unq_entry_num_ind, gor.t09clone_indicator, gor.t09delete_indicator, gor.t08sys_unq_entry_num_ind, gor.t08clone_indicator, gor.t08delete_indicator, gor.t07sys_unq_entry_num_ind, gor.t07clone_indicator, gor.t07delete_indicator, gor.t06sys_unq_entry_num_ind, gor.t06clone_indicator, gor.t06delete_indicator, gor.t05sys_unq_entry_num_ind, gor.t05clone_indicator, gor.t05delete_indicator, gor.t04sys_unq_entry_num_ind, gor.t04clone_indicator, gor.t04delete_indicator, gor.t03sys_unq_entry_num_ind, gor.t03clone_indicator, gor.t03delete_indicator, gor.t02sys_unq_entry_num_ind, gor.t02clone_indicator, gor.t02delete_indicator, gor.t01sys_unq_entry_num_ind, gor.t01clone_indicator, gor.t01delete_indicator, gor.prosecutor_as_third_party, gor.court_extract_available, gor.court_of_appeal_flag, gor.commissioner_of_oath, gor.plea_applicable_flag, gor.appeal_flag, gor.exparte, gor.contested_fee, gor.initial_fee, gor.can_be_bulk, gor.sys_sort_terminal_entry, gor.sys_show_deleted, gor.traffic_control, gor.t30sys_terminal_entry_short, gor.t30standard_entry_identifier, gor.t30entry_prompt, gor.t30entry_format, gor.t29sys_terminal_entry_short, gor.t29standard_entry_identifier, gor.t29entry_prompt, gor.t29entry_format, gor.t28sys_terminal_entry_short, gor.t28standard_entry_identifier, gor.t28entry_prompt, gor.t28entry_format, gor.t27sys_terminal_entry_short, gor.t27standard_entry_identifier, gor.t27entry_prompt, gor.t27entry_format, gor.t26sys_terminal_entry_short, gor.t26standard_entry_identifier, gor.t26entry_prompt, gor.t26entry_format, gor.t25sys_terminal_entry_short, gor.t25standard_entry_identifier, gor.t25entry_prompt, gor.t25entry_format, gor.t24sys_terminal_entry_short, gor.t24standard_entry_identifier, gor.t24entry_prompt, gor.t24entry_format, gor.t23sys_terminal_entry_short, gor.t23standard_entry_identifier, gor.t23entry_prompt, gor.t23entry_format, gor.t22sys_terminal_entry_short, gor.t22standard_entry_identifier, gor.t22entry_prompt, gor.t22entry_format, gor.t21sys_terminal_entry_short, gor.t21standard_entry_identifier, gor.t21entry_prompt, gor.t21entry_format, gor.t20sys_terminal_entry_short, gor.t20standard_entry_identifier, gor.t20entry_prompt, gor.t20entry_format, gor.t19sys_terminal_entry_short, gor.t19standard_entry_identifier, gor.t19entry_prompt, gor.t19entry_format, gor.t18sys_terminal_entry_short, gor.t18standard_entry_identifier, gor.t18entry_prompt, gor.t18entry_format, gor.t17sys_terminal_entry_short, gor.t17standard_entry_identifier, gor.t17entry_prompt, gor.t17entry_format, gor.t16sys_terminal_entry_short, gor.t16standard_entry_identifier, gor.t16entry_prompt, gor.t16entry_format, gor.t15sys_terminal_entry_short, gor.t15standard_entry_identifier, gor.t15entry_prompt, gor.t15entry_format, gor.t14sys_terminal_entry_short, gor.t14standard_entry_identifier, gor.t14entry_prompt, gor.t14entry_format, gor.t13sys_terminal_entry_short, gor.t13standard_entry_identifier, gor.t13entry_prompt, gor.t13entry_format, gor.t12sys_terminal_entry_short, gor.t12standard_entry_identifier, gor.t12entry_prompt, gor.t12entry_format, gor.t11sys_terminal_entry_short, gor.t11standard_entry_identifier, gor.t11entry_prompt, gor.t11entry_format, gor.t10sys_terminal_entry_short, gor.t10standard_entry_identifier, gor.t10entry_prompt, gor.t10entry_format, gor.t09sys_terminal_entry_short, gor.t09standard_entry_identifier, gor.t09entry_prompt, gor.t09entry_format, gor.t08sys_terminal_entry_short, gor.t08standard_entry_identifier, gor.t08entry_prompt, gor.t08entry_format, gor.t07sys_terminal_entry_short, gor.t07standard_entry_identifier, gor.t07entry_prompt, gor.t07entry_format, gor.t06sys_terminal_entry_short, gor.t06standard_entry_identifier, gor.t06entry_prompt, gor.t06entry_format, gor.t05sys_terminal_entry_short, gor.t05standard_entry_identifier, gor.t05entry_prompt, gor.t05entry_format, gor.t04sys_terminal_entry_short, gor.t04standard_entry_identifier, gor.t04entry_prompt, gor.t04entry_format, gor.t03sys_terminal_entry_short, gor.t03standard_entry_identifier, gor.t03entry_prompt, gor.t03entry_format, gor.t02sys_terminal_entry_short, gor.t02standard_entry_identifier, gor.t02entry_prompt, gor.t02entry_format, gor.t01sys_terminal_entry_short, gor.t01standard_entry_identifier, gor.t01entry_prompt, gor.t01entry_format, gor.version_type, gor.sys_terminal_entry_clone, gor.sys_entry_number_sequence, gor.sys_terminal_entry_long, gor.sys_pnld_data_hash, gor.obsolete_indicator, gor.prefix, gor.resentence_activation_cde, gor.boxwork_ntfctn_tmplt, gor.listing_ntfctn_tmplt, gor.breach_type, gor.active_offence_order, gor.applicant_appellant_flag, gor.hearing_code, gor.link_type, gor.summons_template_type, gor.jurisdiction, gor.application_synonym, gor.offence_class, gor.mis_classification, gor.offence_source, gor.offence_type, gor.publishing_status, gor.authoring_status, gor.clone_type_code, gor.cloned_from, gor.sow_reference, gor.sl_offence_act_sec_txt, gor.sl_offence_wording_txt, gor.sl_off_statmnt_fct_txt, gor.blocked, gor.cjs_code, gor.current_editor, gor.entry_prompt_substitution, gor.entry_prmpt_sub_sof, gor.entry_prmpt_sub_sow, gor.user_acts_and_section, gor.user_statement_of_facts, gor.user_offence_wording, gor.principal_offnc_category, gor.location_flag, gor.endorsable_flag, gor.mode_of_trial, gor.max_fine_type_magct_desc, gor.max_fine_type_magct_code, gor.prosecution_time_limit, gor.sl_pnld_standard_off_word, gor.pnld_stndrd_offnc_wording, gor.sl_cjs_title, gor.derived_from_cjs_code, gor.description, gor.maximum_penalty, gor.offence_notes, gor.dvla_code, gor.standard_list, gor.custodial_indicator, gor.cjs_title, gor.reportable, gor.recordable, gor.pss_changed_date, gor.pnld_offence_end_date, gor.pnld_offence_start_date, gor.pnld_date_of_last_update, gor.date_used_to, gor.date_used_from, gor.f_offence_header, gor.f_menu_30, gor.f_menu_29, gor.f_menu_28, gor.f_menu_27, gor.f_menu_26, gor.f_menu_25, gor.f_menu_24, gor.f_menu_23, gor.f_menu_22, gor.f_menu_21, gor.f_menu_20, gor.f_menu_19, gor.f_menu_18, gor.f_menu_17, gor.f_menu_16, gor.f_menu_15, gor.f_menu_14, gor.f_menu_13, gor.f_menu_12, gor.f_menu_11, gor.f_menu_10, gor.f_menu_09, gor.f_menu_08, gor.f_menu_07, gor.f_menu_06, gor.f_menu_05, gor.f_menu_04, gor.f_menu_03, gor.f_menu_02, gor.f_menu_01, gor.f_release_package, gor.t30maximum, gor.t30minimum, gor.t30entry_number, gor.t29maximum, gor.t29minimum, gor.t29entry_number, gor.t28maximum, gor.t28minimum, gor.t28entry_number, gor.t27maximum, gor.t27minimum, gor.t27entry_number, gor.t26maximum, gor.t26minimum, gor.t26entry_number, gor.t25maximum, gor.t25minimum, gor.t25entry_number, gor.t24maximum, gor.t24minimum, gor.t24entry_number, gor.t23maximum, gor.t23minimum, gor.t23entry_number, gor.t22maximum, gor.t22minimum, gor.t22entry_number, gor.t21maximum, gor.t21minimum, gor.t21entry_number, gor.t20maximum, gor.t20minimum, gor.t20entry_number, gor.t19maximum, gor.t19minimum, gor.t19entry_number, gor.t18maximum, gor.t18minimum, gor.t18entry_number, gor.t17maximum, gor.t17minimum, gor.t17entry_number, gor.t16maximum, gor.t16minimum, gor.t16entry_number, gor.t15maximum, gor.t15minimum, gor.t15entry_number, gor.t14maximum, gor.t14minimum, gor.t14entry_number, gor.t13maximum, gor.t13minimum, gor.t13entry_number, gor.t12maximum, gor.t12minimum, gor.t12entry_number, gor.t11maximum, gor.t11minimum, gor.t11entry_number, gor.t10maximum, gor.t10minimum, gor.t10entry_number, gor.t09maximum, gor.t09minimum, gor.t09entry_number, gor.t08maximum, gor.t08minimum, gor.t08entry_number, gor.t07maximum, gor.t07minimum, gor.t07entry_number, gor.t06maximum, gor.t06minimum, gor.t06entry_number, gor.t05maximum, gor.t05minimum, gor.t05entry_number, gor.t04maximum, gor.t04minimum, gor.t04entry_number, gor.t03maximum, gor.t03minimum, gor.t03entry_number, gor.t02maximum, gor.t02minimum, gor.t02entry_number, gor.t01maximum, gor.t01minimum, gor.t01entry_number, gor.pss_csh_csh_id, gor.pss_ofr_id, gor.pss_changed_by, gor.sys_max_entry_number, gor.sys_cloned_to, gor.offence_code, gor.area, gor.proceedings_code, gor.ho_subclass, gor.ho_class, gor.version_number, gor.ofr_id
	FROM csds.gd_offence_revision gor
    WHERE gor.authoring_status = 'Published'
      AND gor.publishing_status <> 'Superseded'
      AND (
            -- 2.1 Active/live future records not already 'Active'
            (gor.date_used_from <= CURRENT_DATE
             AND (gor.date_used_to > CURRENT_DATE OR gor.date_used_to IS NULL)
             AND gor.publishing_status <> 'Active')

            OR

            -- 2.2 Expired records not already 'Inactive'
            (gor.date_used_to < CURRENT_DATE + 1
             AND gor.publishing_status <> 'Inactive')
          )
      AND NOT EXISTS (
            SELECT 1
            FROM csds.sa_offence_revision sa2
            WHERE sa2.b_loadid = v_loadid
              AND sa2.ofr_id = gor.ofr_id
          );


    /* ===============================================================
     * INSERT 3: Insert OTE menus
     *
     * 3.1 Menus belonging to published release packages
     * 3.2 Menus linked to offences inserted above
     * =============================================================== */
    INSERT INTO csds.sa_ote_menu (
        b_loadid,
		b_classname,
		b_credate,
		b_upddate,
		b_creator,
		b_updator,
		version_type, sys_pnld_data_hash, publishing_status, authoring_status, notes, name, changed_date, f_release_package, changed_by, version_number, om_id
    )

    -- 3.1 Menus that were part of a published release package
    SELECT 
        v_loadid,
		gom.b_classname,
		gom.b_credate,
		CURRENT_TIMESTAMP,
		gom.b_creator,
		v_user,
		gom.version_type, gom.sys_pnld_data_hash, gom.publishing_status, gom.authoring_status, gom.notes, gom.name, gom.changed_date, gom.f_release_package, gom.changed_by, gom.version_number, gom.om_id
    FROM csds.sa_release_package srp
    INNER JOIN csds.gd_ote_menu gom 
        ON gom.f_release_package = srp.rp_id
    WHERE srp.b_loadid = v_loadid
      AND srp.status = 'Published'

    UNION

    -- 3.2 Menus associated with offences inserted above
    SELECT DISTINCT
        v_loadid,
		gom.b_classname,
		gom.b_credate,
		CURRENT_TIMESTAMP,
		gom.b_creator,
		v_user,
		gom.version_type, gom.sys_pnld_data_hash, gom.publishing_status, gom.authoring_status, gom.notes, gom.name, gom.changed_date, gom.f_release_package, gom.changed_by, gom.version_number, gom.om_id
    FROM csds.sa_offence_revision o
    CROSS JOIN LATERAL unnest(ARRAY[
        o.f_menu_01, o.f_menu_02, o.f_menu_03, o.f_menu_04, o.f_menu_05,
        o.f_menu_06, o.f_menu_07, o.f_menu_08, o.f_menu_09, o.f_menu_10,
        o.f_menu_11, o.f_menu_12, o.f_menu_13, o.f_menu_14, o.f_menu_15,
        o.f_menu_16, o.f_menu_17, o.f_menu_18, o.f_menu_19, o.f_menu_20,
        o.f_menu_21, o.f_menu_22, o.f_menu_23, o.f_menu_24, o.f_menu_25,
        o.f_menu_26, o.f_menu_27, o.f_menu_28, o.f_menu_29, o.f_menu_30
    ]) AS menu_id
    JOIN csds.gd_ote_menu gom ON gom.om_id = menu_id
    WHERE o.b_loadid = v_loadid
      AND menu_id IS NOT NULL
      AND gom.authoring_status = 'Published';

END;
$procedure$
;
CREATE OR REPLACE PROCEDURE csds.proc_insert_offence_menu_element_definition(v_batchid numeric, v_username text)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_deletes integer;
    v_inserts integer;
    v_updates integer;
BEGIN
--
-- step.1: get a list of the all the element definitions that have been affected for this batch id: 
--
	WITH menu_options AS (
		SELECT f_ote_optn_menu AS om_id, omo_id AS f_offence_menu_options,e01element_number AS element_number,e01entry_format AS entry_format,e01minimum AS oed_min,e01maximum AS oed_max,e01entry_prompt AS entry_prompt FROM csds.gd_ote_menu_options WHERE e01element_number IS NOT NULL AND b_batchid = v_batchid UNION ALL
		SELECT f_ote_optn_menu AS om_id, omo_id AS f_offence_menu_options,e02element_number AS element_number,e02entry_format AS entry_format,e02minimum AS oed_min,e02maximum AS oed_max,e02entry_prompt AS entry_prompt FROM csds.gd_ote_menu_options WHERE e02element_number IS NOT NULL AND b_batchid = v_batchid UNION ALL
		SELECT f_ote_optn_menu AS om_id, omo_id AS f_offence_menu_options,e03element_number AS element_number,e03entry_format AS entry_format,e03minimum AS oed_min,e03maximum AS oed_max,e03entry_prompt AS entry_prompt FROM csds.gd_ote_menu_options WHERE e03element_number IS NOT NULL AND b_batchid = v_batchid UNION ALL
		SELECT f_ote_optn_menu AS om_id, omo_id AS f_offence_menu_options,e04element_number AS element_number,e04entry_format AS entry_format,e04minimum AS oed_min,e04maximum AS oed_max,e04entry_prompt AS entry_prompt FROM csds.gd_ote_menu_options WHERE e04element_number IS NOT NULL AND b_batchid = v_batchid UNION ALL
		SELECT f_ote_optn_menu AS om_id, omo_id AS f_offence_menu_options,e05element_number AS element_number,e05entry_format AS entry_format,e05minimum AS oed_min,e05maximum AS oed_max,e05entry_prompt AS entry_prompt FROM csds.gd_ote_menu_options WHERE e05element_number IS NOT NULL AND b_batchid = v_batchid UNION ALL
		SELECT f_ote_optn_menu AS om_id, omo_id AS f_offence_menu_options,e06element_number AS element_number,e06entry_format AS entry_format,e06minimum AS oed_min,e06maximum AS oed_max,e06entry_prompt AS entry_prompt FROM csds.gd_ote_menu_options WHERE e06element_number IS NOT NULL AND b_batchid = v_batchid UNION ALL
		SELECT f_ote_optn_menu AS om_id, omo_id AS f_offence_menu_options,e07element_number AS element_number,e07entry_format AS entry_format,e07minimum AS oed_min,e07maximum AS oed_max,e07entry_prompt AS entry_prompt FROM csds.gd_ote_menu_options WHERE e07element_number IS NOT NULL AND b_batchid = v_batchid UNION ALL
		SELECT f_ote_optn_menu AS om_id, omo_id AS f_offence_menu_options,e08element_number AS element_number,e08entry_format AS entry_format,e08minimum AS oed_min,e08maximum AS oed_max,e08entry_prompt AS entry_prompt FROM csds.gd_ote_menu_options WHERE e08element_number IS NOT NULL AND b_batchid = v_batchid UNION ALL
		SELECT f_ote_optn_menu AS om_id, omo_id AS f_offence_menu_options,e09element_number AS element_number,e09entry_format AS entry_format,e09minimum AS oed_min,e09maximum AS oed_max,e09entry_prompt AS entry_prompt FROM csds.gd_ote_menu_options WHERE e09element_number IS NOT NULL AND b_batchid = v_batchid UNION ALL
		SELECT f_ote_optn_menu AS om_id, omo_id AS f_offence_menu_options,e10element_number AS element_number,e10entry_format AS entry_format,e10minimum AS oed_min,e10maximum AS oed_max,e10entry_prompt AS entry_prompt FROM csds.gd_ote_menu_options WHERE e10element_number IS NOT NULL AND b_batchid = v_batchid
	)
	,menu_option_elements AS (
	-- step.2.1: identify element definition rows to insert:
		SELECT NULL					  			AS oed_id
		      ,options.element_number 			AS element_number
		      ,options.entry_format	 			AS entry_format
			  ,options.oed_min		  			AS oed_min
			  ,options.oed_max        			AS oed_max
			  ,options.entry_prompt   			AS entry_prompt
			  ,options.f_offence_menu_options	AS f_offence_menu_options
			  ,NULL								AS pss_oed_id
			  ,NULL								AS pss_omop_id 
			  ,'Insert'				  			AS dml_type
		  FROM      menu_options		 			 options
		  LEFT JOIN csds.gd_ote_element_definitions  element
		         ON options.f_offence_menu_options = element.f_offence_menu_options
				AND options.element_number         = element.element_number
		 WHERE element.f_offence_menu_options IS NULL
		--
		UNION ALL
		--
	-- step.2.2: identify element definition rows to update:
		SELECT element.oed_id		  			AS oed_id
		      ,options.element_number 			AS element_number
		      ,options.entry_format	  			AS entry_format
			  ,options.oed_min		  			AS oed_min
			  ,options.oed_max        			AS oed_max
			  ,options.entry_prompt   			AS entry_prompt
			  ,options.f_offence_menu_options	AS f_offence_menu_options
			  ,element.pss_oed_id				AS pss_oed_id
			  ,element.pss_omop_id				AS pss_omop_id 
			  ,'Update'				  			AS dml_type
		  FROM menu_options		 			    options
		  JOIN csds.gd_ote_element_definitions  element
		    ON options.f_offence_menu_options = element.f_offence_menu_options
		   AND options.element_number         = element.element_number
		   AND (
		        COALESCE(options.entry_format,'###') != COALESCE(element.entry_format,'###')
		     OR COALESCE(options.oed_min     ,-9999) != COALESCE(element.oed_min     ,-9999)
		     OR COALESCE(options.oed_max     ,-9999) != COALESCE(element.oed_max     ,-9999)
		     OR COALESCE(options.entry_prompt,'###') != COALESCE(element.entry_prompt,'###')
			    )
		--
		UNION ALL
		--
	-- step.2.3: identify element definition rows to delete:
		SELECT element.oed_id		  			AS oed_id
		      ,options.element_number 			AS element_number
		      ,options.entry_format	  			AS entry_format
			  ,options.oed_min		  			AS oed_min
			  ,options.oed_max        			AS oed_max
			  ,options.entry_prompt   			AS entry_prompt
			  ,options.f_offence_menu_options	AS f_offence_menu_options
			  ,element.pss_oed_id				AS pss_oed_id
			  ,element.pss_omop_id				AS pss_omop_id 
			  ,'Delete'				  			AS dml_type
		  FROM      csds.gd_ote_element_definitions  element
		  LEFT JOIN csds.gd_ote_menu_options 		gdoptions
				 ON gdoptions.omo_id 			   = element.f_offence_menu_options
		  LEFT JOIN menu_options		 			 options
		         ON element.f_offence_menu_options = options.f_offence_menu_options
				AND element.element_number         = options.element_number
		 WHERE options.f_offence_menu_options IS NULL
			 AND EXISTS (
			        -- keep only records where this element's menu (gdoptions.omo_id)
			        -- is one of the menus present in the menu_options CTE
			        SELECT 1
			        FROM menu_options mo
			        WHERE mo.f_offence_menu_options = gdoptions.omo_id
			  )
	)
--
-- step.3: delete element definition content from gd_ote_element_definitions that is no longer required.
--
	,del AS (
		DELETE FROM csds.gd_ote_element_definitions trgt
		 USING menu_option_elements src1
		 WHERE trgt.oed_id   = src1.oed_id
		   AND src1.dml_type = 'Delete'
		RETURNING 1
	)
--
-- step.4: insert element definition that isn't currently on  gd_ote_element_definitions
--
	,ins AS (
		INSERT INTO csds.gd_ote_element_definitions
			(oed_id,b_classname,b_batchid,b_credate,b_upddate,b_creator,b_updator,element_number,entry_format,oed_min,oed_max,entry_prompt,f_offence_menu_options,pss_oed_id,pss_omop_id)
		SELECT nextval('csds.seq_release_package_content'),'OffenceMenuElementDefinition',v_batchid,now(),now(),v_username,v_username,element_number,entry_format,oed_min,oed_max,entry_prompt,f_offence_menu_options,pss_oed_id,pss_omop_id
		  FROM menu_option_elements
		 WHERE dml_type = 'Insert'
		RETURNING 1
	)
--
-- step.5: insert element definition that isn't currently on  gd_ote_element_definitions
--
	,upd AS (
		UPDATE csds.gd_ote_element_definitions trgt
	       SET b_batchid              = v_batchID
	          ,b_upddate              = now()
	          ,b_updator              = v_userName
	          ,entry_format           = src2.entry_format
	          ,oed_min                = src2.oed_min
	          ,oed_max                = src2.oed_max
	          ,entry_prompt           = src2.entry_prompt
	          ,f_offence_menu_options = src2.f_offence_menu_options
			  ,pss_oed_id 			  = src2.pss_oed_id
			  ,pss_omop_id 			  = src2.pss_omop_id
	      FROM menu_option_elements src2
	     WHERE src2.dml_type = 'Update'
	       AND src2.oed_id   = trgt.oed_id
		RETURNING 1
	)
	SELECT (SELECT COUNT(*) FROM del) 
	      ,(SELECT COUNT(*) FROM ins)
		  ,(SELECT COUNT(*) FROM upd)
		  INTO v_deletes, v_inserts, v_updates;
--
	RAISE NOTICE 'Deleted: %, Inserted: %, Updated: %', 
             v_deletes, v_inserts, v_updates;
--
END;
--
$procedure$
;
CREATE OR REPLACE PROCEDURE csds.proc_insert_release_package_content(v_batchid numeric, v_username text, v_serverbaseurl text)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_URLConcat					text := '/mdm-app/CSDS/CrimeStandingDataService/browsing';
	v_RlsePckageStatus			text := 'Open';
	id_RlsePckageOpen			int[];
	id_RlsePckageFinal			int[];
	v_dynamic_sql_a 			text;
	v_dynamic_sql_b 			text;
BEGIN
-- ==========================================================================================================
-- *** Section 1 (START): *** 
-- This section inserts the Offence Revision and Offence Menu records into the gd_release_package_content
-- entity when the authoring_status on respective record type is set to Final.
-- ==========================================================================================================
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
    SELECT 'Offence'															AS rp_content_type
	      ,oRvsn.cjs_code														AS rp_content_name
	      ,v_serverBaseURL||v_URLConcat||'/AllOffences/'||oRvsn.ofr_id			AS rp_content_url
		  ,oRvsn.ofr_id															AS rp_content_key
		  ,oRvsn.f_release_package												AS f_release_package
		  ,oRvsn.offence_notes													AS notes
		  ,oRvsn.authoring_status												AS rp_content_auth_status
      FROM csds.gd_offence_revision			oRvsn
	 WHERE oRvsn.b_batchid = v_batchID
	UNION ALL
    SELECT 'Offence Menu'														AS rp_content_type
	      ,oMenu.name															AS rp_content_name
	      ,v_serverBaseURL||v_URLConcat||'/AllOffenceMenus/'||oMenu.om_id		AS rp_content_url
		  ,oMenu.om_id															AS rp_content_key
		  ,oMenu.f_release_package												AS f_release_package
		  ,oMenu.notes															AS notes
		  ,oMenu.authoring_status												AS rp_content_auth_status
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
		rp_content_id
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
-- ==========================================================================================================
-- *** Section 1 (END): *** 
-- ==========================================================================================================
-- ==========================================================================================================
-- *** Section 2 (START): *** 
-- This section is a two part update for SyReleasePackagePublishError on gd_release_package_content. 
--
-- A. Offence Revision is set to Final and has a release package assigned but its associated Offence Menus is still in Draft or 
--    assigned to a different release packages.
-- B. Offence Menu is set to Final and has a release package assigned but associated Offence Revision is still in Draft or
--    assigned to a different release package.
-- NOTE: This section is only interested in authoring_status Draft and Final.
-- ==========================================================================================================
	-- A. offence revision has one / more offence menus missing from the release package.
	WITH cntnt_pub_err AS (
	SELECT DISTINCT
	       ofr.ofr_id								AS ofr_id
		  ,ofr.f_release_package					AS f_release_package
          ,MAX(CASE WHEN gom.authoring_status = 'Draft' OR (gom.authoring_status = 'Final' AND ofr.f_release_package != gom.f_release_package) 
					THEN 1 ELSE 0 END
				) OVER (PARTITION BY ofr.ofr_id)	AS rp_cntnt_publish_err
      FROM csds.gd_offence_revision ofr
	 CROSS JOIN LATERAL (
     SELECT UNNEST(ARRAY[ofr.f_menu_01,ofr.f_menu_02,ofr.f_menu_03,ofr.f_menu_04,ofr.f_menu_05,ofr.f_menu_06,ofr.f_menu_07,ofr.f_menu_08,ofr.f_menu_09,ofr.f_menu_10
					    ,ofr.f_menu_11,ofr.f_menu_12,ofr.f_menu_13,ofr.f_menu_14,ofr.f_menu_15,ofr.f_menu_16,ofr.f_menu_17,ofr.f_menu_18,ofr.f_menu_19,ofr.f_menu_20
					    ,ofr.f_menu_21,ofr.f_menu_22,ofr.f_menu_23,ofr.f_menu_24,ofr.f_menu_25,ofr.f_menu_26,ofr.f_menu_27,ofr.f_menu_28,ofr.f_menu_29,ofr.f_menu_30
					]) AS menu) 	ofm
	  LEFT JOIN csds.gd_ote_menu  	gom ON ofm.menu = gom.om_id
	WHERE ofr.authoring_status = 'Final'
	  AND ofm.menu IS NOT NULL
	)
	UPDATE csds.gd_release_package_content 	tgt
	   SET sys_rp_cntnt_publish_err = CASE WHEN src.rp_cntnt_publish_err = 1 THEN true ELSE false END
	  FROM cntnt_pub_err 					src
	 WHERE tgt.f_release_package = src.f_release_package
	   AND tgt.rp_content_key    = src.ofr_id
	   AND tgt.rp_content_type   = 'Offence';
	--	
	-- B. offence menu in release package exists but the offence revision is missing.
	WITH cntnt_pub_err AS (
	SELECT DISTINCT
	       ofr.ofr_id								AS ofr_id
		  ,ofm.menu									AS ofr_ofm_menu
		  ,ofr.f_release_package					AS ofr_f_release_package
		  ,ofr.authoring_status						AS ofr_authoring_status
      FROM csds.gd_offence_revision ofr
	 CROSS JOIN LATERAL (
     SELECT UNNEST(ARRAY[ofr.f_menu_01,ofr.f_menu_02,ofr.f_menu_03,ofr.f_menu_04,ofr.f_menu_05,ofr.f_menu_06,ofr.f_menu_07,ofr.f_menu_08,ofr.f_menu_09,ofr.f_menu_10
					    ,ofr.f_menu_11,ofr.f_menu_12,ofr.f_menu_13,ofr.f_menu_14,ofr.f_menu_15,ofr.f_menu_16,ofr.f_menu_17,ofr.f_menu_18,ofr.f_menu_19,ofr.f_menu_20
					    ,ofr.f_menu_21,ofr.f_menu_22,ofr.f_menu_23,ofr.f_menu_24,ofr.f_menu_25,ofr.f_menu_26,ofr.f_menu_27,ofr.f_menu_28,ofr.f_menu_29,ofr.f_menu_30
					]) AS menu) 	ofm
	  WHERE ofm.menu IS NOT NULL
	    AND ofr.authoring_status IN ('Draft','Final')
	)
   ,cntnt_pub_err_upd AS (
	SELECT gom.om_id							AS om_id
		  ,gom.f_release_package				AS f_release_package
		  ,MAX(CASE WHEN gom.f_release_package != COALESCE(ofr_f_release_package,0) THEN 1 ELSE 0 END
				) OVER (PARTITION BY gom.om_id)	AS rp_cntnt_publish_err
			
	  FROM csds.gd_ote_menu gom
	  JOIN cntnt_pub_err	ofr ON gom.om_id = ofr.ofr_ofm_menu
	 WHERE gom.authoring_status = 'Final'
	) 
	UPDATE csds.gd_release_package_content 	tgt
	   SET sys_rp_cntnt_publish_err = CASE WHEN src.rp_cntnt_publish_err = 1 THEN true ELSE false END
	  FROM cntnt_pub_err_upd				src
	 WHERE tgt.f_release_package = src.f_release_package
	   AND tgt.rp_content_key    = src.om_id
	   AND tgt.rp_content_type   = 'Offence Menu';
	--
-- ==========================================================================================================
-- *** Section 2 (END): *** 
-- ==========================================================================================================
-- ==========================================================================================================
-- *** Section 3 (START): *** 
-- This section updates gd_release_package for:
-- 1. release package content count
-- 2. counts for respective record types and 
-- 3. set the sys_rlse_pckg_publish_err if there is an error against any content type in gd_release_package_content.
-- ==========================================================================================================
	-- update release package content count and publish error flag on gd_release_package
	UPDATE csds.gd_release_package AS trgt
	   SET content_count             = COALESCE(cnt.content_count,0)
	      ,sys_rlse_pckg_publish_err = COALESCE(cnt.sys_rlse_pckg_publish_err,false)
		  ,offence_menu_count        = COALESCE(cnt.offence_menu_count,0)
		  ,offence_revision_count    = COALESCE(cnt.offence_revision_count,0)
	  FROM (
			SELECT grp.rp_id																	AS rp_id
			      ,COUNT(grc.rp_content_id) 													AS content_count
				  ,bool_or(sys_rp_cntnt_publish_err)											AS sys_rlse_pckg_publish_err
				  ,COUNT(CASE WHEN rp_content_type = 'Offence Menu' THEN grc.rp_content_id END) AS offence_menu_count
				  ,COUNT(CASE WHEN rp_content_type = 'Offence'      THEN grc.rp_content_id END) AS offence_revision_count
             FROM 	   csds.gd_release_package 				grp 
             LEFT JOIN csds.gd_release_package_content 		grc ON grp.rp_id = grc.f_release_package 
            WHERE grp.rp_id = ANY (id_RlsePckageOpen)
            GROUP BY grp.rp_id 
           ) AS cnt
     WHERE trgt.rp_id = cnt.rp_id;
	--
-- ==========================================================================================================
-- *** Section 3 (END): *** 
-- ==========================================================================================================
--
END;
--
$procedure$
;
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
			,f_menu_01,f_menu_02,f_menu_03,f_menu_04,f_menu_05,f_menu_06,f_menu_07,f_menu_08,f_menu_09,f_menu_10,f_menu_11,f_menu_12,f_menu_13,f_menu_14,f_menu_15
			,f_menu_16,f_menu_17,f_menu_18,f_menu_19,f_menu_20,f_menu_21,f_menu_22,f_menu_23,f_menu_24,f_menu_25,f_menu_26,f_menu_27,f_menu_28,f_menu_29,f_menu_30
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
            ,1,1,1,'MNU','Terminal Entry 01','CO',FALSE,2,1,1,'MNU','Terminal Entry 02','CO',FALSE,3,1,1,'MNU','Terminal Entry 03','CO',FALSE,4,1,1,'MNU','Terminal Entry 04','CO',FALSE
            ,5,1,1,'MNU','Terminal Entry 05','CO',FALSE,6,1,1,'MNU','Terminal Entry 06','CO',FALSE,7,1,1,'MNU','Terminal Entry 07','CO',FALSE,8,1,1,'MNU','Terminal Entry 08','CO',FALSE
            ,9,1,1,'MNU','Terminal Entry 09','CO',FALSE,10,1,1,'MNU','Terminal Entry 10','CO',FALSE,11,1,1,'MNU','Terminal Entry 11','CO',FALSE,12,0,1,'MNU','Terminal Entry 12','CO',FALSE
			,13,1,1,'MNU','Terminal Entry 13','CO',FALSE,14,1,1,'MNU','Terminal Entry 14','CO',FALSE,15,1,1,'MNU','Terminal Entry 15','CO',FALSE,16,1,1,'MNU','Terminal Entry 16','CO',FALSE
			,17,1,1,'MNU','Terminal Entry 17','CO',FALSE,18,1,1,'MNU','Terminal Entry 18','CO',FALSE,19,1,1,'MNU','Terminal Entry 19','CO',FALSE,20,1,1,'MNU','Terminal Entry 20','CO',FALSE
            ,21,1,1,'MNU','Terminal Entry 21','CO',FALSE,22,1,1,'MNU','Terminal Entry 22','CO',FALSE,23,1,1,'MNU','Terminal Entry 23','CO',FALSE,24,1,1,'MNU','Terminal Entry 24','CO',FALSE
            ,25,1,1,'MNU','Terminal Entry 25','CO',FALSE,26,1,1,'MNU','Terminal Entry 26','CO',FALSE,27,1,1,'MNU','Terminal Entry 27','CO',FALSE,28,1,1,'MNU','Terminal Entry 28','CO',FALSE
            ,29,1,1,'MNU','Terminal Entry 29','CO',FALSE,30,1,1,'MNU','Terminal Entry 30','CO',FALSE
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
            v.delete_indicator
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
        f_offnc_trmnl_entry_mnu   = EXCLUDED.f_offnc_trmnl_entry_mnu;

    RAISE NOTICE 'Complete %', v_batchid;
END;

$procedure$
;
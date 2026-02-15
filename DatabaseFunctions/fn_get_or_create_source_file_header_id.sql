-- DROP FUNCTION csds.fn_get_or_create_source_file_header_id(numeric, text, numeric);

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

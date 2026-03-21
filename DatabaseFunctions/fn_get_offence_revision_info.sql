-- DROP FUNCTION csds.fn_get_offence_revision_info(text, int8, text);
-- ====================================================================================================
-- Function: fn_get_offence_revision_info
-- Purpose: Returns specific information about an offence revision depending on the requested type.
-- Parameters:
--   v_cjs_code - The CJS code used to look up offence revision information.
--   v_loadid   - Load ID used to identify records in the staging table.
--   v_type     - Determines which value the function should return:
--                'Version'  -> returns the maximum version number for the CJS code
--                'CJSTitle' -> validates the length of the CJS title based on specific prefixes
-- ====================================================================================================
CREATE OR REPLACE FUNCTION csds.fn_get_offence_revision_info(
    v_cjs_code text, v_loadid bigint, v_type text
)
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
$function$;

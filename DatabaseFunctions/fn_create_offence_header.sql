-- DROP FUNCTION csds.fn_create_offence_header(text,bigint,text);
-- Commented statement that could be used to drop an old procedure if it existed
CREATE OR REPLACE FUNCTION csds.fn_create_offence_header(
	v_cjs_code	text,		-- CJS code used to identify the offence
	v_batchid  	bigint,		-- Batch ID used for processing
    v_username 	text		-- Username of the person or process creating the record
)
RETURNS numeric				-- Function returns the offence header ID
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

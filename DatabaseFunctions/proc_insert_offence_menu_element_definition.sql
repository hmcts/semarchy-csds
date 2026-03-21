-- DROP PROCEDURE csds.proc_insert_offence_menu_element_definition(numeric, text);

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

-- DROP PROCEDURE csds.proc_perf_test_load_offence(int4, varchar, varchar);

CREATE OR REPLACE PROCEDURE csds.proc_perf_test_load_offence(IN v_recordcount integer, IN v_username character varying, IN v_cjsprefix character varying)
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
			  b_loadid
			 ,ofr_id
			 ,b_classname
			 ,b_credate
			 ,b_upddate
			 ,b_creator,b_updator
			-- Business attributes
			 ,recordable
			 ,reportable
			 ,cjs_title
			 ,custodial_indicator
			 ,date_used_from
			 ,date_used_to
			 ,standard_list
			 ,traffic_control
			 ,version_number
			 ,dvla_code
			 ,offence_notes
			 ,maximum_penalty
			 ,description
			 ,derived_from_cjs_code
			 ,ho_class
			 ,ho_subclass
			 ,proceedings_code
			 ,sl_cjs_title
			 ,pnld_stndrd_offnc_wording
			 ,sl_pnld_standard_off_word
			 ,pnld_date_of_last_update
			 ,prosecution_time_limit
			 ,max_fine_type_magct_code
			 ,mode_of_trial
			 ,endorsable_flag
			 ,location_flag
			 ,principal_offnc_category
			 ,user_offence_wording
			 ,user_statement_of_facts
			 ,user_acts_and_section
			 ,entry_prmpt_sub_sow
			 ,entry_prmpt_sub_sof
			 ,current_editor
			 ,cjs_code
			 ,area,blocked
			 ,sl_off_statmnt_fct_txt
			 ,sl_offence_wording_txt
			 ,sl_offence_act_sec_txt
			 ,offence_code
			 ,pnld_offence_start_date
			 ,pnld_offence_end_date
			 ,sow_reference
			 ,cloned_from
			 ,clone_type_code
			 ,sys_cloned_to
			 ,sys_show_deleted
			 ,authoring_status
			 ,publishing_status
			 ,offence_type
			 ,offence_source
			 ,mis_classification
			 ,offence_class
			-- Terminal entries t01–t30
			 ,t01entry_number,t01minimum,t01maximum,t01entry_format,t01entry_prompt,t01standard_entry_identifier,t01delete_indicator,t02entry_number,t02minimum,t02maximum,t02entry_format,t02entry_prompt,t02standard_entry_identifier,t02delete_indicator
             ,t03entry_number,t03minimum,t03maximum,t03entry_format,t03entry_prompt,t03standard_entry_identifier,t03delete_indicator,t04entry_number,t04minimum,t04maximum,t04entry_format,t04entry_prompt,t04standard_entry_identifier,t04delete_indicator
            --,t05entry_number,t05minimum,t05maximum,t05entry_format,t05entry_prompt,t05standard_entry_identifier,t05delete_indicator,t06entry_number,t06minimum,t06maximum,t06entry_format,t06entry_prompt,t06standard_entry_identifier,t06delete_indicator
            --,t07entry_number,t07minimum,t07maximum,t07entry_format,t07entry_prompt,t07standard_entry_identifier,t07delete_indicator,t08entry_number,t08minimum,t08maximum,t08entry_format,t08entry_prompt,t08standard_entry_identifier,t08delete_indicator
            --,t09entry_number,t09minimum,t09maximum,t09entry_format,t09entry_prompt,t09standard_entry_identifier,t09delete_indicator,t10entry_number,t10minimum,t10maximum,t10entry_format,t10entry_prompt,t10standard_entry_identifier,t10delete_indicator
            --,t11entry_number,t11minimum,t11maximum,t11entry_format,t11entry_prompt,t11standard_entry_identifier,t11delete_indicator,t12entry_number,t12minimum,t12maximum,t12entry_format,t12entry_prompt,t12standard_entry_identifier,t12delete_indicator
            --,t13entry_number,t13minimum,t13maximum,t13entry_format,t13entry_prompt,t13standard_entry_identifier,t13delete_indicator,t14entry_number,t14minimum,t14maximum,t14entry_format,t14entry_prompt,t14standard_entry_identifier,t14delete_indicator
            --,t15entry_number,t15minimum,t15maximum,t15entry_format,t15entry_prompt,t15standard_entry_identifier,t15delete_indicator,t16entry_number,t16minimum,t16maximum,t16entry_format,t16entry_prompt,t16standard_entry_identifier,t16delete_indicator
            --,t17entry_number,t17minimum,t17maximum,t17entry_format,t17entry_prompt,t17standard_entry_identifier,t17delete_indicator,t18entry_number,t18minimum,t18maximum,t18entry_format,t18entry_prompt,t18standard_entry_identifier,t18delete_indicator
            --,t19entry_number,t19minimum,t19maximum,t19entry_format,t19entry_prompt,t19standard_entry_identifier,t19delete_indicator,t20entry_number,t20minimum,t20maximum,t20entry_format,t20entry_prompt,t20standard_entry_identifier,t20delete_indicator
            --,t21entry_number,t21minimum,t21maximum,t21entry_format,t21entry_prompt,t21standard_entry_identifier,t21delete_indicator,t22entry_number,t22minimum,t22maximum,t22entry_format,t22entry_prompt,t22standard_entry_identifier,t22delete_indicator
            --,t23entry_number,t23minimum,t23maximum,t23entry_format,t23entry_prompt,t23standard_entry_identifier,t23delete_indicator,t24entry_number,t24minimum,t24maximum,t24entry_format,t24entry_prompt,t24standard_entry_identifier,t24delete_indicator
            --,t25entry_number,t25minimum,t25maximum,t25entry_format,t25entry_prompt,t25standard_entry_identifier,t25delete_indicator,t26entry_number,t26minimum,t26maximum,t26entry_format,t26entry_prompt,t26standard_entry_identifier,t26delete_indicator
            --,t27entry_number,t27minimum,t27maximum,t27entry_format,t27entry_prompt,t27standard_entry_identifier,t27delete_indicator,t28entry_number,t28minimum,t28maximum,t28entry_format,t28entry_prompt,t28standard_entry_identifier,t28delete_indicator
            --,t29entry_number,t29minimum,t29maximum,t29entry_format,t29entry_prompt,t29standard_entry_identifier,t29delete_indicator,t30entry_number,t30minimum,t30maximum,t30entry_format,t30entry_prompt,t30standard_entry_identifier,t30delete_indicator
			-- Reference fields
			,f_offence_header
			,f_menu_01,f_menu_02,f_menu_03,f_menu_04
			--,f_menu_05,f_menu_06,f_menu_07,f_menu_08,f_menu_09,f_menu_10,f_menu_11,f_menu_12,f_menu_13,f_menu_14,f_menu_15
			--,f_menu_16,f_menu_17,f_menu_18,f_menu_19,f_menu_20,f_menu_21,f_menu_22,f_menu_23,f_menu_24,f_menu_25,f_menu_26,f_menu_27,f_menu_28,f_menu_29,f_menu_30
			,can_be_bulk
			,initial_fee
			,contested_fee
			,application_synonym
			,exparte
			,jurisdiction
			,appeal_flag
			,summons_template_type
			,link_type
			,hearing_code
            ,applicant_appellant_flag
			,plea_applicable_flag
			,active_offence_order
			,commissioner_of_oath
			,breach_type
			,court_of_appeal_flag
			,court_extract_available
		    ,listing_ntfctn_tmplt
			,boxwork_ntfctn_tmplt
			,prosecutor_as_third_party
			,resentence_activation_cde
			,prefix
			,obsolete_indicator
			)
		VALUES (
			-- Semarchy technical
			v_load_id,nextval('csds.seq_offence_revision'),'OffenceRevision','2026-01-14 22:41:33.544',CURRENT_TIMESTAMP,v_userName,v_userName
			-- Business attributes
			,'Yes'																-- recordable
			,'Yes'																-- reportable
			,'Speeding over statutory limit'									-- cjs_title 
			,'No'																-- custodial_indicator
			,DATE '2024-01-01'													-- date_used_from
			,NULL																-- date_used_to
			,'Yes'																-- standard_list
			,TRUE																-- traffic_control
			,1																	-- version_number
			,'A123'																-- dvla_code
			,'Test offence notes'												-- offence_notes
			,'Fine and penalty points'											-- maximum_penalty
			,'Driving a motor vehicle above the legal speed limit'				-- description
			,NULL																-- derived_from_cjs_code
			,10																	-- ho_class
			,2																	-- ho_subclass
			,12345																-- proceedings_code
			,'Speeding offence'													-- sl_cjs_title
			,'Exceeding speed limit on public road'								-- pnld_stndrd_offnc_wording
			,'Standard wording for speeding offence'							-- sl_pnld_standard_off_word
			,DATE '2024-06-01'													-- pnld_date_of_last_update
			,'6 months'															-- prosecution_time_limit
			,'F'																-- max_fine_type_magct_code
			,'Summary'															-- mode_of_trial
			,'Y'																-- endorsable_flag
			,'N'																-- location_flag
			,'Road Traffic'														-- principal_offnc_category
			,'User wording'														-- user_offence_wording
			,'User statement of facts'											-- user_statement_of_facts
			,'Road Traffic Act 1988 s.89'										-- user_acts_and_section
			,'Prompt SOW'														-- entry_prmpt_sub_sow
			,'Prompt SOF'														-- entry_prmpt_sub_sof
			,'editor1'															-- current_editor
			,v_cjsprefix||v_counter												-- cjs_code
			,1																	-- area
			,'N'																-- blocked
			,'Statement of facts'												-- sl_off_statmnt_fct_txt
			,'Offence wording'													-- sl_offence_wording_text
			,'Act and section'													-- sl_offence_act_sec_txt
			,123																-- offence_code
			,CURRENT_TIMESTAMP													-- pnld_offence_start_date
			,NULL																-- pnld_offence_end_date
			,'SOW001'															-- sow_ref
			,NULL																-- cloned_from
			,NULL																-- clone_type_code
			,NULL																-- sys_cloned_to
			,FALSE																-- sys_show_deleted
			,'Draft'															-- authoring_status								
			,'Not Published'													-- publishing_status
			,'CR'																-- offence_type
			,'MOJ'																-- offence_source
			,'DAM'																-- mis_classification
			,'1'																-- offence_class
			-- Terminal entries (same pattern values)
            ,1,1,1,'MNU','Terminal Entry 01','CO',FALSE,2,1,1,'MNU','Terminal Entry 02','CO',FALSE,3,1,1,'MNU','Terminal Entry 03','CO',FALSE,4,1,1,'MNU','Terminal Entry 04','CO',FALSE
            --,5,1,1,'MNU','Terminal Entry 05','CO',FALSE,6,1,1,'MNU','Terminal Entry 06','CO',FALSE,7,1,1,'MNU','Terminal Entry 07','CO',FALSE,8,1,1,'MNU','Terminal Entry 08','CO',FALSE
            --,9,1,1,'MNU','Terminal Entry 09','CO',FALSE,10,1,1,'MNU','Terminal Entry 10','CO',FALSE,11,1,1,'MNU','Terminal Entry 11','CO',FALSE,12,0,1,'MNU','Terminal Entry 12','CO',FALSE
			--,13,1,1,'MNU','Terminal Entry 13','CO',FALSE,14,1,1,'MNU','Terminal Entry 14','CO',FALSE,15,1,1,'MNU','Terminal Entry 15','CO',FALSE,16,1,1,'MNU','Terminal Entry 16','CO',FALSE
			--,17,1,1,'MNU','Terminal Entry 17','CO',FALSE,18,1,1,'MNU','Terminal Entry 18','CO',FALSE,19,1,1,'MNU','Terminal Entry 19','CO',FALSE,20,1,1,'MNU','Terminal Entry 20','CO',FALSE
            --,21,1,1,'MNU','Terminal Entry 21','CO',FALSE,22,1,1,'MNU','Terminal Entry 22','CO',FALSE,23,1,1,'MNU','Terminal Entry 23','CO',FALSE,24,1,1,'MNU','Terminal Entry 24','CO',FALSE
            --,25,1,1,'MNU','Terminal Entry 25','CO',FALSE,26,1,1,'MNU','Terminal Entry 26','CO',FALSE,27,1,1,'MNU','Terminal Entry 27','CO',FALSE,28,1,1,'MNU','Terminal Entry 28','CO',FALSE
            --,29,1,1,'MNU','Terminal Entry 29','CO',FALSE,30,1,1,'MNU','Terminal Entry 30','CO',FALSE
			-- Reference fields 
			,v_headerkey
			,'1','1','1','1'
			--,'2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2','2'
			,TRUE																-- can_be_bulk
			,TRUE																-- initial_fee
			,TRUE																-- contested_fee
			,'Synonym'															-- application_synonym
			,TRUE																-- exparte
			,'Crown'															-- jurisdiction
			,TRUE																-- appeal_flag
			,'BREACH'															-- summons_template_type
			,'STANDALONE'														-- link_type
			,'Appeal'															-- hearing_code
			,'Appellant'														-- applicant_appellant_flag
			,TRUE																-- plea_applicable_flag
			,'OFFENCE'															-- active_offence_order
			,TRUE																-- commissioner_of_oath
			,'NOT_APPLICABLE'													-- breach_type
			,TRUE																-- court_of_appeal_flag
			,TRUE																-- court_extract_available
			,'NOT_APPLICABLE'													-- listing_notification_temp
			,'NOTIF'															-- boxwork_notification_temp
			,TRUE																-- prosecutor_as_third_party
			,'N'																-- resentencing_activation_c
			,'N'																-- prefix
			,'N'																-- obsolete_indicator
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

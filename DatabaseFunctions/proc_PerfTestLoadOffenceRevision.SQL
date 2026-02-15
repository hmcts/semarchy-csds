-- DROP PROCEDURE csds.proc_perftestloadoffencerevision(int4, varchar);

CREATE OR REPLACE PROCEDURE csds.proc_perftestloadoffencerevision(v_recordcount integer, v_username character varying, v_cjsprefix character varying)
 LANGUAGE plpgsql
AS $procedure$

DECLARE v_load_id INTEGER;

v_counter INTEGER;
v_headerkey INTEGER;

BEGIN
	-- ============================================
	-- Generate the new LoadID
	-- ============================================
	v_load_id := semarchy_repository.get_new_loadid('CSDS', -- Data Location name
			'OffenceRevision', -- Program / integration identifier
			'Load Offence Revision', -- Load description
			v_userName -- User name
		);

	RAISE NOTICE 'New Load ID: %'
		,v_load_id;


	--- Create Header
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
			,b_creator
			,b_updator
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
			,changed_by
			,changed_date
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
			,entry_prompt_subs_ans
			,current_editor
			,cjs_code
			,area
			,blocked
			,sl_off_statmnt_fct_txt
			,sl_offence_wording_text
			,sl_offence_act_sec_txt
			,offence_code
			,pnld_offence_start_date
			,pnld_offence_end_date
			,sow_ref
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
			-- Terminal entries t01–t50
			,t01entry_number
			,t01minimum
			,t01maximum
			,t01entry_format
			,t01entry_prompt
			,t01standard_entry_identifier
			,t01delete_indicator
			,t01menu_url
			,t02entry_number
			,t02minimum
			,t02maximum
			,t02entry_format
			,t02entry_prompt
			,t02standard_entry_identifier
			,t02delete_indicator
			,t02menu_url
			,t03entry_number
			,t03minimum
			,t03maximum
			,t03entry_format
			,t03entry_prompt
			,t03standard_entry_identifier
			,t03delete_indicator
			,t03menu_url
			,t04entry_number
			,t04minimum
			,t04maximum
			,t04entry_format
			,t04entry_prompt
			,t04standard_entry_identifier
			,t04delete_indicator
			,t04menu_url
			,t05entry_number
			,t05minimum
			,t05maximum
			,t05entry_format
			,t05entry_prompt
			,t05standard_entry_identifier
			,t05delete_indicator
			,t05menu_url
			,t06entry_number
			,t06minimum
			,t06maximum
			,t06entry_format
			,t06entry_prompt
			,t06standard_entry_identifier
			,t06delete_indicator
			,t06menu_url
			,t07entry_number
			,t07minimum
			,t07maximum
			,t07entry_format
			,t07entry_prompt
			,t07standard_entry_identifier
			,t07delete_indicator
			,t07menu_url
			,t08entry_number
			,t08minimum
			,t08maximum
			,t08entry_format
			,t08entry_prompt
			,t08standard_entry_identifier
			,t08delete_indicator
			,t08menu_url
			,t09entry_number
			,t09minimum
			,t09maximum
			,t09entry_format
			,t09entry_prompt
			,t09standard_entry_identifier
			,t09delete_indicator
			,t09menu_url
			,t10entry_number
			,t10minimum
			,t10maximum
			,t10entry_format
			,t10entry_prompt
			,t10standard_entry_identifier
			,t10delete_indicator
			,t10menu_url
			,t11entry_number
			,t11minimum
			,t11maximum
			,t11entry_format
			,t11entry_prompt
			,t11standard_entry_identifier
			,t11delete_indicator
			,t11menu_url
			,t12entry_number
			,t12minimum
			,t12maximum
			,t12entry_format
			,t12entry_prompt
			,t12standard_entry_identifier
			,t12delete_indicator
			,t12menu_url
			,t13entry_number
			,t13minimum
			,t13maximum
			,t13entry_format
			,t13entry_prompt
			,t13standard_entry_identifier
			,t13delete_indicator
			,t13menu_url
			,t14entry_number
			,t14minimum
			,t14maximum
			,t14entry_format
			,t14entry_prompt
			,t14standard_entry_identifier
			,t14delete_indicator
			,t14menu_url
			,t15entry_number
			,t15minimum
			,t15maximum
			,t15entry_format
			,t15entry_prompt
			,t15standard_entry_identifier
			,t15delete_indicator
			,t15menu_url
			,t16entry_number
			,t16minimum
			,t16maximum
			,t16entry_format
			,t16entry_prompt
			,t16standard_entry_identifier
			,t16delete_indicator
			,t16menu_url
			,t17entry_number
			,t17minimum
			,t17maximum
			,t17entry_format
			,t17entry_prompt
			,t17standard_entry_identifier
			,t17delete_indicator
			,t17menu_url
			,t18entry_number
			,t18minimum
			,t18maximum
			,t18entry_format
			,t18entry_prompt
			,t18standard_entry_identifier
			,t18delete_indicator
			,t18menu_url
			,t19entry_number
			,t19minimum
			,t19maximum
			,t19entry_format
			,t19entry_prompt
			,t19standard_entry_identifier
			,t19delete_indicator
			,t19menu_url
			,t20entry_number
			,t20minimum
			,t20maximum
			,t20entry_format
			,t20entry_prompt
			,t20standard_entry_identifier
			,t20delete_indicator
			,t20menu_url
			,t21entry_number
			,t21minimum
			,t21maximum
			,t21entry_format
			,t21entry_prompt
			,t21standard_entry_identifier
			,t21delete_indicator
			,t21menu_url
			,t22entry_number
			,t22minimum
			,t22maximum
			,t22entry_format
			,t22entry_prompt
			,t22standard_entry_identifier
			,t22delete_indicator
			,t22menu_url
			,t23entry_number
			,t23minimum
			,t23maximum
			,t23entry_format
			,t23entry_prompt
			,t23standard_entry_identifier
			,t23delete_indicator
			,t23menu_url
			,t24entry_number
			,t24minimum
			,t24maximum
			,t24entry_format
			,t24entry_prompt
			,t24standard_entry_identifier
			,t24delete_indicator
			,t24menu_url
			,t25entry_number
			,t25minimum
			,t25maximum
			,t25entry_format
			,t25entry_prompt
			,t25standard_entry_identifier
			,t25delete_indicator
			,t25menu_url
			,t26entry_number
			,t26minimum
			,t26maximum
			,t26entry_format
			,t26entry_prompt
			,t26standard_entry_identifier
			,t26delete_indicator
			,t26menu_url
			,t27entry_number
			,t27minimum
			,t27maximum
			,t27entry_format
			,t27entry_prompt
			,t27standard_entry_identifier
			,t27delete_indicator
			,t27menu_url
			,t28entry_number
			,t28minimum
			,t28maximum
			,t28entry_format
			,t28entry_prompt
			,t28standard_entry_identifier
			,t28delete_indicator
			,t28menu_url
			,t29entry_number
			,t29minimum
			,t29maximum
			,t29entry_format
			,t29entry_prompt
			,t29standard_entry_identifier
			,t29delete_indicator
			,t29menu_url
			,t30entry_number
			,t30minimum
			,t30maximum
			,t30entry_format
			,t30entry_prompt
			,t30standard_entry_identifier
			,t30delete_indicator
			,t30menu_url
			,t31entry_number
			,t31minimum
			,t31maximum
			,t31entry_format
			,t31entry_prompt
			,t31standard_entry_identifier
			,t31delete_indicator
			,t31menu_url
			,t32entry_number
			,t32minimum
			,t32maximum
			,t32entry_format
			,t32entry_prompt
			,t32standard_entry_identifier
			,t32delete_indicator
			,t32menu_url
			,t33entry_number
			,t33minimum
			,t33maximum
			,t33entry_format
			,t33entry_prompt
			,t33standard_entry_identifier
			,t33delete_indicator
			,t33menu_url
			,t34entry_number
			,t34minimum
			,t34maximum
			,t34entry_format
			,t34entry_prompt
			,t34standard_entry_identifier
			,t34delete_indicator
			,t34menu_url
			,t35entry_number
			,t35minimum
			,t35maximum
			,t35entry_format
			,t35entry_prompt
			,t35standard_entry_identifier
			,t35delete_indicator
			,t35menu_url
			,t36entry_number
			,t36minimum
			,t36maximum
			,t36entry_format
			,t36entry_prompt
			,t36standard_entry_identifier
			,t36delete_indicator
			,t36menu_url
			,t37entry_number
			,t37minimum
			,t37maximum
			,t37entry_format
			,t37entry_prompt
			,t37standard_entry_identifier
			,t37delete_indicator
			,t37menu_url
			,t38entry_number
			,t38minimum
			,t38maximum
			,t38entry_format
			,t38entry_prompt
			,t38standard_entry_identifier
			,t38delete_indicator
			,t38menu_url
			,t39entry_number
			,t39minimum
			,t39maximum
			,t39entry_format
			,t39entry_prompt
			,t39standard_entry_identifier
			,t39delete_indicator
			,t39menu_url
			,t40entry_number
			,t40minimum
			,t40maximum
			,t40entry_format
			,t40entry_prompt
			,t40standard_entry_identifier
			,t40delete_indicator
			,t40menu_url
			,t41entry_number
			,t41minimum
			,t41maximum
			,t41entry_format
			,t41entry_prompt
			,t41standard_entry_identifier
			,t41delete_indicator
			,t41menu_url
			,t42entry_number
			,t42minimum
			,t42maximum
			,t42entry_format
			,t42entry_prompt
			,t42standard_entry_identifier
			,t42delete_indicator
			,t42menu_url
			,t43entry_number
			,t43minimum
			,t43maximum
			,t43entry_format
			,t43entry_prompt
			,t43standard_entry_identifier
			,t43delete_indicator
			,t43menu_url
			,t44entry_number
			,t44minimum
			,t44maximum
			,t44entry_format
			,t44entry_prompt
			,t44standard_entry_identifier
			,t44delete_indicator
			,t44menu_url
			,t45entry_number
			,t45minimum
			,t45maximum
			,t45entry_format
			,t45entry_prompt
			,t45standard_entry_identifier
			,t45delete_indicator
			,t45menu_url
			,t46entry_number
			,t46minimum
			,t46maximum
			,t46entry_format
			,t46entry_prompt
			,t46standard_entry_identifier
			,t46delete_indicator
			,t46menu_url
			,t47entry_number
			,t47minimum
			,t47maximum
			,t47entry_format
			,t47entry_prompt
			,t47standard_entry_identifier
			,t47delete_indicator
			,t47menu_url
			,t48entry_number
			,t48minimum
			,t48maximum
			,t48entry_format
			,t48entry_prompt
			,t48standard_entry_identifier
			,t48delete_indicator
			,t48menu_url
			,t49entry_number
			,t49minimum
			,t49maximum
			,t49entry_format
			,t49entry_prompt
			,t49standard_entry_identifier
			,t49delete_indicator
			,t49menu_url
			,t50entry_number
			,t50minimum
			,t50maximum
			,t50entry_format
			,t50entry_prompt
			,t50standard_entry_identifier
			,t50delete_indicator
			,t50menu_url
			-- Reference fields
			,f_offence_header
			,f_menu_01
			,f_menu_02
			,f_menu_03
			,f_menu_04
			,f_menu_05
			,f_menu_06
			,f_menu_07
			,f_menu_08
			,f_menu_09
			,f_menu_10
			,f_menu_11
			,f_menu_12
			,f_menu_13
			,f_menu_14
			,f_menu_15
			,f_menu_16
			,f_menu_17
			,f_menu_18
			,f_menu_19
			,f_menu_20
			,f_menu_21
			,f_menu_22
			,f_menu_23
			,f_menu_24
			,f_menu_25
			,f_menu_26
			,f_menu_27
			,f_menu_28
			,f_menu_29
			,f_menu_30
			,f_menu_31
			,f_menu_32
			,f_menu_33
			,f_menu_34
			,f_menu_35
			,f_menu_36
			,f_menu_37
			,f_menu_38
			,f_menu_39
			,f_menu_40
			,f_menu_41
			,f_menu_42
			,f_menu_43
			,f_menu_44
			,f_menu_45
			,f_menu_46
			,f_menu_47
			,f_menu_48
			,f_menu_49
			,f_menu_50
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
			,listing_notification_temp
			,boxwork_notification_temp
			,prosecutor_as_third_party
			,resentencing_activation_c
			,prefix
			,obsolete_indicator
			)
		VALUES (
			-- Semarchy technical
			v_load_id
			,nextval('csds.seq_offence_revision')
			,'OffenceRevision'
			,CURRENT_TIMESTAMP
			,CURRENT_TIMESTAMP
			,v_userName
			,v_userName
			-- Business attributes
			,'Yes'
			,'Yes'
			,'Speeding over statutory limit'
			,'No'
			,DATE '2024-01-01'
			,NULL
			,'Yes'
			,TRUE
			,1
			,'admin'
			,CURRENT_TIMESTAMP
			,'A123'
			,'Test offence notes'
			,'Fine and penalty points'
			,'Driving a motor vehicle above the legal speed limit'
			,NULL
			,10
			,2
			,12345
			,'Speeding offence'
			,'Exceeding speed limit on public road'
			,'Standard wording for speeding offence'
			,DATE '2024-06-01'
			,'6 months'
			,'F'
			,'Summary'
			,'Y'
			,'N'
			,'Road Traffic'
			,'User wording'
			,'User statement of facts'
			,'Road Traffic Act 1988 s.89'
			,'Prompt SOW'
			,'Prompt SOF'
			,'Prompt Answer'
			,'editor1'
			,v_cjsprefix||v_counter
			,1
			,'N'
			,'Statement of facts'
			,'Offence wording'
			,'Act and section'
			,123
			,CURRENT_TIMESTAMP
			,NULL
			,'SOW001'
			,NULL
			,NULL
			,NULL
			,FALSE
			,'Draft'
			,'Not Published'
			,'CR'
			,'MOJ'
			,'DAM'
			,'1'
			-- Terminal entries (same pattern values)
			,1
			,1
			,1
			,'MNU'
			,'Terminal Entry 01'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,2
			,1
			,1
			,'MNU'
			,'Terminal Entry 02'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,3
			,1
			,1
			,'MNU'
			,'Terminal Entry 03'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,4
			,1
			,1
			,'MNU'
			,'Terminal Entry 04'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,5
			,1
			,1
			,'MNU'
			,'Terminal Entry 05'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,6
			,1
			,1
			,'MNU'
			,'Terminal Entry 06'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,7
			,1
			,1
			,'MNU'
			,'Terminal Entry 07'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,8
			,1
			,1
			,'MNU'
			,'Terminal Entry 08'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,9
			,1
			,1
			,'MNU'
			,'Terminal Entry 09'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,10
			,1
			,1
			,'MNU'
			,'Terminal Entry 10'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,11
			,1
			,1
			,'MNU'
			,'Terminal Entry 11'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,12
			,0
			,1
			,'MNU'
			,'Terminal Entry 12'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,13
			,1
			,1
			,'MNU'
			,'Terminal Entry 13'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,14
			,1
			,1
			,'MNU'
			,'Terminal Entry 14'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,15
			,1
			,1
			,'MNU'
			,'Terminal Entry 15'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,16
			,1
			,1
			,'MNU'
			,'Terminal Entry 16'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,17
			,1
			,1
			,'MNU'
			,'Terminal Entry 17'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,18
			,1
			,1
			,'MNU'
			,'Terminal Entry 18'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,19
			,1
			,1
			,'MNU'
			,'Terminal Entry 19'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,20
			,1
			,1
			,'MNU'
			,'Terminal Entry 20'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,21
			,1
			,1
			,'MNU'
			,'Terminal Entry 21'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,22
			,1
			,1
			,'MNU'
			,'Terminal Entry 22'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,23
			,1
			,1
			,'MNU'
			,'Terminal Entry 23'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,24
			,1
			,1
			,'MNU'
			,'Terminal Entry 24'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,25
			,1
			,1
			,'MNU'
			,'Terminal Entry 25'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,26
			,1
			,1
			,'MNU'
			,'Terminal Entry 26'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,27
			,1
			,1
			,'MNU'
			,'Terminal Entry 27'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,28
			,1
			,1
			,'MNU'
			,'Terminal Entry 28'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,29
			,1
			,1
			,'MNU'
			,'Terminal Entry 29'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,30
			,1
			,1
			,'MNU'
			,'Terminal Entry 30'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,31
			,1
			,1
			,'MNU'
			,'Terminal Entry 31'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,32
			,1
			,1
			,'MNU'
			,'Terminal Entry 32'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,33
			,1
			,1
			,'MNU'
			,'Terminal Entry 33'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,34
			,1
			,1
			,'MNU'
			,'Terminal Entry 34'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,35
			,1
			,1
			,'MNU'
			,'Terminal Entry 35'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,36
			,1
			,1
			,'MNU'
			,'Terminal Entry 36'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,37
			,1
			,1
			,'MNU'
			,'Terminal Entry 37'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,38
			,1
			,1
			,'MNU'
			,'Terminal Entry 38'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,39
			,1
			,1
			,'MNU'
			,'Terminal Entry 39'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,40
			,1
			,1
			,'MNU'
			,'Terminal Entry 40'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,41
			,1
			,1
			,'MNU'
			,'Terminal Entry 41'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,42
			,1
			,1
			,'MNU'
			,'Terminal Entry 42'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,43
			,1
			,1
			,'MNU'
			,'Terminal Entry 43'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,44
			,1
			,1
			,'MNU'
			,'Terminal Entry 44'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,45
			,1
			,1
			,'MNU'
			,'Terminal Entry 45'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,46
			,1
			,1
			,'MNU'
			,'Terminal Entry 46'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,47
			,1
			,1
			,'MNU'
			,'Terminal Entry 47'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,48
			,1
			,1
			,'MNU'
			,'Terminal Entry 48'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,49
			,1
			,1
			,'MNU'
			,'Terminal Entry 49'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			,50
			,1
			,1
			,'MNU'
			,'Terminal Entry 50'
			,'CO'
			,FALSE
			,'http://localhost:8088/semarchy/mdm-app/csds2/CrimeStandingDataService/browsing/AllOffenceMenus/125'
			-- Reference fields 
			,v_headerkey
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,'2'
			,TRUE
			,TRUE
			,TRUE
			,'Synonym'
			,TRUE
			,'Crown'
			,TRUE
			,'BREACH'
			,'STANDALONE'
			,'Appeal'
			,'Appellant'
			,TRUE
			,'OFFENCE'
			,TRUE
			,'NOT_APPLICABLE'
			,TRUE
			,TRUE
			,'NOT_APPLICABLE'
			,'NOTIF'
			,TRUE
			,'N'
			,'N'
			,'N'
			);

	v_counter := v_counter + 1;
END

LOOP;

--
-- ============================================
-- Submit Load
-- ============================================
PERFORM semarchy_repository.submit_load(v_load_id, 'OffenceRevision' -- Integration job name
		,v_userName -- Same user as in get_new_loadid
	);END;
	--
	$procedure$
;

-- DROP PROCEDURE csds.proc_insert_release_package_content(numeric, text, text);

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
-- This section is to set/unset SysReleasePackagePublishError on gd_release_package_content. 
-- The various scenarios it caters for are described below:
--
-- Scenario 1:: A Final Offence Revision and Offence Menu can be added to the same release package for publication.
--              Do not set the SysReleasePackagePublishError on Offence Revision record in this scenario.
-- Scenario 2:: Final Offence Revision and Offence Menu are in 2 different release packages.
--              Set SysReleasePackagePublishError on Offence Revision record in this scenario.
-- Scenario 3:: Once the Offence Menu is published as part of a different release package, 
--              unset SysReleasePackagePublishError on Offence Revision record
-- ==========================================================================================================
	WITH cntnt_pub_err AS (
	SELECT DISTINCT
	       ofr.ofr_id													AS ofr_id
		    ,ofr.f_release_package							AS f_release_package
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
			    SELECT grp.rp_id																	 																	AS rp_id
			          ,COUNT(grc.rp_content_id) 																										AS content_count
				        ,bool_or(sys_rp_cntnt_publish_err)																						AS sys_rlse_pckg_publish_err
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

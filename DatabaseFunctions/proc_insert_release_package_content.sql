-- DROP PROCEDURE csds.proc_insert_release_package_content(numeric, text, text);

CREATE OR REPLACE PROCEDURE csds.proc_insert_release_package_content(v_batchID numeric, v_userName text, v_serverBaseURL text)
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
-- ALTER TABLE csds.gd_release_package_content ADD CONSTRAINT usr_release_package_content UNIQUE (f_release_package, rp_content_type, rp_content_key);

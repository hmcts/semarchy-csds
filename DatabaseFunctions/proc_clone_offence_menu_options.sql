-- DROP PROCEDURE csds.proc_clone_offence_menu_options(numeric, text);

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

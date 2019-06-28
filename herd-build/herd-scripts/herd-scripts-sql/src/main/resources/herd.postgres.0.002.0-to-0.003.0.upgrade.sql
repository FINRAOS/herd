/*
* Copyright 2015 herd contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

alter table scrty_role_fn drop constraint scrty_role_fn_fk2;
alter table app_endpoint_lk drop constraint app_endpoint_fk1;

update scrty_role_fn   set scrty_fn_cd = 'FN_DISPLAY_HERD_UI' where scrty_fn_cd = 'FN_DISPLAY_DM_UI';
update app_endpoint_lk set scrty_fn_cd = 'FN_DISPLAY_HERD_UI' where scrty_fn_cd = 'FN_DISPLAY_DM_UI';
update scrty_fn_lk     set scrty_fn_cd = 'FN_DISPLAY_HERD_UI' where scrty_fn_cd = 'FN_DISPLAY_DM_UI';

alter table scrty_role_fn add constraint scrty_role_fn_fk2 foreign key (scrty_fn_cd) references scrty_fn_lk (scrty_fn_cd);
alter table app_endpoint_lk add constraint app_endpoint_fk1 foreign key (scrty_fn_cd) references scrty_fn_lk (scrty_fn_cd);

drop view if exists biz_dt_file_vw;

alter table actn_type_cd_lk alter column actn_type_cd type varchar(50);
alter table actn_type_cd_lk alter column actn_type_ds type varchar(512);
alter table app_endpoint_lk alter column uri type varchar(1024);
alter table bus_objct_data alter column bus_objct_data_stts_cd type varchar(50);
alter table bus_objct_data alter column prtn_value_2_tx type varchar(50);
alter table bus_objct_data alter column prtn_value_3_tx type varchar(50);
alter table bus_objct_data alter column prtn_value_4_tx type varchar(50);
alter table bus_objct_data alter column prtn_value_5_tx type varchar(50);
alter table bus_objct_data_stts_cd_lk alter column bus_objct_data_stts_cd type varchar(50);
alter table bus_objct_data_stts_cd_lk alter column bus_objct_data_stts_ds type varchar(512);
alter table bus_objct_data_stts_hs alter column bus_objct_data_stts_cd type varchar(50);
alter table bus_objct_dfntn alter column data_prvdr_cd type varchar(50);
alter table bus_objct_dfntn alter column desc_tx type varchar(512);
alter table bus_objct_dfntn alter column name_space_cd type varchar(50);
alter table bus_objct_frmt alter column desc_tx type varchar(512);
alter table bus_objct_frmt alter column prtn_key_group_tx type varchar(50);
alter table bus_objct_frmt alter column prtn_key_tx type varchar(50);
alter table bus_objct_frmt alter column usage_cd type varchar(50);
alter table cstm_ddl alter column name_tx type varchar(50);
alter table data_prvdr alter column data_prvdr_cd type varchar(50);
alter table emr_clstr_crtn_log alter column emr_clstr_dfntn_name_tx type varchar(50);
alter table emr_clstr_crtn_log alter column name_space_cd type varchar(50);
alter table emr_clstr_dfntn alter column name_space_cd type varchar(50);
alter table emr_clstr_dfntn alter column name_tx type varchar(50);
alter table file_type_cd_lk alter column file_type_ds type varchar(512);
alter table job_dfntn alter column desc_tx type varchar(512);
alter table job_dfntn alter column name_space_cd type varchar(50);
alter table name_space alter column name_space_cd type varchar(50);
alter table ntfcn_actn alter column actn_type_cd type varchar(50);
alter table ntfcn_event_type_cd_lk alter column ntfcn_event_type_ds type varchar(512);
alter table ntfcn_rgstn alter column name_space_cd type varchar(50);
alter table ntfcn_rgstn alter column name_tx type varchar(256);
alter table ntfcn_rgstn alter column ntfcn_type_cd type varchar(50);
alter table ntfcn_rgstn alter column strge_cd type varchar(50);
alter table ntfcn_rgstn alter column usage_cd type varchar(50);
alter table ntfcn_type_cd_lk alter column ntfcn_type_cd  type varchar(50);
alter table ntfcn_type_cd_lk alter column ntfcn_type_ds type varchar(512);
alter table prtn_key_group alter column prtn_key_group_tx type varchar(50);
alter table schm_clmn alter column clmn_ds type varchar(512);
alter table scrty_fn_lk alter column scrty_fn_ds type varchar(512);
alter table scrty_fn_lk alter column scrty_fn_dsply_nm type varchar(512);
alter table scrty_role alter column scrty_role_ds type varchar(512);
alter table strge alter column strge_cd type varchar(50);
alter table strge alter column strge_pltfm_cd type varchar(50);
alter table strge_atrbt alter column strge_cd type varchar(50);
alter table strge_pltfm alter column strge_pltfm_cd type varchar(50);
alter table strge_unit alter column strge_cd type varchar(50);
alter table xpctd_prtn_value alter column prtn_key_group_tx type varchar(50);
alter table xpctd_prtn_value alter column prtn_value_tx type varchar(50);

create or replace view biz_dt_file_vw as
select sf.strge_file_id,
       df.name_space_cd namespace,
       df.name_tx bus_object_name,
       df.data_prvdr_cd data_provider,
       su.strge_cd storage_code,
       sf.file_size_in_bytes_nb,
       sf.creat_ts::date creat_td
from   strge_file sf
       join strge_unit su on sf.strge_unit_id = su.strge_unit_id
       join bus_objct_data d on su.bus_objct_data_id = d.bus_objct_data_id
       join bus_objct_frmt f on d.bus_objct_frmt_id = f.bus_objct_frmt_id
       join bus_objct_dfntn df on f.bus_objct_dfntn_id = df.bus_objct_dfntn_id;

insert into strge (strge_cd, creat_ts, creat_user_id, updt_ts, updt_user_id, strge_pltfm_cd)
values ('S3_NON_ARCHIVED', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM', 'S3');
insert into strge (strge_cd, creat_ts, creat_user_id, updt_ts, updt_user_id, strge_pltfm_cd)
values ('S3_MANAGED_KMS', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM', 'S3');

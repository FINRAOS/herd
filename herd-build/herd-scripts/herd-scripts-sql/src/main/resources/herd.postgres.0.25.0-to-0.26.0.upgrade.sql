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

alter table ntfcn_rgstn add column old_strge_unit_stts_cd varchar(20);
alter table ntfcn_rgstn add column new_strge_unit_stts_cd varchar(20);
alter table ntfcn_rgstn add constraint ntfcn_rgstn_fk10 foreign key (old_strge_unit_stts_cd) references strge_unit_stts_cd_lk (strge_unit_stts_cd);
alter table ntfcn_rgstn add constraint ntfcn_rgstn_fk11 foreign key (new_strge_unit_stts_cd) references strge_unit_stts_cd_lk (strge_unit_stts_cd);
create index ntfcn_rgstn_ix10 on ntfcn_rgstn (old_strge_unit_stts_cd);
create index ntfcn_rgstn_ix11 on ntfcn_rgstn (new_strge_unit_stts_cd);

insert into ntfcn_type_cd_lk (ntfcn_type_cd, ntfcn_type_ds, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('STRGE_UNIT', 'Storage Unit', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into ntfcn_event_type_cd_lk (ntfcn_event_type_cd, ntfcn_event_type_ds, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('STRGE_UNIT_STTS_CHG', 'Storage Unit Status Change', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_STORAGE_UNIT_NOTIFICATION_REGISTRATIONS_BY_NAMESPACE_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_STORAGE_UNIT_NOTIFICATION_REGISTRATIONS_BY_NOTIFICATION_FILTER_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_STORAGE_UNIT_NOTIFICATION_REGISTRATIONS_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_STORAGE_UNIT_NOTIFICATION_REGISTRATIONS_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_STORAGE_UNIT_NOTIFICATION_REGISTRATIONS_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_STORAGE_UNIT_NOTIFICATION_REGISTRATIONS_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

delete from scrty_fn_lk where scrty_fn_cd = 'FN_STORAGES_UPLOAD_STATS_GET';

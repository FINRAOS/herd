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

create table ntfcn_rgstn_stts_cd_lk
(
 ntfcn_rgstn_stts_cd   varchar(20)  not null,
 ntfcn_rgstn_stts_ds   varchar(512) not null,
 creat_ts              timestamp    not null default current_timestamp,
 creat_user_id         varchar(100) not null,
 updt_ts               timestamp    not null default current_timestamp,
 updt_user_id          varchar(100)
);

alter table ntfcn_rgstn_stts_cd_lk add constraint ntfcn_rgstn_stts_cd_lk_pk primary key (ntfcn_rgstn_stts_cd);

insert into ntfcn_rgstn_stts_cd_lk (ntfcn_rgstn_stts_cd, ntfcn_rgstn_stts_ds, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('ENABLED', 'Enabled', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into ntfcn_rgstn_stts_cd_lk (ntfcn_rgstn_stts_cd, ntfcn_rgstn_stts_ds, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('DISABLED', 'Disabled', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

alter table ntfcn_rgstn add column ntfcn_rgstn_stts_cd varchar(20);

update ntfcn_rgstn 
set    ntfcn_rgstn_stts_cd = 'ENABLED' 
where  ntfcn_rgstn_stts_cd is null;

alter table ntfcn_rgstn alter column ntfcn_rgstn_stts_cd set not null;

alter table ntfcn_rgstn add constraint ntfcn_rgstn_fk9 foreign key (ntfcn_rgstn_stts_cd)
references ntfcn_rgstn_stts_cd_lk (ntfcn_rgstn_stts_cd);

create index ntfcn_rgstn_ix9 on ntfcn_rgstn (ntfcn_rgstn_stts_cd);

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_NOTIFICATION_REGISTRATION_STATUS_PUT','FN_NOTIFICATION_REGISTRATION_STATUS_PUT','FN_NOTIFICATION_REGISTRATION_STATUS_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_BUSINESS_OBJECT_DATA_NOTIFICATION_REGISTRATIONS_PUT','FN_BUSINESS_OBJECT_DATA_NOTIFICATION_REGISTRATIONS_PUT','FN_BUSINESS_OBJECT_DATA_NOTIFICATION_REGISTRATIONS_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_DATA_PROVIDERS_POST', 'FN_DATA_PROVIDERS_POST', 'FN_DATA_PROVIDERS_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_DATA_PROVIDERS_GET', 'FN_DATA_PROVIDERS_GET', 'FN_DATA_PROVIDERS_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_DATA_PROVIDERS_DELETE', 'FN_DATA_PROVIDERS_DELETE', 'FN_DATA_PROVIDERS_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_DATA_PROVIDERS_ALL_GET', 'FN_DATA_PROVIDERS_ALL_GET', 'FN_DATA_PROVIDERS_ALL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');


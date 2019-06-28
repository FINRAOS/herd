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

insert into strge_pltfm (strge_pltfm_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('GLACIER', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

create table strge_plcy_rule_type_cd_lk
(
 strge_plcy_rule_type_cd   varchar(50)  not null,
 strge_plcy_rule_type_ds   varchar(512) not null,
 creat_ts                  timestamp    not null default current_timestamp,
 creat_user_id             varchar(100) not null,
 updt_ts                   timestamp    not null default current_timestamp,
 updt_user_id              varchar(100)
);

alter table strge_plcy_rule_type_cd_lk add constraint strge_plcy_rule_type_cd_lk_pk primary key (strge_plcy_rule_type_cd);

insert into strge_plcy_rule_type_cd_lk (strge_plcy_rule_type_cd, strge_plcy_rule_type_ds, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('DAYS_SINCE_BDATA_REGISTERED', 'Days since Business Object Data was registered', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

create sequence strge_plcy_seq;

create table strge_plcy
(
 strge_plcy_id              bigint       not null,
 name_space_cd              varchar(50)  not null,
 name_tx                    varchar(50)  not null,
 strge_cd                   varchar(50)  not null,
 dstnt_strge_cd             varchar(50)  not null,
 strge_plcy_rule_type_cd    varchar(50)  not null,
 strge_plcy_rule_value_nb   bigint       not null,
 bus_objct_dfntn_id         bigint       null,
 usage_cd                   varchar(50)  null,
 file_type_cd               varchar(20)  null,
 creat_ts                   timestamp    not null default current_timestamp,
 creat_user_id              varchar(100) not null,
 updt_ts                    timestamp    not null default current_timestamp,
 updt_user_id               varchar(100)
);

alter table strge_plcy add constraint strge_plcy_pk primary key (strge_plcy_id);

create unique index strge_plcy_ak on strge_plcy (name_space_cd, name_tx);

alter table strge_plcy add constraint strge_plcy_fk1 foreign key (name_space_cd)
references name_space (name_space_cd);

alter table strge_plcy add constraint strge_plcy_fk2 foreign key (strge_cd)
references strge (strge_cd);

create index strge_plcy_ix1 on strge_plcy (strge_cd);

alter table strge_plcy add constraint strge_plcy_fk3 foreign key (dstnt_strge_cd)
references strge (strge_cd);

create index strge_plcy_ix2 on strge_plcy (dstnt_strge_cd);

alter table strge_plcy add constraint strge_plcy_fk4 foreign key (strge_plcy_rule_type_cd)
references strge_plcy_rule_type_cd_lk (strge_plcy_rule_type_cd);

create index strge_plcy_ix3 on strge_plcy (strge_plcy_rule_type_cd);

alter table strge_plcy add constraint strge_plcy_fk5 foreign key (bus_objct_dfntn_id)
references bus_objct_dfntn (bus_objct_dfntn_id);

create index strge_plcy_ix4 on strge_plcy (bus_objct_dfntn_id);

alter table strge_plcy add constraint strge_plcy_fk6 foreign key (file_type_cd)
references file_type_cd_lk (file_type_cd);

create index strge_plcy_ix5 on strge_plcy (file_type_cd);

create index strge_plcy_ix6 on strge_plcy (usage_cd);

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_STORAGE_POLICIES_POST','FN_STORAGE_POLICIES_POST','FN_STORAGE_POLICIES_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_STORAGE_POLICIES_GET','FN_STORAGE_POLICIES_GET','FN_STORAGE_POLICIES_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_BUSINESS_OBJECT_DATA_UPLOAD_CREDENTIAL_GET','FN_BUSINESS_OBJECT_DATA_UPLOAD_CREDENTIAL_GET','FN_BUSINESS_OBJECT_DATA_UPLOAD_CREDENTIAL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_BUSINESS_OBJECT_DATA_DOWNLOAD_CREDENTIAL_GET','FN_BUSINESS_OBJECT_DATA_DOWNLOAD_CREDENTIAL_GET','FN_BUSINESS_OBJECT_DATA_DOWNLOAD_CREDENTIAL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

update scrty_fn_lk
set    scrty_fn_dsply_nm = 'FN_DISPLAY_HERD_UI',
       scrty_fn_ds = 'FN_DISPLAY_HERD_UI',
       updt_ts = current_timestamp
where  scrty_fn_cd = 'FN_DISPLAY_HERD_UI';

alter table scrty_fn_lk alter column scrty_fn_ds set not null;
alter table file_type_cd_lk alter column file_type_ds set not null;

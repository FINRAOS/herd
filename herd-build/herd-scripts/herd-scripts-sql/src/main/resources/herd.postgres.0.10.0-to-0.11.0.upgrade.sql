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

create sequence user_name_space_athrn_seq;

create table user_name_space_athrn
(
 user_name_space_athrn_id   bigint       not null,
 user_id                    varchar(50)  not null,
 name_space_cd              varchar(50)  not null,
 read_prmsn_fl              char(1)      not null,
 write_prmsn_fl             char(1)      not null,
 exct_prmsn_fl              char(1)      not null,
 grant_prmsn_fl             char(1)      not null,
 creat_ts                   timestamp    not null default current_timestamp,
 creat_user_id              varchar(100) not null,
 updt_ts                    timestamp    not null default current_timestamp,
 updt_user_id               varchar(100)
);

alter table user_name_space_athrn add constraint user_name_space_athrn_pk primary key (user_name_space_athrn_id);

create unique index user_name_space_athrn_ak on user_name_space_athrn (user_id, name_space_cd);

alter table user_name_space_athrn add constraint user_name_space_athrn_fk1 foreign key (name_space_cd)
references name_space (name_space_cd);

create index user_name_space_athrn_ix1 on user_name_space_athrn (name_space_cd);

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_USER_NAMESPACE_AUTHORIZATIONS_POST','FN_USER_NAMESPACE_AUTHORIZATIONS_POST','FN_USER_NAMESPACE_AUTHORIZATIONS_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_USER_NAMESPACE_AUTHORIZATIONS_PUT','FN_USER_NAMESPACE_AUTHORIZATIONS_PUT','FN_USER_NAMESPACE_AUTHORIZATIONS_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_USER_NAMESPACE_AUTHORIZATIONS_GET','FN_USER_NAMESPACE_AUTHORIZATIONS_GET','FN_USER_NAMESPACE_AUTHORIZATIONS_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_USER_NAMESPACE_AUTHORIZATIONS_DELETE','FN_USER_NAMESPACE_AUTHORIZATIONS_DELETE','FN_USER_NAMESPACE_AUTHORIZATIONS_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_USER_NAMESPACE_AUTHORIZATIONS_ALL_GET','FN_USER_NAMESPACE_AUTHORIZATIONS_ALL_GET','FN_USER_NAMESPACE_AUTHORIZATIONS_ALL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

alter table bus_objct_data_stts_cd_lk add column pre_rgstn_stts_fl char(1);

update bus_objct_data_stts_cd_lk 
set    pre_rgstn_stts_fl = 'N' 
where  pre_rgstn_stts_fl is null;

update bus_objct_data_stts_cd_lk
set    pre_rgstn_stts_fl = 'Y'
where  bus_objct_data_stts_cd in ('UPLOADING');

alter table bus_objct_data_stts_cd_lk alter column pre_rgstn_stts_fl set not null;

insert into bus_objct_data_stts_cd_lk (bus_objct_data_stts_cd, bus_objct_data_stts_ds, creat_ts, creat_user_id, updt_ts, updt_user_id, pre_rgstn_stts_fl)
values ('PROCESSING', 'Processing', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM', 'Y');

insert into bus_objct_data_stts_cd_lk (bus_objct_data_stts_cd, bus_objct_data_stts_ds, creat_ts, creat_user_id, updt_ts, updt_user_id, pre_rgstn_stts_fl)
values ('PENDING_VALID', 'Pending Valid', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM', 'Y');

update bus_objct_data_stts_cd_lk
set    bus_objct_data_stts_ds = 'Re-Encrypting'
where  bus_objct_data_stts_cd = 'RE-ENCRYPTING'; 
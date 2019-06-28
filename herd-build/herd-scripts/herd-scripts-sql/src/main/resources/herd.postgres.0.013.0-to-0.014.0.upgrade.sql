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

create sequence name_space_iam_role_athrn_seq;

create table name_space_iam_role_athrn
(
 name_space_iam_role_athrn_id bigint       not null,
 name_space_cd                varchar(50)  not null,
 iam_role_nm                  varchar(64)  not null,
 athrn_ds                     varchar(64),
 creat_ts                     timestamp    not null default current_timestamp,
 creat_user_id                varchar(100) not null,
 updt_ts                      timestamp    not null default current_timestamp,
 updt_user_id                 varchar(100)
);

alter table name_space_iam_role_athrn add constraint name_space_iam_role_athrn_pk primary key (name_space_iam_role_athrn_id);

create unique index name_space_iam_role_athrn_ak on name_space_iam_role_athrn (name_space_cd, iam_role_nm);

alter table name_space_iam_role_athrn add constraint name_space_iam_role_athrn_fk1 foreign key (name_space_cd)
references name_space (name_space_cd);

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_NAMESPACE_IAM_ROLE_AUTHORIZATIONS_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_NAMESPACE_IAM_ROLE_AUTHORIZATIONS_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_NAMESPACE_IAM_ROLE_AUTHORIZATIONS_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_NAMESPACE_IAM_ROLE_AUTHORIZATIONS_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_NAMESPACE_IAM_ROLE_AUTHORIZATIONS_ALL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

alter table bus_objct_data_atrbt_dfntn add column pblsh_fl char(1);

update bus_objct_data_atrbt_dfntn 
set    pblsh_fl = 'N' 
where  pblsh_fl is null;

alter table bus_objct_data_atrbt_dfntn alter column pblsh_fl set not null;

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_STORAGE_UNIT_DOWNLOAD_CREDENTIAL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_STORAGE_UNIT_UPLOAD_CREDENTIAL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

delete from scrty_fn_lk where scrty_fn_cd = 'FN_USER_NAMESPACE_AUTHORIZATIONS_BY_USERID_GET';

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_USER_NAMESPACE_AUTHORIZATIONS_BY_USERID_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_USER_NAMESPACE_AUTHORIZATIONS_BY_NAMESPACE_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into file_type_cd_lk (file_type_cd, file_type_ds, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('CSV', 'CSV file', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into file_type_cd_lk (file_type_cd, file_type_ds, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('XML', 'XML file', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

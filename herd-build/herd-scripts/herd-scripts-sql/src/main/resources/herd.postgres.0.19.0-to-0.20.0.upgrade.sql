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

create sequence bus_objct_dfntn_clmn_seq;

create table bus_objct_dfntn_clmn (
 bus_objct_dfntn_clmn_id    bigint       not null,
 bus_objct_dfntn_id         bigint       not null,
 clmn_name_tx               varchar(100) not null,
 clmn_ds                    varchar(512),
 creat_ts                   timestamp    not null default current_timestamp,
 creat_user_id              varchar(100) not null,
 updt_ts                    timestamp    not null default current_timestamp,
 updt_user_id               varchar(100)
);

alter table bus_objct_dfntn_clmn add constraint bus_objct_dfntn_clmn_pk primary key (bus_objct_dfntn_clmn_id);

create unique index bus_objct_dfntn_clmn_ak on bus_objct_dfntn_clmn (bus_objct_dfntn_id, clmn_name_tx);

alter table bus_objct_dfntn_clmn add constraint bus_objct_dfntn_clmn_fk1 foreign key (bus_objct_dfntn_id) 
references bus_objct_dfntn (bus_objct_dfntn_id);

alter table schm_clmn add column bus_objct_dfntn_clmn_id bigint;

alter table schm_clmn add constraint schm_clmn_fk2 foreign key (bus_objct_dfntn_clmn_id) 
references bus_objct_dfntn_clmn (bus_objct_dfntn_clmn_id);

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_BUSINESS_OBJECT_DEFINITION_COLUMNS_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_BUSINESS_OBJECT_DEFINITION_COLUMNS_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_BUSINESS_OBJECT_DEFINITION_COLUMNS_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_BUSINESS_OBJECT_DEFINITION_COLUMNS_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_BUSINESS_OBJECT_DEFINITION_COLUMNS_ALL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

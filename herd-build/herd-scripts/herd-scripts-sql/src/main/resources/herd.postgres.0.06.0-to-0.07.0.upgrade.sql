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

create table strge_unit_stts_cd_lk
(
 strge_unit_stts_cd   varchar(20)  not null,
 strge_unit_stts_ds   varchar(512) not null,
 avlbl_fl             char(1)      not null, 
 creat_ts             timestamp    not null default current_timestamp,
 creat_user_id        varchar(100) not null,
 updt_ts              timestamp    not null default current_timestamp,
 updt_user_id         varchar(100)
);

alter table strge_unit_stts_cd_lk add constraint strge_unit_stts_cd_lk_pk primary key (strge_unit_stts_cd);

insert into strge_unit_stts_cd_lk (strge_unit_stts_cd, strge_unit_stts_ds, avlbl_fl, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('ENABLED', 'Enabled', 'Y', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into strge_unit_stts_cd_lk (strge_unit_stts_cd, strge_unit_stts_ds, avlbl_fl, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('DISABLED', 'Disabled', 'N', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into strge_unit_stts_cd_lk (strge_unit_stts_cd, strge_unit_stts_ds, avlbl_fl, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('ARCHIVING', 'Archiving', 'N', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

create sequence strge_unit_stts_hs_seq;

create table strge_unit_stts_hs
(
 strge_unit_stts_hs_id   bigint        not null,
 strge_unit_id           bigint        not null,
 strge_unit_stts_cd      varchar(20)   not null,
 rsn_tx                  varchar(1000) not null,
 creat_ts                timestamp     not null default current_timestamp,
 creat_user_id           varchar(100)  not null,
 updt_ts                 timestamp     not null default current_timestamp,
 updt_user_id            varchar(100)
);

alter table strge_unit_stts_hs add constraint strge_unit_stts_hs_pk primary key (strge_unit_stts_hs_id);

create unique index strge_unit_stts_hs_ak on strge_unit_stts_hs (strge_unit_id, strge_unit_stts_cd, creat_ts);

alter table strge_unit_stts_hs add constraint strge_unit_stts_hs_fk1 foreign key (strge_unit_id)
references strge_unit (strge_unit_id);

create index strge_unit_stts_hs_ix1 on strge_unit_stts_hs (strge_unit_id);

alter table strge_unit_stts_hs add constraint strge_unit_stts_hs_fk2 foreign key (strge_unit_stts_cd)
references strge_unit_stts_cd_lk (strge_unit_stts_cd);

create index strge_unit_stts_hs_ix2 on strge_unit_stts_hs (strge_unit_stts_cd);

alter table strge_unit add column strge_unit_stts_cd varchar(20);

update strge_unit 
set    strge_unit_stts_cd = 'ENABLED' 
where  strge_unit_stts_cd is null;

alter table strge_unit alter column strge_unit_stts_cd set not null;

alter table strge_unit add constraint strge_unit_fk3 foreign key (strge_unit_stts_cd)
references strge_unit_stts_cd_lk (strge_unit_stts_cd);

create index strge_unit_ix3 on strge_unit (strge_unit_stts_cd);

alter table strge_unit add column prnt_strge_unit_id bigint;

alter table strge_unit add constraint strge_unit_fk4 foreign key (prnt_strge_unit_id)
references strge_unit (strge_unit_id);

create index strge_unit_ix4 on strge_unit (prnt_strge_unit_id);

alter table strge_file add column archive_id varchar(512);

alter table bus_objct_dfntn drop column lgcy_fl;

alter table ntfcn_rgstn alter column bus_objct_dfntn_id set not null;

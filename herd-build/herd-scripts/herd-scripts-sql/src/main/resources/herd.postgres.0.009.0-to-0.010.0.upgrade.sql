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

create table strge_plcy_stts_cd_lk
(
 strge_plcy_stts_cd   varchar(20)  not null,
 strge_plcy_stts_ds   varchar(512) not null,
 creat_ts             timestamp    not null default current_timestamp,
 creat_user_id        varchar(100) not null,
 updt_ts              timestamp    not null default current_timestamp,
 updt_user_id         varchar(100)
);

alter table strge_plcy_stts_cd_lk add constraint strge_plcy_stts_cd_lk_pk primary key (strge_plcy_stts_cd);

insert into strge_plcy_stts_cd_lk (strge_plcy_stts_cd, strge_plcy_stts_ds, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('ENABLED', 'Enabled', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into strge_plcy_stts_cd_lk (strge_plcy_stts_cd, strge_plcy_stts_ds, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('DISABLED', 'Disabled', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

alter table strge_plcy add column strge_plcy_stts_cd varchar(20);

update strge_plcy 
set    strge_plcy_stts_cd = 'ENABLED' 
where  strge_plcy_stts_cd is null;

alter table strge_plcy alter column strge_plcy_stts_cd set not null;

alter table strge_plcy add constraint strge_plcy_fk7 foreign key (strge_plcy_stts_cd)
references strge_plcy_stts_cd_lk (strge_plcy_stts_cd);

create index strge_plcy_ix7 on strge_plcy (strge_plcy_stts_cd);

alter table strge_plcy add column vrsn_nb bigint;

update strge_plcy 
set    vrsn_nb = 0 
where  vrsn_nb is null;

alter table strge_plcy alter column vrsn_nb set not null;

drop index strge_plcy_ak;
create unique index strge_plcy_ak on strge_plcy (name_space_cd, name_tx, vrsn_nb);

alter table strge_plcy add column ltst_vrsn_fl char(1);

update strge_plcy 
set    ltst_vrsn_fl = 'Y' 
where  ltst_vrsn_fl is null;

alter table strge_plcy alter column ltst_vrsn_fl set not null;

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_STORAGE_POLICIES_PUT','FN_STORAGE_POLICIES_PUT','FN_STORAGE_POLICIES_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

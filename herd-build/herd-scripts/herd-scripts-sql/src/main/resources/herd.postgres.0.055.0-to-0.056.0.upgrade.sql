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

-- create rtntn_type_cd_lk table

create table rtntn_type_cd_lk
(
  rtntn_type_cd              varchar(50)     NOT NULL,
  creat_ts                   timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
  creat_user_id              varchar(100)    NOT NULL,
  updt_ts                    timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
  updt_user_id               varchar(100)    NOT NULL
);

alter table rtntn_type_cd_lk add constraint rtntn_type_cd_lk_pk primary key (rtntn_type_cd); 

-- alter bus_objct_frmt table

alter table bus_objct_frmt add column rec_fl char(1);
alter table bus_objct_frmt add column rtntn_prd_days bigint;
alter table bus_objct_frmt add column rtntn_type_cd varchar(50);

alter table bus_objct_frmt add constraint bus_objct_frmt_ck2 check (rec_fl in ('Y', 'N'));

alter table bus_objct_frmt add constraint bus_objct_frmt_fk4 foreign key (rtntn_type_cd) references rtntn_type_cd_lk(rtntn_type_cd);

create index bus_objct_frmt_ix6 ON bus_objct_frmt(rtntn_type_cd);

-- scrty_fn_lk records

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_FORMAT_RETENTION_INFORMATION_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

-- pre-populate rtntn_type_cd_lk table

insert into rtntn_type_cd_lk (rtntn_type_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('PARTITION_VALUE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

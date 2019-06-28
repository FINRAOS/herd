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

create sequence bus_objct_frmt_atrbt_seq;

create table bus_objct_frmt_atrbt
(
 bus_objct_frmt_atrbt_id    bigint not null,
 bus_objct_frmt_id      bigint not null,
 atrbt_nm               varchar(100) not null,
 atrbt_value_tx         varchar(4000),
 creat_ts               timestamp not null default now(),
 creat_user_id          varchar(100) not null,
 updt_ts                timestamp not null default now(),
 updt_user_id           varchar(100)
);

alter table bus_objct_frmt_atrbt add constraint bus_objct_frmt_atrbt_pk primary key (bus_objct_frmt_atrbt_id);

create unique index bus_objct_frmt_atrbt_ak on bus_objct_frmt_atrbt (bus_objct_frmt_id, atrbt_nm);

alter table bus_objct_frmt_atrbt add constraint bus_objct_frmt_atrbt_fk1 foreign key (bus_objct_frmt_id)
references bus_objct_frmt (bus_objct_frmt_id);

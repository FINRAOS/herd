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
-----------------------------------------------------------------

create table bus_objct_dfntn_chg
(
  bus_objct_dfntn_chg_id     int8            NOT NULL,
  bus_objct_dfntn_id         int8            NOT NULL,
  desc_tx                    varchar(24000),
  dsply_name_tx              varchar(100),
  creat_ts                   timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
  creat_user_id              varchar(100)    NOT NULL,
  updt_ts                    timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
  updt_user_id               varchar(100)    NOT NULL,
  frmt_usage_cd              varchar(50),
  frmt_file_type_cd          varchar(20)
);

alter table bus_objct_dfntn_chg add constraint bus_objct_dfntn_chg_pk  primary key (bus_objct_dfntn_chg_id); 
alter table bus_objct_dfntn_chg add constraint bus_objct_dfntn_chg_fk1 foreign key (bus_objct_dfntn_id) references bus_objct_dfntn(bus_objct_dfntn_id);

create index bus_objct_dfntn_chg_ix1 ON bus_objct_dfntn_chg(bus_objct_dfntn_id);

create sequence bus_objct_dfntn_chg_seq;

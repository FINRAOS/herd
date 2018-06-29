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

-- bus_objct_dfntn_dscr_sgstn_stts_cd_lk

create table bus_objct_dfntn_dscr_sgstn_stts_cd_lk
(
    bus_objct_dfntn_dscr_sgstn_stts_cd VARCHAR(255)    NOT NULL,
    creat_ts                           TIMESTAMP       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id                      VARCHAR(100)    NOT NULL,
    updt_ts                            TIMESTAMP       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id                       VARCHAR(100)    NOT NULL
);

alter table bus_objct_dfntn_dscr_sgstn_stts_cd_lk add constraint bus_objct_dfntn_dscr_sgstn_stts_cd_lk_pk  primary key (bus_objct_dfntn_dscr_sgstn_stts_cd); 

insert into bus_objct_dfntn_dscr_sgstn_stts_cd_lk (bus_objct_dfntn_dscr_sgstn_stts_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('ACCEPTED', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into bus_objct_dfntn_dscr_sgstn_stts_cd_lk (bus_objct_dfntn_dscr_sgstn_stts_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('PENDING', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');


-- bus_objct_dfntn_dscr_sgstn

alter table bus_objct_dfntn_dscr_sgstn add column bus_objct_dfntn_dscr_sgstn_stts_cd varchar(255) NOT NULL default 'PENDING';

alter table bus_objct_dfntn_dscr_sgstn add constraint bus_objct_dfntn_dscr_sgstn_fk2 foreign key (bus_objct_dfntn_dscr_sgstn_stts_cd) references bus_objct_dfntn_dscr_sgstn_stts_cd_lk(bus_objct_dfntn_dscr_sgstn_stts_cd);

create index bus_objct_dfntn_dscr_sgstn_ix2 ON bus_objct_dfntn_dscr_sgstn(bus_objct_dfntn_dscr_sgstn_stts_cd);


--scrty_fn_lk

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_SEARCH_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_ACCEPTANCE_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');


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

-- bus_objct_dfntn_dscr_sgstn

create table bus_objct_dfntn_dscr_sgstn
(
    bus_objct_dfntn_dscr_sgstn_id BIGINT          NOT NULL,
    bus_objct_dfntn_id            BIGINT          NOT NULL,
    user_id                       VARCHAR(100)    NOT NULL,
    dscr_sgstn_tx                 VARCHAR(24000)  NOT NULL,
    creat_ts                      TIMESTAMP       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id                 VARCHAR(100)    NOT NULL,
    updt_ts                       TIMESTAMP       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id                  VARCHAR(100)    NOT NULL
);

alter table bus_objct_dfntn_dscr_sgstn add constraint bus_objct_dfntn_dscr_sgstn_pk  primary key (bus_objct_dfntn_dscr_sgstn_id); 
alter table bus_objct_dfntn_dscr_sgstn add constraint bus_objct_dfntn_dscr_sgstn_fk1 foreign key (bus_objct_dfntn_id) references bus_objct_dfntn (bus_objct_dfntn_id);

create index bus_objct_dfntn_dscr_sgstn_ix1 on bus_objct_dfntn_dscr_sgstn (bus_objct_dfntn_id);
create unique index bus_objct_dfntn_dscr_sgstn_ak on bus_objct_dfntn_dscr_sgstn (bus_objct_dfntn_id, user_id);

create sequence bus_objct_dfntn_dscr_sgstn_seq;

-- strge_unit indexes

create index strge_unit_ix1 on strge_unit (strge_cd);
create index strge_unit_ix4 on strge_unit (drcty_path_tx);

--scrty_fn_lk

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_ALL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

--file_type_cd_lk

insert into file_type_cd_lk (file_type_cd, file_type_ds, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('PDF', 'PDF file', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');


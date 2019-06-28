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

-- xtrnl_intrfc

create table xtrnl_intrfc
(
    xtrnl_intrfc_cd             VARCHAR(50)    NOT NULL,
    dsply_name_tx               VARCHAR(100)   NOT NULL,
    desc_tx                     TEXT,
    creat_ts                    TIMESTAMP      NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id               VARCHAR(100)   NOT NULL,
    updt_ts                     TIMESTAMP      NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id                VARCHAR(100)   NOT NULL
);

alter table xtrnl_intrfc add constraint xtrnl_intrfc_pk primary key (xtrnl_intrfc_cd); 

-- bus_objct_data

create index bus_objct_data_ix4 ON bus_objct_data(prtn_value_tx, prtn_value_2_tx, prtn_value_3_tx, prtn_value_4_tx, prtn_value_5_tx, vrsn_nb);

--scrty_fn_lk

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_EXTERNAL_INTERFACES_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_EXTERNAL_INTERFACES_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_EXTERNAL_INTERFACES_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_EXTERNAL_INTERFACES_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_EXTERNAL_INTERFACES_ALL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');


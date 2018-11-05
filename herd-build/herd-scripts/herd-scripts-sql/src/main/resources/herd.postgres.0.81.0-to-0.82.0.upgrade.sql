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

create table bus_objct_frmt_xtrnl_intrfc
(
    bus_objct_frmt_xtrnl_intrfc_id BIGINT          NOT NULL,
    bus_objct_frmt_id              BIGINT          NOT NULL,
    xtrnl_intrfc_cd                VARCHAR(50)     NOT NULL,
    creat_ts                       TIMESTAMP       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id                  VARCHAR(100)    NOT NULL,
    updt_ts                        TIMESTAMP       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id                   VARCHAR(100)    NOT NULL
);

alter table bus_objct_frmt_xtrnl_intrfc add constraint bus_objct_frmt_xtrnl_intrfc_pk  primary key (bus_objct_frmt_xtrnl_intrfc_id); 
alter table bus_objct_frmt_xtrnl_intrfc add constraint bus_objct_frmt_xtrnl_intrfc_fk1 foreign key (bus_objct_frmt_id) references bus_objct_frmt (bus_objct_frmt_id);
alter table bus_objct_frmt_xtrnl_intrfc add constraint bus_objct_frmt_xtrnl_intrfc_fk2 foreign key (xtrnl_intrfc_cd) references xtrnl_intrfc (xtrnl_intrfc_cd);

create index bus_objct_frmt_xtrnl_intrfc_ix1 on bus_objct_frmt_xtrnl_intrfc (bus_objct_frmt_id);
create index bus_objct_frmt_xtrnl_intrfc_ix2 on bus_objct_frmt_xtrnl_intrfc (xtrnl_intrfc_cd);
create unique index bus_objct_frmt_xtrnl_intrfc_ak on bus_objct_frmt_xtrnl_intrfc (bus_objct_frmt_id, xtrnl_intrfc_cd);

create sequence bus_objct_frmt_xtrnl_intrfc_seq;

--scrty_fn_lk

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_FORMAT_EXTERNAL_INTERFACES_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_FORMAT_EXTERNAL_INTERFACES_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_FORMAT_EXTERNAL_INTERFACES_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');


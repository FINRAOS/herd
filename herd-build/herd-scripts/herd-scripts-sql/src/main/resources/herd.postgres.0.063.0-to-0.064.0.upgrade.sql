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

-- bus_objct_dfntn_clmn_chg

create table bus_objct_dfntn_clmn_chg
(
    bus_objct_dfntn_clmn_chg_id BIGINT         NOT NULL,
    bus_objct_dfntn_clmn_id     BIGINT         NOT NULL,
    clmn_ds                     VARCHAR(4000),
    creat_ts                    TIMESTAMP      NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id               VARCHAR(100)   NOT NULL,
    updt_ts                     TIMESTAMP      NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id                VARCHAR(100)   NOT NULL
);

alter table bus_objct_dfntn_clmn_chg add constraint bus_objct_dfntn_clmn_chg_pk  primary key (bus_objct_dfntn_clmn_chg_id); 
alter table bus_objct_dfntn_clmn_chg add constraint bus_objct_dfntn_clmn_chg_fk1 foreign key (bus_objct_dfntn_clmn_id) references bus_objct_dfntn_clmn(bus_objct_dfntn_clmn_id);

create index bus_objct_dfntn_clmn_chg_ix1 on bus_objct_dfntn_clmn_chg (bus_objct_dfntn_clmn_id);

create sequence bus_objct_dfntn_clmn_chg_seq;

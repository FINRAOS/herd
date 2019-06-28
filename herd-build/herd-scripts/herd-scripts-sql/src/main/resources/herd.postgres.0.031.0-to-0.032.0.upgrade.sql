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

CREATE TABLE bus_objct_dfntn_subj_mttr_xprt  
( 
    bus_objct_dfntn_subj_mttr_xprt_id   int8            NOT NULL,
    bus_objct_dfntn_id                  int8            NOT NULL,
    user_id                             varchar(50)     NOT NULL,
    creat_ts                            timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id                       varchar(100)    NOT NULL,
    updt_ts                             timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id                        varchar(100)
);
    
ALTER TABLE bus_objct_dfntn_subj_mttr_xprt ADD CONSTRAINT bus_objct_dfntn_subj_mttr_xprt_pk   PRIMARY KEY(bus_objct_dfntn_subj_mttr_xprt_id);
ALTER TABLE bus_objct_dfntn_subj_mttr_xprt ADD CONSTRAINT bus_objct_dfntn_subj_mttr_xprt_fk1  FOREIGN KEY(bus_objct_dfntn_id) REFERENCES bus_objct_dfntn(bus_objct_dfntn_id);

CREATE INDEX bus_objct_dfntn_subj_mttr_xprt_ak  ON bus_objct_dfntn_subj_mttr_xprt(bus_objct_dfntn_id);
CREATE INDEX bus_objct_dfntn_subj_mttr_xprt_ak2  ON bus_objct_dfntn_subj_mttr_xprt(user_id);

CREATE SEQUENCE bus_objct_dfntn_subj_mttr_xprt_seq;

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_SUBJECT_MATTER_EXPERTS_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_SUBJECT_MATTER_EXPERTS_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_SUBJECT_MATTER_EXPERTS_BY_BUSINESS_OBJECT_DEFINITION_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DATA_RETRY_STORAGE_POLICY_TRANSITION_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
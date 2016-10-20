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

CREATE TABLE tag_prnt  ( 
    tag_id            int8 			NOT NULL,
    prnt_tag_id       int8 			NULL
    );
alter table tag_prnt add constraint tag_prnt_pk primary key (tag_id);
ALTER TABLE tag_prnt ADD CONSTRAINT tag_prnt_fk1 FOREIGN KEY(tag_id) REFERENCES tag(tag_id);
ALTER TABLE tag_prnt ADD CONSTRAINT tag_prnt_fk2 FOREIGN KEY(prnt_tag_id) REFERENCES tag(tag_id);

CREATE INDEX tag_prnt_ix1 ON tag_prnt(prnt_tag_id);

CREATE TABLE bus_objct_dfntn_tag  ( 
    bus_objct_dfntn_tag_id  int8 			NOT NULL,
    bus_objct_dfntn_id      int8 			NOT NULL,
    tag_id                  int8 			NOT NULL,
    creat_ts                timestamp 	NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id           varchar(100) 	NOT NULL,
    updt_ts                 timestamp 	NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id            varchar(100) 	NULL
    );
    
alter table bus_objct_dfntn_tag ADD CONSTRAINT bus_objct_dfntn_tag_pk   primary key(bus_objct_dfntn_tag_id);
ALTER TABLE bus_objct_dfntn_tag ADD CONSTRAINT bus_objct_dfntn_tag_fk1  FOREIGN KEY(bus_objct_dfntn_id) REFERENCES bus_objct_dfntn(bus_objct_dfntn_id);
ALTER TABLE bus_objct_dfntn_tag ADD CONSTRAINT bus_objct_dfntn_tag_fk2  FOREIGN KEY(tag_id) REFERENCES tag(tag_id);

CREATE UNIQUE INDEX bus_objct_dfntn_tag_ak  ON bus_objct_dfntn_tag(bus_objct_dfntn_id,tag_id);
CREATE        INDEX bus_objct_dfntn_tag_ix1 ON bus_objct_dfntn_tag(bus_objct_dfntn_id);
CREATE        INDEX bus_objct_dfntn_tag_ix2 ON bus_objct_dfntn_tag(tag_id);

CREATE SEQUENCE bus_objct_dfntn_tag_seq;

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_TAGS_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_TAGS_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_TAGS_BY_BUSINESS_OBJECT_DEFINITION_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DEFINITION_TAGS_BY_TAG_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

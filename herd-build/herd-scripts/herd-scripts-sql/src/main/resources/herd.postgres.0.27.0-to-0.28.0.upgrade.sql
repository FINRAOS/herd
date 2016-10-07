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

CREATE TABLE tag_type  ( 
    tag_type_cd       varchar(30) 	NOT NULL,
    dsply_name_tx     varchar(30) 	NOT NULL,
    pstn_nb           int8 			NOT NULL,
    creat_ts          timestamp 	NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id     varchar(100) 	NOT NULL,
    updt_ts           timestamp 	NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id      varchar(100) 	NULL
    );
alter table tag_type add constraint tag_type_pk primary key (tag_type_cd);

CREATE UNIQUE INDEX tag_type_ix1 ON tag_type(dsply_name_tx);

CREATE TABLE tag  ( 
	tag_id			int8			NOT NULL,
    tag_type_cd     varchar(30) 	NOT NULL,
    tag_cd       	varchar(30) 	NOT NULL,
    dsply_name_tx   varchar(100) 	NOT NULL,
    desc_tx     	varchar(24000) 	NULL,
    creat_ts        timestamp 		NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id   varchar(100) 	NOT NULL,
    updt_ts         timestamp 		NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id    varchar(100) 	NULL
    );
	
alter table tag add constraint tag_pk primary key (tag_id);
ALTER TABLE tag ADD CONSTRAINT tag_fk1 	FOREIGN KEY(tag_type_cd) REFERENCES tag_type(tag_type_cd);

CREATE UNIQUE INDEX tag_ak ON tag(tag_type_cd,tag_cd);
CREATE UNIQUE INDEX tag_ix1 ON tag(tag_type_cd,dsply_name_tx);

CREATE SEQUENCE tag_seq;

ALTER TABLE bus_objct_dfntn ALTER COLUMN  desc_tx TYPE varchar(24000);
ALTER TABLE bus_objct_dfntn ADD  dsply_name_tx  varchar(100) null;

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_BUSINESS_OBJECT_DEFINITIONS_DESCRIPTIVE_INFO_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_TAG_TYPES_ALL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_TAG_TYPES_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_TAG_TYPES_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_TAG_TYPES_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_TAG_TYPES_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_TAGS_ALL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_TAGS_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_TAGS_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_TAGS_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_TAGS_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

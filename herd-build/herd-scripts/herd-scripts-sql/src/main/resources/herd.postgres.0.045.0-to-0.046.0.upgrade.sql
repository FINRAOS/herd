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

CREATE TABLE msg_type_cd_lk  (
    msg_type_cd       varchar(30)       NOT NULL,
    creat_ts          timestamp         NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id     varchar(100)      NOT NULL,
    updt_ts           timestamp         NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id      varchar(100)      NULL
    );
    
alter table msg_type_cd_lk add constraint msg_type_cd_lk_pk primary key (msg_type_cd);

CREATE TABLE ntfcn_msg (
    ntfcn_msg_id      int8              NOT NULL,
    msg_type_cd       varchar(30)       NOT NULL,
    msg_dstnt         varchar(100)      NOT NULL,
    msg_tx            text              NULL,
    creat_ts          timestamp         NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id     varchar(100)      NOT NULL,
    updt_ts           timestamp         NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id      varchar(100)      NULL
);

ALTER TABLE ntfcn_msg add constraint ntfcn_msg_pk   primary key (ntfcn_msg_id);
ALTER TABLE ntfcn_msg ADD CONSTRAINT ntfcn_msg_fk1  FOREIGN KEY (msg_type_cd) REFERENCES msg_type_cd_lk(msg_type_cd);

CREATE SEQUENCE ntfcn_msg_seq;

insert into msg_type_cd_lk (msg_type_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('SQS', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into msg_type_cd_lk (msg_type_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('SNS', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

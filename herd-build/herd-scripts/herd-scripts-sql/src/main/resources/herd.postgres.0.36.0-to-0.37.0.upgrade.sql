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

CREATE TABLE srch_idx_type_cd_lk  
( 
    srch_idx_type_cd                    varchar(255)    NOT NULL,
    creat_ts                            timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id                       varchar(100)    NOT NULL,
    updt_ts                             timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id                        varchar(100)
);
    
ALTER TABLE srch_idx_type_cd_lk ADD CONSTRAINT srch_idx_type_cd_lk_pk PRIMARY KEY(srch_idx_type_cd);

CREATE TABLE srch_idx_stts_cd_lk  
( 
    srch_idx_stts_cd                    varchar(255)    NOT NULL,
    creat_ts                            timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id                       varchar(100)    NOT NULL,
    updt_ts                             timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id                        varchar(100)
);
    
ALTER TABLE srch_idx_stts_cd_lk ADD CONSTRAINT srch_idx_stts_cd_lk_pk   PRIMARY KEY(srch_idx_stts_cd);

CREATE TABLE srch_idx  
( 
    srch_idx_nm                         varchar(255)    NOT NULL,
    srch_idx_type_cd                    varchar(255)    NOT NULL,
    srch_idx_stts_cd                    varchar(255)    NOT NULL,
    creat_ts                            timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id                       varchar(100)    NOT NULL,
    updt_ts                             timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id                        varchar(100)
);
    
ALTER TABLE srch_idx ADD CONSTRAINT srch_idx_pk   PRIMARY KEY(srch_idx_nm);
ALTER TABLE srch_idx ADD CONSTRAINT srch_idx_fk1  FOREIGN KEY(srch_idx_type_cd) REFERENCES srch_idx_type_cd_lk(srch_idx_type_cd);
ALTER TABLE srch_idx ADD CONSTRAINT srch_idx_fk2  FOREIGN KEY(srch_idx_stts_cd) REFERENCES srch_idx_stts_cd_lk(srch_idx_stts_cd);


INSERT INTO srch_idx_type_cd_lk(srch_idx_type_cd,creat_ts,creat_user_id,updt_ts,updt_user_id)
VALUES ('BUS_OBJCT_DFNTN',current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

INSERT INTO srch_idx_stts_cd_lk(srch_idx_stts_cd,creat_ts,creat_user_id,updt_ts,updt_user_id)
VALUES ('BUILDING',current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

INSERT INTO srch_idx_stts_cd_lk(srch_idx_stts_cd,creat_ts,creat_user_id,updt_ts,updt_user_id)
VALUES ('READY',current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_SEARCH_INDEXES_ALL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_SEARCH_INDEXES_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_SEARCH_INDEXES_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_SEARCH_INDEXES_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

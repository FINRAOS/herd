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

create table bus_objct_frmt_prnt
(
  bus_objct_frmt_id        int8       not null,
  prnt_bus_objct_frmt_id   int8       not null
);

alter table bus_objct_frmt_prnt add constraint bus_objct_frmt_prnt_pk  primary key (bus_objct_frmt_id, prnt_bus_objct_frmt_id); 
alter table bus_objct_frmt_prnt add constraint bus_objct_frmt_prnt_fk1 foreign key (bus_objct_frmt_id)      references bus_objct_frmt(bus_objct_frmt_id);
alter table bus_objct_frmt_prnt add constraint bus_objct_frmt_prnt_fk2 foreign key (prnt_bus_objct_frmt_id) references bus_objct_frmt(bus_objct_frmt_id);

create index bus_objct_frmt_prnt_ix1 on bus_objct_frmt_prnt(bus_objct_frmt_id);
create index bus_objct_frmt_prnt_ix2 on bus_objct_frmt_prnt(prnt_bus_objct_frmt_id);

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_FORMAT_PARENTS_PUT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_SEARCH_INDEXES_VALIDATION_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

delete from scrty_fn_lk     where scrty_fn_cd in ('FN_BUSINESS_OBJECT_DEFINITIONS_VALIDATE_INDEX_GET');

CREATE TABLE strge_plcy_trnsn_type_cd_lk  
( 
    strge_plcy_trnsn_type_cd            varchar(20)     NOT NULL,
    creat_ts                            timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id                       varchar(100)    NOT NULL,
    updt_ts                             timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id                        varchar(100)
);
    
ALTER TABLE strge_plcy_trnsn_type_cd_lk ADD CONSTRAINT strge_plcy_trnsn_type_cd_lk_pk PRIMARY KEY(strge_plcy_trnsn_type_cd);

INSERT INTO strge_plcy_trnsn_type_cd_lk(strge_plcy_trnsn_type_cd,creat_ts,creat_user_id,updt_ts,updt_user_id)
VALUES ('GLACIER',current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

INSERT INTO strge_unit_stts_cd_lk(strge_unit_stts_cd,strge_unit_stts_ds,avlbl_fl,creat_ts,creat_user_id,updt_ts,updt_user_id)
VALUES ('ARCHIVED','ARCHIVED','N',current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

DELETE FROM strge_plcy;

DROP INDEX strge_plcy_ix2;

ALTER TABLE     strge_plcy 
                drop column dstnt_strge_cd, 
                add column strge_plcy_trnsn_type_cd varchar(20) not null;
                
ALTER TABLE strge_plcy ADD CONSTRAINT strge_plcy_fk3  FOREIGN KEY(strge_plcy_trnsn_type_cd) REFERENCES strge_plcy_trnsn_type_cd_lk(strge_plcy_trnsn_type_cd);

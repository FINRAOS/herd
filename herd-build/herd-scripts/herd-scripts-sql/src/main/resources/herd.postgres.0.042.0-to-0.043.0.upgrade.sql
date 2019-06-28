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

create table glbl_atrbt_dfntn_level_cd_lk
(
  glbl_atrbt_dfntn_level_cd  varchar(50)     NOT NULL,
  creat_ts                   timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
  creat_user_id              varchar(100)    NOT NULL,
  updt_ts                    timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
  updt_user_id               varchar(100)
);

alter table glbl_atrbt_dfntn_level_cd_lk add constraint glbl_atrbt_dfntn_level_cd_lk_pk  primary key (glbl_atrbt_dfntn_level_cd); 

create table glbl_atrbt_dfntn
(
  glbl_atrbt_dfntn_id        int8            NOT NULL,
  glbl_atrbt_dfntn_level_cd  varchar(50)     NOT NULL, 
  glbl_atrbt_dfntn_name_tx   varchar(50)     NOT NULL,
  creat_ts                   timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
  creat_user_id              varchar(100)    NOT NULL,
  updt_ts                    timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
  updt_user_id               varchar(100)
);

alter table glbl_atrbt_dfntn add constraint glbl_atrbt_dfntn_pk  primary key (glbl_atrbt_dfntn_id); 
alter table glbl_atrbt_dfntn add constraint glbl_atrbt_dfntn_fk1 foreign key (glbl_atrbt_dfntn_level_cd) references glbl_atrbt_dfntn_level_cd_lk(glbl_atrbt_dfntn_level_cd);
CREATE UNIQUE INDEX glbl_atrbt_dfntn_ak  ON glbl_atrbt_dfntn(glbl_atrbt_dfntn_level_cd,glbl_atrbt_dfntn_name_tx);

CREATE SEQUENCE glbl_atrbt_dfntn_seq;

create table atrbt_value_list
(
  atrbt_value_list_id        int8            NOT NULL,
  name_space_cd              varchar(50)     NOT NULL,
  atrbt_value_list_nm        varchar(50)     NOT NULL,
  creat_ts                   timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
  creat_user_id              varchar(100)    NOT NULL,
  updt_ts                    timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
  updt_user_id               varchar(100)
);

alter table atrbt_value_list add constraint atrbt_value_list_pk  primary key (atrbt_value_list_id); 
alter table atrbt_value_list add constraint atrbt_value_list_fk1 foreign key (name_space_cd) references name_space(name_space_cd);

CREATE UNIQUE INDEX atrbt_value_list_ak  ON atrbt_value_list(name_space_cd,atrbt_value_list_nm);

CREATE SEQUENCE atrbt_value_list_seq;


create table alwd_atrbt_value
(
  alwd_atrbt_value_id        int8            NOT NULL,
  atrbt_value_list_id        int8            NOT NULL,
  alwd_atrbt_value           varchar(50)     NOT NULL,
  creat_ts                   timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
  creat_user_id              varchar(100)    NOT NULL,
  updt_ts                    timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
  updt_user_id               varchar(100)
);

alter table alwd_atrbt_value add constraint alwd_atrbt_value_pk  primary key (alwd_atrbt_value_id); 
alter table alwd_atrbt_value add constraint alwd_atrbt_value_fk1 foreign key (atrbt_value_list_id) references atrbt_value_list(atrbt_value_list_id);

CREATE UNIQUE INDEX alwd_atrbt_value_ak  ON alwd_atrbt_value(atrbt_value_list_id,alwd_atrbt_value);

CREATE SEQUENCE alwd_atrbt_value_seq;

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_ALLOWED_ATTRIBUTE_VALUES_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_ALLOWED_ATTRIBUTE_VALUES_ALL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_ALLOWED_ATTRIBUTE_VALUES_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');


insert into glbl_atrbt_dfntn_level_cd_lk (glbl_atrbt_dfntn_level_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('BUS_OBJCT_FRMT', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');


insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_GLOBAL_ATTRIBUTE_DEFINITIONS_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_GLOBAL_ATTRIBUTE_DEFINITIONS_ALL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_GLOBAL_ATTRIBUTE_DEFINITIONS_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');



insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_ATTRIBUTE_VALUE_LISTS_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_ATTRIBUTE_VALUE_LISTS_GET_ALL', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_ATTRIBUTE_VALUE_LISTS_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_ATTRIBUTE_VALUE_LISTS_DELETE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into strge_unit_stts_cd_lk (strge_unit_stts_cd, strge_unit_stts_ds, avlbl_fl,creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('RESTORED','RESTORED','Y',current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM'); 

alter table strge_unit drop column prnt_strge_unit_id; 
alter table strge_unit add column  rstr_xprtn_ts timestamp without time zone NULL; 

alter table alwd_atrbt_value rename COLUMN alwd_atrbt_value to alwd_atrbt_value_tx;    

delete from scrty_fn_lk     where scrty_fn_cd = 'FN_DISPLAY_HERD_UI';


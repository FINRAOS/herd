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

alter table db_user alter column user_id type varchar(50);

alter table db_user add column creat_user_id varchar(100);

update db_user 
set    creat_user_id = 'SYSTEM' 
where  creat_user_id is null;

alter table db_user alter column creat_user_id set not null;

alter table db_user add column updt_user_id varchar(100);

update db_user 
set    updt_user_id = 'SYSTEM' 
where  updt_user_id is null;

alter table db_user add column db_user_id varchar(30);

update db_user 
set    db_user_id = user_id 
where  db_user_id is null;

alter table db_user add column db_user_fl char(1);

update db_user 
set    db_user_fl = 'Y' 
where  db_user_fl is null;

alter table db_user alter column db_user_fl set not null;

alter table db_user add column name_space_athrn_admin_fl char(1);

update db_user 
set    name_space_athrn_admin_fl = 'N' 
where  name_space_athrn_admin_fl is null;

alter table db_user alter column name_space_athrn_admin_fl set not null;

alter table db_user rename to user_tbl;

alter index db_user_pk rename to user_tbl_pk;

create unique index user_tbl_ak on user_tbl (last_nm, first_nm);

alter table scrty_fn_lk drop column scrty_fn_dsply_nm;
alter table scrty_fn_lk drop column scrty_fn_ds;

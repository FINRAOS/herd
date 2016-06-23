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

insert into scrty_fn_lk (scrty_fn_cd, scrty_fn_dsply_nm, scrty_fn_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_JOBS_GET_BY_ID','FN_JOBS_GET_BY_ID','FN_JOBS_GET_BY_ID', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

alter table ntfcn_rgstn add column old_bus_objct_data_stts_cd varchar(50);
alter table ntfcn_rgstn add column new_bus_objct_data_stts_cd varchar(50);

alter table ntfcn_rgstn add constraint ntfcn_rgstn_fk7 foreign key (old_bus_objct_data_stts_cd) references bus_objct_data_stts_cd_lk (bus_objct_data_stts_cd);
alter table ntfcn_rgstn add constraint ntfcn_rgstn_fk8 foreign key (new_bus_objct_data_stts_cd) references bus_objct_data_stts_cd_lk (bus_objct_data_stts_cd);

create index ntfcn_rgstn_ix7 on ntfcn_rgstn (old_bus_objct_data_stts_cd);
create index ntfcn_rgstn_ix8 on ntfcn_rgstn (new_bus_objct_data_stts_cd);

alter table ntfcn_actn add constraint ntfcn_actn_fk3 foreign key (actn_type_cd) references actn_type_cd_lk (actn_type_cd);

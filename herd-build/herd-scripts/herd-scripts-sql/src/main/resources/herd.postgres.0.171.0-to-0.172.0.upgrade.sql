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

alter table bus_objct_frmt add column enbl_bus_objct_data_pblsh_atrbts_chg_event_ntfcn_fl char(1) NOT NULL default 'N';

alter table bus_objct_frmt add constraint bus_objct_frmt_ck4 check (enbl_bus_objct_data_pblsh_atrbts_chg_event_ntfcn_fl in ('Y', 'N'));

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_JOB_DEFINITIONS_ALL_GET', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

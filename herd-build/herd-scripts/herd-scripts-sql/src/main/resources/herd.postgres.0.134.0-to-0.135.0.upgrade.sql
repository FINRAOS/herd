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

alter table bus_objct_dfntn_clmn add column schm_clmn_name_tx varchar(100);

alter table strge_plcy add column do_not_trnsn_ltst_valid_fl char(1) NOT NULL default 'N';
alter table strge_plcy add constraint strge_plcy_ck2 check (do_not_trnsn_ltst_valid_fl in ('Y', 'N'));
alter table strge_plcy add constraint strge_plcy_ck1 check (ltst_vrsn_fl in ('Y', 'N'));

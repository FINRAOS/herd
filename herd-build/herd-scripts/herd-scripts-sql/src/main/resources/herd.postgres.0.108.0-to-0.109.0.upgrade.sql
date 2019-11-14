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

--bus_objct_data_atrbt_dfntn

alter table bus_objct_data_atrbt_dfntn add column pblsh_for_fltr_fl char(1) NOT NULL default 'N';

--bus_objct_frmt

alter table bus_objct_frmt add column cstm_row_frmt_tx varchar(500);

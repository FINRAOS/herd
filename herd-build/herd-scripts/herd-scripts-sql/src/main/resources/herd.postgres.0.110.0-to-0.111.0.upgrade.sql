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

alter table dmrowner.bus_objct_data alter column prtn_value_tx type varchar(100);
alter table dmrowner.bus_objct_data alter column prtn_value_2_tx type varchar(100);

alter table dmrowner.bus_objct_data alter column prtn_value_3_tx type varchar(100), alter column prtn_value_4_tx type varchar(100), alter column prtn_value_5_tx type varchar(100);

alter table emr_clstr_crtn_log alter column emr_clstr_name_tx type varchar(500);

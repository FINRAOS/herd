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

insert into strge_plcy_rule_type_cd_lk (strge_plcy_rule_type_cd,strge_plcy_rule_type_ds, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('DAYS_SINCE_BDATA_PRIMARY_PARTITION_VALUE','Days since Business Object Data Primary Partition Value', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

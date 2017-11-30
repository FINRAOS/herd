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

-- strge_unit table

alter table dmrowner.strge_unit add column final_destroy_ts timestamp without time zone;

-- populate new value in strge_unit_stts_cd_lk table

insert into strge_unit_stts_cd_lk (strge_unit_stts_cd, strge_unit_stts_ds, avlbl_fl, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('DISABLING', 'Disabling', 'N', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

--scrty_fn_lk

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_BUSINESS_OBJECT_DATA_DESTROY_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

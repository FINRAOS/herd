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

create unique index bus_objct_data_ak2 on bus_objct_data (bus_objct_frmt_id, prtn_value_tx, vrsn_nb) where (prtn_value_2_tx is null and prtn_value_3_tx is null and prtn_value_4_tx is null and prtn_value_5_tx is null);
create unique index bus_objct_data_ak3 on bus_objct_data (bus_objct_frmt_id, prtn_value_tx, prtn_value_2_tx, vrsn_nb) where (prtn_value_3_tx is null and prtn_value_4_tx is null and prtn_value_5_tx is null);
create unique index bus_objct_data_ak4 on bus_objct_data (bus_objct_frmt_id, prtn_value_tx, prtn_value_2_tx, prtn_value_3_tx, vrsn_nb) where (prtn_value_4_tx is null and prtn_value_5_tx is null);
create unique index bus_objct_data_ak5 on bus_objct_data (bus_objct_frmt_id, prtn_value_tx, prtn_value_2_tx, prtn_value_3_tx, prtn_value_4_tx, vrsn_nb) where (prtn_value_5_tx is null);

insert into strge_unit_stts_cd_lk (strge_unit_stts_cd, strge_unit_stts_ds, avlbl_fl, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('RESTORING', 'Restoring', 'N', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) 
values ('FN_BUSINESS_OBJECT_DATA_RESTORE_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into strge (strge_cd, creat_ts, creat_user_id, updt_ts, updt_user_id, strge_pltfm_cd)
values ('GLACIER_MANAGED', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM', 'GLACIER');

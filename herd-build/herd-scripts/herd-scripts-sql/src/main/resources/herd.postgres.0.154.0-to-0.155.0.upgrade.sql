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

DROP INDEX bus_objct_data_ak;
DROP INDEX bus_objct_data_ak2;
DROP INDEX bus_objct_data_ak3;
DROP INDEX bus_objct_data_ak4;
DROP INDEX bus_objct_data_ak5;
CREATE UNIQUE INDEX bus_objct_data_ak ON bus_objct_data USING btree (bus_objct_frmt_id, prtn_value_tx, coalesce(prtn_value_2_tx,''), coalesce(prtn_value_3_tx,''), coalesce(prtn_value_4_tx,''), coalesce(prtn_value_5_tx,''), vrsn_nb);

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
CREATE TABLE bus_objct_dfntn_smpl_data_file  
( 
    bus_objct_dfntn_smpl_data_file_id   int8 			NOT NULL,
    bus_objct_dfntn_id                  int8 			NOT NULL,
    file_nm                             varchar(100) 	NOT NULL,
    drcty_path_tx                       varchar(1024) 	NOT NULL,
    file_size_in_bytes_nb               int8 			NOT NULL,
    strge_cd                            varchar(50)     NOT NULL,
    creat_ts                            timestamp 	    NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id                       varchar(100) 	NOT NULL,
    updt_ts                             timestamp 	    NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id                        varchar(100)
);
ALTER TABLE bus_objct_dfntn_smpl_data_file ADD CONSTRAINT bus_objct_dfntn_smpl_data_file_pk   PRIMARY KEY(bus_objct_dfntn_smpl_data_file_id);
ALTER TABLE bus_objct_dfntn_smpl_data_file ADD CONSTRAINT bus_objct_dfntn_smpl_data_file_fk1  FOREIGN KEY(bus_objct_dfntn_id) REFERENCES bus_objct_dfntn(bus_objct_dfntn_id);
ALTER TABLE bus_objct_dfntn_smpl_data_file ADD CONSTRAINT bus_objct_dfntn_smpl_data_file_fk2  FOREIGN KEY(strge_cd) REFERENCES strge(strge_cd);
CREATE UNIQUE INDEX bus_objct_dfntn_smpl_data_file_ak  ON bus_objct_dfntn_smpl_data_file(bus_objct_dfntn_id,file_nm,drcty_path_tx);
CREATE SEQUENCE bus_objct_dfntn_smpl_data_file_seq;
ALTER TABLE bus_objct_dfntn ADD desc_bus_objct_frmt_id int8 NULL;
ALTER TABLE bus_objct_dfntn ADD CONSTRAINT bus_objct_dfntn_fk3 FOREIGN KEY(desc_bus_objct_frmt_id) REFERENCES bus_objct_frmt(bus_objct_frmt_id);

insert into file_type_cd_lk (file_type_cd,file_type_ds,creat_ts,creat_user_id,updt_ts,updt_user_id)
values ('ZIP','ZIP file',current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into scrty_fn_lk (scrty_fn_cd, creat_ts, creat_user_id, updt_ts, updt_user_id)
values ('FN_DOWNLOAD_BUSINESS_OBJECT_DEFINITION_SAMPLE_DATA_FILE_POST', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

insert into strge (strge_cd,creat_ts,creat_user_id,updt_ts,updt_user_id,strge_pltfm_cd)
values ('S3_MANAGED_SAMPLE_DATA',current_timestamp, 'SYSTEM', current_timestamp,'SYSTEM','S3');
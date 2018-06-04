-- Copyright 2015 herd contributors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Change the CNFGN_CL column from a default VARCHAR to a CLOB to handle large data.
ALTER TABLE EMR_CLSTR_DFNTN ALTER COLUMN CNFGN_CL CLOB;
ALTER TABLE CSTM_DDL ALTER COLUMN DDL_CL CLOB;
ALTER TABLE EMR_CLSTR_CRTN_LOG ALTER COLUMN EMR_CLSTR_DFNTN_CL CLOB;
ALTER TABLE EMR_CLSTR_DFNTN ALTER COLUMN CNFGN_CL CLOB;
ALTER TABLE NTFCN_MSG ALTER COLUMN MSG_TX CLOB;

-- Add a constraint to ensure each row in "Business Object Data Parents" is unique. This is needed for a JUnit that ensures duplicate parents aren't allowed.
ALTER TABLE BUS_OBJCT_DATA_PRNT ADD CONSTRAINT BUS_OBJCT_DATA_PRNT_PK PRIMARY KEY (BUS_OBJCT_DATA_ID, PRNT_BUS_OBJCT_DATA_ID);

-- Ensure only "Y" or "N" can be used for the business object format latest version flag.
ALTER TABLE BUS_OBJCT_FRMT ADD CONSTRAINT BUS_OBJCT_FRMT_CK1 CHECK (LTST_VRSN_FL = 'Y' OR LTST_VRSN_FL = 'N');

-- Create a composite key. This is tested in a JUnit by attempting to insert 2 rows with the same format and format version.
CREATE UNIQUE INDEX BUS_OBJCT_FRMT_AK ON BUS_OBJCT_FRMT (BUS_OBJCT_DFNTN_ID, USAGE_CD, FILE_TYPE_CD, FRMT_VRSN_NB);

-- TODO: Need to revisit this to see if the H2 in-memory database supports views. If so, we can un-ignore a JUnit that uses it.
-- CREATE OR REPLACE VIEW biz_dt_file_vw AS
-- SELECT sf.strge_file_id,
--    df.name_space_cd AS namespace,
--    df.name_tx AS bus_object_name,
--    df.data_prvdr_cd AS data_provider,
--    su.strge_cd AS storage_code,
--    sf.file_size_in_bytes_nb,
--    sf.creat_ts::date AS creat_td
--   FROM strge_file sf
--     JOIN strge_unit su ON sf.strge_unit_id = su.strge_unit_id
--     JOIN bus_objct_data d ON su.bus_objct_data_id = d.bus_objct_data_id
--     JOIN bus_objct_frmt f ON d.bus_objct_frmt_id = f.bus_objct_frmt_id
--     JOIN bus_objct_dfntn df ON f.bus_objct_dfntn_id = df.bus_objct_dfntn_id;

-- Insert reference data. --

-- S3 Storage Platform and S3 Managed Storage.
insert into STRGE_PLTFM (STRGE_PLTFM_CD, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('S3', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE (STRGE_CD, STRGE_PLTFM_CD, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('S3_MANAGED', 'S3', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE (STRGE_CD, STRGE_PLTFM_CD, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('S3_MANAGED_LOADING_DOCK', 'S3', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE (STRGE_CD, STRGE_PLTFM_CD, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('S3_MANAGED_EXTERNAL', 'S3', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_ATRBT (STRGE_ATRBT_ID, STRGE_CD, ATRBT_NM, ATRBT_VALUE_TX, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values (nextval('STRGE_ATRBT_SEQ'), 'S3_MANAGED', 'bucket.name', 'TEST-S3-MANAGED-BUCKET', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_ATRBT (STRGE_ATRBT_ID, STRGE_CD, ATRBT_NM, ATRBT_VALUE_TX, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values (nextval('STRGE_ATRBT_SEQ'), 'S3_MANAGED', 'key.prefix.velocity.template', '$namespace/$dataProviderName/$businessObjectFormatUsage/$businessObjectFormatFileType/$businessObjectDefinitionName/schm-v$businessObjectFormatVersion/data-v$businessObjectDataVersion/$businessObjectFormatPartitionKey=$businessObjectDataPartitionValue#if($CollectionUtils.isNotEmpty($businessObjectDataSubPartitions.keySet()))#foreach($subPartitionKey in $businessObjectDataSubPartitions.keySet())/$subPartitionKey=$businessObjectDataSubPartitions.get($subPartitionKey)#end#end', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_ATRBT (STRGE_ATRBT_ID, STRGE_CD, ATRBT_NM, ATRBT_VALUE_TX, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values (nextval('STRGE_ATRBT_SEQ'), 'S3_MANAGED', 'validate.path.prefix', 'true', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_ATRBT (STRGE_ATRBT_ID, STRGE_CD, ATRBT_NM, ATRBT_VALUE_TX, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values (nextval('STRGE_ATRBT_SEQ'), 'S3_MANAGED', 'validate.file.existence', 'true', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_ATRBT (STRGE_ATRBT_ID, STRGE_CD, ATRBT_NM, ATRBT_VALUE_TX, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values (nextval('STRGE_ATRBT_SEQ'), 'S3_MANAGED', 'validate.file.size', 'true', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_ATRBT (STRGE_ATRBT_ID, STRGE_CD, ATRBT_NM, ATRBT_VALUE_TX, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values (nextval('STRGE_ATRBT_SEQ'), 'S3_MANAGED_LOADING_DOCK', 'bucket.name', 'TEST-S3-MANAGED-LOADING-DOCK-BUCKET', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_ATRBT (STRGE_ATRBT_ID, STRGE_CD, ATRBT_NM, ATRBT_VALUE_TX, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values (nextval('STRGE_ATRBT_SEQ'), 'S3_MANAGED_LOADING_DOCK', 'key.prefix.velocity.template', '$environment/$namespace/$businessObjectDataPartitionValue', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_ATRBT (STRGE_ATRBT_ID, STRGE_CD, ATRBT_NM, ATRBT_VALUE_TX, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values (nextval('STRGE_ATRBT_SEQ'), 'S3_MANAGED_LOADING_DOCK', 'upload.role.arn', 'arn:aws:iam:123456789012:role/uploader', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_ATRBT (STRGE_ATRBT_ID, STRGE_CD, ATRBT_NM, ATRBT_VALUE_TX, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values (nextval('STRGE_ATRBT_SEQ'), 'S3_MANAGED_LOADING_DOCK', 'kms.key.id', 'arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_ATRBT (STRGE_ATRBT_ID, STRGE_CD, ATRBT_NM, ATRBT_VALUE_TX, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values (nextval('STRGE_ATRBT_SEQ'), 'S3_MANAGED_EXTERNAL', 'bucket.name', 'TEST-S3-MANAGED-EXTERNAL-BUCKET', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_ATRBT (STRGE_ATRBT_ID, STRGE_CD, ATRBT_NM, ATRBT_VALUE_TX, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values (nextval('STRGE_ATRBT_SEQ'), 'S3_MANAGED_EXTERNAL', 'key.prefix.velocity.template', '$environment/$namespace/$businessObjectDataPartitionValue', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_ATRBT (STRGE_ATRBT_ID, STRGE_CD, ATRBT_NM, ATRBT_VALUE_TX, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values (nextval('STRGE_ATRBT_SEQ'), 'S3_MANAGED_EXTERNAL', 'download.role.arn', 'arn:aws:iam:123456789012:role/downloader', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_ATRBT (STRGE_ATRBT_ID, STRGE_CD, ATRBT_NM, ATRBT_VALUE_TX, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values (nextval('STRGE_ATRBT_SEQ'), 'S3_MANAGED_EXTERNAL', 'kms.key.id', 'arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');

-- Business Object Data Statuses.
insert into BUS_OBJCT_DATA_STTS_CD_LK (BUS_OBJCT_DATA_STTS_CD, BUS_OBJCT_DATA_STTS_DS, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID, pre_rgstn_stts_fl) values ('RE-ENCRYPTING', 'RE-ENCRYPTING', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM', 'N');
insert into BUS_OBJCT_DATA_STTS_CD_LK (BUS_OBJCT_DATA_STTS_CD, BUS_OBJCT_DATA_STTS_DS, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID, pre_rgstn_stts_fl) values ('EXPIRED', 'EXPIRED', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM', 'N');
insert into BUS_OBJCT_DATA_STTS_CD_LK (BUS_OBJCT_DATA_STTS_CD, BUS_OBJCT_DATA_STTS_DS, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID, pre_rgstn_stts_fl) values ('INVALID', 'INVALID', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM', 'N');
insert into BUS_OBJCT_DATA_STTS_CD_LK (BUS_OBJCT_DATA_STTS_CD, BUS_OBJCT_DATA_STTS_DS, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID, pre_rgstn_stts_fl) values ('ARCHIVED', 'ARCHIVED', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM', 'N');
insert into BUS_OBJCT_DATA_STTS_CD_LK (BUS_OBJCT_DATA_STTS_CD, BUS_OBJCT_DATA_STTS_DS, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID, pre_rgstn_stts_fl) values ('VALID', 'VALID', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM', 'N');
insert into BUS_OBJCT_DATA_STTS_CD_LK (BUS_OBJCT_DATA_STTS_CD, BUS_OBJCT_DATA_STTS_DS, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID, pre_rgstn_stts_fl) values ('UPLOADING', 'UPLOADING', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM', 'Y');
insert into BUS_OBJCT_DATA_STTS_CD_LK (BUS_OBJCT_DATA_STTS_CD, BUS_OBJCT_DATA_STTS_DS, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID, pre_rgstn_stts_fl) values ('DELETED', 'DELETED', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM', 'N');

-- Storage Unit Statuses.
insert into STRGE_UNIT_STTS_CD_LK (STRGE_UNIT_STTS_CD, STRGE_UNIT_STTS_DS, AVLBL_FL, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('ENABLED', 'ENABLED', 'Y', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_UNIT_STTS_CD_LK (STRGE_UNIT_STTS_CD, STRGE_UNIT_STTS_DS, AVLBL_FL, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('DISABLED', 'DISABLED', 'N', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_UNIT_STTS_CD_LK (STRGE_UNIT_STTS_CD, STRGE_UNIT_STTS_DS, AVLBL_FL, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('ARCHIVING', 'ARCHIVING', 'N', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_UNIT_STTS_CD_LK (STRGE_UNIT_STTS_CD, STRGE_UNIT_STTS_DS, AVLBL_FL, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('ARCHIVED', 'ARCHIVED', 'N', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_UNIT_STTS_CD_LK (STRGE_UNIT_STTS_CD, STRGE_UNIT_STTS_DS, AVLBL_FL, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('RESTORING', 'RESTORING', 'N', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_UNIT_STTS_CD_LK (STRGE_UNIT_STTS_CD, STRGE_UNIT_STTS_DS, AVLBL_FL, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('RESTORED', 'RESTORED', 'Y', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_UNIT_STTS_CD_LK (STRGE_UNIT_STTS_CD, STRGE_UNIT_STTS_DS, AVLBL_FL, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('EXPIRING', 'EXPIRING', 'N', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_UNIT_STTS_CD_LK (STRGE_UNIT_STTS_CD, STRGE_UNIT_STTS_DS, AVLBL_FL, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('DISABLING', 'DISABLING', 'N', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');

-- Storage Policy Statuses.
insert into STRGE_PLCY_STTS_CD_LK (STRGE_PLCY_STTS_CD, STRGE_PLCY_STTS_DS, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('ENABLED', 'Enabled', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into STRGE_PLCY_STTS_CD_LK (STRGE_PLCY_STTS_CD, STRGE_PLCY_STTS_DS, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('DISABLED', 'Disabled', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');

-- Security for JUnits (e.g. SecurityUserWrapperTest)
insert into SCRTY_FN_LK (SCRTY_FN_CD, CREAT_TS, CREAT_USER_ID, UPDT_TS, UPDT_USER_ID) values ('FN_BUSINESS_OBJECT_DEFINITIONS_POST', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');

-- Notification Registration Status
insert into ntfcn_rgstn_stts_cd_lk (ntfcn_rgstn_stts_cd, ntfcn_rgstn_stts_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) values ('ENABLED', 'Enabled', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into ntfcn_rgstn_stts_cd_lk (ntfcn_rgstn_stts_cd, ntfcn_rgstn_stts_ds, creat_ts, creat_user_id, updt_ts, updt_user_id) values ('DISABLED', 'Disabled', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');

-- Message Type
insert into msg_type_cd_lk (msg_type_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) values ('SQS', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');
insert into msg_type_cd_lk (msg_type_cd, creat_ts, creat_user_id, updt_ts, updt_user_id) values ('SNS', DATE '2015-01-01', 'SYSTEM', DATE '2015-01-01', 'SYSTEM');

-- Retention Type
insert into rtntn_type_cd_lk(rtntn_type_cd) values ('PARTITION_VALUE');
insert into rtntn_type_cd_lk(rtntn_type_cd) values ('BDATA_RETENTION_DATE');

-- Business object definition description suggestion
insert into bus_objct_dfntn_dscr_sgstn_stts_cd_lk(bus_objct_dfntn_dscr_sgstn_stts_cd) values ('PENDING');
insert into bus_objct_dfntn_dscr_sgstn_stts_cd_lk(bus_objct_dfntn_dscr_sgstn_stts_cd) values ('ACCEPTED');

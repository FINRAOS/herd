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

----------------------------
-- Configure parameters   --
----------------------------
SET @SpringDatabase='POSTGRESQL';
SET @HibernateDialect='org.hibernate.dialect.PostgreSQL9Dialect';
SET @QuartzDelegate='org.quartz.impl.jdbcjobstore.PostgreSQLDelegate';
SET @EnvName='';
SET @HttpProxyHost='';
SET @HttpProxyPort='';
SET @S3Endpoint='';
SET @S3ManagedBucket='';
SET @S3LoadingDockBucket='';
SET @S3ExternalBucket='';

-- Staging bucket is where the various herd scripts are stored
SET @S3StagingBucketAndKeyPrefix='bucket-name/key-prefix';

SET @IamUploaderArn='';
SET @IamDownloaderArn='';
SET @KmsLoadingDockArn='';
SET @KmsExternalArn='';
SET @OozieWorkflowLocation='HERD_SCRIPTS/emr/bootstrap/herd_oozie_wrapper/';
SET @OozieHdfsCopyScript='s3_hdfs_copy_script.sh';
SET @OozieSecurityGroup='';
SET @SqsIncomingQueue='';
SET @SqsOutgoingQueue='';

------------------------------------------------------
-- Configure Storage Attributes for managed buckets --
------------------------------------------------------
INSERT INTO strge_atrbt VALUES (nextval('strge_atrbt_seq'), 'S3_MANAGED', 'bucket.name', '@S3ManagedBucket', current_timestamp, 'SYSTEM');
INSERT INTO strge_atrbt VALUES (nextval('strge_atrbt_seq'), 'S3_MANAGED_LOADING_DOCK', 'bucket.name', '@S3LoadingDockBucket', current_timestamp, 'SYSTEM');
INSERT INTO strge_atrbt VALUES (nextval('strge_atrbt_seq'), 'S3_MANAGED_EXTERNAL', 'bucket.name', '@S3ExternalBucket', current_timestamp, 'SYSTEM');
INSERT INTO strge_atrbt VALUES (nextval('strge_atrbt_seq'), 'S3_MANAGED', 's3.endpoint', '@S3Endpoint', current_timestamp, 'SYSTEM');
INSERT INTO strge_atrbt VALUES (nextval('strge_atrbt_seq'), 'S3_MANAGED_LOADING_DOCK', 's3.endpoint', '@S3Endpoint', current_timestamp, 'SYSTEM');
INSERT INTO strge_atrbt VALUES (nextval('strge_atrbt_seq'), 'S3_MANAGED_EXTERNAL', 's3.endpoint', '@S3Endpoint', current_timestamp, 'SYSTEM');


-------------------------
-- Name of Environment --
-------------------------
INSERT INTO cnfgn VALUES ('herd.environment', '@EnvName', NULL);
INSERT INTO cnfgn VALUES ('s3.staging.resources.base', '@EnvName', NULL);

-------------------
-- Database Type --
-------------------
INSERT INTO cnfgn VALUES ('org.springframework.orm.jpa.vendor.Database', '@SpringDatabase', NULL);
INSERT INTO cnfgn VALUES ('hibernate.dialect', '@HibernateDialect', NULL);
INSERT INTO cnfgn VALUES ('org.quartz.jobStore.driverDelegateClass', '@QuartzDelegate', NULL);

-----------------------
-- Proxy Information --
-----------------------
INSERT INTO cnfgn VALUES ('http.proxy.hostname', '@HttpProxyHost', NULL);
INSERT INTO cnfgn VALUES ('http.proxy.port', '@HttpProxyPort', NULL);

--------------------------------
-- Large File System Security --
--------------------------------
INSERT INTO cnfgn VALUES ('aws.loading.dock.uploader.role.arn', '@IamUploaderArn', NULL);
INSERT INTO cnfgn VALUES ('aws.external.downloader.role.arn', '@IamDownloaderArn', NULL);
INSERT INTO cnfgn VALUES ('aws.kms.loading.dock.key.id', '@KmsLoadingDockArn', NULL);
INSERT INTO cnfgn VALUES ('aws.kms.external.key.id', '@KmsExternalArn', NULL);

------------------
-- Bucket Names --
------------------
INSERT INTO cnfgn VALUES ('s3.managed.bucket.name', '@S3ManagedBucket', NULL);
INSERT INTO cnfgn VALUES ('s3.staging.bucket.name', '@S3StagingBucketAndKeyPrefix', NULL);
INSERT INTO cnfgn VALUES ('s3.endpoint', '@S3Endpoint', NULL);

------------------------------
-- Oozie Scripts and Config --
------------------------------
INSERT INTO cnfgn VALUES ('emr.oozie.herd.wrapper.workflow.s3.location', '@OozieWorkflowLocation', NULL);
INSERT INTO cnfgn VALUES ('emr.s3.hdfs.copy.script', '@OozieHdfsCopyScript', NULL);
INSERT INTO cnfgn VALUES ('emr.herd.support.security.group', '@OozieSecurityGroup', NULL);

----------------------
-- Disable security --
----------------------
INSERT INTO cnfgn VALUES ('security.enabled.spel.expression', 'false', NULL);

---------------------------------
-- Notification Configurations --
---------------------------------
INSERT INTO cnfgn VALUES ('herd.notification.sqs.outgoing.queue.name', '@SqsOutgoingQueue', NULL);
INSERT INTO cnfgn VALUES ('herd.notification.sqs.incoming.queue.name', '@SqsIncomingQueue', NULL);
INSERT INTO cnfgn VALUES ('herd.notification.sqs.enabled', 'false', NULL);


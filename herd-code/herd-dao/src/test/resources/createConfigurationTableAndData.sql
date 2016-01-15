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



-- Create/Re-create the configuration table.
drop table CNFGN if exists cascade;
create table CNFGN (
	CNFGN_KEY_NM varchar(100) not null,
	CNFGN_VALUE_DS varchar(4000),
	CNFGN_VALUE_CL CLOB,
	primary key (CNFGN_KEY_NM)
);

-- Insert configuration values into table.

insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('s3.managed.bucket.name', 'TEST-S3-MANAGED-BUCKET');
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('org.springframework.orm.jpa.vendor.Database', 'H2');
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('hibernate.dialect', 'org.hibernate.dialect.H2Dialect');
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('herd.notification.sqs.incoming.queue.name', 'HERD_INCOMING_QUEUE');
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('herd.notification.sqs.outgoing.queue.name', 'HERD_OUTGOING_QUEUE');
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('herd.environment', 'TEST');
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('herd.notification.sqs.environment', 'Development');
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('herd.notification.sqs.enabled', 'true');

insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('herd.notification.sqs.sys.monitor.request.xpath.properties', 'incoming_message_correlation_id=/monitor/header/correlation-id
incoming_message_context_message_type=/monitor/payload/contextMessageTypeToPublish
');

insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('herd.notification.sqs.sys.monitor.response.velocity.template', '<?xml version="1.1" encoding="UTF-8"?>
<datamgt:monitor xmlns:datamgt="http://testDomain/system-monitor">
   <header>
      <producer>
         <name>testDomain/testApplication</name>
         <environment>$herd_notification_sqs_environment</environment>
      </producer>
      <creation>
         <datetime>$current_time</datetime>
      </creation>
#if($StringUtils.isNotEmpty($incoming_message_correlation_id))      <correlation-id>$incoming_message_correlation_id</correlation-id>
#end
      <context-message-type>$incoming_message_context_message_type</context-message-type>
      <system-message-type>NoError</system-message-type>
      <xsd>http://testDomain/system-monitor.xsd</xsd>
   </header>
   <payload>
      <contextMessageTypeToPublish />
   </payload>
</datamgt:monitor>');

insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('herd.notification.sqs.business.object.data.status.change.velocity.template', '<?xml version="1.1" encoding="UTF-8"?>
<datamgt:TestApplicationEvent xmlns:datamgt="http://testDomain/testApplication/testApplication-event">
   <header>
      <producer>
         <name>testDomain/testApplication</name>
         <environment>$herd_notification_sqs_environment</environment>
      </producer>
      <creation>
         <datetime>$current_time</datetime>
      </creation>
      <correlation-id>BusinessObjectData_$businessObjectDataId</correlation-id>
      <context-message-type>testDomain/testApplication/BusinessObjectDataStatusChanged</context-message-type>
      <system-message-type>NoError</system-message-type>
      <xsd>http://testDomain/testApplication/testApplication-event.xsd</xsd>
      <event-id>
         <system-name>testDomain/testApplication</system-name>
         <system-unique-id>$uuid</system-unique-id>
      </event-id>
   </header>
   <payload>
      <eventDate>$current_time</eventDate>
      <datamgtEvent>
         <businessObjectDataStatusChanged>
            <businessObjectDataKey>
               <namespace>$businessObjectDataKey.namespace</namespace>
               <businessObjectDefinitionName>$businessObjectDataKey.businessObjectDefinitionName</businessObjectDefinitionName>
               <businessObjectFormatUsage>$businessObjectDataKey.businessObjectFormatUsage</businessObjectFormatUsage>
               <businessObjectFormatFileType>$businessObjectDataKey.businessObjectFormatFileType</businessObjectFormatFileType>
               <businessObjectFormatVersion>$businessObjectDataKey.businessObjectFormatVersion</businessObjectFormatVersion>
               <partitionValue>$businessObjectDataKey.partitionValue</partitionValue>
#if($CollectionUtils.isNotEmpty($businessObjectDataKey.subPartitionValues))               <subPartitionValues>
#foreach ($subPartitionValue in $businessObjectDataKey.subPartitionValues)                  <partitionValue>$subPartitionValue</partitionValue>
#end
               </subPartitionValues>
#end
               <businessObjectDataVersion>$businessObjectDataKey.businessObjectDataVersion</businessObjectDataVersion>
            </businessObjectDataKey>
            <newBusinessObjectDataStatus>$newBusinessObjectDataStatus</newBusinessObjectDataStatus>
#if($StringUtils.isNotEmpty($oldBusinessObjectDataStatus))            <oldBusinessObjectDataStatus>$oldBusinessObjectDataStatus</oldBusinessObjectDataStatus>
#end
         </businessObjectDataStatusChanged>
      </datamgtEvent>
   </payload>
   <soa-audit>
      <triggered-date-time>$current_time</triggered-date-time>
      <triggered-by-username>$username</triggered-by-username>
      <transmission-id>$uuid</transmission-id>
   </soa-audit>
</datamgt:TestApplicationEvent>');

-- KooZRDgwRgdfsTP+60l+nQ== ("test")
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('emr.default.service.iam.role.name', 'KooZRDgwRgdfsTP+60l+nQ==');
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('emr.default.ec2.node.iam.profile.name', 'KooZRDgwRgdfsTP+60l+nQ==');
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('aws.external.downloader.role.arn', 'KooZRDgwRgdfsTP+60l+nQ==');
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('aws.loading.dock.uploader.role.arn', 'KooZRDgwRgdfsTP+60l+nQ==');

-- arn:aws:kms:loading-dock:test
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('aws.kms.loading.dock.key.id', 'MFrWh7WWTsEWDJugTZsrJRNK8XY8KqZ+01e/ERshVPk=');

-- arn:aws:kms:external:test
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('aws.kms.external.key.id', 'yM7SPkNPeS3pBvjzfenbzo5VR2HEmNeYfuUSJXnSixY=');

insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('org.quartz.jobStore.driverDelegateClass', 'org.quartz.impl.jdbcjobstore.StdJDBCDelegate');
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('emr.s3.hdfs.copy.script', 's3_hdfs_copy_script.sh');
insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('emr.oozie.herd.wrapper.workflow.s3.location', 'HERD_SCRIPTS/emr/bootstrap/herd_oozie_wrapper/');

insert into CNFGN (CNFGN_KEY_NM, CNFGN_VALUE_DS) values ('storage.policy.selector.job.sqs.queue.name', 'STORAGE_POLICY_SELECTOR_SQS_QUEUE_NAME');

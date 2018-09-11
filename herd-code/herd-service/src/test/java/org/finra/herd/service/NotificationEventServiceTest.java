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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.JobAction;
import org.finra.herd.model.api.xml.JobDefinition;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.impl.BusinessObjectDataNotificationJobActionServiceImpl;
import org.finra.herd.service.impl.StorageUnitStatusChangeNotificationJobActionServiceImpl;

/**
 * This class tests functionality within the notification event service.
 */
public class NotificationEventServiceTest extends AbstractServiceTest
{
    @Test
    public void testProcessBusinessObjectDataRegistrationNotificationEventSync() throws Exception
    {
        // Create job definition
        JobDefinition jobDefinition = jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH);

        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA));

        // Create a business object format with a schema.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns());

        // Create business object data with storage units.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3);
        storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a business object data notification registration entity.
        notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, null, jobActions, NotificationRegistrationStatusEntity.ENABLED);

        // Create and persist a business object data notification registration entity.
        notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME_2),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME_2, BDATA_STATUS, null, jobActions, NotificationRegistrationStatusEntity.ENABLED);

        // Trigger the notification
        List<Object> notificationActions = notificationEventService
            .processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN,
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), BDATA_STATUS, null);

        // Validate the result job.
        Job job = (Job) notificationActions.get(0);
        assertEquals(new Job(job.getId(), null, TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, null, null, null, null, Arrays
            .asList(new Parameter("notification_businessObjectDefinitionName", BDEF_NAME),
                new Parameter("notification_partitionValues", PARTITION_VALUE + "|" + StringUtils.join(SUBPARTITION_VALUES, "|")),
                new Parameter("notification_namespace", NAMESPACE), new Parameter("notification_businessObjectData",
                    jsonHelper.objectToJson(businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity))),
                new Parameter("notification_businessObjectFormatUsage", FORMAT_USAGE_CODE),
                new Parameter(HERD_WORKFLOW_ENVIRONMENT, configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT)),
                new Parameter("notification_businessObjectDefinitionNamespace", BDEF_NAMESPACE),
                new Parameter("notification_newBusinessObjectDataStatus", BDATA_STATUS), new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1),
                new Parameter("notification_businessObjectDataVersion", DATA_VERSION.toString()), new Parameter("notification_name", NOTIFICATION_NAME),
                new Parameter("notification_oldBusinessObjectDataStatus", null),
                new Parameter("notification_partitionColumnNames", "PRTN_CLMN001|PRTN_CLMN002|PRTN_CLMN003|PRTN_CLMN004|PRTN_CLMN005"),
                new Parameter("notification_businessObjectDataEventType", NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name()),
                new Parameter("notification_businessObjectFormatVersion", FORMAT_VERSION.toString()),
                new Parameter("notification_businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE),
                new Parameter("notification_correlationData", CORRELATION_DATA)), null, null, null), job);
    }

    @Test
    public void testProcessBusinessObjectDataRegistrationNotificationEventSyncAssertFireEnabledOnly() throws Exception
    {
        // Create job definition
        JobDefinition jobDefinition = jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH);

        List<JobAction> jobActions1 = Arrays.asList(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA));

        List<JobAction> jobActions2 = Arrays.asList(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA_2));

        // Create a business object format with a schema.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns());

        // Create business object data with storage units.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3);
        storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a business object data notification registration entity that is enabled.
        notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, null, jobActions1, NotificationRegistrationStatusEntity.ENABLED);

        // Create and persist a business object data notification registration entity that is disabled.
        notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME_2),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME_2, BDATA_STATUS, null, jobActions2, NotificationRegistrationStatusEntity.DISABLED);

        // Trigger the notification
        List<Object> notificationActions = notificationEventService
            .processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN,
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), BDATA_STATUS, null);

        assertEquals(1, notificationActions.size());
        assertEquals(Job.class, notificationActions.get(0).getClass());
        Job job = (Job) notificationActions.get(0);
        List<Parameter> parameters = job.getParameters();
        boolean found = false;
        for (Parameter parameter : parameters)
        {
            String name = parameter.getName();
            if ("notification_correlationData".equals(name))
            {
                assertEquals(CORRELATION_DATA, parameter.getValue());
                found = true;
                break;
            }
        }
        assertTrue(found);
    }

    @Test
    public void testProcessBusinessObjectDataRegistrationNotificationEventSyncAssertNoopWhenAllDisabled() throws Exception
    {
        // Create job definition
        JobDefinition jobDefinition = jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH);

        List<JobAction> jobActions1 = Arrays.asList(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA));

        List<JobAction> jobActions2 = Arrays.asList(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA_2));

        // Create a business object format with a schema.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns());

        // Create business object data with storage units.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3);
        storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a business object data notification registration entity that is enabled.
        notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, null, jobActions1, NotificationRegistrationStatusEntity.DISABLED);

        // Create and persist a business object data notification registration entity that is disabled.
        notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME_2),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME_2, BDATA_STATUS, null, jobActions2, NotificationRegistrationStatusEntity.DISABLED);

        // Trigger the notification
        List<Object> notificationActions = notificationEventService
            .processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN,
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), BDATA_STATUS, null);

        assertEquals(0, notificationActions.size());
    }

    @Test
    public void testProcessBusinessObjectDataRegistrationNotificationEventSyncMissingOptional() throws Exception
    {
        // Create job definition
        JobDefinition jobDefinition = jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH);

        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA));

        // Create business object data without storage units.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Create and persist a business object data notification registration entity.
        notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(), BDEF_NAMESPACE, BDEF_NAME, null, null, null, null, null, null,
                jobActions, NotificationRegistrationStatusEntity.ENABLED);

        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Trigger the notification
        List<Object> notificationActions = notificationEventService
            .processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN, businessObjectDataKey,
                BDATA_STATUS, null);

        Job job = (Job) notificationActions.get(0);
        assertNotNull(job);
        assertEquals(TEST_ACTIVITI_NAMESPACE_CD, job.getNamespace());
        assertEquals(TEST_ACTIVITI_JOB_NAME, job.getJobName());

        Parameter parameter = null;
        for (Parameter param : job.getParameters())
        {
            if (param.getName().equals("notification_correlationData"))
            {
                parameter = param;
                break;
            }
        }
        assertNotNull(parameter);
        assertEquals(CORRELATION_DATA, parameter.getValue());
    }

    @Test
    public void testProcessBusinessObjectDataRegistrationNotificationEventSyncMultipleNotifications() throws Exception
    {
        // Create job definition
        JobDefinition jobDefinition = jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH);

        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA));

        // Create business object data with storage units.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3);
        storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a business object data notification registration entity.
        notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, null, jobActions, NotificationRegistrationStatusEntity.ENABLED);

        notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME_2),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, null, jobActions, NotificationRegistrationStatusEntity.ENABLED);

        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Trigger the notification
        List<Object> notificationActions = notificationEventService
            .processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN, businessObjectDataKey,
                BDATA_STATUS, null);

        assertTrue(notificationActions.size() == 2);

        for (Object notificationAction : notificationActions)
        {
            Job job = (Job) notificationAction;
            assertNotNull(job);
            assertEquals(TEST_ACTIVITI_NAMESPACE_CD, job.getNamespace());
            assertEquals(TEST_ACTIVITI_JOB_NAME, job.getJobName());

            Parameter parameter = null;
            for (Parameter param : job.getParameters())
            {
                if (param.getName().equals("notification_correlationData"))
                {
                    parameter = param;
                    break;
                }
            }
            assertNotNull(parameter);
            assertEquals(CORRELATION_DATA, parameter.getValue());
        }
    }

    @Test
    public void testProcessBusinessObjectDataRegistrationNotificationEventSyncNoNotification() throws Exception
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity required for testing.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Trigger the notification
        List<Object> notificationActions = notificationEventService
            .processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN, businessObjectDataKey,
                BDATA_STATUS, null);

        assertTrue(CollectionUtils.isEmpty(notificationActions));
    }

    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEventSync() throws Exception
    {
        runProcessBusinessObjectDataStatusChangeNotificationEventSyncTest();
    }

    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEventSyncWithInfoLoggingEnabled() throws Exception
    {
        // Get the logger and the current logger level.
        LogLevel origLogLevel = getLogLevel(BusinessObjectDataNotificationJobActionServiceImpl.class);

        // Set logging level to INFO.
        setLogLevel(BusinessObjectDataNotificationJobActionServiceImpl.class, LogLevel.INFO);

        // Run the test and reset the logging level back to the original value.
        try
        {
            runProcessBusinessObjectDataStatusChangeNotificationEventSyncTest();
        }
        finally
        {
            setLogLevel(BusinessObjectDataNotificationJobActionServiceImpl.class, origLogLevel);
        }
    }

    @Test
    public void testProcessStorageUnitStatusChangeNotificationEventSync() throws Exception
    {
        runProcessStorageUnitStatusChangeNotificationEventSyncTest();
    }

    @Test
    public void testProcessStorageUnitStatusChangeNotificationEventSyncWithInfoLoggingEnabled() throws Exception
    {
        // Get the logger and the current logger level.
        LogLevel origLogLevel = getLogLevel(StorageUnitStatusChangeNotificationJobActionServiceImpl.class);

        // Set logging level to INFO.
        setLogLevel(StorageUnitStatusChangeNotificationJobActionServiceImpl.class, LogLevel.INFO);

        // Run the test and reset the logging level back to the original value.
        try
        {
            runProcessStorageUnitStatusChangeNotificationEventSyncTest();
        }
        finally
        {
            setLogLevel(StorageUnitStatusChangeNotificationJobActionServiceImpl.class, origLogLevel);
        }
    }

    private void runProcessBusinessObjectDataStatusChangeNotificationEventSyncTest() throws Exception
    {
        // Create a job definition.
        JobDefinition jobDefinition = jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH);

        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA));

        // Create a business object format with a schema.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns());

        // Create business object data with a storage unit.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.UPLOADING);
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3);
        storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a business object data notification registration entity.
        notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME, BusinessObjectDataStatusEntity.VALID, BusinessObjectDataStatusEntity.UPLOADING, jobActions,
                NotificationRegistrationStatusEntity.ENABLED);

        // Trigger the notification.
        List<Object> notificationActions = notificationEventService
            .processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG,
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), BusinessObjectDataStatusEntity.VALID, BusinessObjectDataStatusEntity.UPLOADING);

        // Validate the result job.
        Job job = (Job) notificationActions.get(0);
        assertEquals(new Job(job.getId(), null, TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, null, null, null, null, Arrays
            .asList(new Parameter("notification_businessObjectDefinitionName", BDEF_NAME),
                new Parameter("notification_partitionValues", PARTITION_VALUE + "|" + StringUtils.join(SUBPARTITION_VALUES, "|")),
                new Parameter("notification_namespace", NAMESPACE), new Parameter("notification_businessObjectData",
                    jsonHelper.objectToJson(businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity))),
                new Parameter("notification_businessObjectFormatUsage", FORMAT_USAGE_CODE),
                new Parameter(HERD_WORKFLOW_ENVIRONMENT, configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT)),
                new Parameter("notification_businessObjectDefinitionNamespace", BDEF_NAMESPACE),
                new Parameter("notification_newBusinessObjectDataStatus", BusinessObjectDataStatusEntity.VALID),
                new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1), new Parameter("notification_businessObjectDataVersion", DATA_VERSION.toString()),
                new Parameter("notification_name", NOTIFICATION_NAME),
                new Parameter("notification_oldBusinessObjectDataStatus", BusinessObjectDataStatusEntity.UPLOADING),
                new Parameter("notification_partitionColumnNames", "PRTN_CLMN001|PRTN_CLMN002|PRTN_CLMN003|PRTN_CLMN004|PRTN_CLMN005"),
                new Parameter("notification_businessObjectDataEventType", NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG.name()),
                new Parameter("notification_businessObjectFormatVersion", FORMAT_VERSION.toString()),
                new Parameter("notification_businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE),
                new Parameter("notification_correlationData", CORRELATION_DATA)), null, null, null), job);
    }

    private void runProcessStorageUnitStatusChangeNotificationEventSyncTest() throws Exception
    {
        // Create a job definition.
        JobDefinition jobDefinition = jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH);

        // Create a job action.
        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA));

        // Create a business object format with a schema.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns());

        // Create a business object data with a storage unit having a DISABLED status.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.VALID);
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3);
        storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.DISABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, StorageUnitStatusEntity.ENABLED, StorageUnitStatusEntity.DISABLED, jobActions,
            NotificationRegistrationStatusEntity.ENABLED);

        // Trigger the notification.
        List<Object> notificationActions = notificationEventService
            .processStorageUnitNotificationEventSync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG,
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), STORAGE_NAME, StorageUnitStatusEntity.ENABLED, StorageUnitStatusEntity.DISABLED);

        // Validate the result job.
        Job job = (Job) notificationActions.get(0);
        assertEquals(new Job(job.getId(), null, TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, null, null, null, null, Arrays
            .asList(new Parameter("notification_businessObjectDefinitionName", BDEF_NAME),
                new Parameter("notification_partitionValues", PARTITION_VALUE + "|" + StringUtils.join(SUBPARTITION_VALUES, "|")),
                new Parameter("notification_namespace", NAMESPACE), new Parameter("notification_businessObjectData",
                    jsonHelper.objectToJson(businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity))),
                new Parameter("notification_businessObjectFormatUsage", FORMAT_USAGE_CODE),
                new Parameter(HERD_WORKFLOW_ENVIRONMENT, configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT)),
                new Parameter("notification_businessObjectDefinitionNamespace", BDEF_NAMESPACE), new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1),
                new Parameter("notification_storageUnitEventType", NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name()),
                new Parameter("notification_oldStorageUnitStatus", StorageUnitStatusEntity.DISABLED),
                new Parameter("notification_businessObjectDataVersion", DATA_VERSION.toString()), new Parameter("notification_name", NOTIFICATION_NAME),
                new Parameter("notification_partitionColumnNames", "PRTN_CLMN001|PRTN_CLMN002|PRTN_CLMN003|PRTN_CLMN004|PRTN_CLMN005"),
                new Parameter("notification_businessObjectFormatVersion", FORMAT_VERSION.toString()),
                new Parameter("notification_businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE), new Parameter("notification_storageName", STORAGE_NAME),
                new Parameter("notification_newStorageUnitStatus", StorageUnitStatusEntity.ENABLED),
                new Parameter("notification_correlationData", CORRELATION_DATA)), null, null, null), job);
    }
}

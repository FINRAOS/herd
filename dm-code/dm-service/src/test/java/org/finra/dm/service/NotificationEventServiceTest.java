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
package org.finra.dm.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.util.CollectionUtils;

import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.NotificationEventTypeEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.jpa.StoragePlatformEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.Job;
import org.finra.dm.model.api.xml.JobAction;
import org.finra.dm.model.api.xml.JobDefinition;
import org.finra.dm.model.api.xml.Parameter;

/**
 * This class tests functionality within the notification event service.
 */
public class NotificationEventServiceTest extends AbstractServiceTest
{
    @Test
    public void testProcessBusinessObjectDataNotificationEventSync() throws Exception
    {
        // Create job definition
        JobDefinition jobDefinition = createJobDefinition(ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH);

        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA));

        // Create business object data with storage units.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
        StorageEntity storageEntity = createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3);
        createStorageUnitEntity(storageEntity, businessObjectDataEntity);

        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME,
            NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN.name(), BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            STORAGE_NAME, jobActions);

        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME_2,
            NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN.name(), BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            STORAGE_NAME_2, jobActions);

        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Trigger the notification
        List<Object> notificationActions = notificationEventService
            .processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN, businessObjectDataKey);

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
    public void testProcessBusinessObjectDataNotificationEventSyncMissingOptional() throws Exception
    {
        // Create job definition
        JobDefinition jobDefinition = createJobDefinition(ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH);

        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA));

        // Create business object data without storage units.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME,
            NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN.name(), BOD_NAME, null, null, null, null, jobActions);

        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Trigger the notification
        List<Object> notificationActions = notificationEventService
            .processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN, businessObjectDataKey);

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
    public void testProcessBusinessObjectDataNotificationEventSyncNoNotification() throws Exception
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Trigger the notification
        List<Object> notificationActions = notificationEventService
            .processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN, businessObjectDataKey);

        assertTrue(CollectionUtils.isEmpty(notificationActions));
    }

    @Test
    public void testProcessBusinessObjectDataNotificationEventSyncMultipleNotifications() throws Exception
    {
        // Create job definition
        JobDefinition jobDefinition = createJobDefinition(ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH);

        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA));

        // Create business object data with storage units.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
        StorageEntity storageEntity = createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3);
        createStorageUnitEntity(storageEntity, businessObjectDataEntity);

        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME,
            NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN.name(), BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            STORAGE_NAME, jobActions);

        createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME_2,
            NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN.name(), BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            STORAGE_NAME, jobActions);

        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Trigger the notification
        List<Object> notificationActions = notificationEventService
            .processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN, businessObjectDataKey);

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
}

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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.JobAction;
import org.finra.herd.model.api.xml.JobDefinition;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.dto.BusinessObjectDataNotificationEventParamsDto;
import org.finra.herd.model.dto.StorageUnitNotificationEventParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationJobActionEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.model.jpa.NotificationTypeEntity;

public class BusinessObjectDataNotificationJobActionServiceTest extends AbstractServiceTest
{
    @Test
    public void testGetIdentifyingInformation() throws Exception
    {
        // Create a job definition.
        JobDefinition jobDefinition = jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH);

        // Create a job action.
        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA));

        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME, null, null, jobActions, NotificationRegistrationStatusEntity.ENABLED);

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create and initiate an instance of the business object data notification event parameters DTO.
        BusinessObjectDataNotificationEventParamsDto businessObjectDataNotificationEventParams = new BusinessObjectDataNotificationEventParamsDto();
        businessObjectDataNotificationEventParams.setBusinessObjectData(businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity));
        businessObjectDataNotificationEventParams.setBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity);
        businessObjectDataNotificationEventParams.setEventType(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name());
        businessObjectDataNotificationEventParams
            .setNotificationJobAction((NotificationJobActionEntity) businessObjectDataNotificationRegistrationEntity.getNotificationActions().toArray()[0]);
        businessObjectDataNotificationEventParams.setStorageName(STORAGE_NAME);

        // Get the notification action service for the business object data registration event.
        NotificationActionService notificationActionService = notificationActionFactory
            .getNotificationActionHandler(NotificationTypeEntity.NOTIFICATION_TYPE_BDATA,
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name());

        // Validate the identifying information for the notification event.
        String expectedValue =
            String.format("namespace: \"%s\", actionId: \"%s\" with " + businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey) +
                ", storageName: \"%s\"", businessObjectDataNotificationRegistrationEntity.getNamespace().getCode(),
                ((NotificationJobActionEntity) businessObjectDataNotificationRegistrationEntity.getNotificationActions().toArray()[0]).getId(), STORAGE_NAME);
        assertEquals(expectedValue, notificationActionService.getIdentifyingInformation(businessObjectDataNotificationEventParams, businessObjectDataHelper));
    }

    @Test
    public void testGetIdentifyingInformationInvalidParamsDto()
    {
        // Get the notification action service for the business object data registration event.
        NotificationActionService notificationActionService = notificationActionFactory
            .getNotificationActionHandler(NotificationTypeEntity.NOTIFICATION_TYPE_BDATA,
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name());

        // Try to retrieve the identifying information for the notification event using an invalid type of the parameters DTO.
        try
        {
            notificationActionService.getIdentifyingInformation(new StorageUnitNotificationEventParamsDto(), businessObjectDataHelper);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals("Notification event parameters DTO passed to the method must be an instance of BusinessObjectDataNotificationEventParamsDto.",
                e.getMessage());
        }
    }

    @Test
    public void testPerformNotificationActionInvalidParamsDto() throws Exception
    {
        // Get the notification action service for the business object data registration event.
        NotificationActionService notificationActionService = notificationActionFactory
            .getNotificationActionHandler(NotificationTypeEntity.NOTIFICATION_TYPE_BDATA,
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name());

        // Try to perform a notification action for the business object data registration event with an invalid type of the parameters DTO.
        try
        {
            notificationActionService.performNotificationAction(new StorageUnitNotificationEventParamsDto());
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals("Notification event parameters DTO passed to the method must be an instance of BusinessObjectDataNotificationEventParamsDto.",
                e.getMessage());
        }
    }
}

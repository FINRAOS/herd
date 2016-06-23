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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.JobAction;
import org.finra.herd.model.api.xml.JobDefinition;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.dto.BusinessObjectDataNotificationEventParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationJobActionEntity;
import org.finra.herd.model.jpa.NotificationTypeEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.NotificationActionService;

/**
 * Tests the business object data notification handler class.
 */
public class BusinessObjectDataNotificationJobActionHandlerTest extends AbstractServiceTest
{
    @Autowired
    private NotificationActionFactory notificationActionFactory;

    @Test
    public void testGetIdentifyingInformation() throws Exception
    {
        // Create job definition
        JobDefinition jobDefinition = createJobDefinition(ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH);

        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(jobDefinition.getNamespace(), jobDefinition.getJobName(), CORRELATION_DATA));

        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME, null, null, jobActions);

        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        BusinessObjectDataNotificationEventParamsDto businessObjectDataNotificationEventParams = new BusinessObjectDataNotificationEventParamsDto();

        businessObjectDataNotificationEventParams.setBusinessObjectData(businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity));
        businessObjectDataNotificationEventParams.setBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity);
        businessObjectDataNotificationEventParams.setEventType(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name());
        businessObjectDataNotificationEventParams
            .setNotificationJobAction((NotificationJobActionEntity) businessObjectDataNotificationRegistrationEntity.getNotificationActions().toArray()[0]);
        businessObjectDataNotificationEventParams.setStorageName(STORAGE_NAME);

        String expectedValue =
            String.format("namespace: \"%s\", actionId: \"%s\" with " + businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey) +
                ", storageName: \"%s\"", businessObjectDataNotificationRegistrationEntity.getNamespace().getCode(),
                ((NotificationJobActionEntity) businessObjectDataNotificationRegistrationEntity.getNotificationActions().toArray()[0]).getId(), STORAGE_NAME);

        NotificationActionService notificationActionService = notificationActionFactory
            .getNotificationActionHandler(NotificationTypeEntity.NOTIFICATION_TYPE_BDATA,
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name());
        assertEquals(expectedValue, notificationActionService.getIdentifyingInformation(businessObjectDataNotificationEventParams, businessObjectDataHelper));
    }
}
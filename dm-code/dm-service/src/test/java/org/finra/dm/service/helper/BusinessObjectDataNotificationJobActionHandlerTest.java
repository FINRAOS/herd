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
package org.finra.dm.service.helper;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.dm.model.dto.BusinessObjectDataNotificationEventParamsDto;
import org.finra.dm.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.dm.model.jpa.NotificationEventTypeEntity;
import org.finra.dm.model.jpa.NotificationJobActionEntity;
import org.finra.dm.model.jpa.NotificationTypeEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.JobAction;
import org.finra.dm.model.api.xml.JobDefinition;
import org.finra.dm.service.AbstractServiceTest;
import org.finra.dm.service.NotificationActionService;

/**
 * Tests the business object data notification handler class.
 */
public class BusinessObjectDataNotificationJobActionHandlerTest extends AbstractServiceTest
{
    @Autowired
    DmHelper dmHelper;

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
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME,
                NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN.name(), BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                STORAGE_NAME, jobActions);

        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        BusinessObjectDataNotificationEventParamsDto businessObjectDataNotificationEventParams = new BusinessObjectDataNotificationEventParamsDto();

        businessObjectDataNotificationEventParams.setBusinessObjectDataKey(businessObjectDataKey);
        businessObjectDataNotificationEventParams.setBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity);
        businessObjectDataNotificationEventParams.setEventType(NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN.name());
        businessObjectDataNotificationEventParams
            .setNotificationJobAction((NotificationJobActionEntity) businessObjectDataNotificationRegistrationEntity.getNotificationActions().toArray()[0]);
        businessObjectDataNotificationEventParams.setStorageName(STORAGE_NAME);

        String expectedValue = String
            .format("namespace: \"%s\", actionId: \"%s\" with " + dmHelper.businessObjectDataKeyToString(businessObjectDataKey) + ", storageName: \"%s\"",
                businessObjectDataNotificationRegistrationEntity.getNamespace().getCode(),
                ((NotificationJobActionEntity) businessObjectDataNotificationRegistrationEntity.getNotificationActions().toArray()[0]).getId(), STORAGE_NAME);

        NotificationActionService notificationActionService = notificationActionFactory
            .getNotificationActionHandler(NotificationTypeEntity.NOTIFICATION_TYPE_BDATA,
                NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN.name());
        assertEquals(expectedValue, notificationActionService.getIdentifyingInformation(businessObjectDataNotificationEventParams, dmHelper));
    }
}
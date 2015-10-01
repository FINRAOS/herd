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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import org.finra.dm.dao.helper.DmDaoSecurityHelper;
import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.jpa.JmsMessageEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.service.impl.SqsNotificationEventServiceImpl;

/**
 * This class tests functionality within the SQS notification event service.
 */
public class SqsNotificationEventServiceTest extends AbstractServiceTest
{
    @Test
    public void testSqsBusinessObjectDataStatusChangeNotificationEvent() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = createTestValidBusinessObjectData();
        BusinessObjectDataKey businessObjectDataKey = dmDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Trigger the notification
        JmsMessageEntity jmsMessageEntity = sqsNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID);

        // Validate the message.
        validateBusinessObjectDataStatusChangeMessage(jmsMessageEntity.getMessageText(), businessObjectDataKey, businessObjectDataEntity.getId(),
            DmDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID, BusinessObjectDataStatusEntity.INVALID);
    }

    @Test
    public void testSqsBusinessObjectDataStatusChangeNotificationEventNoOldStatus() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = createTestValidBusinessObjectData();
        BusinessObjectDataKey businessObjectDataKey = dmDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Trigger the notification
        JmsMessageEntity jmsMessageEntity = sqsNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID, null);

        // Validate the message.
        validateBusinessObjectDataStatusChangeMessage(jmsMessageEntity.getMessageText(), businessObjectDataKey, businessObjectDataEntity.getId(),
            DmDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID, null);
    }

    @Test
    public void testSqsBusinessObjectDataStatusChangeNotificationEventDmSqsNotificationNotEnabled() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = createTestValidBusinessObjectData();
        BusinessObjectDataKey businessObjectDataKey = dmDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.DM_NOTIFICATION_SQS_ENABLED.getKey(), false);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Trigger the notification
            assertNull(sqsNotificationEventService
                .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                    BusinessObjectDataStatusEntity.UPLOADING));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testSqsBusinessObjectDataStatusChangeNotificationEventSqsQueueNotDefined() throws Exception
    {
        Logger.getLogger(SqsNotificationEventServiceImpl.class).setLevel(Level.OFF);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = createTestValidBusinessObjectData();
        BusinessObjectDataKey businessObjectDataKey = dmDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.DM_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME.getKey(), null);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Trigger the notification
            sqsNotificationEventService.processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.UPLOADING);
            fail("Suppose to throw IllegalStateException.");
        }
        catch (IllegalStateException ex)
        {
            assertEquals(String.format("SQS queue name not found. Ensure the \"%s\" configuration entry is configured.",
                ConfigurationValue.DM_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME.getKey()), ex.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testProcessSystemMonitorNotificationEvent() throws Exception
    {
        // Trigger the notification
        JmsMessageEntity jmsMessageEntity = sqsNotificationEventService.processSystemMonitorNotificationEvent(getTestSystemMonitorIncomingMessage());

        // Validate message.
        assertNotNull(jmsMessageEntity);
        assertNotNull(jmsMessageEntity.getMessageText());

        validateSystemMonitorResponse(jmsMessageEntity.getMessageText());
    }

    @Test
    public void testProcessSystemMonitorNotificationEventDmSqsNotificationNotEnabled() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.DM_NOTIFICATION_SQS_ENABLED.getKey(), false);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Trigger the notification
            assertNull(sqsNotificationEventService.processSystemMonitorNotificationEvent(getTestSystemMonitorIncomingMessage()));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testProcessSystemMonitorNotificationEventSqsQueueNotDefined() throws Exception
    {
        Logger.getLogger(SqsNotificationEventServiceImpl.class).setLevel(Level.OFF);

        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.DM_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME.getKey(), null);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Trigger the notification
            sqsNotificationEventService.processSystemMonitorNotificationEvent(getTestSystemMonitorIncomingMessage());
            fail("Suppose to throw IllegalStateException.");
        }
        catch (IllegalStateException ex)
        {
            assertEquals(String.format("SQS queue name not found. Ensure the \"%s\" configuration entry is configured.",
                ConfigurationValue.DM_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME.getKey()), ex.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testProcessSystemMonitorNotificationEventNoMessageReturned() throws Exception
    {
        // Override the configuration to remove the velocity template for building the system monitor response.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.DM_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(), null);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Trigger the notification which should return null since no velocity template is configured.
            // A warning message should also be logged.
            assertNull(sqsNotificationEventService.processSystemMonitorNotificationEvent(getTestSystemMonitorIncomingMessage()));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }
}

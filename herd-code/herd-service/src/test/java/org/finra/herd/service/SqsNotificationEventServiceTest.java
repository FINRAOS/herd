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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.JmsMessage;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.service.impl.SqsNotificationEventServiceImpl;

/**
 * This class tests functionality within the SQS notification event service.
 */
public class SqsNotificationEventServiceTest extends AbstractServiceTest
{
    @Test
    public void testSqsBusinessObjectDataStatusChangeNotificationEvent() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createTestValidBusinessObjectData(SUBPARTITION_VALUES, businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectDefinitionServiceTestHelper.getNewAttributes());
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Trigger the notification
        JmsMessage jmsMessage = sqsNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID);

        // Validate the message.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessage(jmsMessage.getMessageText(), businessObjectDataKey, businessObjectDataEntity.getId(),
                HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID, BusinessObjectDataStatusEntity.INVALID,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3)));
    }

    @Test
    public void testSqsBusinessObjectDataStatusChangeNotificationEventNoOldStatus() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper.createTestValidBusinessObjectData();
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Trigger the notification
        JmsMessage jmsMessage = sqsNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID, null);

        // Validate the message.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessage(jmsMessage.getMessageText(), businessObjectDataKey, businessObjectDataEntity.getId(),
                HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID, null, NO_ATTRIBUTES);
    }

    @Test
    public void testSqsBusinessObjectDataStatusChangeNotificationEventNoBusinessObjectDataAttributeDefinitions() throws Exception
    {
        // Create a business object data entity with attributes, but without any attribute definitions.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, businessObjectDefinitionServiceTestHelper.getNewAttributes());
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Trigger the notification
        JmsMessage jmsMessage = sqsNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID);

        // Validate the message.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessage(jmsMessage.getMessageText(), businessObjectDataKey, businessObjectDataEntity.getId(),
                HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID, BusinessObjectDataStatusEntity.INVALID, NO_ATTRIBUTES);
    }

    @Test
    public void testSqsBusinessObjectDataStatusChangeNotificationEventNoBusinessObjectDataAttributes() throws Exception
    {
        // Create a business object data entity with attribute definitions, but without any attributes.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createTestValidBusinessObjectData(SUBPARTITION_VALUES, businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), NO_ATTRIBUTES);
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Trigger the notification
        JmsMessage jmsMessage = sqsNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID);

        // Validate the message.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessage(jmsMessage.getMessageText(), businessObjectDataKey, businessObjectDataEntity.getId(),
                HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID, BusinessObjectDataStatusEntity.INVALID, NO_ATTRIBUTES);
    }

    @Test
    public void testSqsBusinessObjectDataStatusChangeNotificationEventAttributeDefinitionCaseInsensitivity() throws Exception
    {
        // Create a business object data entity with a publishable attributes that have relative attribute definitions in upper and lower cases.
        List<AttributeDefinition> testAttributeDefinitions = Arrays
            .asList(new AttributeDefinition(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), PUBLISH_ATTRIBUTE),
                new AttributeDefinition(ATTRIBUTE_NAME_2_MIXED_CASE.toLowerCase(), PUBLISH_ATTRIBUTE));
        List<Attribute> testAttributes = Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_NAME_1_MIXED_CASE),
            new Attribute(ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_NAME_2_MIXED_CASE));
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, testAttributeDefinitions, testAttributes);
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Trigger the notification
        JmsMessage jmsMessage = sqsNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID);

        // Validate the message.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessage(jmsMessage.getMessageText(), businessObjectDataKey, businessObjectDataEntity.getId(),
                HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID, BusinessObjectDataStatusEntity.INVALID, testAttributes);
    }

    @Test
    public void testSqsBusinessObjectDataStatusChangeNotificationEventBusinessObjectDataAttributeNullValue() throws Exception
    {
        // Create a business object data entity with a publishable attributes that has a null value.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createTestValidBusinessObjectData(SUBPARTITION_VALUES, Arrays.asList(new AttributeDefinition(ATTRIBUTE_NAME_1_MIXED_CASE, PUBLISH_ATTRIBUTE)),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, null)));
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Trigger the notification
        JmsMessage jmsMessage = sqsNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID);

        // Validate the message.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessage(jmsMessage.getMessageText(), businessObjectDataKey, businessObjectDataEntity.getId(),
                HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID, BusinessObjectDataStatusEntity.INVALID,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, null)));
    }

    @Test
    public void testSqsBusinessObjectDataStatusChangeNotificationEventBusinessObjectDataAttributeSpecialValues() throws Exception
    {
        // Create a business object data entity with a publishable attributes that have special values such as a blank text and an empty string.
        List<Attribute> testAttributes =
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT), new Attribute(ATTRIBUTE_NAME_2_MIXED_CASE, EMPTY_STRING));
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, Arrays
            .asList(new AttributeDefinition(ATTRIBUTE_NAME_1_MIXED_CASE, PUBLISH_ATTRIBUTE),
                new AttributeDefinition(ATTRIBUTE_NAME_2_MIXED_CASE, PUBLISH_ATTRIBUTE)), testAttributes);
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Trigger the notification
        JmsMessage jmsMessage = sqsNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID);

        // Validate the message.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessage(jmsMessage.getMessageText(), businessObjectDataKey, businessObjectDataEntity.getId(),
                HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID, BusinessObjectDataStatusEntity.INVALID, testAttributes);
    }

    @Test
    public void testSqsBusinessObjectDataStatusChangeNotificationEventHerdSqsNotificationNotEnabled() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper.createTestValidBusinessObjectData();
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_ENABLED.getKey(), false);
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
        setLogLevel(SqsNotificationEventServiceImpl.class, LogLevel.OFF);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper.createTestValidBusinessObjectData();
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME.getKey(), null);
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
                ConfigurationValue.HERD_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME.getKey()), ex.getMessage());
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
        JmsMessage jmsMessage = sqsNotificationEventService.processSystemMonitorNotificationEvent(getTestSystemMonitorIncomingMessage());

        // Validate message.
        assertNotNull(jmsMessage);
        assertNotNull(jmsMessage.getMessageText());

        validateSystemMonitorResponse(jmsMessage.getMessageText());
    }

    @Test
    public void testProcessSystemMonitorNotificationEventHerdSqsNotificationNotEnabled() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_ENABLED.getKey(), false);
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
        setLogLevel(SqsNotificationEventServiceImpl.class, LogLevel.OFF);

        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME.getKey(), null);
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
                ConfigurationValue.HERD_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME.getKey()), ex.getMessage());
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
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(), null);
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

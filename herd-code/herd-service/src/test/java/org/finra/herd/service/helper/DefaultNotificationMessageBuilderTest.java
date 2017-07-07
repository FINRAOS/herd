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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.MessageHeaderDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinitions;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.ConfigurationEntity;
import org.finra.herd.model.jpa.MessageTypeEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * Tests the functionality within the default notification message builder.
 */
public class DefaultNotificationMessageBuilderTest extends AbstractServiceTest
{
    @Autowired
    private NotificationMessageBuilder defaultNotificationMessageBuilder;

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesNoMessageDefinitions() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Override configuration, so there will be no notification message definitions configured in the system.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(null);
        configurationDao.saveAndRefresh(configurationEntity);

        // Trigger the notification and validate the results.
        assertEquals(0,
            messageNotificationEventService.processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2).size());

        // Override configuration, so there will be an empty list of notification message definitions configured in the system.
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions()));
        configurationDao.saveAndRefresh(configurationEntity);

        assertEquals(0,
            messageNotificationEventService.processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2).size());
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesWithJsonPayload() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Arrays.asList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build the notification message
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
            NO_ATTRIBUTES, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesWithJsonPayloadNoOldBusinessObjectDataStatus() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Arrays.asList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build the notification message
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, NO_BDATA_STATUS);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, NO_BDATA_STATUS,
            NO_ATTRIBUTES, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesWithJsonPayloadNoSubPartitionValues() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(NO_SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Create the expected business object data key.
        BusinessObjectDataKey expectedBusinessObjectDataKey = (BusinessObjectDataKey) businessObjectDataKey.clone();
        expectedBusinessObjectDataKey.setSubPartitionValues(null);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Arrays.asList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build the notification message
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, expectedBusinessObjectDataKey, BDATA_STATUS,
            BDATA_STATUS_2, NO_ATTRIBUTES, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesWithJsonPayloadWithMultipleAttributes() throws Exception
    {
        // Create a list of attributes.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_2));

        // Create a list of attribute definitions.
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE));

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, attributeDefinitions, attributes);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Arrays.asList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build the notification message
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
            attributes, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesWithJsonPayloadWithSingleAttribute() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createTestValidBusinessObjectData(SUBPARTITION_VALUES, businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Arrays.asList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build the notification message
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
            Collections.singletonList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3)), NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesWithJsonPayloadWithSingleSubPartitionValue() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES.subList(0, 1), NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Arrays.asList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build the notification message
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
            NO_ATTRIBUTES, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesWithXmlPayload() throws Exception
    {
        // Test message building with default user and maximum supported number of sub-partition values.
        testBuildBusinessObjectDataStatusChangeMessagesWithXmlPayloadHelper(SUBPARTITION_VALUES, HerdDaoSecurityHelper.SYSTEM_USER);
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesWithXmlPayloadNoSubPartitionValues() throws Exception
    {
        // Test notification message building with default user and without sub-partition values.
        testBuildBusinessObjectDataStatusChangeMessagesWithXmlPayloadHelper(NO_SUBPARTITION_VALUES, HerdDaoSecurityHelper.SYSTEM_USER);
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesWithXmlPayloadWithMessageHeaders() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Create message header keys.
        final String MESSAGE_HEADER_KEY_ENVIRONMENT = "environment";
        final String MESSAGE_HEADER_KEY_MESSAGE_TYPE = "messageType";
        final String MESSAGE_HEADER_KEY_MESSAGE_VERSION = "messageVersion";
        final String MESSAGE_HEADER_KEY_SOURCE_SYSTEM = "sourceSystem";
        final String MESSAGE_HEADER_KEY_MESSAGE_ID = "messageId";
        final String MESSAGE_HEADER_KEY_USER_ID = "userId";

        // Create message header definitions.
        List<MessageHeaderDefinition> messageHeaderDefinitions = new ArrayList<>();
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_ENVIRONMENT, "$herd_environment"));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_MESSAGE_TYPE, MESSAGE_TYPE));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_MESSAGE_VERSION, MESSAGE_VERSION));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_SOURCE_SYSTEM, SOURCE_SYSTEM));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_MESSAGE_ID, "$uuid"));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_USER_ID, "$username"));

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Arrays.asList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML,
                messageHeaderDefinitions)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build the notification message
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(6, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey,
                businessObjectDataEntity.getId(), HerdDaoSecurityHelper.SYSTEM_USER, BDATA_STATUS, BDATA_STATUS_2, NO_ATTRIBUTES, Arrays
                .asList(new MessageHeader(MESSAGE_HEADER_KEY_ENVIRONMENT, configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT)),
                    new MessageHeader(MESSAGE_HEADER_KEY_MESSAGE_TYPE, MESSAGE_TYPE), new MessageHeader(MESSAGE_HEADER_KEY_MESSAGE_VERSION, MESSAGE_VERSION),
                    new MessageHeader(MESSAGE_HEADER_KEY_SOURCE_SYSTEM, SOURCE_SYSTEM), new MessageHeader(MESSAGE_HEADER_KEY_MESSAGE_ID, uuid),
                    new MessageHeader(MESSAGE_HEADER_KEY_USER_ID, HerdDaoSecurityHelper.SYSTEM_USER)), result.get(0));
    }

    @SuppressWarnings("serial")
    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesWithXmlPayloadWithUserInContext() throws Exception
    {
        Authentication originalAuthentication = SecurityContextHolder.getContext().getAuthentication();
        try
        {
            SecurityContextHolder.getContext().setAuthentication(new Authentication()
            {
                @Override
                public String getName()
                {
                    return null;
                }

                @Override
                public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException
                {
                }

                @Override
                public boolean isAuthenticated()
                {
                    return false;
                }

                @Override
                public Object getPrincipal()
                {
                    List<GrantedAuthority> authorities = Collections.emptyList();
                    return new User("testUsername", "", authorities);
                }

                @Override
                public Object getDetails()
                {
                    return null;
                }

                @Override
                public Object getCredentials()
                {
                    return null;
                }

                @Override
                public Collection<? extends GrantedAuthority> getAuthorities()
                {
                    return null;
                }
            });

            // Test message building using 4 sub-partitions and a non-default user.
            testBuildBusinessObjectDataStatusChangeMessagesWithXmlPayloadHelper(SUBPARTITION_VALUES, "testUsername");
        }
        finally
        {
            // Restore the original authentication.
            SecurityContextHolder.getContext().setAuthentication(originalAuthentication);
        }
    }

    @Test
    public void testBuildSystemMonitorResponse() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(),
            SYSTEM_MONITOR_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Call the method under test.
            NotificationMessage result = defaultNotificationMessageBuilder.buildSystemMonitorResponse(getTestSystemMonitorIncomingMessage());

            // Validate the results.
            validateSystemMonitorResponseNotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), HERD_OUTGOING_QUEUE, result);
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testBuildSystemMonitorResponseInvalidVelocityTemplate() throws Exception
    {
        // Override the configuration to use an invalid velocity template for building the system monitor response.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(), "#if($missingEndOfIfStatement");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Try to build a system monitor response message when velocity template is invalid.
            defaultNotificationMessageBuilder.buildSystemMonitorResponse(getTestSystemMonitorIncomingMessage());
            fail();
        }
        catch (ParseErrorException e)
        {
            assertTrue(e.getMessage().startsWith("Encountered \"<EOF>\" at systemMonitorResponse[line 1, column 28]"));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testBuildSystemMonitorResponseInvalidXpathExpression() throws Exception
    {
        // Override the configuration to use an invalid XPath expression.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(),
            SYSTEM_MONITOR_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML);
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES.getKey(), "key=///");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Try to build a system monitor response message when xpath expression is invalid.
            defaultNotificationMessageBuilder.buildSystemMonitorResponse(getTestSystemMonitorIncomingMessage());
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().startsWith("XPath expression \"///\" could not be evaluated against payload"));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testBuildSystemMonitorResponseInvalidXpathProperties() throws Exception
    {
        // Override configuration to set an invalid XPath properties format (this happens when an invalid unicode character is found).
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(),
            SYSTEM_MONITOR_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML);
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES.getKey(), "key=\\uxxxx");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // When the XPath properties are not a valid format, an IllegalStateException should be thrown.
            defaultNotificationMessageBuilder.buildSystemMonitorResponse(getTestSystemMonitorIncomingMessage());
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Unable to load XPath properties from configuration with key '%s'",
                ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES.getKey()), e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testBuildSystemMonitorResponseNoMessageVelocityTemplate() throws Exception
    {
        // Override the configuration to remove the velocity template for building the system monitor response.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(), null);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // When no velocity template is present, the response message should be null.
            assertNull(defaultNotificationMessageBuilder.buildSystemMonitorResponse(getTestSystemMonitorIncomingMessage()));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testBuildSystemMonitorResponseNoXPathExpressions() throws Exception
    {
        // Override the configuration to remove the XPath expressions when building the system monitor response.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(),
            SYSTEM_MONITOR_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML);
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES.getKey(), null);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Try to get a system monitor response when XPath expressions are removed.
            // This should throw a MethodInvocationException since velocity template contains an undefined variable.
            defaultNotificationMessageBuilder.buildSystemMonitorResponse(getTestSystemMonitorIncomingMessage());
            fail();
        }
        catch (MethodInvocationException e)
        {
            assertEquals(String.format("Variable $incoming_message_correlation_id has not been set at systemMonitorResponse[line 11, column 29]"),
                e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    /**
     * Builds a notification message for a business object data status change event and validates it.
     *
     * @param subPartitionValues the list of sub-partition values, may be empty or null
     * @param expectedTriggeredByUsername the expected "triggered by" user name
     */
    private void testBuildBusinessObjectDataStatusChangeMessagesWithXmlPayloadHelper(List<String> subPartitionValues, String expectedTriggeredByUsername)
        throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(subPartitionValues, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Arrays.asList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build the notification message
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey,
                businessObjectDataEntity.getId(), expectedTriggeredByUsername, BDATA_STATUS, BDATA_STATUS_2, NO_ATTRIBUTES, NO_MESSAGE_HEADERS, result.get(0));
    }

    /**
     * Validates a business object data status change notification message with JSON payload.
     *
     * @param expectedMessageType the expected message type
     * @param expectedMessageDestination the expected message destination
     * @param expectedBusinessObjectDataKey the expected business object data key
     * @param expectedNewBusinessObjectDataStatus the expected new business object data status
     * @param expectedOldBusinessObjectDataStatus the expected old business object data status
     * @param expectedBusinessObjectDataAttributes the list of expected business object data attributes
     * @param expectedMessageHeaders the list of expected message headers
     * @param notificationMessage the notification message to be validated
     */
    private void validateBusinessObjectDataStatusChangeMessageWithJsonPayload(String expectedMessageType, String expectedMessageDestination,
        BusinessObjectDataKey expectedBusinessObjectDataKey, String expectedNewBusinessObjectDataStatus, String expectedOldBusinessObjectDataStatus,
        List<Attribute> expectedBusinessObjectDataAttributes, List<MessageHeader> expectedMessageHeaders, NotificationMessage notificationMessage)
        throws IOException
    {
        assertNotNull(notificationMessage);

        assertEquals(expectedMessageType, notificationMessage.getMessageType());
        assertEquals(expectedMessageDestination, notificationMessage.getMessageDestination());

        BusinessObjectDataStatusChangeJsonMessagePayload businessObjectDataStatusChangeJsonMessagePayload =
            jsonHelper.unmarshallJsonToObject(BusinessObjectDataStatusChangeJsonMessagePayload.class, notificationMessage.getMessageText());

        assertEquals(StringUtils.length(businessObjectDataStatusChangeJsonMessagePayload.eventDate), StringUtils.length(HerdDateUtils.now().toString()));
        assertEquals(expectedBusinessObjectDataKey, businessObjectDataStatusChangeJsonMessagePayload.businessObjectDataKey);
        assertEquals(expectedNewBusinessObjectDataStatus, businessObjectDataStatusChangeJsonMessagePayload.newBusinessObjectDataStatus);
        assertEquals(expectedOldBusinessObjectDataStatus, businessObjectDataStatusChangeJsonMessagePayload.oldBusinessObjectDataStatus);

        assertEquals(CollectionUtils.size(expectedBusinessObjectDataAttributes),
            CollectionUtils.size(businessObjectDataStatusChangeJsonMessagePayload.attributes));
        if (CollectionUtils.isNotEmpty(expectedBusinessObjectDataAttributes))
        {
            for (Attribute expectedAttribute : expectedBusinessObjectDataAttributes)
            {
                assertTrue(businessObjectDataStatusChangeJsonMessagePayload.attributes.containsKey(expectedAttribute.getName()));
                assertEquals(expectedAttribute.getValue(), businessObjectDataStatusChangeJsonMessagePayload.attributes.get(expectedAttribute.getName()));
            }
        }

        assertEquals(expectedMessageHeaders, notificationMessage.getMessageHeaders());
    }

    private static class BusinessObjectDataStatusChangeJsonMessagePayload
    {
        public Map<String, String> attributes;

        public BusinessObjectDataKey businessObjectDataKey;

        public String eventDate;

        public String newBusinessObjectDataStatus;

        public String oldBusinessObjectDataStatus;
    }
}

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
package org.finra.herd.service.helper.notification;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.NotificationMessageDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinitions;
import org.finra.herd.model.dto.BusinessObjectFormatVersionChangeNotificationEvent;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.ConfigurationEntity;

/**
 * Tests the functionality for BusinessObjectDefinitionDescriptionSuggestionChangeMessageBuilder.
 */
public class BusinessObjectFormatVersionChangeMessageBuilderTest extends AbstractNotificationMessageBuilderTestHelper
{
    @Autowired
    private BusinessObjectFormatVersionChangeMessageBuilder businessObjectFormatVersionChangeMessageBuilder;

    @Test
    public void testBuildBusinessObjectFormatVersionChangeMessagesJsonPayload() throws Exception
    {
        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectFormatVersionChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, FORMAT_VERSION_2.toString()));

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        validateBusinessObjectFormatVersionChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectFormatKey,
            businessObjectFormatKey.getBusinessObjectFormatVersion().toString(), FORMAT_VERSION_2.toString(), getExpectedMessageHeaders(uuid), result.get(0));
    }

    @Test
    public void testBuildBusinessObjectFormatVersionChangeMessagesJsonPayloadInvalidMessageType() throws Exception
    {
        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(INVALID_VALUE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            businessObjectFormatVersionChangeMessageBuilder
                .buildNotificationMessages(new BusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, FORMAT_VERSION_2.toString()));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Only \"%s\" notification message type is supported. Please update \"%s\" configuration entry.", MESSAGE_TYPE_SNS,
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildBusinessObjectFormatVersionChangeMessagesJsonPayloadNoMessageDestination() throws Exception
    {
        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, NO_MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            businessObjectFormatVersionChangeMessageBuilder
                .buildNotificationMessages(new BusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, FORMAT_VERSION_2.toString()));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message destination must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildBusinessObjectFormatVersionChangeMessagesJsonPayloadNoMessageHeaders() throws Exception
    {
        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectFormatVersionChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, FORMAT_VERSION_2.toString()));

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectFormatVersionChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectFormatKey,
            businessObjectFormatKey.getBusinessObjectFormatVersion().toString(), FORMAT_VERSION_2.toString(), NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectFormatVersionChangeMessagesJsonPayloadNoMessageType() throws Exception
    {
        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(NO_MESSAGE_TYPE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            businessObjectFormatVersionChangeMessageBuilder
                .buildNotificationMessages(new BusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, FORMAT_VERSION_2.toString()));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message type must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildBusinessObjectFormatVersionChangeMessagesJsonPayloadNoOldBusinessObjectFormatVersion() throws Exception
    {
        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectFormatVersionChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION));

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectFormatVersionChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectFormatKey,
            businessObjectFormatKey.getBusinessObjectFormatVersion().toString(), NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectFormatVersionChangeMessagesNoMessageDefinitions() throws Exception
    {
        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(null);
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0, businessObjectFormatVersionChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION))
            .size());

        // Override configuration, so there will be an empty list of notification message definitions configured in the system.
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions()));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0, businessObjectFormatVersionChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION))
            .size());
    }

    @Test
    public void testGetBusinessObjectFormatVersionChangeMessageVelocityContextMap()
    {
        // Create a business object format key with values that require JSON and XML escaping.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE + SUFFIX_UNESCAPED, BDEF_NAME + SUFFIX_UNESCAPED, FORMAT_USAGE_CODE + SUFFIX_UNESCAPED,
                FORMAT_FILE_TYPE_CODE + SUFFIX_UNESCAPED, FORMAT_VERSION_2);

        // Call the method under test.
        Map<String, Object> result = businessObjectFormatVersionChangeMessageBuilder.getNotificationMessageVelocityContextMap(
            new BusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, String.valueOf(FORMAT_VERSION)));

        // Create an expected JSON escaped business object format key.
        BusinessObjectFormatKey expectedBusinessObjectFormatKeyWithJson =
            new BusinessObjectFormatKey(BDEF_NAMESPACE + SUFFIX_ESCAPED_JSON, BDEF_NAME + SUFFIX_ESCAPED_JSON, FORMAT_USAGE_CODE + SUFFIX_ESCAPED_JSON,
                FORMAT_FILE_TYPE_CODE + SUFFIX_ESCAPED_JSON, FORMAT_VERSION_2);

        // Create an expected XML escaped business object format key.
        BusinessObjectFormatKey expectedBusinessObjectFormatKeyWithXml =
            new BusinessObjectFormatKey(BDEF_NAMESPACE + SUFFIX_ESCAPED_XML, BDEF_NAME + SUFFIX_ESCAPED_XML, FORMAT_USAGE_CODE + SUFFIX_ESCAPED_XML,
                FORMAT_FILE_TYPE_CODE + SUFFIX_ESCAPED_XML, FORMAT_VERSION_2);

        // Validate the results.
        assertEquals(8, CollectionUtils.size(result));

        assertEquals(businessObjectFormatKey, result.get("businessObjectFormatKey"));
        assertEquals(expectedBusinessObjectFormatKeyWithJson, result.get("businessObjectFormatKeyWithJson"));
        assertEquals(expectedBusinessObjectFormatKeyWithXml, result.get("businessObjectFormatKeyWithXml"));

        assertEquals(FORMAT_VERSION_2, result.get("newBusinessObjectFormatVersion"));
        assertEquals(FORMAT_VERSION.toString(), result.get("oldBusinessObjectFormatVersion"));

        assertEquals(BDEF_NAMESPACE + SUFFIX_UNESCAPED, result.get("namespace"));
        assertEquals(BDEF_NAMESPACE + SUFFIX_ESCAPED_JSON, result.get("namespaceWithJson"));
        assertEquals(BDEF_NAMESPACE + SUFFIX_ESCAPED_XML, result.get("namespaceWithXml"));
    }

    /**
     * Validates a business object data status change notification message with JSON payload.
     *
     * @param expectedMessageType the expected message type
     * @param expectedMessageDestination the expected message destination
     * @param expectedBusinessObjectFormatKey the expected business object format key
     * @param expectedNewBusinessObjectFormatVersion the expected new business object format version
     * @param expectedOldBusinessObjectFormatVersion the expected old business object format version
     * @param expectedMessageHeaders the list of expected message headers
     * @param notificationMessage the notification message to be validated
     */
    private void validateBusinessObjectFormatVersionChangeMessageWithJsonPayload(String expectedMessageType, String expectedMessageDestination,
        BusinessObjectFormatKey expectedBusinessObjectFormatKey, String expectedNewBusinessObjectFormatVersion, String expectedOldBusinessObjectFormatVersion,
        List<MessageHeader> expectedMessageHeaders, NotificationMessage notificationMessage) throws IOException
    {
        assertNotNull(notificationMessage);

        assertEquals(expectedMessageType, notificationMessage.getMessageType());
        assertEquals(expectedMessageDestination, notificationMessage.getMessageDestination());

        BusinessObjectFormatVersionChangeJsonMessagePayload businessObjectFormatVersionChangeJsonMessagePayload =
            jsonHelper.unmarshallJsonToObject(BusinessObjectFormatVersionChangeJsonMessagePayload.class, notificationMessage.getMessageText());

        assertEquals(StringUtils.length(businessObjectFormatVersionChangeJsonMessagePayload.eventDate), StringUtils.length(HerdDateUtils.now().toString()));
        assertEquals(expectedBusinessObjectFormatKey, businessObjectFormatVersionChangeJsonMessagePayload.businessObjectFormatKey);
        assertEquals(expectedNewBusinessObjectFormatVersion, businessObjectFormatVersionChangeJsonMessagePayload.newBusinessObjectFormatVersion);
        assertEquals(expectedOldBusinessObjectFormatVersion, businessObjectFormatVersionChangeJsonMessagePayload.oldBusinessObjectFormatVersion);
        assertEquals(expectedMessageHeaders, notificationMessage.getMessageHeaders());
    }

    private static class BusinessObjectFormatVersionChangeJsonMessagePayload
    {
        public BusinessObjectFormatKey businessObjectFormatKey;

        public String eventDate;

        public String newBusinessObjectFormatVersion;

        public String oldBusinessObjectFormatVersion;
    }

}

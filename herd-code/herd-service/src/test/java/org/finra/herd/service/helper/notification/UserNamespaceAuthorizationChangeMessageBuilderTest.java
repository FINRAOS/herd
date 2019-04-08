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

import org.finra.herd.model.api.xml.NotificationMessageDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinitions;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.dto.UserNamespaceAuthorizationChangeNotificationEvent;
import org.finra.herd.model.jpa.ConfigurationEntity;

/**
 * Tests the functionality for UserNamespaceAuthorizationChangeMessageBuilder.
 */
public class UserNamespaceAuthorizationChangeMessageBuilderTest extends AbstractNotificationMessageBuilderTestHelper
{
    @Autowired
    private UserNamespaceAuthorizationChangeMessageBuilder userNamespaceAuthorizationChangeMessageBuilder;

    @Test
    public void testBuildUserNamespaceAuthorizationChangeMessagesJsonPayload() throws Exception
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey userNamespaceAuthorizationKey = new UserNamespaceAuthorizationKey(USER_ID, BDEF_NAMESPACE);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_USER_NAMESPACE_AUTHORIZATION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                USER_NAMESPACE_AUTHORIZATION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = userNamespaceAuthorizationChangeMessageBuilder
            .buildNotificationMessages(new UserNamespaceAuthorizationChangeNotificationEvent(userNamespaceAuthorizationKey));

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        validateUserNamespaceAuthorizationChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, userNamespaceAuthorizationKey,
            getExpectedMessageHeaders(uuid), result.get(0));
    }

    @Test
    public void testBuildUserNamespaceAuthorizationChangeMessagesJsonPayloadInvalidMessageType() throws Exception
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey userNamespaceAuthorizationKey = new UserNamespaceAuthorizationKey(USER_ID, BDEF_NAMESPACE);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_USER_NAMESPACE_AUTHORIZATION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(INVALID_VALUE, MESSAGE_DESTINATION,
                USER_NAMESPACE_AUTHORIZATION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            userNamespaceAuthorizationChangeMessageBuilder
                .buildNotificationMessages(new UserNamespaceAuthorizationChangeNotificationEvent(userNamespaceAuthorizationKey));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Only \"%s\" notification message type is supported. Please update \"%s\" configuration entry.", MESSAGE_TYPE_SNS,
                ConfigurationValue.HERD_NOTIFICATION_USER_NAMESPACE_AUTHORIZATION_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildUserNamespaceAuthorizationChangeMessagesJsonPayloadNoMessageDestination() throws Exception
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey userNamespaceAuthorizationKey = new UserNamespaceAuthorizationKey(USER_ID, BDEF_NAMESPACE);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_USER_NAMESPACE_AUTHORIZATION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, NO_MESSAGE_DESTINATION,
                USER_NAMESPACE_AUTHORIZATION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            userNamespaceAuthorizationChangeMessageBuilder
                .buildNotificationMessages(new UserNamespaceAuthorizationChangeNotificationEvent(userNamespaceAuthorizationKey));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message destination must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_USER_NAMESPACE_AUTHORIZATION_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildUserNamespaceAuthorizationChangeMessagesJsonPayloadNoMessageHeaders() throws Exception
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey userNamespaceAuthorizationKey = new UserNamespaceAuthorizationKey(USER_ID, BDEF_NAMESPACE);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_USER_NAMESPACE_AUTHORIZATION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                USER_NAMESPACE_AUTHORIZATION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = userNamespaceAuthorizationChangeMessageBuilder
            .buildNotificationMessages(new UserNamespaceAuthorizationChangeNotificationEvent(userNamespaceAuthorizationKey));

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        validateUserNamespaceAuthorizationChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, userNamespaceAuthorizationKey, NO_MESSAGE_HEADERS,
            result.get(0));
    }

    @Test
    public void testBuildUserNamespaceAuthorizationChangeMessagesJsonPayloadNoMessageType() throws Exception
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey userNamespaceAuthorizationKey = new UserNamespaceAuthorizationKey(USER_ID, BDEF_NAMESPACE);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_USER_NAMESPACE_AUTHORIZATION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(NO_MESSAGE_TYPE, MESSAGE_DESTINATION,
                USER_NAMESPACE_AUTHORIZATION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            userNamespaceAuthorizationChangeMessageBuilder
                .buildNotificationMessages(new UserNamespaceAuthorizationChangeNotificationEvent(userNamespaceAuthorizationKey));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message type must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_USER_NAMESPACE_AUTHORIZATION_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildUserNamespaceAuthorizationChangeMessagesNoMessageDefinitions() throws Exception
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey userNamespaceAuthorizationKey = new UserNamespaceAuthorizationKey(USER_ID, BDEF_NAMESPACE);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_USER_NAMESPACE_AUTHORIZATION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(null);
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0, userNamespaceAuthorizationChangeMessageBuilder
            .buildNotificationMessages(new UserNamespaceAuthorizationChangeNotificationEvent(userNamespaceAuthorizationKey)).size());

        // Override configuration, so there will be an empty list of notification message definitions configured in the system.
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions()));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0, userNamespaceAuthorizationChangeMessageBuilder
            .buildNotificationMessages(new UserNamespaceAuthorizationChangeNotificationEvent(userNamespaceAuthorizationKey)).size());
    }

    @Test
    public void testgetUserNamespaceAuthorizationChangeMessageVelocityContextMap()
    {
        // Create a user namespace authorization key with values that require JSON and XML escaping.
        UserNamespaceAuthorizationKey userNamespaceAuthorizationKey =
            new UserNamespaceAuthorizationKey(USER_ID + SUFFIX_UNESCAPED, BDEF_NAMESPACE + SUFFIX_UNESCAPED);

        // Call the method under test.
        Map<String, Object> result = userNamespaceAuthorizationChangeMessageBuilder
            .getNotificationMessageVelocityContextMap(new UserNamespaceAuthorizationChangeNotificationEvent(userNamespaceAuthorizationKey));

        // Create an expected JSON escaped user namespace authorization key.
        UserNamespaceAuthorizationKey expectedUserNamespaceAuthorizationKeyWithJson =
            new UserNamespaceAuthorizationKey(USER_ID + SUFFIX_ESCAPED_JSON, BDEF_NAMESPACE + SUFFIX_ESCAPED_JSON);

        // Create an expected XML escaped user namespace authorization key.
        UserNamespaceAuthorizationKey expectedUserNamespaceAuthorizationKeyWithXml =
            new UserNamespaceAuthorizationKey(USER_ID + SUFFIX_ESCAPED_XML, BDEF_NAMESPACE + SUFFIX_ESCAPED_XML);

        // Validate the results.
        assertEquals(6, CollectionUtils.size(result));

        assertEquals(userNamespaceAuthorizationKey, result.get("userNamespaceAuthorizationKey"));
        assertEquals(expectedUserNamespaceAuthorizationKeyWithJson, result.get("userNamespaceAuthorizationKeyWithJson"));
        assertEquals(expectedUserNamespaceAuthorizationKeyWithXml, result.get("userNamespaceAuthorizationKeyWithXml"));

        assertEquals(BDEF_NAMESPACE + SUFFIX_UNESCAPED, result.get("namespace"));
        assertEquals(BDEF_NAMESPACE + SUFFIX_ESCAPED_JSON, result.get("namespaceWithJson"));
        assertEquals(BDEF_NAMESPACE + SUFFIX_ESCAPED_XML, result.get("namespaceWithXml"));
    }

    /**
     * Validates a user namespace authorization change notification message with JSON payload.
     *
     * @param expectedMessageType the expected message type
     * @param expectedMessageDestination the expected message destination
     * @param expectedUserNamespaceAuthorizationKey the expected user namespace authorization key
     * @param expectedMessageHeaders the list of expected message headers
     * @param notificationMessage the notification message to be validated
     */
    private void validateUserNamespaceAuthorizationChangeMessageWithJsonPayload(String expectedMessageType, String expectedMessageDestination,
        UserNamespaceAuthorizationKey expectedUserNamespaceAuthorizationKey, List<MessageHeader> expectedMessageHeaders,
        NotificationMessage notificationMessage) throws IOException
    {
        assertNotNull(notificationMessage);

        assertEquals(expectedMessageType, notificationMessage.getMessageType());
        assertEquals(expectedMessageDestination, notificationMessage.getMessageDestination());

        UserNamespaceAuthorizationChangeJsonMessagePayload businessObjectFormatVersionChangeJsonMessagePayload =
            jsonHelper.unmarshallJsonToObject(UserNamespaceAuthorizationChangeJsonMessagePayload.class, notificationMessage.getMessageText());

        assertEquals(expectedUserNamespaceAuthorizationKey.getUserId(), businessObjectFormatVersionChangeJsonMessagePayload.userId);
        assertEquals(expectedUserNamespaceAuthorizationKey.getNamespace(), businessObjectFormatVersionChangeJsonMessagePayload.namespace);
        assertEquals(expectedMessageHeaders, notificationMessage.getMessageHeaders());
    }

    private static class UserNamespaceAuthorizationChangeJsonMessagePayload
    {
        public String userId;

        public String namespace;
    }

}

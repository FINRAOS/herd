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

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.NotificationMessageDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinitions;
import org.finra.herd.model.dto.BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity;
import org.finra.herd.model.jpa.ConfigurationEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

/**
 * Tests the functionality for BusinessObjectDefinitionDescriptionSuggestionChangeMessageBuilder.
 */
public class BusinessObjectDefinitionDescriptionSuggestionChangeMessageBuilderTest extends AbstractNotificationMessageBuilderTestHelper
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionChangeMessageBuilder businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder;

    @Test
    public void testBuildBusinessObjectDefinitionDescriptionSuggestionChangeMessages() throws Exception
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(BDEF_NAMESPACE);

        // Create user namespace authorisations with user ids in reverse order and with WRITE and WRITE_DESCRIPTIVE_CONTENT namespace permissions.
        userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(USER_ID_3, namespaceEntity, Lists.newArrayList(NamespacePermissionEnum.WRITE));
        userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(USER_ID_2, namespaceEntity, Lists.newArrayList(NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT));

        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create a business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name(), CREATED_BY,
                CREATED_ON);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                namespaceEntity.getCode()));

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        validateBusinessObjectDefinitionDescriptionSuggestionChangeMessage(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDefinitionDescriptionSuggestion,
            UPDATED_BY, UPDATED_ON.toString(), Lists.newArrayList(CREATED_BY, USER_ID_2, USER_ID_3),
            String.format("https://udc.dev.finra.org/data-entities/%s/%s", BDEF_NAMESPACE, BDEF_NAME), getExpectedMessageHeaders(uuid), result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDefinitionDescriptionSuggestionChangeMessagesInvalidMessageType() throws Exception
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(BDEF_NAMESPACE);

        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create a business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name(), CREATED_BY,
                CREATED_ON);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(INVALID_VALUE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder.buildNotificationMessages(
                new BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                    namespaceEntity.getCode()));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Only \"%s\" notification message type is supported. Please update \"%s\" configuration entry.", MESSAGE_TYPE_SNS,
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildBusinessObjectDefinitionDescriptionSuggestionChangeMessagesInvalidNamespace() throws Exception
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format("namespaceEntity must not be null for namespace code \"invalidNamespace\""));

        // Create a namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(BDEF_NAMESPACE);

        // Create user namespace authorisations with user ids in reverse order and with WRITE and WRITE_DESCRIPTIVE_CONTENT namespace permissions.
        userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(USER_ID_3, namespaceEntity, Lists.newArrayList(NamespacePermissionEnum.WRITE));
        userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(USER_ID_2, namespaceEntity, Lists.newArrayList(NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT));

        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create a business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name(), CREATED_BY,
                CREATED_ON);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message with invalid namesapce
        businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                "invalidNamespace"));

    }

    @Test
    public void testBuildBusinessObjectDefinitionDescriptionSuggestionChangeMessagesNoMessageDefinitions() throws Exception
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(BDEF_NAMESPACE);

        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create a business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name(), CREATED_BY,
                CREATED_ON);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(null);
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0, businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                namespaceEntity.getCode())).size());

        // Override configuration, so there will be an empty list of notification message definitions configured in the system.
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions()));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0, businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                namespaceEntity.getCode())).size());
    }

    @Test
    public void testBuildBusinessObjectDefinitionDescriptionSuggestionChangeMessagesNoMessageDestination() throws Exception
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(BDEF_NAMESPACE);

        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create a business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name(), CREATED_BY,
                CREATED_ON);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, NO_MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder.buildNotificationMessages(
                new BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                    namespaceEntity.getCode()));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message destination must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildBusinessObjectDefinitionDescriptionSuggestionChangeMessagesNoMessageHeaders() throws Exception
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(BDEF_NAMESPACE);

        // Create user namespace authorisations with user ids in reverse order and with WRITE and WRITE_DESCRIPTIVE_CONTENT namespace permissions.
        userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(USER_ID_3, namespaceEntity, Lists.newArrayList(NamespacePermissionEnum.WRITE));
        userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(USER_ID_2, namespaceEntity, Lists.newArrayList(NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT));

        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create a business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name(), CREATED_BY,
                CREATED_ON);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                namespaceEntity.getCode()));

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDefinitionDescriptionSuggestionChangeMessage(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDefinitionDescriptionSuggestion,
            UPDATED_BY, UPDATED_ON.toString(), Lists.newArrayList(CREATED_BY, USER_ID_2, USER_ID_3),
            String.format("https://udc.dev.finra.org/data-entities/%s/%s", BDEF_NAMESPACE, BDEF_NAME), NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDefinitionDescriptionSuggestionChangeMessagesNoMessageType() throws Exception
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(BDEF_NAMESPACE);

        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create a business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name(), CREATED_BY,
                CREATED_ON);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(NO_MESSAGE_TYPE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder.buildNotificationMessages(
                new BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                    namespaceEntity.getCode()));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message type must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildBusinessObjectDefinitionDescriptionSuggestionChangeMessagesOnlyOneUserIdInNotificationList() throws Exception
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(BDEF_NAMESPACE);

        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create a business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name(), CREATED_BY,
                CREATED_ON);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                namespaceEntity.getCode()));

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        validateBusinessObjectDefinitionDescriptionSuggestionChangeMessage(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDefinitionDescriptionSuggestion,
            UPDATED_BY, UPDATED_ON.toString(), Lists.newArrayList(CREATED_BY),
            String.format("https://udc.dev.finra.org/data-entities/%s/%s", BDEF_NAMESPACE, BDEF_NAME), getExpectedMessageHeaders(uuid), result.get(0));
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionChangeMessageVelocityContextMap()
    {
        // Create a business object definition description suggestion key with values that require JSON and XML escaping.
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(BDEF_NAMESPACE + SUFFIX_UNESCAPED, BDEF_NAME + SUFFIX_UNESCAPED, USER_ID + SUFFIX_UNESCAPED);

        // Create a business object definition description suggestion with values that require JSON and XML escaping.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION + SUFFIX_UNESCAPED,
                BDEF_DESCRIPTION_SUGGESTION_STATUS + SUFFIX_UNESCAPED, USER_ID + SUFFIX_UNESCAPED, CREATED_ON);

        // Create and persist a namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());

        // Call the method under test.
        Map<String, Object> result = businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder.getNotificationMessageVelocityContextMap(
            new BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion,
                USER_ID_2 + SUFFIX_UNESCAPED, UPDATED_ON, namespaceEntity.getCode()));

        // Create an expected JSON escaped business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey expectedBusinessObjectDefinitionDescriptionSuggestionKeyWithJson =
            new BusinessObjectDefinitionDescriptionSuggestionKey(BDEF_NAMESPACE + SUFFIX_ESCAPED_JSON, BDEF_NAME + SUFFIX_ESCAPED_JSON,
                USER_ID + SUFFIX_ESCAPED_JSON);

        // Create an expected JSON escaped business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestion expectedBusinessObjectDefinitionDescriptionSuggestionWithJson =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, expectedBusinessObjectDefinitionDescriptionSuggestionKeyWithJson,
                DESCRIPTION_SUGGESTION + SUFFIX_ESCAPED_JSON, BDEF_DESCRIPTION_SUGGESTION_STATUS + SUFFIX_ESCAPED_JSON, USER_ID + SUFFIX_ESCAPED_JSON,
                CREATED_ON);

        // Create an expected XML escaped business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey expectedBusinessObjectDefinitionDescriptionSuggestionKeyWithXml =
            new BusinessObjectDefinitionDescriptionSuggestionKey(BDEF_NAMESPACE + SUFFIX_ESCAPED_XML, BDEF_NAME + SUFFIX_ESCAPED_XML,
                USER_ID + SUFFIX_ESCAPED_XML);

        // Create an expected XML escaped business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestion expectedBusinessObjectDefinitionDescriptionSuggestionWithXml =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, expectedBusinessObjectDefinitionDescriptionSuggestionKeyWithXml,
                DESCRIPTION_SUGGESTION + SUFFIX_ESCAPED_XML, BDEF_DESCRIPTION_SUGGESTION_STATUS + SUFFIX_ESCAPED_XML, USER_ID + SUFFIX_ESCAPED_XML, CREATED_ON);

        // Create expected notification lists.
        List<String> expectedNotificationList = Lists.newArrayList(USER_ID + SUFFIX_UNESCAPED);
        List<String> expectedNotificationListWithJson = Lists.newArrayList(USER_ID + SUFFIX_ESCAPED_JSON);
        List<String> expectedNotificationListWithXml = Lists.newArrayList(USER_ID + SUFFIX_ESCAPED_XML);

        // Validate the results.
        assertEquals(16, CollectionUtils.size(result));

        assertEquals(businessObjectDefinitionDescriptionSuggestion, result.get("businessObjectDefinitionDescriptionSuggestion"));
        assertEquals(expectedBusinessObjectDefinitionDescriptionSuggestionWithJson, result.get("businessObjectDefinitionDescriptionSuggestionWithJson"));
        assertEquals(expectedBusinessObjectDefinitionDescriptionSuggestionWithXml, result.get("businessObjectDefinitionDescriptionSuggestionWithXml"));

        assertEquals(businessObjectDefinitionDescriptionSuggestionKey, result.get("businessObjectDefinitionDescriptionSuggestionKey"));
        assertEquals(expectedBusinessObjectDefinitionDescriptionSuggestionKeyWithJson, result.get("businessObjectDefinitionDescriptionSuggestionKeyWithJson"));
        assertEquals(expectedBusinessObjectDefinitionDescriptionSuggestionKeyWithXml, result.get("businessObjectDefinitionDescriptionSuggestionKeyWithXml"));

        assertEquals(USER_ID_2 + SUFFIX_UNESCAPED, result.get("lastUpdatedByUserId"));
        assertEquals(USER_ID_2 + SUFFIX_ESCAPED_JSON, result.get("lastUpdatedByUserIdWithJson"));
        assertEquals(USER_ID_2 + SUFFIX_ESCAPED_XML, result.get("lastUpdatedByUserIdWithXml"));

        assertEquals(UPDATED_ON, result.get("lastUpdatedOn"));

        assertEquals(expectedNotificationList, result.get("notificationList"));
        assertEquals(expectedNotificationListWithJson, result.get("notificationListWithJson"));
        assertEquals(expectedNotificationListWithXml, result.get("notificationListWithXml"));

        assertEquals(BDEF_NAMESPACE + SUFFIX_UNESCAPED, result.get("namespace"));
        assertEquals(BDEF_NAMESPACE + SUFFIX_ESCAPED_JSON, result.get("namespaceWithJson"));
        assertEquals(BDEF_NAMESPACE + SUFFIX_ESCAPED_XML, result.get("namespaceWithXml"));
    }

    /**
     * Validates a business object definition description suggestion change notification message.
     *
     * @param expectedMessageType the expected message type
     * @param expectedMessageDestination the expected message destination
     * @param expectedBusinessObjectDefinitionDescriptionSuggestion the expected business object definition description suggestion
     * @param expectedLastUpdatedByUserId the expected User ID of the user who last updated this business object definition description suggestion
     * @param expectedLastUpdatedOn the expected timestamp when this business object definition description suggestion was last updated on
     * @param expectedNotificationList the expected notification list
     * @param expectedBusinessObjectDefinitionUri the expected UDC URI for the business object definition
     * @param expectedMessageHeaders the list of expected message headers
     * @param notificationMessage the notification message to be validated
     */
    private void validateBusinessObjectDefinitionDescriptionSuggestionChangeMessage(String expectedMessageType, String expectedMessageDestination,
        BusinessObjectDefinitionDescriptionSuggestion expectedBusinessObjectDefinitionDescriptionSuggestion, String expectedLastUpdatedByUserId,
        String expectedLastUpdatedOn, List<String> expectedNotificationList, String expectedBusinessObjectDefinitionUri,
        List<MessageHeader> expectedMessageHeaders, NotificationMessage notificationMessage) throws IOException
    {
        assertNotNull(notificationMessage);

        assertEquals(expectedMessageType, notificationMessage.getMessageType());
        assertEquals(expectedMessageDestination, notificationMessage.getMessageDestination());

        BusinessObjectDefinitionDescriptionSuggestionChangeJsonMessagePayload businessObjectDefinitionDescriptionSuggestionChangeJsonMessagePayload = jsonHelper
            .unmarshallJsonToObject(BusinessObjectDefinitionDescriptionSuggestionChangeJsonMessagePayload.class, notificationMessage.getMessageText());

        assertEquals(StringUtils.length(businessObjectDefinitionDescriptionSuggestionChangeJsonMessagePayload.eventDate),
            StringUtils.length(HerdDateUtils.now().toString()));
        assertEquals(expectedBusinessObjectDefinitionDescriptionSuggestion.getBusinessObjectDefinitionDescriptionSuggestionKey(),
            businessObjectDefinitionDescriptionSuggestionChangeJsonMessagePayload.businessObjectDefinitionDescriptionSuggestionKey);
        assertEquals(expectedBusinessObjectDefinitionDescriptionSuggestion.getStatus(),
            businessObjectDefinitionDescriptionSuggestionChangeJsonMessagePayload.status);
        assertEquals(expectedBusinessObjectDefinitionDescriptionSuggestion.getCreatedByUserId(),
            businessObjectDefinitionDescriptionSuggestionChangeJsonMessagePayload.createdByUserId);
        assertEquals(expectedBusinessObjectDefinitionDescriptionSuggestion.getCreatedOn().toString(),
            businessObjectDefinitionDescriptionSuggestionChangeJsonMessagePayload.createdOn);
        assertEquals(expectedLastUpdatedByUserId, businessObjectDefinitionDescriptionSuggestionChangeJsonMessagePayload.lastUpdatedByUserId);
        assertEquals(expectedLastUpdatedOn, businessObjectDefinitionDescriptionSuggestionChangeJsonMessagePayload.lastUpdatedOn);
        assertEquals(expectedNotificationList, businessObjectDefinitionDescriptionSuggestionChangeJsonMessagePayload.notificationList);
        assertEquals(expectedBusinessObjectDefinitionUri, businessObjectDefinitionDescriptionSuggestionChangeJsonMessagePayload.businessObjectDefinitionUri);

        assertEquals(expectedMessageHeaders, notificationMessage.getMessageHeaders());
    }

    private static class BusinessObjectDefinitionDescriptionSuggestionChangeJsonMessagePayload
    {
        public BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey;

        public String businessObjectDefinitionUri;

        public String createdByUserId;

        public String createdOn;

        public String eventDate;

        public String lastUpdatedByUserId;

        public String lastUpdatedOn;

        public List<String> notificationList;

        public String status;
    }
}

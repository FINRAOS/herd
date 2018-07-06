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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Lists;
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
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.MessageHeaderDefinition;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.NotificationMessageDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinitions;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity;
import org.finra.herd.model.jpa.ConfigurationEntity;
import org.finra.herd.model.jpa.MessageTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * Tests the functionality within the default notification message builder.
 */
public class DefaultNotificationMessageBuilderTest extends AbstractServiceTest
{
    @Autowired
    private NotificationMessageBuilder defaultNotificationMessageBuilder;

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesJsonPayload() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
            NO_ATTRIBUTES, getExpectedMessageHeaders(uuid), result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesJsonPayloadMultipleAttributes() throws Exception
    {
        // Create a list of attributes.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_2));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_3_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_3));

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
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
            attributes.subList(0, 2), NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesJsonPayloadNoMessageDestination() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, NO_MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message destination must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesJsonPayloadNoMessageHeaders() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
            NO_ATTRIBUTES, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesJsonPayloadNoMessageType() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(NO_MESSAGE_TYPE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message type must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesJsonPayloadNoOldBusinessObjectDataStatus() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, NO_BDATA_STATUS);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, NO_BDATA_STATUS,
            NO_ATTRIBUTES, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesJsonPayloadNoSubPartitionValues() throws Exception
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
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, expectedBusinessObjectDataKey, BDATA_STATUS,
            BDATA_STATUS_2, NO_ATTRIBUTES, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesJsonPayloadSingleAttribute() throws Exception
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
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
            Collections.singletonList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3)), NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesJsonPayloadSingleSubPartitionValue() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES.subList(0, 1), NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
            NO_ATTRIBUTES, NO_MESSAGE_HEADERS, result.get(0));
    }

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

        // Try to build a notification message and validate the results.
        assertEquals(0,
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2).size());

        // Override configuration, so there will be an empty list of notification message definitions configured in the system.
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions()));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0,
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2).size());
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesXmlPayload() throws Exception
    {
        // Test message building with default user and maximum supported number of sub-partition values.
        testBuildBusinessObjectDataStatusChangeMessagesWithXmlPayloadHelper(SUBPARTITION_VALUES, HerdDaoSecurityHelper.SYSTEM_USER);
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesXmlPayloadAndMessageHeaders() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML,
                getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey,
                businessObjectDataEntity.getId(), HerdDaoSecurityHelper.SYSTEM_USER, BDATA_STATUS, BDATA_STATUS_2, NO_ATTRIBUTES,
                getExpectedMessageHeaders(uuid), result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesXmlPayloadNoSubPartitionValues() throws Exception
    {
        // Test notification message building with default user and without sub-partition values.
        testBuildBusinessObjectDataStatusChangeMessagesWithXmlPayloadHelper(NO_SUBPARTITION_VALUES, HerdDaoSecurityHelper.SYSTEM_USER);
    }

    @SuppressWarnings("serial")
    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesXmlPayloadUserInContext() throws Exception
    {
        Authentication originalAuthentication = SecurityContextHolder.getContext().getAuthentication();
        try
        {
            SecurityContextHolder.getContext().setAuthentication(new Authentication()
            {
                @Override
                public Collection<? extends GrantedAuthority> getAuthorities()
                {
                    return null;
                }

                @Override
                public Object getCredentials()
                {
                    return null;
                }

                @Override
                public Object getDetails()
                {
                    return null;
                }

                @Override
                public String getName()
                {
                    return null;
                }

                @Override
                public Object getPrincipal()
                {
                    List<GrantedAuthority> authorities = Collections.emptyList();
                    return new User("testUsername", "", authorities);
                }

                @Override
                public boolean isAuthenticated()
                {
                    return false;
                }

                @Override
                public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException
                {
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
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = defaultNotificationMessageBuilder
            .buildBusinessObjectDefinitionDescriptionSuggestionChangeMessages(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                namespaceEntity);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        validateBusinessObjectDefinitionDescriptionSuggestionChangeMessage(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDefinitionDescriptionSuggestion,
            UPDATED_BY, UPDATED_ON.toString(), Lists.newArrayList(CREATED_BY, USER_ID_2, USER_ID_3),
            String.format("https://udc.dev.finra.org/data-entities/%s/%s", BDEF_NAMESPACE, BDEF_NAME), getExpectedMessageHeaders(uuid), result.get(0));
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
        assertEquals(0, defaultNotificationMessageBuilder
            .buildBusinessObjectDefinitionDescriptionSuggestionChangeMessages(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                namespaceEntity).size());

        // Override configuration, so there will be an empty list of notification message definitions configured in the system.
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions()));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0, defaultNotificationMessageBuilder
            .buildBusinessObjectDefinitionDescriptionSuggestionChangeMessages(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                namespaceEntity).size());
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
            new NotificationMessageDefinition(MESSAGE_TYPE, NO_MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            defaultNotificationMessageBuilder
                .buildBusinessObjectDefinitionDescriptionSuggestionChangeMessages(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                    namespaceEntity);
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
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = defaultNotificationMessageBuilder
            .buildBusinessObjectDefinitionDescriptionSuggestionChangeMessages(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                namespaceEntity);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDefinitionDescriptionSuggestionChangeMessage(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDefinitionDescriptionSuggestion,
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
            defaultNotificationMessageBuilder
                .buildBusinessObjectDefinitionDescriptionSuggestionChangeMessages(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                    namespaceEntity);
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
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = defaultNotificationMessageBuilder
            .buildBusinessObjectDefinitionDescriptionSuggestionChangeMessages(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                namespaceEntity);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        validateBusinessObjectDefinitionDescriptionSuggestionChangeMessage(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDefinitionDescriptionSuggestion,
            UPDATED_BY, UPDATED_ON.toString(), Lists.newArrayList(CREATED_BY),
            String.format("https://udc.dev.finra.org/data-entities/%s/%s", BDEF_NAMESPACE, BDEF_NAME), getExpectedMessageHeaders(uuid), result.get(0));
    }

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
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectFormatVersionChangeMessages(businessObjectFormatKey, FORMAT_VERSION_2.toString());

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        validateBusinessObjectFormatVersionChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectFormatKey,
            businessObjectFormatKey.getBusinessObjectFormatVersion().toString(), FORMAT_VERSION_2.toString(), getExpectedMessageHeaders(uuid), result.get(0));
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
            new NotificationMessageDefinition(MESSAGE_TYPE, NO_MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            defaultNotificationMessageBuilder.buildBusinessObjectFormatVersionChangeMessages(businessObjectFormatKey, FORMAT_VERSION_2.toString());
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
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectFormatVersionChangeMessages(businessObjectFormatKey, FORMAT_VERSION_2.toString());

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectFormatVersionChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectFormatKey,
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
            defaultNotificationMessageBuilder.buildBusinessObjectFormatVersionChangeMessages(businessObjectFormatKey, FORMAT_VERSION_2.toString());
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
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result =
            defaultNotificationMessageBuilder.buildBusinessObjectFormatVersionChangeMessages(businessObjectFormatKey, NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectFormatVersionChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectFormatKey,
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
        assertEquals(0,
            defaultNotificationMessageBuilder.buildBusinessObjectFormatVersionChangeMessages(businessObjectFormatKey, NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION)
                .size());

        // Override configuration, so there will be an empty list of notification message definitions configured in the system.
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions()));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0,
            defaultNotificationMessageBuilder.buildBusinessObjectFormatVersionChangeMessages(businessObjectFormatKey, NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION)
                .size());
    }

    @Test
    public void testBuildStorageUnitStatusChangeMessagesJsonPayload() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, STORAGE_UNIT_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = defaultNotificationMessageBuilder
            .buildStorageUnitStatusChangeMessages(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        validateStorageUnitStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS,
            STORAGE_UNIT_STATUS_2, getExpectedMessageHeaders(uuid), result.get(0));
    }

    @Test
    public void testBuildStorageUnitStatusChangeMessagesJsonPayloadNoMessageDestination() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, NO_MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                Collections.singletonList(new MessageHeaderDefinition(KEY, VALUE)))))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            defaultNotificationMessageBuilder
                .buildStorageUnitStatusChangeMessages(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2);
            fail();
        }
        catch (IllegalStateException illegalStateException)
        {
            assertEquals(String.format("Notification message destination must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey()), illegalStateException.getMessage());
        }
    }

    @Test
    public void testBuildStorageUnitStatusChangeMessagesJsonPayloadNoMessageHeaders() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, STORAGE_UNIT_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = defaultNotificationMessageBuilder
            .buildStorageUnitStatusChangeMessages(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        validateStorageUnitStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS,
            STORAGE_UNIT_STATUS_2, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildStorageUnitStatusChangeMessagesJsonPayloadNoMessageType() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(NO_MESSAGE_TYPE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                Collections.singletonList(new MessageHeaderDefinition(KEY, VALUE)))))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            defaultNotificationMessageBuilder
                .buildStorageUnitStatusChangeMessages(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2);
            fail();
        }
        catch (IllegalStateException illegalStateException)
        {
            assertEquals(String.format("Notification message type must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey()), illegalStateException.getMessage());
        }
    }

    @Test
    public void testBuildStorageUnitStatusChangeMessagesJsonPayloadNoOldStorageUnitStatus() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, STORAGE_UNIT_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = defaultNotificationMessageBuilder
            .buildStorageUnitStatusChangeMessages(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        validateStorageUnitStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS,
            NO_STORAGE_UNIT_STATUS, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildStorageUnitStatusChangeMessagesJsonPayloadNoSubPartitionValues() throws Exception
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
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, STORAGE_UNIT_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = defaultNotificationMessageBuilder
            .buildStorageUnitStatusChangeMessages(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        validateStorageUnitStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, expectedBusinessObjectDataKey, STORAGE_NAME,
            STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildStorageUnitStatusChangeMessagesJsonPayloadSingleSubPartitionValue() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES.subList(0, 1), NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, STORAGE_UNIT_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = defaultNotificationMessageBuilder
            .buildStorageUnitStatusChangeMessages(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        validateStorageUnitStatusChangeMessageWithJsonPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS,
            STORAGE_UNIT_STATUS_2, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildStorageUnitStatusChangeMessagesNoMessageDefinitions() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Override configuration, so there will be no notification message definitions configured in the system.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(null);
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0, defaultNotificationMessageBuilder
            .buildStorageUnitStatusChangeMessages(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2).size());

        // Override configuration, so there will be an empty list of notification message definitions configured in the system.
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions()));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0, defaultNotificationMessageBuilder
            .buildStorageUnitStatusChangeMessages(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2).size());
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
    public void testBuildSystemMonitorResponseInvalidXmlRequestPayload() throws Exception
    {
        // Override the configuration to remove the XPath expressions when building the system monitor response.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(),
            SYSTEM_MONITOR_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Try to get a system monitor response when request payload contains invalid XML.
            defaultNotificationMessageBuilder.buildSystemMonitorResponse(INVALID_VALUE);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Payload is not valid XML:\n%s", INVALID_VALUE), e.getMessage());
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
            assertEquals("Variable $incoming_message_correlation_id has not been set at systemMonitorResponse[line 11, column 29]", e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    /**
     * Returns a list of expected message headers.
     *
     * @param expectedUuid the expected UUID
     *
     * @return the list of message headers
     */
    private List<MessageHeader> getExpectedMessageHeaders(String expectedUuid)
    {
        List<MessageHeader> messageHeaders = new ArrayList<>();

        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_ENVIRONMENT, configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT)));
        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_MESSAGE_TYPE, MESSAGE_TYPE));
        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_MESSAGE_VERSION, MESSAGE_VERSION));
        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_SOURCE_SYSTEM, SOURCE_SYSTEM));
        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_MESSAGE_ID, expectedUuid));
        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_USER_ID, HerdDaoSecurityHelper.SYSTEM_USER));
        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_NAMESPACE, BDEF_NAMESPACE));

        return messageHeaders;
    }

    /**
     * Returns a list of message header definitions.
     *
     * @return the list of message header definitions
     */
    private List<MessageHeaderDefinition> getMessageHeaderDefinitions()
    {
        List<MessageHeaderDefinition> messageHeaderDefinitions = new ArrayList<>();

        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_ENVIRONMENT, "$herd_environment"));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_MESSAGE_TYPE, MESSAGE_TYPE));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_MESSAGE_VERSION, MESSAGE_VERSION));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_SOURCE_SYSTEM, SOURCE_SYSTEM));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_MESSAGE_ID, "$uuid"));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_USER_ID, "$username"));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_NAMESPACE, "$namespace"));

        return messageHeaderDefinitions;
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
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
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

    /**
     * Validates a storage unit status change notification message with JSON payload.
     *
     * @param expectedMessageType the expected message type
     * @param expectedMessageDestination the expected message destination
     * @param expectedBusinessObjectDataKey the expected business object data key
     * @param expectedStorageName the expected storage name
     * @param expectedNewStorageUnitStatus the expected new business object data status
     * @param expectedOldStorageUnitStatus the expected old business object data status
     * @param expectedMessageHeaders the list of expected message headers
     * @param notificationMessage the notification message to be validated
     */
    private void validateStorageUnitStatusChangeMessageWithJsonPayload(String expectedMessageType, String expectedMessageDestination,
        BusinessObjectDataKey expectedBusinessObjectDataKey, String expectedStorageName, String expectedNewStorageUnitStatus,
        String expectedOldStorageUnitStatus, List<MessageHeader> expectedMessageHeaders, NotificationMessage notificationMessage) throws IOException
    {
        assertNotNull(notificationMessage);

        assertEquals(expectedMessageType, notificationMessage.getMessageType());
        assertEquals(expectedMessageDestination, notificationMessage.getMessageDestination());

        StorageUnitStatusChangeJsonMessagePayload storageUnitStatusChangeJsonMessagePayload =
            jsonHelper.unmarshallJsonToObject(StorageUnitStatusChangeJsonMessagePayload.class, notificationMessage.getMessageText());

        assertEquals(StringUtils.length(storageUnitStatusChangeJsonMessagePayload.eventDate), StringUtils.length(HerdDateUtils.now().toString()));
        assertEquals(expectedBusinessObjectDataKey, storageUnitStatusChangeJsonMessagePayload.businessObjectDataKey);
        assertEquals(expectedStorageName, storageUnitStatusChangeJsonMessagePayload.storageName);
        assertEquals(expectedNewStorageUnitStatus, storageUnitStatusChangeJsonMessagePayload.newStorageUnitStatus);
        assertEquals(expectedOldStorageUnitStatus, storageUnitStatusChangeJsonMessagePayload.oldStorageUnitStatus);
        assertEquals(expectedMessageHeaders, notificationMessage.getMessageHeaders());
    }

    private static class StorageUnitStatusChangeJsonMessagePayload
    {
        public BusinessObjectDataKey businessObjectDataKey;

        public String eventDate;

        public String newStorageUnitStatus;

        public String oldStorageUnitStatus;

        public String storageName;
    }

    private static class BusinessObjectDataStatusChangeJsonMessagePayload
    {
        public Map<String, String> attributes;

        public BusinessObjectDataKey businessObjectDataKey;

        public String eventDate;

        public String newBusinessObjectDataStatus;

        public String oldBusinessObjectDataStatus;
    }

    private static class BusinessObjectFormatVersionChangeJsonMessagePayload
    {
        public BusinessObjectFormatKey businessObjectFormatKey;

        public String eventDate;

        public String newBusinessObjectFormatVersion;

        public String oldBusinessObjectFormatVersion;
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

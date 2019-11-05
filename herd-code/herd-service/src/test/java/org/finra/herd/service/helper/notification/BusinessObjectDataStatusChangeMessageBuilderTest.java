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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.finra.herd.model.api.xml.NotificationMessageDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinitions;
import org.finra.herd.model.dto.BusinessObjectDataStatusChangeNotificationEvent;
import org.finra.herd.model.dto.BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent;
import org.finra.herd.model.dto.BusinessObjectFormatVersionChangeNotificationEvent;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.dto.StorageUnitStatusChangeNotificationEvent;
import org.finra.herd.model.dto.UserNamespaceAuthorizationChangeNotificationEvent;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.ConfigurationEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * Tests the functionality for BusinessObjectDataStatusChangeMessageBuilder.
 */
public class BusinessObjectDataStatusChangeMessageBuilderTest extends AbstractNotificationMessageBuilderTestHelper
{
    @Autowired
    private BusinessObjectDataStatusChangeMessageBuilder businessObjectDataStatusChangeMessageBuilder;

    @Test
    public void testAddObjectPropertyToContextOnAbstractNotificationMessageBuilder()
    {
        // Create an empty context map
        Map<String, Object> context = new LinkedHashMap<>();

        // Create test property values.
        Object propertyValue = new Object();
        Object jsonEscapedPropertyValue = new Object();
        Object xmlEscapedPropertyValue = new Object();

        // Call the method under test.
        businessObjectDataStatusChangeMessageBuilder
            .addObjectPropertyToContext(context, ATTRIBUTE_NAME + SUFFIX_UNESCAPED, propertyValue, jsonEscapedPropertyValue, xmlEscapedPropertyValue);

        // Validate the results.
        assertEquals(3, CollectionUtils.size(context));
        assertEquals(propertyValue, context.get(ATTRIBUTE_NAME + SUFFIX_UNESCAPED));
        assertEquals(jsonEscapedPropertyValue, context.get(ATTRIBUTE_NAME + SUFFIX_UNESCAPED + "WithJson"));
        assertEquals(xmlEscapedPropertyValue, context.get(ATTRIBUTE_NAME + SUFFIX_UNESCAPED + "WithXml"));
    }

    @Test
    public void testAddStringPropertyToContextOnAbstractNotificationMessageBuilder()
    {
        // Create an empty context map.
        Map<String, Object> context = new LinkedHashMap<>();

        // Call the method under test.
        businessObjectDataStatusChangeMessageBuilder.addStringPropertyToContext(context, ATTRIBUTE_NAME + SUFFIX_UNESCAPED, ATTRIBUTE_VALUE + SUFFIX_UNESCAPED);

        // Validate the results.
        assertEquals(3, CollectionUtils.size(context));
        assertEquals(ATTRIBUTE_VALUE + SUFFIX_UNESCAPED, context.get(ATTRIBUTE_NAME + SUFFIX_UNESCAPED));
        assertEquals(ATTRIBUTE_VALUE + SUFFIX_ESCAPED_JSON, context.get(ATTRIBUTE_NAME + SUFFIX_UNESCAPED + "WithJson"));
        assertEquals(ATTRIBUTE_VALUE + SUFFIX_ESCAPED_XML, context.get(ATTRIBUTE_NAME + SUFFIX_UNESCAPED + "WithXml"));
    }

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
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataStatusChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
            NO_ATTRIBUTES, getExpectedMessageHeaders(uuid), result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesJsonPayloadInvalidMessageType() throws Exception
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
            new NotificationMessageDefinition(INVALID_VALUE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            businessObjectDataStatusChangeMessageBuilder
                .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Only \"%s\" notification message type is supported. Please update \"%s\" configuration entry.", MESSAGE_TYPE_SNS,
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
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
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE, AbstractServiceTest.NO_PUBLISH_FOR_FILTER));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE, AbstractServiceTest.NO_PUBLISH_FOR_FILTER));

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, attributeDefinitions, attributes);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataStatusChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
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
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, NO_MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            businessObjectDataStatusChangeMessageBuilder
                .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));
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
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataStatusChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
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
            businessObjectDataStatusChangeMessageBuilder
                .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));
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
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataStatusChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, NO_BDATA_STATUS));

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS,
            NO_BDATA_STATUS, NO_ATTRIBUTES, NO_MESSAGE_HEADERS, result.get(0));
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
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataStatusChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, expectedBusinessObjectDataKey, BDATA_STATUS,
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
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataStatusChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
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
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataStatusChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataStatusChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
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
        assertEquals(0, businessObjectDataStatusChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2)).size());

        // Override configuration, so there will be an empty list of notification message definitions configured in the system.
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions()));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0, businessObjectDataStatusChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2)).size());
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
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataStatusChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey,
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
    public void testGetBaseVelocityContextMap()
    {
        // Call the method under test.
        Map<String, Object> result = businessObjectDataStatusChangeMessageBuilder.getBaseVelocityContextMap();

        // Validate the results.
        assertEquals(11, CollectionUtils.size(result));
    }

    @Test
    public void testGetBaseVelocityContextMapHelper()
    {
        // Create names for properties that identify herd environment.
        final String herdEnvironmentKey = "herd_environment";

        // Get configuration value that identify herd environment.
        final String herdEnvironmentValue = configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT);

        // Call the method under test.
        Map<String, Object> result = businessObjectDataStatusChangeMessageBuilder
            .getBaseVelocityContextMapHelper(USER_ID + SUFFIX_UNESCAPED, herdEnvironmentKey + SUFFIX_UNESCAPED, herdEnvironmentValue + SUFFIX_UNESCAPED);

        // Validate the results.
        assertEquals(11, CollectionUtils.size(result));

        assertEquals(herdEnvironmentValue + SUFFIX_UNESCAPED, result.get(herdEnvironmentKey + SUFFIX_UNESCAPED));
        assertEquals(herdEnvironmentValue + SUFFIX_ESCAPED_JSON, result.get(herdEnvironmentKey + SUFFIX_ESCAPED_JSON + "_with_json"));
        assertEquals(herdEnvironmentValue + SUFFIX_ESCAPED_XML, result.get(herdEnvironmentKey + SUFFIX_ESCAPED_XML + "_with_xml"));

        assertEquals(USER_ID + SUFFIX_UNESCAPED, result.get("username"));
        assertEquals(USER_ID + SUFFIX_ESCAPED_JSON, result.get("usernameWithJson"));
        assertEquals(USER_ID + SUFFIX_ESCAPED_XML, result.get("usernameWithXml"));
    }

    @Test
    public void testGetBusinessObjectDataStatusChangeMessageVelocityContextMap()
    {
        // Create a business object data key with values that require JSON and XML escaping.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE + SUFFIX_UNESCAPED, BDEF_NAME + SUFFIX_UNESCAPED, FORMAT_USAGE_CODE + SUFFIX_UNESCAPED,
                FORMAT_FILE_TYPE_CODE + SUFFIX_UNESCAPED, FORMAT_VERSION, PARTITION_VALUE + SUFFIX_UNESCAPED, Lists
                .newArrayList(SUBPARTITION_VALUES.get(0) + SUFFIX_UNESCAPED, SUBPARTITION_VALUES.get(1) + SUFFIX_UNESCAPED,
                    SUBPARTITION_VALUES.get(2) + SUFFIX_UNESCAPED, SUBPARTITION_VALUES.get(3) + SUFFIX_UNESCAPED), DATA_VERSION);

        // Create a list of attributes that require JSON and XML escaping.
        // An attribute with a null value is required for code coverage.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE + SUFFIX_UNESCAPED, ATTRIBUTE_VALUE_1 + SUFFIX_UNESCAPED));
        attributes.add(new Attribute(ATTRIBUTE_NAME_2_MIXED_CASE + SUFFIX_UNESCAPED, ATTRIBUTE_VALUE_2 + SUFFIX_UNESCAPED));
        attributes.add(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE + SUFFIX_UNESCAPED, null));

        // Create and persist a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create and persist business object data attribute entities.
        for (Attribute attribute : attributes)
        {
            businessObjectDataAttributeDaoTestHelper
                .createBusinessObjectDataAttributeEntity(businessObjectDataEntity, attribute.getName(), attribute.getValue());
        }

        // Add business object data attribute definitions to the business object format.
        for (Attribute attribute : attributes)
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectDataAttributeDefinitionEntity(businessObjectDataEntity.getBusinessObjectFormat(), attribute.getName(), PUBLISH_ATTRIBUTE, NO_PUBLISH_FOR_FILTER);
        }

        // Call the method under test.
        Map<String, Object> result = businessObjectDataStatusChangeMessageBuilder.getNotificationMessageVelocityContextMap(
            new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS_2 + SUFFIX_UNESCAPED, BDATA_STATUS + SUFFIX_UNESCAPED));

        // Create a map of expected businessObjectDataAttributes.
        Map<String, String> expectedBusinessObjectDataAttributes = new LinkedHashMap<>();
        expectedBusinessObjectDataAttributes.put(ATTRIBUTE_NAME_1_MIXED_CASE + SUFFIX_UNESCAPED, ATTRIBUTE_VALUE_1 + SUFFIX_UNESCAPED);
        expectedBusinessObjectDataAttributes.put(ATTRIBUTE_NAME_2_MIXED_CASE + SUFFIX_UNESCAPED, ATTRIBUTE_VALUE_2 + SUFFIX_UNESCAPED);
        expectedBusinessObjectDataAttributes.put(ATTRIBUTE_NAME_3_MIXED_CASE + SUFFIX_UNESCAPED, null);

        // Create a map of expected businessObjectDataAttributes.
        Map<String, String> expectedBusinessObjectDataAttributesWithJson = new LinkedHashMap<>();
        expectedBusinessObjectDataAttributesWithJson.put(ATTRIBUTE_NAME_1_MIXED_CASE + SUFFIX_ESCAPED_JSON, ATTRIBUTE_VALUE_1 + SUFFIX_ESCAPED_JSON);
        expectedBusinessObjectDataAttributesWithJson.put(ATTRIBUTE_NAME_2_MIXED_CASE + SUFFIX_ESCAPED_JSON, ATTRIBUTE_VALUE_2 + SUFFIX_ESCAPED_JSON);
        expectedBusinessObjectDataAttributesWithJson.put(ATTRIBUTE_NAME_3_MIXED_CASE + SUFFIX_ESCAPED_JSON, null);

        // Create a map of expected XML escaped businessObjectDataAttributes.
        Map<String, String> expectedBusinessObjectDataAttributesWithXml = new LinkedHashMap<>();
        expectedBusinessObjectDataAttributesWithXml.put(ATTRIBUTE_NAME_1_MIXED_CASE + SUFFIX_ESCAPED_XML, ATTRIBUTE_VALUE_1 + SUFFIX_ESCAPED_XML);
        expectedBusinessObjectDataAttributesWithXml.put(ATTRIBUTE_NAME_2_MIXED_CASE + SUFFIX_ESCAPED_XML, ATTRIBUTE_VALUE_2 + SUFFIX_ESCAPED_XML);
        expectedBusinessObjectDataAttributesWithXml.put(ATTRIBUTE_NAME_3_MIXED_CASE + SUFFIX_ESCAPED_XML, null);

        // Create an expected JSON escaped business object data key.
        BusinessObjectDataKey expectedBusinessObjectDataKeyWithJson =
            new BusinessObjectDataKey(BDEF_NAMESPACE + SUFFIX_ESCAPED_JSON, BDEF_NAME + SUFFIX_ESCAPED_JSON, FORMAT_USAGE_CODE + SUFFIX_ESCAPED_JSON,
                FORMAT_FILE_TYPE_CODE + SUFFIX_ESCAPED_JSON, FORMAT_VERSION, PARTITION_VALUE + SUFFIX_ESCAPED_JSON, Lists
                .newArrayList(SUBPARTITION_VALUES.get(0) + SUFFIX_ESCAPED_JSON, SUBPARTITION_VALUES.get(1) + SUFFIX_ESCAPED_JSON,
                    SUBPARTITION_VALUES.get(2) + SUFFIX_ESCAPED_JSON, SUBPARTITION_VALUES.get(3) + SUFFIX_ESCAPED_JSON), DATA_VERSION);

        // Create an expected XML escaped business object data key.
        BusinessObjectDataKey expectedBusinessObjectDataKeyWithXml =
            new BusinessObjectDataKey(BDEF_NAMESPACE + SUFFIX_ESCAPED_XML, BDEF_NAME + SUFFIX_ESCAPED_XML, FORMAT_USAGE_CODE + SUFFIX_ESCAPED_XML,
                FORMAT_FILE_TYPE_CODE + SUFFIX_ESCAPED_XML, FORMAT_VERSION, PARTITION_VALUE + SUFFIX_ESCAPED_XML, Lists
                .newArrayList(SUBPARTITION_VALUES.get(0) + SUFFIX_ESCAPED_XML, SUBPARTITION_VALUES.get(1) + SUFFIX_ESCAPED_XML,
                    SUBPARTITION_VALUES.get(2) + SUFFIX_ESCAPED_XML, SUBPARTITION_VALUES.get(3) + SUFFIX_ESCAPED_XML), DATA_VERSION);

        // Validate the results.
        assertEquals(16, CollectionUtils.size(result));

        assertEquals(businessObjectDataKey, result.get("businessObjectDataKey"));
        assertEquals(expectedBusinessObjectDataKeyWithJson, result.get("businessObjectDataKeyWithJson"));
        assertEquals(expectedBusinessObjectDataKeyWithXml, result.get("businessObjectDataKeyWithXml"));

        assertEquals(BDATA_STATUS_2 + SUFFIX_UNESCAPED, result.get("newBusinessObjectDataStatus"));
        assertEquals(BDATA_STATUS_2 + SUFFIX_ESCAPED_JSON, result.get("newBusinessObjectDataStatusWithJson"));
        assertEquals(BDATA_STATUS_2 + SUFFIX_ESCAPED_XML, result.get("newBusinessObjectDataStatusWithXml"));

        assertEquals(BDATA_STATUS + SUFFIX_UNESCAPED, result.get("oldBusinessObjectDataStatus"));
        assertEquals(BDATA_STATUS + SUFFIX_ESCAPED_JSON, result.get("oldBusinessObjectDataStatusWithJson"));
        assertEquals(BDATA_STATUS + SUFFIX_ESCAPED_XML, result.get("oldBusinessObjectDataStatusWithXml"));

        assertEquals(businessObjectDataEntity.getId(), result.get("businessObjectDataId"));

        assertEquals(expectedBusinessObjectDataAttributes, result.get("businessObjectDataAttributes"));
        assertEquals(expectedBusinessObjectDataAttributesWithJson, result.get("businessObjectDataAttributesWithJson"));
        assertEquals(expectedBusinessObjectDataAttributesWithXml, result.get("businessObjectDataAttributesWithXml"));

        assertEquals(BDEF_NAMESPACE + SUFFIX_UNESCAPED, result.get("namespace"));
        assertEquals(BDEF_NAMESPACE + SUFFIX_ESCAPED_JSON, result.get("namespaceWithJson"));
        assertEquals(BDEF_NAMESPACE + SUFFIX_ESCAPED_XML, result.get("namespaceWithXml"));
    }

    @Test
    public void testGetEventTypeMessageDefinitionKeyMapOnAbstractNotificationMessageBuilder()
    {
        Map<Class<?>, ConfigurationValue> map = businessObjectDataStatusChangeMessageBuilder.getEventTypeMessageDefinitionKeyMap();
        assertEquals(5, map.size());
        assertEquals(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS,
            map.get(BusinessObjectDataStatusChangeNotificationEvent.class));
        assertEquals(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS,
            map.get(BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent.class));
        assertEquals(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS,
            map.get(BusinessObjectFormatVersionChangeNotificationEvent.class));
        assertEquals(ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS,
            map.get(StorageUnitStatusChangeNotificationEvent.class));
        assertEquals(ConfigurationValue.HERD_NOTIFICATION_USER_NAMESPACE_AUTHORIZATION_CHANGE_MESSAGE_DEFINITIONS,
            map.get(UserNamespaceAuthorizationChangeNotificationEvent.class));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesFilterAttributeHeader() throws Exception
    {
        // Create a list of attributes.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_2));

        // Create a list of attribute definitions.
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE, AbstractServiceTest.NO_PUBLISH_FOR_FILTER));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE, AbstractServiceTest.PUBLISH_FOR_FILTER));

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
                businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, attributeDefinitions, attributes);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
                new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                        BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataStatusChangeMessageBuilder
                .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(8, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));

        List<MessageHeader> expectedMessageHeaders = getExpectedMessageHeaders(uuid);
        expectedMessageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_FILTER_ATTRIBUTE_VALUE, AbstractServiceTest.ATTRIBUTE_VALUE_2));

        businessObjectDataServiceTestHelper
                .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey,
                        businessObjectDataEntity.getId(), HerdDaoSecurityHelper.SYSTEM_USER, BDATA_STATUS, BDATA_STATUS_2,
                        attributes, expectedMessageHeaders, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataStatusChangeMessagesNoFilterAttributeHeader() throws Exception
    {
        // Create a list of attributes.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_2));

        // Create a list of attribute definitions.
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE, AbstractServiceTest.NO_PUBLISH_FOR_FILTER));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE, AbstractServiceTest.NO_PUBLISH_FOR_FILTER));

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
                businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, attributeDefinitions, attributes);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
                new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                        BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataStatusChangeMessageBuilder
                .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));

        List<MessageHeader> expectedMessageHeaders = getExpectedMessageHeaders(uuid);

        businessObjectDataServiceTestHelper
                .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey,
                        businessObjectDataEntity.getId(), HerdDaoSecurityHelper.SYSTEM_USER, BDATA_STATUS, BDATA_STATUS_2,
                        attributes, expectedMessageHeaders, result.get(0));
    }

    @Test(expected = IllegalStateException.class)
    public void testBuildBusinessObjectDataStatusChangeMessagesMoreThanOneFilterAttributeHeader() throws Exception
    {
        // Create a list of attributes.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_2));

        // Create a list of attribute definitions.
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE, AbstractServiceTest.PUBLISH_FOR_FILTER));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE, AbstractServiceTest.PUBLISH_FOR_FILTER));

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
                businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, attributeDefinitions, attributes);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
                new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                        BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        businessObjectDataStatusChangeMessageBuilder
                .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));
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
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataStatusChangeMessageBuilder
            .buildNotificationMessages(new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2));

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey,
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

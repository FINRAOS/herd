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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.NotificationMessageDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinitions;
import org.finra.herd.model.dto.AttributeDto;
import org.finra.herd.model.dto.BusinessObjectDataPublishedAttributesChangeNotificationEvent;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.ConfigurationEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * Tests the functionality for UserNamespaceAuthorizationChangeMessageBuilder.
 */
public class BusinessObjectDataPublishedAttributeChangeMessageBuilderTest extends AbstractNotificationMessageBuilderTestHelper
{
    @Autowired
    private BusinessObjectDataPublishedAttributesChangeMessageBuilder businessObjectDataPublishedAttributesChangeMessageBuilder;

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
        businessObjectDataPublishedAttributesChangeMessageBuilder
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
        businessObjectDataPublishedAttributesChangeMessageBuilder
            .addStringPropertyToContext(context, ATTRIBUTE_NAME + SUFFIX_UNESCAPED, ATTRIBUTE_VALUE + SUFFIX_UNESCAPED);

        // Validate the results.
        assertEquals(3, CollectionUtils.size(context));
        assertEquals(ATTRIBUTE_VALUE + SUFFIX_UNESCAPED, context.get(ATTRIBUTE_NAME + SUFFIX_UNESCAPED));
        assertEquals(ATTRIBUTE_VALUE + SUFFIX_ESCAPED_JSON, context.get(ATTRIBUTE_NAME + SUFFIX_UNESCAPED + "WithJson"));
        assertEquals(ATTRIBUTE_VALUE + SUFFIX_ESCAPED_XML, context.get(ATTRIBUTE_NAME + SUFFIX_UNESCAPED + "WithXml"));
    }

    @Test
    public void testBuildBusinessObjectDataPublishedAttributesChangeJsonMessagePayload() throws Exception
    {
        // Create a list of attributes.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1));

        // Create a list of attribute definitions.
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE,
            AbstractServiceTest.NO_PUBLISH_FOR_FILTER));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE,
            AbstractServiceTest.NO_PUBLISH_FOR_FILTER));

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, attributeDefinitions, attributes);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Create a list of old published business object data attributes.
        List<AttributeDto> oldPublishedBusinessObjectDataAttributes =
            Arrays.asList(new AttributeDto(ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2), new AttributeDto(ATTRIBUTE_NAME_3, ATTRIBUTE_VALUE_3));

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataPublishedAttributesChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes));

        // Create a map of expected old business object data attributes.
        Map<String, String> expectedOldBusinessObjectDataAttributes = new HashMap<>();
        for (AttributeDto attributeDto : oldPublishedBusinessObjectDataAttributes)
        {
            expectedOldBusinessObjectDataAttributes.put(attributeDto.getName(), attributeDto.getValue());
        }

        // Create a map of expected new business object data attributes.
        Map<String, String> expectedNewBusinessObjectDataAttributes = new HashMap<>();
        for (Attribute attribute : attributes)
        {
            expectedNewBusinessObjectDataAttributes.put(attribute.getName(), attribute.getValue());
        }

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        validateBusinessObjectDataPublishedAttributeChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey,
            expectedNewBusinessObjectDataAttributes, expectedOldBusinessObjectDataAttributes, getExpectedMessageHeaders(uuid), result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataPublishedAttributesChangeJsonMessagePayloadMultipleAttributes() throws Exception
    {
        // Create a list of attributes.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_2));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_3_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_3));

        // Create a list of attribute definitions.
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE,
            AbstractServiceTest.NO_PUBLISH_FOR_FILTER));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE,
            AbstractServiceTest.NO_PUBLISH_FOR_FILTER));

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, attributeDefinitions, attributes);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Create a list of old published business object data attributes.
        List<AttributeDto> oldPublishedBusinessObjectDataAttributes =
            Arrays.asList(new AttributeDto(ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2), new AttributeDto(ATTRIBUTE_NAME_3, ATTRIBUTE_VALUE_3));

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataPublishedAttributesChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes));

        // Create a map of expected old business object data attributes.
        Map<String, String> expectedOldBusinessObjectDataAttributes = new HashMap<>();
        for (AttributeDto attributeDto : oldPublishedBusinessObjectDataAttributes)
        {
            expectedOldBusinessObjectDataAttributes.put(attributeDto.getName(), attributeDto.getValue());
        }

        // Create a map of expected new business object data attributes.
        Map<String, String> expectedNewBusinessObjectDataAttributes = new HashMap<>();
        for (Attribute attribute : attributes)
        {
            expectedNewBusinessObjectDataAttributes.put(attribute.getName(), attribute.getValue());
        }
        // Remove it since no publishable flag for ATTRIBUTE_NAME_3_MIXED_CASE.
        expectedNewBusinessObjectDataAttributes.remove(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        validateBusinessObjectDataPublishedAttributeChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey,
            expectedNewBusinessObjectDataAttributes, expectedOldBusinessObjectDataAttributes, getExpectedMessageHeaders(uuid), result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataPublishedAttributesChangeJsonMessagePayloadNoOldBusinessObjectDataAttributes() throws Exception
    {
        // Create a list of attributes.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_2));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_3_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_3));

        // Create a list of attribute definitions.
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE,
            AbstractServiceTest.NO_PUBLISH_FOR_FILTER));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE,
            AbstractServiceTest.NO_PUBLISH_FOR_FILTER));

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, attributeDefinitions, attributes);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Create a list of old published business object data attributes.
        List<AttributeDto> oldPublishedBusinessObjectDataAttributes = null;

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, getMessageHeaderDefinitions())))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataPublishedAttributesChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes));

        // Create a map of expected new business object data attributes.
        Map<String, String> expectedNewBusinessObjectDataAttributes = new HashMap<>();
        for (Attribute attribute : attributes)
        {
            expectedNewBusinessObjectDataAttributes.put(attribute.getName(), attribute.getValue());
        }
        // Remove it since no publishable flag for ATTRIBUTE_NAME_3_MIXED_CASE.
        expectedNewBusinessObjectDataAttributes.remove(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3);

        // Validate the notification message.
        // assertEquals(1, result);
        assertEquals(1, CollectionUtils.size(result));
        assertEquals(7, CollectionUtils.size(result.get(0).getMessageHeaders()));
        String uuid = result.get(0).getMessageHeaders().get(4).getValue();
        assertEquals(UUID.randomUUID().toString().length(), StringUtils.length(uuid));
        validateBusinessObjectDataPublishedAttributeChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey,
            expectedNewBusinessObjectDataAttributes, NO_OLD_BUSINESS_OBJECT_DATA_ATTRIBUTES, getExpectedMessageHeaders(uuid), result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataPublishedAttributesChangeJsonMessageNoMessageType() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Create a list of old published business object data attributes.
        List<AttributeDto> oldPublishedBusinessObjectDataAttributes =
            Arrays.asList(new AttributeDto(ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2), new AttributeDto(ATTRIBUTE_NAME_3, ATTRIBUTE_VALUE_3));

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(NO_MESSAGE_TYPE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            businessObjectDataPublishedAttributesChangeMessageBuilder.buildNotificationMessages(
                new BusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message type must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildBusinessObjectDataPublishedAttributesChangeJsonMessageInvalidMessageType() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Create a list of old published business object data attributes.
        List<AttributeDto> oldPublishedBusinessObjectDataAttributes =
            Arrays.asList(new AttributeDto(ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2), new AttributeDto(ATTRIBUTE_NAME_3, ATTRIBUTE_VALUE_3));

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(INVALID_VALUE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            businessObjectDataPublishedAttributesChangeMessageBuilder.buildNotificationMessages(
                new BusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Only \"%s\" notification message type is supported. Please update \"%s\" configuration entry.", MESSAGE_TYPE_SNS,
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildBusinessObjectDataPublishedAttributesChangeJsonMessageNoMessageHeaders() throws Exception
    {
        // Create a list of attributes.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1));

        // Create a list of attribute definitions.
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE,
            AbstractServiceTest.NO_PUBLISH_FOR_FILTER));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE,
            AbstractServiceTest.NO_PUBLISH_FOR_FILTER));

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, attributeDefinitions, attributes);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Create a list of old published business object data attributes.
        List<AttributeDto> oldPublishedBusinessObjectDataAttributes =
            Arrays.asList(new AttributeDto(ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2), new AttributeDto(ATTRIBUTE_NAME_3, ATTRIBUTE_VALUE_3));

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataPublishedAttributesChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes));

        // Create a map of expected old business object data attributes.
        Map<String, String> expectedOldBusinessObjectDataAttributes = new HashMap<>();
        for (AttributeDto attributeDto : oldPublishedBusinessObjectDataAttributes)
        {
            expectedOldBusinessObjectDataAttributes.put(attributeDto.getName(), attributeDto.getValue());
        }

        // Create a map of expected new business object data attributes.
        Map<String, String> expectedNewBusinessObjectDataAttributes = new HashMap<>();
        for (Attribute attribute : attributes)
        {
            expectedNewBusinessObjectDataAttributes.put(attribute.getName(), attribute.getValue());
        }

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataPublishedAttributeChangeMessageWithJsonPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey,
            expectedNewBusinessObjectDataAttributes, expectedOldBusinessObjectDataAttributes, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataPublishedAttributesChangeJsonMessageNoMessageDestination() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Create a list of old published business object data attributes.
        List<AttributeDto> oldPublishedBusinessObjectDataAttributes =
            Arrays.asList(new AttributeDto(ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2), new AttributeDto(ATTRIBUTE_NAME_3, ATTRIBUTE_VALUE_3));

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, NO_MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message.
        try
        {
            businessObjectDataPublishedAttributesChangeMessageBuilder.buildNotificationMessages(
                new BusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message destination must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testBuildBusinessObjectDataPublishedAttributesChangeJsonMessageNoMessageDefinitions() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Create a list of old published business object data attributes.
        List<AttributeDto> oldPublishedBusinessObjectDataAttributes =
            Arrays.asList(new AttributeDto(ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2), new AttributeDto(ATTRIBUTE_NAME_3, ATTRIBUTE_VALUE_3));

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(null);
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0, businessObjectDataPublishedAttributesChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes)).size());

        // Override configuration, so there will be an empty list of notification message definitions configured in the system.
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions()));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0, businessObjectDataPublishedAttributesChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes)).size());
    }

    @Test
    public void testBuildBusinessObjectDataPublishedAttributesChangeXmlMessagePayloadMultipleAttributes() throws Exception
    {
        // Create a list of attributes.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_2));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_3_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_3));

        // Create a list of attribute definitions.
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE,
            AbstractServiceTest.NO_PUBLISH_FOR_FILTER));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE,
            AbstractServiceTest.NO_PUBLISH_FOR_FILTER));

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, attributeDefinitions, attributes);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Create a list of old published business object data attributes.
        List<AttributeDto> oldPublishedBusinessObjectDataAttributes =
            Arrays.asList(new AttributeDto(ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2), new AttributeDto(ATTRIBUTE_NAME_3, ATTRIBUTE_VALUE_3));

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataPublishedAttributesChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes));

        // Create a map of expected old business object data attributes.
        Map<String, String> expectedOldBusinessObjectDataAttributes = new HashMap<>();
        for (AttributeDto attributeDto : oldPublishedBusinessObjectDataAttributes)
        {
            expectedOldBusinessObjectDataAttributes.put(attributeDto.getName(), attributeDto.getValue());
        }

        // Create a map of expected new business object data attributes.
        Map<String, String> expectedNewBusinessObjectDataAttributes = new HashMap<>();
        for (Attribute attribute : attributes)
        {
            expectedNewBusinessObjectDataAttributes.put(attribute.getName(), attribute.getValue());
        }
        // Remove it since no publishable flag for ATTRIBUTE_NAME_3_MIXED_CASE.
        expectedNewBusinessObjectDataAttributes.remove(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataPublishedAttributeChangeMessageWithXmlPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey,
            HerdDaoSecurityHelper.SYSTEM_USER, expectedNewBusinessObjectDataAttributes, expectedOldBusinessObjectDataAttributes, NO_MESSAGE_HEADERS,
            result.get(0));
    }

    @Test
    public void testBuildBusinessObjectDataPublishedAttributesChangeXmlMessagePayloadNoOldAttributes() throws Exception
    {
        // Create a list of attributes.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_2));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_3_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_3));

        // Create a list of attribute definitions.
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE,
            AbstractServiceTest.NO_PUBLISH_FOR_FILTER));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE,
            AbstractServiceTest.NO_PUBLISH_FOR_FILTER));

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, attributeDefinitions, attributes);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Create a list of old published business object data attributes.
        List<AttributeDto> oldPublishedBusinessObjectDataAttributes = null;

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_DATA_PUBLISHED_ATTRIBUTES_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Build a notification message.
        List<NotificationMessage> result = businessObjectDataPublishedAttributesChangeMessageBuilder.buildNotificationMessages(
            new BusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes));

        // Create a map of expected new business object data attributes.
        Map<String, String> expectedNewBusinessObjectDataAttributes = new HashMap<>();
        for (Attribute attribute : attributes)
        {
            expectedNewBusinessObjectDataAttributes.put(attribute.getName(), attribute.getValue());
        }
        // Remove it since no publishable flag for ATTRIBUTE_NAME_3_MIXED_CASE.
        expectedNewBusinessObjectDataAttributes.remove(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3);

        // Validate the notification message.
        assertEquals(1, CollectionUtils.size(result));
        validateBusinessObjectDataPublishedAttributeChangeMessageWithXmlPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey,
            HerdDaoSecurityHelper.SYSTEM_USER, expectedNewBusinessObjectDataAttributes, NO_OLD_BUSINESS_OBJECT_DATA_ATTRIBUTES, NO_MESSAGE_HEADERS,
            result.get(0));
    }

    /**
     * Validates a business object data published attributes change notification message with JSON payload.
     *
     * @param expectedMessageType                     the expected message type
     * @param expectedMessageDestination              the expected message destination
     * @param expectedBusinessObjectDataKey           the expected business object data key
     * @param expectedNewBusinessObjectDataAttributes the expected new business object data attributes
     * @param expectedOldBusinessObjectDataAttributes the expected old business object data attributes
     * @param expectedMessageHeaders                  the list of expected message headers
     * @param notificationMessage                     the notification message to be validated
     */
    private void validateBusinessObjectDataPublishedAttributeChangeMessageWithJsonPayload(String expectedMessageType, String expectedMessageDestination,
        BusinessObjectDataKey expectedBusinessObjectDataKey, Map<String, String> expectedNewBusinessObjectDataAttributes,
        Map<String, String> expectedOldBusinessObjectDataAttributes, List<MessageHeader> expectedMessageHeaders, NotificationMessage notificationMessage)
        throws IOException
    {
        assertNotNull(notificationMessage);
        assertEquals(expectedMessageType, notificationMessage.getMessageType());
        assertEquals(expectedMessageDestination, notificationMessage.getMessageDestination());

        BusinessObjectDataAttributesChangeJsonMessagePayload businessObjectDataAttributesChangeJsonMessagePayload =
            jsonHelper.unmarshallJsonToObject(BusinessObjectDataAttributesChangeJsonMessagePayload.class, notificationMessage.getMessageText());

        assertEquals(StringUtils.length(businessObjectDataAttributesChangeJsonMessagePayload.eventDate), StringUtils.length(HerdDateUtils.now().toString()));
        assertEquals(expectedBusinessObjectDataKey, businessObjectDataAttributesChangeJsonMessagePayload.businessObjectDataKey);
        assertEquals(expectedNewBusinessObjectDataAttributes, businessObjectDataAttributesChangeJsonMessagePayload.newBusinessObjectDataAttributes);
        assertEquals(expectedOldBusinessObjectDataAttributes, businessObjectDataAttributesChangeJsonMessagePayload.oldBusinessObjectDataAttributes);

        assertEquals(expectedMessageHeaders, notificationMessage.getMessageHeaders());
    }

    /**
     * Validates a business object data published attributes change notification message with XML payload.
     *
     * @param expectedMessageType                     the expected message type
     * @param expectedMessageDestination              the expected message destination
     * @param expectedBusinessObjectDataKey           the expected business object data key
     * @param expectedUsername                        the expected username
     * @param expectedNewBusinessObjectDataAttributes the expected new business object data attributes
     * @param expectedOldBusinessObjectDataAttributes the expected old business object data attributes
     * @param expectedMessageHeaders                  the list of expected message headers
     * @param notificationMessage                     the notification message to be validated
     */
    private void validateBusinessObjectDataPublishedAttributeChangeMessageWithXmlPayload(String expectedMessageType, String expectedMessageDestination,
        BusinessObjectDataKey expectedBusinessObjectDataKey, String expectedUsername, Map<String, String> expectedNewBusinessObjectDataAttributes,
        Map<String, String> expectedOldBusinessObjectDataAttributes, List<MessageHeader> expectedMessageHeaders, NotificationMessage notificationMessage)
    {
        assertNotNull(notificationMessage);

        assertEquals(expectedMessageType, notificationMessage.getMessageType());
        assertEquals(expectedMessageDestination, notificationMessage.getMessageDestination());

        String messageText = notificationMessage.getMessageText();

        validateXmlFieldPresent(messageText, "triggered-by-username", expectedUsername);
        validateXmlFieldPresent(messageText, "context-message-type", "testDomain/testApplication/BusinessObjectDataPublishedAttributesChanged");

        if (expectedNewBusinessObjectDataAttributes == null)
        {
            validateXmlFieldNotPresent(messageText, "oldBusinessObjectDataAttributes");
        }
        else
        {

            for (String name : expectedNewBusinessObjectDataAttributes.keySet())
            {
                // Validate each expected "<newBusinessObjectDataAttribute>" XML tag.
                // Please note that null attribute value is expected to be published as an empty string.
                validateXmlFieldPresent(messageText, "newBusinessObjectDataAttribute", "name", name,
                    expectedNewBusinessObjectDataAttributes.get(name) == null ? AbstractServiceTest.EMPTY_STRING :
                        expectedNewBusinessObjectDataAttributes.get(name));
            }
        }

        if (expectedOldBusinessObjectDataAttributes == null)
        {
            validateXmlFieldNotPresent(messageText, "oldBusinessObjectDataAttributes");
        }
        else
        {
            for (String name : expectedOldBusinessObjectDataAttributes.keySet())
            {
                // Validate each expected "<oldBusinessObjectDataAttribute>" XML tag.
                // Please note that null attribute value is expected to be published as an empty string.
                validateXmlFieldPresent(messageText, "oldBusinessObjectDataAttribute", "name", name,
                    expectedOldBusinessObjectDataAttributes.get(name) == null ? AbstractServiceTest.EMPTY_STRING :
                        expectedOldBusinessObjectDataAttributes.get(name));
            }
        }

        validateXmlFieldPresent(messageText, "namespace", expectedBusinessObjectDataKey.getNamespace());
        validateXmlFieldPresent(messageText, "businessObjectDefinitionName", expectedBusinessObjectDataKey.getBusinessObjectDefinitionName());
        validateXmlFieldPresent(messageText, "businessObjectFormatUsage", expectedBusinessObjectDataKey.getBusinessObjectFormatUsage());
        validateXmlFieldPresent(messageText, "businessObjectFormatFileType", expectedBusinessObjectDataKey.getBusinessObjectFormatFileType());
        validateXmlFieldPresent(messageText, "businessObjectFormatVersion", expectedBusinessObjectDataKey.getBusinessObjectFormatVersion());

        assertEquals(expectedMessageHeaders, notificationMessage.getMessageHeaders());
    }

    /**
     * Validates that a specified XML tag and value are present in the message.
     *
     * @param message    the XML message.
     * @param xmlTagName the XML tag name (without the '<', '/', and '>' characters).
     * @param value      the value of the data for the tag.
     */
    private void validateXmlFieldPresent(String message, String xmlTagName, Object value)
    {
        assertTrue(xmlTagName + " \"" + value + "\" expected, but not found.",
            message.contains("<" + xmlTagName + ">" + (value == null ? null : value.toString()) + "</" + xmlTagName + ">"));
    }

    /**
     * Validates that a specified XML opening and closing set of tags are not present in the message.
     *
     * @param message    the XML message.
     * @param xmlTagName the XML tag name (without the '<', '/', and '>' characters).
     */
    private void validateXmlFieldNotPresent(String message, String xmlTagName)
    {
        for (String xmlTag : Arrays.asList(String.format("<%s>", xmlTagName), String.format("</%s>", xmlTagName)))
        {
            assertTrue(String.format("%s tag not expected, but found.", xmlTag), !message.contains(xmlTag));
        }
    }

    /**
     * Validates that the specified XML tag with the specified tag attribute and tag value is present in the message.
     *
     * @param message              the XML message.
     * @param xmlTagName           the XML tag name (without the '<', '/', and '>' characters).
     * @param xmlTagAttributeName  the tag attribute name.
     * @param xmlTagAttributeValue the tag attribute value.
     * @param xmlTagValue          the value of the data for the tag.
     */
    private void validateXmlFieldPresent(String message, String xmlTagName, String xmlTagAttributeName, String xmlTagAttributeValue, Object xmlTagValue)
    {
        assertTrue(String.format("<%s> is expected, but not found or does not match expected attribute and/or value.", xmlTagName), message.contains(String
            .format("<%s %s=\"%s\">%s</%s>", xmlTagName, xmlTagAttributeName, xmlTagAttributeValue, xmlTagValue == null ? null : xmlTagValue.toString(),
                xmlTagName)));
    }

    private static class BusinessObjectDataAttributesChangeJsonMessagePayload
    {
        public Map<String, String> newBusinessObjectDataAttributes;
        public Map<String, String> oldBusinessObjectDataAttributes;
        public BusinessObjectDataKey businessObjectDataKey;
        public String eventDate;
    }
}

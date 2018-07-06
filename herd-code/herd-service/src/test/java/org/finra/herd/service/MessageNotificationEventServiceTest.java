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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;

import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.MessageHeaderDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinitions;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.ConfigurationEntity;
import org.finra.herd.model.jpa.MessageTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.StorageEntity;

/**
 * This class tests functionality within the message notification event service.
 */
public class MessageNotificationEventServiceTest extends AbstractServiceTest
{
    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEvent() throws Exception
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
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Trigger the notification.
        List<NotificationMessage> result = messageNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey,
                businessObjectDataEntity.getId(), HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID, Collections.singletonList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3)),
                NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEventAttributeDefinitionCaseInsensitivity() throws Exception
    {
        // Create a business object data entity with a publishable attributes that have relative attribute definitions in upper and lower cases.
        List<AttributeDefinition> testAttributeDefinitions = Arrays
            .asList(new AttributeDefinition(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), PUBLISH_ATTRIBUTE),
                new AttributeDefinition(ATTRIBUTE_NAME_2_MIXED_CASE.toLowerCase(), PUBLISH_ATTRIBUTE));
        List<Attribute> testAttributes = Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_NAME_1_MIXED_CASE),
            new Attribute(ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_NAME_2_MIXED_CASE));
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, testAttributeDefinitions, testAttributes);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Trigger the notification.
        List<NotificationMessage> result = messageNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey,
                businessObjectDataEntity.getId(), HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID, testAttributes, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEventBusinessObjectDataAttributeNullValue() throws Exception
    {
        // Create a business object data entity with a publishable attributes that has a null value.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES,
            Collections.singletonList(new AttributeDefinition(ATTRIBUTE_NAME_1_MIXED_CASE, PUBLISH_ATTRIBUTE)),
            Collections.singletonList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, null)));

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Trigger the notification.
        List<NotificationMessage> result = messageNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey,
                businessObjectDataEntity.getId(), HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID, Collections.singletonList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, null)), NO_MESSAGE_HEADERS,
                result.get(0));
    }

    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEventBusinessObjectDataAttributeSpecialValues() throws Exception
    {
        // Create a business object data entity with a publishable attributes that have special values such as a blank text and an empty string.
        List<Attribute> testAttributes =
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT), new Attribute(ATTRIBUTE_NAME_2_MIXED_CASE, EMPTY_STRING));
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper.createTestValidBusinessObjectData(SUBPARTITION_VALUES, Arrays
            .asList(new AttributeDefinition(ATTRIBUTE_NAME_1_MIXED_CASE, PUBLISH_ATTRIBUTE),
                new AttributeDefinition(ATTRIBUTE_NAME_2_MIXED_CASE, PUBLISH_ATTRIBUTE)), testAttributes);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Trigger the notification.
        List<NotificationMessage> result = messageNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey,
                businessObjectDataEntity.getId(), HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID, testAttributes, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEventHerdSqsNotificationNotEnabled() throws Exception
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
            // Trigger the notification.
            List<NotificationMessage> result = messageNotificationEventService
                .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                    BusinessObjectDataStatusEntity.UPLOADING);

            // Validate the results.
            assertTrue(CollectionUtils.isEmpty(result));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEventNoBusinessObjectDataAttributeDefinitions() throws Exception
    {
        // Create a business object data entity with attributes, but without any attribute definitions.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Trigger the notification.
        List<NotificationMessage> result = messageNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey,
                businessObjectDataEntity.getId(), HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID, NO_ATTRIBUTES, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEventNoBusinessObjectDataAttributes() throws Exception
    {
        // Create a business object data entity with attribute definitions, but without any attributes.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createTestValidBusinessObjectData(SUBPARTITION_VALUES, businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), NO_ATTRIBUTES);

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Trigger the notification.
        List<NotificationMessage> result = messageNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey,
                businessObjectDataEntity.getId(), HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID, NO_ATTRIBUTES, NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEventNoMessageDestination() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper.createTestValidBusinessObjectData();

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration, so there will be no message definition specified in the relative notification message definition.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, NO_MESSAGE_DESTINATION, MESSAGE_TEXT,
                Collections.singletonList(new MessageHeaderDefinition(KEY, VALUE)))))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to trigger the notification.
        try
        {
            messageNotificationEventService.processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.UPLOADING);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message destination must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEventNoMessageHeaderDefinitions() throws Exception
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
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);


        // Trigger the notification.
        List<NotificationMessage> result = messageNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.UPLOADING);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey,
                businessObjectDataEntity.getId(), HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.UPLOADING, Collections.singletonList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3)),
                NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEventNoMessageType() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper.createTestValidBusinessObjectData();

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration, so there will be no message type specified in the relative notification message definition.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(NO_MESSAGE_TYPE, MESSAGE_DESTINATION, MESSAGE_TEXT,
                Collections.singletonList(new MessageHeaderDefinition(KEY, VALUE)))))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to trigger the notification.
        try
        {
            messageNotificationEventService.processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.UPLOADING);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message type must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEventNoMessageVelocityTemplate() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper.createTestValidBusinessObjectData();

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration, so there will be no velocity template specified in the relative notification message definition.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, NO_MESSAGE_VELOCITY_TEMPLATE,
                Collections.singletonList(new MessageHeaderDefinition(KEY, VALUE)))))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Trigger the notification.
        List<NotificationMessage> result = messageNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.UPLOADING);

        // Validate the results.
        assertEquals(0, CollectionUtils.size(result));
    }

    @Test
    public void testProcessBusinessObjectDataStatusChangeNotificationEventNoOldStatus() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper.createTestValidBusinessObjectData();

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML,
                NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Trigger the message notification.
        List<NotificationMessage> result = messageNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID, NO_BDATA_STATUS);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectDataKey,
                businessObjectDataEntity.getId(), HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID, null, NO_ATTRIBUTES,
                NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testProcessBusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent() throws Exception
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
                BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Process an event.
        List<NotificationMessage> result = messageNotificationEventService
            .processBusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                namespaceEntity);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
    }

    @Test
    public void testProcessBusinessObjectFormatVersionChangeNotificationEvent() throws Exception
    {
        // Create a business object format entity.
        BusinessObjectDataInvalidateUnregisteredRequest request =
            new BusinessObjectDataInvalidateUnregisteredRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, StorageEntity.MANAGED_STORAGE);

        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatServiceTestHelper.createBusinessObjectFormat(request);

        // Get a business object data key.
        BusinessObjectFormatKey businessObjectFormatKey = businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormatEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML,
                Collections.singletonList(new MessageHeaderDefinition(KEY, VALUE)))))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Trigger the notification.
        List<NotificationMessage> result = messageNotificationEventService
            .processBusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormatVersionChangeMessageWithXmlPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectFormatKey,
                HerdDaoSecurityHelper.SYSTEM_USER, businessObjectFormatKey.getBusinessObjectFormatVersion().toString(), NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION,
                Collections.singletonList(new MessageHeader(KEY, VALUE)), result.get(0));
    }

    @Test
    public void testProcessBusinessObjectFormatVersionChangeNotificationEventHerdSqsNotificationNotEnabled() throws Exception
    {
        // Create a business object format entity.
        BusinessObjectDataInvalidateUnregisteredRequest request =
            new BusinessObjectDataInvalidateUnregisteredRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, StorageEntity.MANAGED_STORAGE);

        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatServiceTestHelper.createBusinessObjectFormat(request);

        // Get a business object data key.
        BusinessObjectFormatKey businessObjectFormatKey = businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormatEntity);

        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_ENABLED.getKey(), false);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Trigger the notification.
            List<NotificationMessage> result = messageNotificationEventService
                .processBusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION);

            // Validate the results.
            assertTrue(CollectionUtils.isEmpty(result));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testProcessBusinessObjectFormatVersionChangeNotificationEventNoMessageDestination() throws Exception
    {
        // Create a business object format entity.
        BusinessObjectDataInvalidateUnregisteredRequest request =
            new BusinessObjectDataInvalidateUnregisteredRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, StorageEntity.MANAGED_STORAGE);

        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatServiceTestHelper.createBusinessObjectFormat(request);

        // Get a business object data key.
        BusinessObjectFormatKey businessObjectFormatKey = businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormatEntity);

        // Override configuration, so there will be no message definition specified in the relative notification message definition.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, NO_MESSAGE_DESTINATION, MESSAGE_TEXT,
                Collections.singletonList(new MessageHeaderDefinition(KEY, VALUE)))))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to trigger the notification.
        try
        {
            // Trigger the notification.
            messageNotificationEventService
                .processBusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION);

            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message destination must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testProcessBusinessObjectFormatVersionChangeNotificationEventNoMessageType() throws Exception
    {
        // Create a business object format entity.
        BusinessObjectDataInvalidateUnregisteredRequest request =
            new BusinessObjectDataInvalidateUnregisteredRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, StorageEntity.MANAGED_STORAGE);

        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatServiceTestHelper.createBusinessObjectFormat(request);

        // Get a business object data key.
        BusinessObjectFormatKey businessObjectFormatKey = businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormatEntity);

        // Override configuration, so there will be no message type specified in the relative notification message definition.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(NO_MESSAGE_TYPE, MESSAGE_DESTINATION, MESSAGE_TEXT,
                Collections.singletonList(new MessageHeaderDefinition(KEY, VALUE)))))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to trigger the notification.
        try
        {
            // Trigger the notification.
            messageNotificationEventService
                .processBusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION);

            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message type must be specified. Please update \"%s\" configuration entry.",
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey()), e.getMessage());
        }
    }

    @Test
    public void testProcessBusinessObjectFormatVersionChangeNotificationEventWithNoMessageHeaders() throws Exception
    {
        // Create a business object format entity.
        BusinessObjectDataInvalidateUnregisteredRequest request =
            new BusinessObjectDataInvalidateUnregisteredRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, StorageEntity.MANAGED_STORAGE);

        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatServiceTestHelper.createBusinessObjectFormat(request);

        // Get a business object data key.
        BusinessObjectFormatKey businessObjectFormatKey = businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormatEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION,
                BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Trigger the notification.
        List<NotificationMessage> result = messageNotificationEventService
            .processBusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatKey, NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormatVersionChangeMessageWithXmlPayload(MESSAGE_TYPE, MESSAGE_DESTINATION, businessObjectFormatKey,
                HerdDaoSecurityHelper.SYSTEM_USER, businessObjectFormatKey.getBusinessObjectFormatVersion().toString(), NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION,
                NO_MESSAGE_HEADERS, result.get(0));
    }

    @Test
    public void testProcessStorageUnitStatusChangeNotificationEvent() throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createTestValidBusinessObjectData(SUBPARTITION_VALUES, businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Override configuration.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions(Collections.singletonList(
            new NotificationMessageDefinition(MESSAGE_TYPE, MESSAGE_DESTINATION, STORAGE_UNIT_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON,
                Collections.singletonList(new MessageHeaderDefinition(KEY, VALUE)))))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Trigger the notification.
        List<NotificationMessage> result = messageNotificationEventService
            .processStorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2);

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
    }

    @Test
    public void testProcessSystemMonitorNotificationEvent() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(),
            SYSTEM_MONITOR_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Trigger the notification.
            List<NotificationMessage> result = messageNotificationEventService.processSystemMonitorNotificationEvent(getTestSystemMonitorIncomingMessage());

            // Validate the results.
            assertEquals(1, CollectionUtils.size(result));
            validateSystemMonitorResponseNotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), HERD_OUTGOING_QUEUE, result.get(0));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
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
            // Trigger the notification.
            List<NotificationMessage> result = messageNotificationEventService.processSystemMonitorNotificationEvent(getTestSystemMonitorIncomingMessage());

            // Validate the results.
            assertEquals(0, result.size());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testProcessSystemMonitorNotificationEventNoMessageVelocityTemplate() throws Exception
    {
        // Override configuration to remove the velocity template used for building the system monitor response.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(), null);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Trigger the notification.
            List<NotificationMessage> result = messageNotificationEventService.processSystemMonitorNotificationEvent(getTestSystemMonitorIncomingMessage());

            // Validate the results. Result list should contain no notification messages since no velocity template is configured.
            assertEquals(0, result.size());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testProcessSystemMonitorNotificationEventSqsOutgoingQueueNotDefined() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(),
            SYSTEM_MONITOR_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML);
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME.getKey(), null);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Trigger the notification.
            messageNotificationEventService.processSystemMonitorNotificationEvent(getTestSystemMonitorIncomingMessage());
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
}

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
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.MessageHeaderDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinitions;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.dto.StorageUnitStatusChangeNotificationEvent;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.ConfigurationEntity;
import org.finra.herd.service.helper.notification.AbstractNotificationMessageBuilderTestHelper;
import org.finra.herd.service.helper.notification.StorageUnitStatusChangeMessageBuilder;

/**
 * Tests the functionality for StorageUnitStatusChangeMessageBuilder.
 */
public class StorageUnitStatusChangeMessageBuilderTest extends AbstractNotificationMessageBuilderTestHelper
{
    @Autowired
    private StorageUnitStatusChangeMessageBuilder storageUnitStatusChangeMessageBuilder;

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
        List<NotificationMessage> result = storageUnitStatusChangeMessageBuilder.buildNotificationMessages(
            new StorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2));

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
            storageUnitStatusChangeMessageBuilder.buildNotificationMessages(
                new StorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2));
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
        List<NotificationMessage> result = storageUnitStatusChangeMessageBuilder.buildNotificationMessages(
            new StorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2));

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
            storageUnitStatusChangeMessageBuilder.buildNotificationMessages(
                new StorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2));
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
        List<NotificationMessage> result = storageUnitStatusChangeMessageBuilder.buildNotificationMessages(
            new StorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS));

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
        List<NotificationMessage> result = storageUnitStatusChangeMessageBuilder.buildNotificationMessages(
            new StorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2));

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
        List<NotificationMessage> result = storageUnitStatusChangeMessageBuilder.buildNotificationMessages(
            new StorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2));

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
        assertEquals(0, storageUnitStatusChangeMessageBuilder.buildNotificationMessages(
            new StorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2)).size());

        // Override configuration, so there will be an empty list of notification message definitions configured in the system.
        configurationEntity.setValueClob(xmlHelper.objectToXml(new NotificationMessageDefinitions()));
        configurationDao.saveAndRefresh(configurationEntity);

        // Try to build a notification message and validate the results.
        assertEquals(0, storageUnitStatusChangeMessageBuilder.buildNotificationMessages(
            new StorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2)).size());
    }

    @Test
    public void testGetStorageUnitStatusChangeMessageVelocityContextMap()
    {
        // Create a business object data key with values that require JSON and XML escaping.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE + SUFFIX_UNESCAPED, BDEF_NAME + SUFFIX_UNESCAPED, FORMAT_USAGE_CODE + SUFFIX_UNESCAPED,
                FORMAT_FILE_TYPE_CODE + SUFFIX_UNESCAPED, FORMAT_VERSION, PARTITION_VALUE + SUFFIX_UNESCAPED, Lists
                .newArrayList(SUBPARTITION_VALUES.get(0) + SUFFIX_UNESCAPED, SUBPARTITION_VALUES.get(1) + SUFFIX_UNESCAPED,
                    SUBPARTITION_VALUES.get(2) + SUFFIX_UNESCAPED, SUBPARTITION_VALUES.get(3) + SUFFIX_UNESCAPED), DATA_VERSION);

        // Call the method under test.
        Map<String, Object> result = storageUnitStatusChangeMessageBuilder.getNotificationMessageVelocityContextMap(
            new StorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME + SUFFIX_UNESCAPED, STORAGE_UNIT_STATUS_2 + SUFFIX_UNESCAPED,
                STORAGE_UNIT_STATUS + SUFFIX_UNESCAPED));

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
        assertEquals(15, CollectionUtils.size(result));

        assertEquals(businessObjectDataKey, result.get("businessObjectDataKey"));
        assertEquals(expectedBusinessObjectDataKeyWithJson, result.get("businessObjectDataKeyWithJson"));
        assertEquals(expectedBusinessObjectDataKeyWithXml, result.get("businessObjectDataKeyWithXml"));

        assertEquals(STORAGE_NAME + SUFFIX_UNESCAPED, result.get("storageName"));
        assertEquals(STORAGE_NAME + SUFFIX_ESCAPED_JSON, result.get("storageNameWithJson"));
        assertEquals(STORAGE_NAME + SUFFIX_ESCAPED_XML, result.get("storageNameWithXml"));

        assertEquals(STORAGE_UNIT_STATUS_2 + SUFFIX_UNESCAPED, result.get("newStorageUnitStatus"));
        assertEquals(STORAGE_UNIT_STATUS_2 + SUFFIX_ESCAPED_JSON, result.get("newStorageUnitStatusWithJson"));
        assertEquals(STORAGE_UNIT_STATUS_2 + SUFFIX_ESCAPED_XML, result.get("newStorageUnitStatusWithXml"));

        assertEquals(STORAGE_UNIT_STATUS + SUFFIX_UNESCAPED, result.get("oldStorageUnitStatus"));
        assertEquals(STORAGE_UNIT_STATUS + SUFFIX_ESCAPED_JSON, result.get("oldStorageUnitStatusWithJson"));
        assertEquals(STORAGE_UNIT_STATUS + SUFFIX_ESCAPED_XML, result.get("oldStorageUnitStatusWithXml"));

        assertEquals(BDEF_NAMESPACE + SUFFIX_UNESCAPED, result.get("namespace"));
        assertEquals(BDEF_NAMESPACE + SUFFIX_ESCAPED_JSON, result.get("namespaceWithJson"));
        assertEquals(BDEF_NAMESPACE + SUFFIX_ESCAPED_XML, result.get("namespaceWithXml"));
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

}

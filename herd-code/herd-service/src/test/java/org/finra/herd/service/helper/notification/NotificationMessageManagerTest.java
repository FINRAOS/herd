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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.NotificationMessageDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinitions;
import org.finra.herd.model.dto.BusinessObjectDataStatusChangeNotificationEvent;
import org.finra.herd.model.dto.BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent;
import org.finra.herd.model.dto.BusinessObjectFormatVersionChangeNotificationEvent;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.dto.StorageUnitStatusChangeNotificationEvent;
import org.finra.herd.model.dto.UserNamespaceAuthorizationChangeNotificationEvent;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.ConfigurationEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * The tests for the class NotificationMessageManager.
 */
public class NotificationMessageManagerTest extends AbstractServiceTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Autowired
    private NotificationMessageManager notificationMessageManager;

    @Autowired
    private BusinessObjectDataStatusChangeMessageBuilder businessObjectDataStatusChangeMessageBuilder;

    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionChangeMessageBuilder businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder;

    @Autowired
    private BusinessObjectFormatVersionChangeMessageBuilder businessObjectFormatVersionChangeMessageBuilder;

    @Autowired
    private StorageUnitStatusChangeMessageBuilder storageUnitStatusChangeMessageBuilder;

    @Autowired
    private UserNamespaceAuthorizationChangeMessageBuilder userNamespaceAuthorizationChangeMessageBuilder;

    @Test
    public void testGetEventTypeNotificationMessageBuilderMap()
    {
        Map<Class<?>, NotificationMessageBuilder> map = notificationMessageManager.getEventTypeNotificationMessageBuilderMap();
        assertNotNull(map);
        assertEquals(5, map.size());
        assertEquals(businessObjectDataStatusChangeMessageBuilder, map.get(BusinessObjectDataStatusChangeNotificationEvent.class));
        assertEquals(businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder,
            map.get(BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent.class));
        assertEquals(businessObjectFormatVersionChangeMessageBuilder, map.get(BusinessObjectFormatVersionChangeNotificationEvent.class));
        assertEquals(storageUnitStatusChangeMessageBuilder, map.get(StorageUnitStatusChangeNotificationEvent.class));
        assertEquals(userNamespaceAuthorizationChangeMessageBuilder, map.get(UserNamespaceAuthorizationChangeNotificationEvent.class));
    }

    @Test
    public void testBuildNotificationMessagesNullNotificationEvent()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Parameter \"notificationEvent\" must not be null");

        notificationMessageManager.buildNotificationMessages(null);
    }

    @Test
    public void testBuildNotificationMessagesHappyPath() throws Exception
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
                BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML, NO_MESSAGE_HEADER_DEFINITIONS)))));
        configurationDao.saveAndRefresh(configurationEntity);

        // Trigger the notification.
        List<NotificationMessage> result = notificationMessageManager.buildNotificationMessages(
            new BusinessObjectDataStatusChangeNotificationEvent(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID));

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataStatusChangeMessageWithXmlPayload(MESSAGE_TYPE_SNS, MESSAGE_DESTINATION, businessObjectDataKey,
                businessObjectDataEntity.getId(), HerdDaoSecurityHelper.SYSTEM_USER, BusinessObjectDataStatusEntity.VALID,
                BusinessObjectDataStatusEntity.INVALID, Collections.singletonList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3)),
                NO_MESSAGE_HEADERS, result.get(0));
    }
}

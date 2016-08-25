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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import javax.servlet.ServletRequest;

import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.StorageUnitNotificationFilter;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistration;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationCreateRequest;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationKeys;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationUpdateRequest;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.model.jpa.StorageUnitNotificationRegistrationEntity;

/**
 * This class tests various functionality within the storage unit notification registration REST controller.
 */
public class StorageUnitNotificationRegistrationRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateStorageUnitNotificationRegistration()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting();

        // Create a business object data notification.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationRestController
            .createStorageUnitNotificationRegistration(new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                    STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
                NotificationRegistrationStatusEntity.ENABLED));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED), resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testDeleteStorageUnitNotificationRegistration()
    {
        NotificationRegistrationKey storageUnitNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(storageUnitNotificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Validate that this business object data notification exists.
        assertNotNull(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(storageUnitNotificationRegistrationKey));

        // Delete this business object data notification.
        StorageUnitNotificationRegistration deletedStorageUnitNotificationRegistration =
            storageUnitNotificationRegistrationRestController.deleteStorageUnitNotification(NAMESPACE, NOTIFICATION_NAME);

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(storageUnitNotificationRegistrationEntity.getId(), storageUnitNotificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED), deletedStorageUnitNotificationRegistration);

        // Ensure that this business object data notification is no longer there.
        assertNull(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(storageUnitNotificationRegistrationKey));
    }

    @Test
    public void testGetStorageUnitNotificationRegistration()
    {
        NotificationRegistrationKey storageUnitNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(storageUnitNotificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve the business object data notification.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration =
            storageUnitNotificationRegistrationRestController.getStorageUnitNotificationRegistration(NAMESPACE, NOTIFICATION_NAME);

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(storageUnitNotificationRegistrationEntity.getId(), storageUnitNotificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED), resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNamespace()
    {
        // Create and persist business object data notification entities.
        for (NotificationRegistrationKey storageUnitNotificationRegistrationKey : notificationRegistrationDaoTestHelper.getTestNotificationRegistrationKeys())
        {
            notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(storageUnitNotificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);
        }

        // Retrieve a list of business object data notification registration keys.
        StorageUnitNotificationRegistrationKeys resultStorageUnitNotificationRegistrationKeys =
            storageUnitNotificationRegistrationRestController.getStorageUnitNotificationRegistrationsByNamespace(NAMESPACE);

        // Validate the returned object.
        assertEquals(notificationRegistrationDaoTestHelper.getExpectedNotificationRegistrationKeys(),
            resultStorageUnitNotificationRegistrationKeys.getStorageUnitNotificationRegistrationKeys());
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNotificationFilter()
    {
        // Create a business object data notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve a list of business object data notification registration keys.
        StorageUnitNotificationRegistrationKeys resultStorageUnitNotificationRegistrationKeys = storageUnitNotificationRegistrationRestController
            .getStorageUnitNotificationRegistrationsByNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                getServletRequestWithNotificationFilterParameters());

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistrationKeys(Arrays.asList(notificationRegistrationKey)), resultStorageUnitNotificationRegistrationKeys);
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNotificationFilterMissingOptionalParameters()
    {
        // Create a business object data notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve a list of business object data notification registration keys by not specifying optional parameters.
        StorageUnitNotificationRegistrationKeys resultStorageUnitNotificationRegistrationKeys = storageUnitNotificationRegistrationRestController
            .getStorageUnitNotificationRegistrationsByNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE,
                getServletRequestWithNotificationFilterParameters());

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistrationKeys(Arrays.asList(notificationRegistrationKey)), resultStorageUnitNotificationRegistrationKeys);
    }

    @Test
    public void testUpdateStorageUnitNotificationRegistration()
    {
        NotificationRegistrationKey storageUnitNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create database entities required for testing.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(NAMESPACE, Arrays
            .asList(NOTIFICATION_EVENT_TYPE, NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name()), BDEF_NAMESPACE_2, BDEF_NAME_2,
            Arrays.asList(FORMAT_FILE_TYPE_CODE, FORMAT_FILE_TYPE_CODE_2), Arrays.asList(STORAGE_NAME, STORAGE_NAME_2),
            Arrays.asList(STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS_3, STORAGE_UNIT_STATUS_4),
            notificationRegistrationDaoTestHelper.getTestJobActions2());

        // Create and persist a business object data notification registration entity.
        notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(storageUnitNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Update the business object data notification registration.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationRestController
            .updateStorageUnitNotificationRegistration(NAMESPACE, NOTIFICATION_NAME,
                new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2,
                        STORAGE_NAME_2, STORAGE_UNIT_STATUS_3, STORAGE_UNIT_STATUS_4), notificationRegistrationDaoTestHelper.getTestJobActions2(),
                    NotificationRegistrationStatusEntity.DISABLED));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(), storageUnitNotificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2,
                STORAGE_UNIT_STATUS_3, STORAGE_UNIT_STATUS_4), notificationRegistrationDaoTestHelper.getTestJobActions2(),
            NotificationRegistrationStatusEntity.DISABLED), resultStorageUnitNotificationRegistration);
    }

    /**
     * Gets a servlet request with hard coded notification filter parameters.
     *
     * @return the servlet request
     */
    private ServletRequest getServletRequestWithNotificationFilterParameters()
    {
        // Create a servlet request that contains hard coded business object data notification filter parameters.
        MockHttpServletRequest servletRequest = new MockHttpServletRequest();
        servletRequest.setParameter("businessObjectDefinitionNamespace", BDEF_NAMESPACE);
        servletRequest.setParameter("businessObjectDefinitionName", BDEF_NAME);
        servletRequest.setParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE);
        servletRequest.setParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE);
        return servletRequest;
    }
}

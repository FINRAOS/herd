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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.StorageUnitNotificationFilter;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.model.jpa.StorageUnitNotificationRegistrationEntity;

public class StorageUnitNotificationRegistrationDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetStorageUnitNotificationRegistrationByAltKey()
    {
        NotificationRegistrationKey storageUnitNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(storageUnitNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve this business object data notification.
        StorageUnitNotificationRegistrationEntity resultStorageUnitNotificationEntity =
            storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(storageUnitNotificationRegistrationKey);

        // Validate the returned object.
        assertNotNull(resultStorageUnitNotificationEntity);
        assertEquals(storageUnitNotificationRegistrationEntity.getId(), resultStorageUnitNotificationEntity.getId());
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationKeysByNamespace()
    {
        // Create and persist a set of business object data notification registration entities.
        for (NotificationRegistrationKey storageUnitNotificationRegistrationKey : notificationRegistrationDaoTestHelper.getTestNotificationRegistrationKeys())
        {
            notificationRegistrationDaoTestHelper
                .createStorageUnitNotificationRegistrationEntity(storageUnitNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE, BDEF_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                    notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);
        }

        // Retrieve a list of business object data notification registration keys for the specified namespace.
        List<NotificationRegistrationKey> resultKeys = storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationKeysByNamespace(NAMESPACE);

        // Validate the returned object.
        assertEquals(notificationRegistrationDaoTestHelper.getExpectedNotificationRegistrationKeys(), resultKeys);
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationKeysByNotificationFilter()
    {
        // Create a business object data notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve a list of business object data notification registration keys.
        assertEquals(Arrays.asList(notificationRegistrationKey), storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrationKeysByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                    NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));

        // Retrieve a list of business object data notification registration keys by not specifying optional parameters.
        assertEquals(Arrays.asList(notificationRegistrationKey), storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrationKeysByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                    NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));
        assertEquals(Arrays.asList(notificationRegistrationKey), storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrationKeysByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                    NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));

        // Get business object data attribute by passing all case-insensitive parameters in uppercase.
        assertEquals(Arrays.asList(notificationRegistrationKey), storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrationKeysByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                    FORMAT_FILE_TYPE_CODE.toUpperCase(), NO_FORMAT_VERSION, NO_STORAGE_NAME, NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));

        // Get business object data attribute by passing all case-insensitive parameters in lowercase.
        assertEquals(Arrays.asList(notificationRegistrationKey), storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrationKeysByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                    FORMAT_FILE_TYPE_CODE.toLowerCase(), NO_FORMAT_VERSION, NO_STORAGE_NAME, NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));

        // Try invalid values for all input parameters.
        assertTrue(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationKeysByNotificationFilter(
            new StorageUnitNotificationFilter("I_DO_NO_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)).isEmpty());
        assertTrue(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationKeysByNotificationFilter(
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, "I_DO_NO_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)).isEmpty());
        assertTrue(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationKeysByNotificationFilter(
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NO_EXIST", FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)).isEmpty());
        assertTrue(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationKeysByNotificationFilter(
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NO_EXIST", NO_FORMAT_VERSION, NO_STORAGE_NAME,
                NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)).isEmpty());
    }
}

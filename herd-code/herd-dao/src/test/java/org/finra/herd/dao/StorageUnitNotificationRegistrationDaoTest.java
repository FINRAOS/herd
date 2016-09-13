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
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
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

        // Create and persist a storage unit notification registration entity.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(storageUnitNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve this storage unit notification registration.
        StorageUnitNotificationRegistrationEntity resultStorageUnitNotificationEntity =
            storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(storageUnitNotificationRegistrationKey);

        // Validate the returned object.
        assertNotNull(resultStorageUnitNotificationEntity);
        assertEquals(storageUnitNotificationRegistrationEntity.getId(), resultStorageUnitNotificationEntity.getId());
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationKeysByNamespace()
    {
        // Create and persist a set of storage unit notification registration entities.
        for (NotificationRegistrationKey storageUnitNotificationRegistrationKey : notificationRegistrationDaoTestHelper.getTestNotificationRegistrationKeys())
        {
            notificationRegistrationDaoTestHelper
                .createStorageUnitNotificationRegistrationEntity(storageUnitNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE, BDEF_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                    notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);
        }

        // Retrieve a list of storage unit notification registration keys for the specified namespace.
        List<NotificationRegistrationKey> resultKeys = storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationKeysByNamespace(NAMESPACE);

        // Validate the returned object.
        assertEquals(notificationRegistrationDaoTestHelper.getExpectedNotificationRegistrationKeys(), resultKeys);
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationKeysByNotificationFilter()
    {
        // Create a storage unit notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve a list of storage unit notification registration keys.
        assertEquals(Arrays.asList(notificationRegistrationKey), storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrationKeysByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                    NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));

        // Retrieve a list of storage unit notification registration keys by not specifying optional parameters.
        assertEquals(Arrays.asList(notificationRegistrationKey), storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrationKeysByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                    NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));
        assertEquals(Arrays.asList(notificationRegistrationKey), storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrationKeysByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                    NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));

        // Get storage unit notification registration keys by passing all case-insensitive parameters in uppercase.
        assertEquals(Arrays.asList(notificationRegistrationKey), storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrationKeysByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                    FORMAT_FILE_TYPE_CODE.toUpperCase(), NO_FORMAT_VERSION, NO_STORAGE_NAME, NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));

        // Get storage unit notification registration keys by passing all case-insensitive parameters in lowercase.
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

    @Test
    public void testGetStorageUnitNotificationRegistrations()
    {
        // Create and persist a storage unit notification registration entity with all optional parameters specified.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Retrieve the storage unit notification registration matching the filter criteria.
        List<StorageUnitNotificationRegistrationEntity> result = storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                NotificationRegistrationStatusEntity.ENABLED);

        // Validate the returned object.
        assertEquals(Arrays.asList(storageUnitNotificationRegistrationEntity), result);
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsInvalidInputs()
    {
        // Create and persist a storage unit notification registration entity with all optional parameters specified.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve the storage unit notification registration with all input parameters matching the filter criteria.
        List<StorageUnitNotificationRegistrationEntity> result = storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, NotificationRegistrationStatusEntity.ENABLED);

        // Validate the returned object.
        assertEquals(Arrays.asList(storageUnitNotificationRegistrationEntity), result);

        // Try to retrieve the storage unit notification registration when using invalid business object definition namespace.
        assertTrue(CollectionUtils.isEmpty(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION), STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the storage unit notification registration when using invalid business object definition name.
        assertTrue(CollectionUtils.isEmpty(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION), STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the storage unit notification registration when using invalid business object format usage.
        assertTrue(CollectionUtils.isEmpty(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the storage unit notification registration when using invalid format file type.
        assertTrue(CollectionUtils.isEmpty(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the storage unit notification registration when using invalid business object format version.
        assertTrue(CollectionUtils.isEmpty(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION), STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the storage unit notification registration when using invalid storage name.
        assertTrue(CollectionUtils.isEmpty(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), "I_DO_NOT_EXIST", STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the storage unit notification registration when using invalid new storage unit status.
        assertTrue(CollectionUtils.isEmpty(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), STORAGE_NAME, "I_DO_NOT_EXIST", STORAGE_UNIT_STATUS_2, NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the storage unit notification registration when using invalid old storage unit status.
        assertTrue(CollectionUtils.isEmpty(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), STORAGE_NAME, STORAGE_UNIT_STATUS, "I_DO_NOT_EXIST", NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the storage unit notification registration when using invalid notification registration status.
        assertTrue(CollectionUtils.isEmpty(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, NotificationRegistrationStatusEntity.DISABLED)));
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsMissingOptionalFilterParameters()
    {
        // Create and persist a storage unit notification registration entity with all optional filter parameters missing.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                BDEF_NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, STORAGE_NAME, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Retrieve the storage unit notification registration matching the filter criteria.
        List<StorageUnitNotificationRegistrationEntity> result = storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                NotificationRegistrationStatusEntity.ENABLED);

        // Validate the returned object.
        assertEquals(Arrays.asList(storageUnitNotificationRegistrationEntity), result);

        // Retrieve the storage unit notification registration matching the filter criteria when old storage unit status is null.
        result = storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS,
                NotificationRegistrationStatusEntity.ENABLED);

        // Validate the returned object.
        assertEquals(Arrays.asList(storageUnitNotificationRegistrationEntity), result);
    }
}

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
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationFilter;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;

public class BusinessObjectDataNotificationRegistrationDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetBusinessObjectDataNotificationRegistrationByAltKey()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve this business object data notification registration.
        BusinessObjectDataNotificationRegistrationEntity resultBusinessObjectDataNotificationEntity =
            businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(businessObjectDataNotificationRegistrationKey);

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataNotificationEntity);
        assertEquals(businessObjectDataNotificationRegistrationEntity.getId(), resultBusinessObjectDataNotificationEntity.getId());
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationKeysByNamespace()
    {
        // Create and persist a set of business object data notification registration entities.
        for (NotificationRegistrationKey businessObjectDataNotificationRegistrationKey : notificationRegistrationDaoTestHelper
            .getTestNotificationRegistrationKeys())
        {
            notificationRegistrationDaoTestHelper
                .createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE,
                    BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                    notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);
        }

        // Retrieve a list of business object data notification registration keys for the specified namespace.
        List<NotificationRegistrationKey> resultKeys =
            businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationKeysByNamespace(NAMESPACE);

        // Validate the returned object.
        assertEquals(notificationRegistrationDaoTestHelper.getExpectedNotificationRegistrationKeys(), resultKeys);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationKeysByNotificationFilter()
    {
        // Create a business object data notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(notificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve a list of business object data notification registration keys.
        assertEquals(Arrays.asList(notificationRegistrationKey), businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrationKeysByNotificationFilter(
                new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION,
                    NO_STORAGE_NAME, NO_BDATA_STATUS, NO_BDATA_STATUS)));

        // Retrieve a list of business object data notification registration keys by not specifying optional parameters.
        assertEquals(Arrays.asList(notificationRegistrationKey), businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrationKeysByNotificationFilter(
                new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION,
                    NO_STORAGE_NAME, NO_BDATA_STATUS, NO_BDATA_STATUS)));
        assertEquals(Arrays.asList(notificationRegistrationKey), businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrationKeysByNotificationFilter(
                new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION,
                    NO_STORAGE_NAME, NO_BDATA_STATUS, NO_BDATA_STATUS)));

        // Get business object data notification registration keys by passing all case-insensitive parameters in uppercase.
        assertEquals(Arrays.asList(notificationRegistrationKey), businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrationKeysByNotificationFilter(
                new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                    FORMAT_FILE_TYPE_CODE.toUpperCase(), NO_FORMAT_VERSION, NO_STORAGE_NAME, NO_BDATA_STATUS, NO_BDATA_STATUS)));

        // Get business object data notification registration keys by passing all case-insensitive parameters in lowercase.
        assertEquals(Arrays.asList(notificationRegistrationKey), businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrationKeysByNotificationFilter(
                new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                    FORMAT_FILE_TYPE_CODE.toLowerCase(), NO_FORMAT_VERSION, NO_STORAGE_NAME, NO_BDATA_STATUS, NO_BDATA_STATUS)));

        // Try invalid values for all input parameters.
        assertTrue(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationKeysByNotificationFilter(
            new BusinessObjectDataNotificationFilter("I_DO_NO_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                NO_BDATA_STATUS, NO_BDATA_STATUS)).isEmpty());
        assertTrue(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationKeysByNotificationFilter(
            new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, "I_DO_NO_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION,
                NO_STORAGE_NAME, NO_BDATA_STATUS, NO_BDATA_STATUS)).isEmpty());
        assertTrue(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationKeysByNotificationFilter(
            new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NO_EXIST", FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                NO_BDATA_STATUS, NO_BDATA_STATUS)).isEmpty());
        assertTrue(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationKeysByNotificationFilter(
            new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NO_EXIST", NO_FORMAT_VERSION, NO_STORAGE_NAME,
                NO_BDATA_STATUS, NO_BDATA_STATUS)).isEmpty());
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrations()
    {
        // Create and persist a business object data notification registration entity with all optional parameters specified.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Retrieve the business object notification registration matching the filter criteria.
        List<BusinessObjectDataNotificationRegistrationEntity> result = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
                NotificationRegistrationStatusEntity.ENABLED);

        // Validate the returned object.
        assertEquals(Arrays.asList(businessObjectDataNotificationRegistrationEntity), result);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsInvalidInputs()
    {
        // Create and persist a business object data notification registration entity with all optional parameters specified.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve the business object notification with all input parameters matching the filter criteria.
        List<BusinessObjectDataNotificationRegistrationEntity> result = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), BDATA_STATUS, BDATA_STATUS_2, NotificationRegistrationStatusEntity.ENABLED);

        // Validate the returned object.
        assertEquals(Arrays.asList(businessObjectDataNotificationRegistrationEntity), result);

        // Try to retrieve the business object data notification registration when using invalid business object definition namespace.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION), BDATA_STATUS, BDATA_STATUS_2, NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the business object data notification registration when using invalid business object definition name.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION), BDATA_STATUS, BDATA_STATUS_2, NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the business object data notification registration when using invalid business object format usage.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, BDATA_STATUS_2, NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the business object data notification registration when using invalid format file type.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, BDATA_STATUS_2, NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the business object data notification registration when using invalid business object format version.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION), BDATA_STATUS, BDATA_STATUS_2, NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the business object data notification registration when using invalid new business object data status.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), "I_DO_NOT_EXIST", BDATA_STATUS_2, NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the business object data notification registration when using invalid old business object data status.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, "I_DO_NOT_EXIST", NotificationRegistrationStatusEntity.ENABLED)));

        // Try to retrieve the business object data notification registration when using invalid notification registration status.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, BDATA_STATUS_2, NotificationRegistrationStatusEntity.DISABLED)));
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsMissingOptionalFilterParameters()
    {
        // Create and persist a business object data notification registration entity with all optional filter parameters missing.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                BDEF_NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME, NO_BDATA_STATUS, NO_BDATA_STATUS,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Retrieve the business object notification registration matching the filter criteria.
        List<BusinessObjectDataNotificationRegistrationEntity> result = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2,
                NotificationRegistrationStatusEntity.ENABLED);

        // Validate the returned object.
        assertEquals(Arrays.asList(businessObjectDataNotificationRegistrationEntity), result);

        // Retrieve the business object notification registration matching the filter criteria when old business object data status is null.
        result = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey, BDATA_STATUS, NO_BDATA_STATUS,
                NotificationRegistrationStatusEntity.ENABLED);

        // Validate the returned object.
        assertEquals(Arrays.asList(businessObjectDataNotificationRegistrationEntity), result);
    }
}

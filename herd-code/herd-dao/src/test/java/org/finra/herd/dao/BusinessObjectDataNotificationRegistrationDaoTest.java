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

import java.util.List;

import org.junit.Test;
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;

public class BusinessObjectDataNotificationRegistrationDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetBusinessObjectDataNotificationRegistrationByAltKey()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(
            NAMESPACE_CD, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Retrieve this business object data notification.
        BusinessObjectDataNotificationRegistrationEntity resultBusinessObjectDataNotificationEntity = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrationByAltKey(businessObjectDataNotificationRegistrationKey);

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataNotificationEntity);
        assertEquals(businessObjectDataNotificationRegistrationEntity.getId(), resultBusinessObjectDataNotificationEntity.getId());
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationKeys()
    {
        // Create and persist a set of business object data notification registration entities.
        for (NotificationRegistrationKey businessObjectDataNotificationRegistrationKey : getTestBusinessObjectDataNotificationRegistrationKeys())
        {
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());
        }

        // Retrieve a list of business object data notification registration keys for the specified namespace.
        List<NotificationRegistrationKey> resultKeys = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrationKeys(NAMESPACE_CD);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectDataNotificationRegistrationKeys(), resultKeys);
    }

    /**
     * Tests the getBusinessObjectDataNotificationRegistrations() method. Tests the happy path scenario by providing all the parameters.
     */
    @Test
    public void testGetBusinessObjectDataNotificationRegistrations()
    {
        // Create and persist a business object data notification registration entity with all optional parameters specified.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME),
                NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS,
                BDATA_STATUS_2, getTestJobActions());

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION);

        // Retrieve the business object notification matching the filter criteria.
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntities = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the returned object.
        assertNotNull(businessObjectDataNotificationEntities);
        assertEquals(1, businessObjectDataNotificationEntities.size());
        assertEquals(businessObjectDataNotificationRegistrationEntity.getId(), businessObjectDataNotificationEntities.get(0).getId());

        // Try to retrieve the business object notification when it is not matching the filter criteria.
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntities2 = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey, BDATA_STATUS_2, null);

        // Validate the returned object.
        assertNotNull(businessObjectDataNotificationEntities2);
        assertEquals(0, businessObjectDataNotificationEntities2.size());
    }

    /**
     * Tests the getBusinessObjectDataNotificationRegistrations() method.
     */
    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsMissingOptionalFilterParameters()
    {
        // Create and persist a business object data notification registration entity with all optional filter parameters missing.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME),
                NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, null, null, null, null, null, null, getTestJobActions());

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION);

        // Retrieve the business object notification matching the filter criteria.
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntities = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the returned object.
        assertNotNull(businessObjectDataNotificationEntities);
        assertEquals(1, businessObjectDataNotificationEntities.size());
        assertEquals(businessObjectDataNotificationRegistrationEntity.getId(), businessObjectDataNotificationEntities.get(0).getId());

        // Retrieve the business object notification matching the filter criteria.
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntities2 = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey, BDATA_STATUS, null);

        // Validate the returned object.
        assertNotNull(businessObjectDataNotificationEntities2);
        assertEquals(1, businessObjectDataNotificationEntities2.size());
        assertEquals(businessObjectDataNotificationRegistrationEntity.getId(), businessObjectDataNotificationEntities2.get(0).getId());
    }

    /**
     * Tests the getBusinessObjectDataNotificationRegistrations() method.
     */
    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsMissingNewBusinessObjectDataStatusFilterParameter()
    {
        // Create and persist a business object data notification registration entity with a missing new business object data status filter parameter.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME),
                NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, null, BDATA_STATUS_2,
                getTestJobActions());

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION);

        // Retrieve the business object notification matching the filter criteria.
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntities = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_2);

        // Validate the returned object.
        assertNotNull(businessObjectDataNotificationEntities);
        assertEquals(1, businessObjectDataNotificationEntities.size());
        assertEquals(businessObjectDataNotificationRegistrationEntity.getId(), businessObjectDataNotificationEntities.get(0).getId());

        // Try to retrieve the business object notification when old business object data status (actual value) is not matching the filter criteria.
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntities2 = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey, BDATA_STATUS, BDATA_STATUS_3);

        // Validate the returned object.
        assertNotNull(businessObjectDataNotificationEntities2);
        assertEquals(0, businessObjectDataNotificationEntities2.size());

        // Try to retrieve the business object notification when old business object data status (null value) is not matching the filter criteria.
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntities3 = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey, BDATA_STATUS, null);

        // Validate the returned object.
        assertNotNull(businessObjectDataNotificationEntities3);
        assertEquals(0, businessObjectDataNotificationEntities3.size());
    }

    /**
     * Tests the getBusinessObjectDataNotificationRegistrations() method when input parameters not matching filter criteria.
     */
    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsInvalidInputs()
    {
        // Create and persist a business object data notification registration entity with all optional parameters specified.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME),
                NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS,
                BDATA_STATUS_2, getTestJobActions());

        // Retrieve the business object notification with all input parameters matching the filter criteria.
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntities = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE, new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION), BDATA_STATUS, BDATA_STATUS_2);

        // Validate the returned object.
        assertNotNull(businessObjectDataNotificationEntities);
        assertEquals(1, businessObjectDataNotificationEntities.size());
        assertEquals(businessObjectDataNotificationRegistrationEntity.getId(), businessObjectDataNotificationEntities.get(0).getId());

        // Try to retrieve the business object notification when using invalid namespace.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey("I_DO_NOT_EXIST", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION), BDATA_STATUS, BDATA_STATUS_2)));

        // Try to retrieve the business object notification when using invalid business object definition name.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BOD_NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION), BDATA_STATUS, BDATA_STATUS_2)));

        // Try to retrieve the business object notification when using invalid business object format usage.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, BDATA_STATUS_2)));

        // Try to retrieve the business object notification when using invalid format file type.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, BDATA_STATUS_2)));

        // Try to retrieve the business object notification when using invalid business object format version.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION), BDATA_STATUS, BDATA_STATUS_2)));

        // Try to retrieve the business object notification when using invalid new business object data status.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), "I_DO_NOT_EXIST", BDATA_STATUS_2)));

        // Try to retrieve the business object notification when using invalid old business object data status.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, "I_DO_NOT_EXIST")));
    }
}

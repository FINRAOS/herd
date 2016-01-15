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

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataNotificationFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistration;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationKeys;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;

/**
 * This class tests various functionality within the business object data notification registration REST controller.
 */
public class BusinessObjectDataNotificationRegistrationRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateBusinessObjectDataNotificationRegistration()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Create a business object data notification.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationRestController
            .createBusinessObjectDataNotificationRegistration(createBusinessObjectDataNotificationRegistrationCreateRequest(
                businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), getTestJobActions(),
            "ENABLED"), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testUpdateBusinessObjectDataNotificationRegistration()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE_CD, Arrays.asList(NOTIFICATION_EVENT_TYPE,
            NOTIFICATION_EVENT_TYPE_2), BOD_NAMESPACE_2, BOD_NAME_2, Arrays.asList(FORMAT_FILE_TYPE_CODE, FORMAT_FILE_TYPE_CODE_2), Arrays.asList(STORAGE_NAME,
                STORAGE_NAME_2), Arrays.asList(BDATA_STATUS, BDATA_STATUS_2, BDATA_STATUS_3, BDATA_STATUS_4), getTestJobActions2());

        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Update the business object data notification registration.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationRestController
            .updateBusinessObjectDataNotificationRegistration(NAMESPACE_CD, NOTIFICATION_NAME, createBusinessObjectDataNotificationRegistrationUpdateRequest(
                NOTIFICATION_EVENT_TYPE_2, BOD_NAMESPACE_2, BOD_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2,
                BDATA_STATUS_3, BDATA_STATUS_4, getTestJobActions2(), "ENABLED"));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE_2, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE_2, BOD_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2, BDATA_STATUS_3, BDATA_STATUS_4), getTestJobActions2(),
            "ENABLED"), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistration()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Retrieve the business object data notification.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationRestController
            .getBusinessObjectDataNotificationRegistration(NAMESPACE_CD, NOTIFICATION_NAME);

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), getTestJobActions(),
            "ENABLED"), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistration()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Validate that this business object data notification exists.
        assertNotNull(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(
            businessObjectDataNotificationRegistrationKey));

        // Delete this business object data notification.
        BusinessObjectDataNotificationRegistration deletedBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationRestController
            .deleteBusinessObjectDataNotification(NAMESPACE_CD, NOTIFICATION_NAME);

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), getTestJobActions(),
            "ENABLED"), deletedBusinessObjectDataNotificationRegistration);

        // Ensure that this business object data notification is no longer there.
        assertNull(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(
            businessObjectDataNotificationRegistrationKey));
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrations()
    {
        // Create and persist business object data notification entities.
        for (NotificationRegistrationKey businessObjectDataNotificationRegistrationKey : getTestBusinessObjectDataNotificationRegistrationKeys())
        {
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());
        }

        // Retrieve a list of business object data notification registration keys.
        BusinessObjectDataNotificationRegistrationKeys resultBusinessObjectDataNotificationRegistrationKeys =
            businessObjectDataNotificationRegistrationRestController.getBusinessObjectDataNotificationRegistrations(NAMESPACE_CD);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectDataNotificationRegistrationKeys(), resultBusinessObjectDataNotificationRegistrationKeys
            .getBusinessObjectDataNotificationRegistrationKeys());
    }
}

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
package org.finra.dm.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import org.finra.dm.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistration;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationKey;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationKeys;

/**
 * This class tests various functionality within the business object data notification registration REST controller.
 */
public class BusinessObjectDataNotificationRegistrationRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateBusinessObjectDataNotificationRegistration()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Create a business object data notification.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationRestController
            .createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions()));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(null, NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions(), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistration()
    {
        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());

        // Retrieve the business object data notification.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration =
            businessObjectDataNotificationRegistrationRestController.getBusinessObjectDataNotificationRegistration(NAMESPACE_CD, NOTIFICATION_NAME);

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(), NAMESPACE_CD, NOTIFICATION_NAME,
            NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions(),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrations()
    {
        // Create and persist business object data notification entities.
        for (BusinessObjectDataNotificationRegistrationKey key : getTestBusinessObjectDataNotificationRegistrationKeys())
        {
            createBusinessObjectDataNotificationRegistrationEntity(key.getNamespace(), key.getNotificationName(), NOTIFICATION_EVENT_TYPE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());
        }

        // Retrieve a list of business object data notification registration keys.
        BusinessObjectDataNotificationRegistrationKeys resultKeys =
            businessObjectDataNotificationRegistrationRestController.getBusinessObjectDataNotificationRegistrations(NAMESPACE_CD);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectDataNotificationRegistrationKeys(), resultKeys.getBusinessObjectDataNotificationRegistrationKeys());
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistration()
    {
        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());

        // Validate that this business object data notification exists.
        BusinessObjectDataNotificationRegistrationKey businessObjectDataNotificationKey =
            new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);
        assertNotNull(dmDao.getBusinessObjectDataNotificationByAltKey(businessObjectDataNotificationKey));

        // Delete this business object data notification.
        BusinessObjectDataNotificationRegistration deletedBusinessObjectDataNotificationRegistration =
            businessObjectDataNotificationRegistrationRestController.deleteBusinessObjectDataNotification(NAMESPACE_CD, NOTIFICATION_NAME);

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(), NAMESPACE_CD, NOTIFICATION_NAME,
            NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions(),
            deletedBusinessObjectDataNotificationRegistration);

        // Ensure that this business object data notification is no longer there.
        assertNull(dmDao.getBusinessObjectDataNotificationByAltKey(businessObjectDataNotificationKey));
    }
}

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
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Create a business object data notification.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationRestController
            .createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
                    BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                    businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                BDATA_STATUS, BDATA_STATUS_2), businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions(), "ENABLED"),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistration()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            businessObjectDataNotificationRegistrationDaoTestHelper
                .createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE,
                    BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                    businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions());

        // Retrieve the business object data notification.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration =
            businessObjectDataNotificationRegistrationRestController.getBusinessObjectDataNotificationRegistration(NAMESPACE, NOTIFICATION_NAME);

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                BDATA_STATUS, BDATA_STATUS_2), businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions(), "ENABLED"),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testUpdateBusinessObjectDataNotificationRegistration()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE, Arrays.asList(NOTIFICATION_EVENT_TYPE, NOTIFICATION_EVENT_TYPE_2),
            BDEF_NAMESPACE_2, BDEF_NAME_2, Arrays.asList(FORMAT_FILE_TYPE_CODE, FORMAT_FILE_TYPE_CODE_2), Arrays.asList(STORAGE_NAME, STORAGE_NAME_2),
            Arrays.asList(BDATA_STATUS, BDATA_STATUS_2, BDATA_STATUS_3, BDATA_STATUS_4),
            businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions2());

        // Create and persist a business object data notification registration entity.
        businessObjectDataNotificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions());

        // Update the business object data notification registration.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationRestController
            .updateBusinessObjectDataNotificationRegistration(NAMESPACE, NOTIFICATION_NAME,
                createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE_2, BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2,
                    FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2, BDATA_STATUS_3, BDATA_STATUS_4,
                    businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions2(), "ENABLED"));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE_2,
            new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2,
                STORAGE_NAME_2, BDATA_STATUS_3, BDATA_STATUS_4), businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions2(), "ENABLED"),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistration()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            businessObjectDataNotificationRegistrationDaoTestHelper
                .createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE,
                    BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                    businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions());

        // Validate that this business object data notification exists.
        assertNotNull(
            businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(businessObjectDataNotificationRegistrationKey));

        // Delete this business object data notification.
        BusinessObjectDataNotificationRegistration deletedBusinessObjectDataNotificationRegistration =
            businessObjectDataNotificationRegistrationRestController.deleteBusinessObjectDataNotification(NAMESPACE, NOTIFICATION_NAME);

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
            new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                BDATA_STATUS, BDATA_STATUS_2), businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions(), "ENABLED"),
            deletedBusinessObjectDataNotificationRegistration);

        // Ensure that this business object data notification is no longer there.
        assertNull(
            businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(businessObjectDataNotificationRegistrationKey));
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsByNamespace()
    {
        // Create and persist business object data notification entities.
        for (NotificationRegistrationKey businessObjectDataNotificationRegistrationKey : getTestBusinessObjectDataNotificationRegistrationKeys())
        {
            businessObjectDataNotificationRegistrationDaoTestHelper
                .createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE,
                    BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                    businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions());
        }

        // Retrieve a list of business object data notification registration keys.
        BusinessObjectDataNotificationRegistrationKeys resultBusinessObjectDataNotificationRegistrationKeys =
            businessObjectDataNotificationRegistrationRestController.getBusinessObjectDataNotificationRegistrationsByNamespace(NAMESPACE);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectDataNotificationRegistrationKeys(),
            resultBusinessObjectDataNotificationRegistrationKeys.getBusinessObjectDataNotificationRegistrationKeys());
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsByNotificationFilter()
    {
        // Create a business object data notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        businessObjectDataNotificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(notificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions());

        // Retrieve a list of business object data notification registration keys.
        BusinessObjectDataNotificationRegistrationKeys resultBusinessObjectDataNotificationRegistrationKeys =
            businessObjectDataNotificationRegistrationRestController
                .getBusinessObjectDataNotificationRegistrationsByNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                    getServletRequestWithNotificationFilterParameters());

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistrationKeys(Arrays.asList(notificationRegistrationKey)),
            resultBusinessObjectDataNotificationRegistrationKeys);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsByNotificationFilterMissingOptionalParameters()
    {
        // Create a business object data notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        businessObjectDataNotificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(notificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions());

        // Retrieve a list of business object data notification registration keys by not specifying optional parameters.
        BusinessObjectDataNotificationRegistrationKeys resultBusinessObjectDataNotificationRegistrationKeys =
            businessObjectDataNotificationRegistrationRestController
                .getBusinessObjectDataNotificationRegistrationsByNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE,
                    getServletRequestWithNotificationFilterParameters());

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistrationKeys(Arrays.asList(notificationRegistrationKey)),
            resultBusinessObjectDataNotificationRegistrationKeys);
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

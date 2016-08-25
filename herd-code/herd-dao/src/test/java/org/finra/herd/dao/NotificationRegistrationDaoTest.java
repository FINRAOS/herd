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
import static org.junit.Assert.assertNull;

import org.junit.Test;

import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.herd.model.jpa.NotificationRegistrationEntity;

public class NotificationRegistrationDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetNotificationRegistrationAssertReturnEntityWhenRegistrationExist()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);
        businessObjectDataNotificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS,
                businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions());

        NotificationRegistrationEntity notificationRegistration = notificationRegistrationDao
            .getNotificationRegistration(businessObjectDataNotificationRegistrationKey.getNamespace(),
                businessObjectDataNotificationRegistrationKey.getNotificationName());

        assertNotNull(notificationRegistration);
        assertEquals(BusinessObjectDataNotificationRegistrationEntity.class, notificationRegistration.getClass());
    }

    @Test
    public void testGetNotificationRegistrationAssertReturnNullWhenRegistrationDoesNotExist()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);
        businessObjectDataNotificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS,
                businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions());

        NotificationRegistrationEntity notificationRegistration =
            notificationRegistrationDao.getNotificationRegistration(businessObjectDataNotificationRegistrationKey.getNamespace(), "DOES_NOT_EXIST");

        assertNull(notificationRegistration);
    }

    @Test
    public void testGetNotificationRegistrationAssertReturnEntityWhenRegistrationExistAndDifferentCase()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey =
            new NotificationRegistrationKey(NAMESPACE.toLowerCase(), NOTIFICATION_NAME.toLowerCase());
        businessObjectDataNotificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS,
                businessObjectDataNotificationRegistrationDaoTestHelper.getTestJobActions());

        NotificationRegistrationEntity notificationRegistration = notificationRegistrationDao
            .getNotificationRegistration(businessObjectDataNotificationRegistrationKey.getNamespace(),
                businessObjectDataNotificationRegistrationKey.getNotificationName());

        assertNotNull(notificationRegistration);
        assertEquals(BusinessObjectDataNotificationRegistrationEntity.class, notificationRegistration.getClass());
    }
}

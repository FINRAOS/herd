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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationRegistrationEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.service.AbstractServiceTest;

public class NotificationRegistrationDaoHelperTest extends AbstractServiceTest
{
    @Test
    public void testGetNotificationRegistrationAssertReturnEntityWhenEntityExists()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, BDEF_NAME);
        notificationRegistrationDaoTestHelper.createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG.name(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        NotificationRegistrationEntity notificationRegistrationEntity = notificationRegistrationDaoHelper.getNotificationRegistration(NAMESPACE, BDEF_NAME);
        assertNotNull(notificationRegistrationEntity);
        assertEquals(NAMESPACE, notificationRegistrationEntity.getNamespace().getCode());
        assertEquals(BDEF_NAME, notificationRegistrationEntity.getName());
    }

    @Test
    public void testGetNotificationRegistrationAssertThrowWhenEntityNotExist()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, BDEF_NAME);
        notificationRegistrationDaoTestHelper.createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG.name(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            notificationRegistrationDaoHelper.getNotificationRegistration(NAMESPACE, "DOES_NOT_EXIST");
            fail();
        }
        catch (Exception e)
        {
            assertEquals(ObjectNotFoundException.class, e.getClass());
            assertEquals("The notification registration with namespace \"" + NAMESPACE + "\" and name \"DOES_NOT_EXIST\" was not found.", e.getMessage());
        }
    }
}

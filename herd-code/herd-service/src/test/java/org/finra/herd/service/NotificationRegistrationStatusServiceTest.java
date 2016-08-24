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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.NotificationRegistrationStatusUpdateRequest;
import org.finra.herd.model.api.xml.NotificationRegistrationStatusUpdateResponse;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;

public class NotificationRegistrationStatusServiceTest extends AbstractServiceTest
{
    @Test
    public void testUpdateNotificationRegistrationStatusAssertUpdateSuccess()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        notificationRegistrationDaoTestHelper.createBusinessObjectDataNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG.name(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        NotificationRegistrationStatusUpdateResponse response = notificationRegistrationStatusService
            .updateNotificationRegistrationStatus(NAMESPACE, NOTIFICATION_NAME,
                new NotificationRegistrationStatusUpdateRequest(NotificationRegistrationStatusEntity.DISABLED));

        assertEquals(new NotificationRegistrationStatusUpdateResponse(notificationRegistrationKey, NotificationRegistrationStatusEntity.DISABLED), response);
    }

    @Test
    public void testUpdateNotificationRegistrationStatusAssertUpdateSuccessCaseInsensitive()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        notificationRegistrationDaoTestHelper.createBusinessObjectDataNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG.name(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        NotificationRegistrationStatusUpdateResponse response = notificationRegistrationStatusService
            .updateNotificationRegistrationStatus(NAMESPACE, NOTIFICATION_NAME,
                new NotificationRegistrationStatusUpdateRequest(NotificationRegistrationStatusEntity.DISABLED.toLowerCase()));

        assertEquals(new NotificationRegistrationStatusUpdateResponse(notificationRegistrationKey, NotificationRegistrationStatusEntity.DISABLED), response);
    }

    @Test
    public void testUpdateNotificationRegistrationStatusAssertUpdateSuccessTrim()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        notificationRegistrationDaoTestHelper.createBusinessObjectDataNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG.name(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        NotificationRegistrationStatusUpdateResponse response = notificationRegistrationStatusService
            .updateNotificationRegistrationStatus(addWhitespace(NAMESPACE), addWhitespace(NOTIFICATION_NAME),
                new NotificationRegistrationStatusUpdateRequest(addWhitespace(NotificationRegistrationStatusEntity.DISABLED)));

        assertEquals(new NotificationRegistrationStatusUpdateResponse(notificationRegistrationKey, NotificationRegistrationStatusEntity.DISABLED), response);
    }

    @Test
    public void testUpdateNotificationRegistrationStatusAssertThrowWhenNamespaceNull()
    {
        try
        {
            notificationRegistrationStatusService.updateNotificationRegistrationStatus(null, NOTIFICATION_NAME,
                new NotificationRegistrationStatusUpdateRequest(NotificationRegistrationStatusEntity.DISABLED));
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("The namespace must be specified", e.getMessage());
        }
    }

    @Test
    public void testUpdateNotificationRegistrationStatusAssertThrowWhenNamespaceBlank()
    {
        try
        {
            notificationRegistrationStatusService.updateNotificationRegistrationStatus(BLANK_TEXT, NOTIFICATION_NAME,
                new NotificationRegistrationStatusUpdateRequest(NotificationRegistrationStatusEntity.DISABLED));
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("The namespace must be specified", e.getMessage());
        }
    }

    @Test
    public void testUpdateNotificationRegistrationStatusAssertThrowWhenNameNull()
    {
        try
        {
            notificationRegistrationStatusService.updateNotificationRegistrationStatus(NAMESPACE, null,
                new NotificationRegistrationStatusUpdateRequest(NotificationRegistrationStatusEntity.DISABLED));
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("The notification name must be specified", e.getMessage());
        }
    }

    @Test
    public void testUpdateNotificationRegistrationStatusAssertThrowWhenNameBlank()
    {
        try
        {
            notificationRegistrationStatusService.updateNotificationRegistrationStatus(NAMESPACE, BLANK_TEXT,
                new NotificationRegistrationStatusUpdateRequest(NotificationRegistrationStatusEntity.DISABLED));
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("The notification name must be specified", e.getMessage());
        }
    }

    @Test
    public void testUpdateNotificationRegistrationStatusAssertThrowWhenStatusNull()
    {
        try
        {
            notificationRegistrationStatusService
                .updateNotificationRegistrationStatus(NAMESPACE, NOTIFICATION_NAME, new NotificationRegistrationStatusUpdateRequest(null));
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("The notification registration status must be specified", e.getMessage());
        }
    }

    @Test
    public void testUpdateNotificationRegistrationStatusAssertThrowWhenStatusBlank()
    {
        try
        {
            notificationRegistrationStatusService
                .updateNotificationRegistrationStatus(NAMESPACE, NOTIFICATION_NAME, new NotificationRegistrationStatusUpdateRequest(BLANK_TEXT));
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("The notification registration status must be specified", e.getMessage());
        }
    }

    @Test
    public void testUpdateNotificationRegistrationStatusAssertThrowNotificationNotFound()
    {
        try
        {
            notificationRegistrationStatusService.updateNotificationRegistrationStatus(NAMESPACE, "DOES_NOT_EXIST",
                new NotificationRegistrationStatusUpdateRequest(NotificationRegistrationStatusEntity.DISABLED));
            fail();
        }
        catch (Exception e)
        {
            assertEquals(ObjectNotFoundException.class, e.getClass());
            assertEquals("The notification registration with namespace \"" + NAMESPACE + "\" and name \"DOES_NOT_EXIST\" was not found.", e.getMessage());
        }
    }
}

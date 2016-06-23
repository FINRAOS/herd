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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.NotificationRegistrationStatusUpdateRequest;
import org.finra.herd.model.api.xml.NotificationRegistrationStatusUpdateResponse;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;

public class NotificationRegistrationStatusServiceTest extends AbstractServiceTest
{
    @Test
    public void testUpdateNotificationRegistrationStatusAssertUpdateSuccess()
    {
        BusinessObjectDataNotificationRegistrationEntity notificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, getTestJobActions());
        notificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusDaoHelper.getNotificationRegistrationStatus("ENABLED"));

        NotificationRegistrationStatusUpdateRequest notificationRegistrationStatusUpdateRequest = new NotificationRegistrationStatusUpdateRequest("DISABLED");
        NotificationRegistrationStatusUpdateResponse response = notificationRegistrationStatusService
            .updateNotificationRegistrationStatus(NAMESPACE, NOTIFICATION_NAME, notificationRegistrationStatusUpdateRequest);
        assertNotNull(response);
        NotificationRegistrationKey notificationRegistrationKey = response.getNotificationRegistrationKey();
        assertNotNull(notificationRegistrationKey);
        assertEquals(NAMESPACE, notificationRegistrationKey.getNamespace());
        assertEquals(NOTIFICATION_NAME, notificationRegistrationKey.getNotificationName());
        assertEquals("DISABLED", response.getNotificationRegistrationStatus());
    }

    @Test
    public void testUpdateNotificationRegistrationStatusAssertUpdateSuccessCaseInsensitive()
    {
        BusinessObjectDataNotificationRegistrationEntity notificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, getTestJobActions());
        notificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusDaoHelper.getNotificationRegistrationStatus("ENABLED"));

        NotificationRegistrationStatusUpdateRequest notificationRegistrationStatusUpdateRequest = new NotificationRegistrationStatusUpdateRequest("disabled");
        NotificationRegistrationStatusUpdateResponse response = notificationRegistrationStatusService
            .updateNotificationRegistrationStatus(NAMESPACE.toLowerCase(), NOTIFICATION_NAME.toLowerCase(), notificationRegistrationStatusUpdateRequest);
        assertNotNull(response);
        NotificationRegistrationKey notificationRegistrationKey = response.getNotificationRegistrationKey();
        assertNotNull(notificationRegistrationKey);
        assertEquals(NAMESPACE, notificationRegistrationKey.getNamespace());
        assertEquals(NOTIFICATION_NAME, notificationRegistrationKey.getNotificationName());
        assertEquals("DISABLED", response.getNotificationRegistrationStatus());
    }

    @Test
    public void testUpdateNotificationRegistrationStatusAssertUpdateSuccessTrim()
    {
        BusinessObjectDataNotificationRegistrationEntity notificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, getTestJobActions());
        notificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusDaoHelper.getNotificationRegistrationStatus("ENABLED"));

        NotificationRegistrationStatusUpdateRequest notificationRegistrationStatusUpdateRequest =
            new NotificationRegistrationStatusUpdateRequest(BLANK_TEXT + "DISABLED" + BLANK_TEXT);
        NotificationRegistrationStatusUpdateResponse response = notificationRegistrationStatusService
            .updateNotificationRegistrationStatus(BLANK_TEXT + NAMESPACE + BLANK_TEXT, BLANK_TEXT + NOTIFICATION_NAME + BLANK_TEXT,
                notificationRegistrationStatusUpdateRequest);
        assertNotNull(response);
        NotificationRegistrationKey notificationRegistrationKey = response.getNotificationRegistrationKey();
        assertNotNull(notificationRegistrationKey);
        assertEquals(NAMESPACE, notificationRegistrationKey.getNamespace());
        assertEquals(NOTIFICATION_NAME, notificationRegistrationKey.getNotificationName());
        assertEquals("DISABLED", response.getNotificationRegistrationStatus());
    }

    @Test
    public void testUpdateNotificationRegistrationStatusAssertThrowWhenNamespaceNull()
    {
        BusinessObjectDataNotificationRegistrationEntity notificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, getTestJobActions());
        notificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusDaoHelper.getNotificationRegistrationStatus("ENABLED"));

        NotificationRegistrationStatusUpdateRequest notificationRegistrationStatusUpdateRequest = new NotificationRegistrationStatusUpdateRequest("DISABLED");
        try
        {
            notificationRegistrationStatusService.updateNotificationRegistrationStatus(null, NOTIFICATION_NAME, notificationRegistrationStatusUpdateRequest);
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
        BusinessObjectDataNotificationRegistrationEntity notificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, getTestJobActions());
        notificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusDaoHelper.getNotificationRegistrationStatus("ENABLED"));

        NotificationRegistrationStatusUpdateRequest notificationRegistrationStatusUpdateRequest = new NotificationRegistrationStatusUpdateRequest("DISABLED");
        try
        {
            notificationRegistrationStatusService
                .updateNotificationRegistrationStatus(BLANK_TEXT, NOTIFICATION_NAME, notificationRegistrationStatusUpdateRequest);
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
        BusinessObjectDataNotificationRegistrationEntity notificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, getTestJobActions());
        notificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusDaoHelper.getNotificationRegistrationStatus("ENABLED"));

        NotificationRegistrationStatusUpdateRequest notificationRegistrationStatusUpdateRequest = new NotificationRegistrationStatusUpdateRequest("DISABLED");
        try
        {
            notificationRegistrationStatusService.updateNotificationRegistrationStatus(NAMESPACE, null, notificationRegistrationStatusUpdateRequest);
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
        BusinessObjectDataNotificationRegistrationEntity notificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, getTestJobActions());
        notificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusDaoHelper.getNotificationRegistrationStatus("ENABLED"));

        NotificationRegistrationStatusUpdateRequest notificationRegistrationStatusUpdateRequest = new NotificationRegistrationStatusUpdateRequest("DISABLED");
        try
        {
            notificationRegistrationStatusService.updateNotificationRegistrationStatus(NAMESPACE, BLANK_TEXT, notificationRegistrationStatusUpdateRequest);
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
        BusinessObjectDataNotificationRegistrationEntity notificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, getTestJobActions());
        notificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusDaoHelper.getNotificationRegistrationStatus("ENABLED"));

        NotificationRegistrationStatusUpdateRequest notificationRegistrationStatusUpdateRequest = new NotificationRegistrationStatusUpdateRequest(null);
        try
        {
            notificationRegistrationStatusService
                .updateNotificationRegistrationStatus(NAMESPACE, NOTIFICATION_NAME, notificationRegistrationStatusUpdateRequest);
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
        BusinessObjectDataNotificationRegistrationEntity notificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, getTestJobActions());
        notificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusDaoHelper.getNotificationRegistrationStatus("ENABLED"));

        NotificationRegistrationStatusUpdateRequest notificationRegistrationStatusUpdateRequest = new NotificationRegistrationStatusUpdateRequest(BLANK_TEXT);
        try
        {
            notificationRegistrationStatusService
                .updateNotificationRegistrationStatus(NAMESPACE, NOTIFICATION_NAME, notificationRegistrationStatusUpdateRequest);
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
        BusinessObjectDataNotificationRegistrationEntity notificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE,
                NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS, getTestJobActions());
        notificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusDaoHelper.getNotificationRegistrationStatus("ENABLED"));

        NotificationRegistrationStatusUpdateRequest notificationRegistrationStatusUpdateRequest = new NotificationRegistrationStatusUpdateRequest("DISABLED");
        try
        {
            notificationRegistrationStatusService
                .updateNotificationRegistrationStatus(NAMESPACE, "DOES_NOT_EXIST", notificationRegistrationStatusUpdateRequest);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(ObjectNotFoundException.class, e.getClass());
            assertEquals("The notification registration with namespace \"" + NAMESPACE + "\" and name \"DOES_NOT_EXIST\" was not found.", e.getMessage());
        }
    }
}

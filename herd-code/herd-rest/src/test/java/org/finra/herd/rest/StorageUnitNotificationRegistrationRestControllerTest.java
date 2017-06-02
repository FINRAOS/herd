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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import javax.servlet.ServletRequest;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.mock.web.MockHttpServletRequest;

import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.StorageUnitNotificationFilter;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistration;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationCreateRequest;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationKeys;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationUpdateRequest;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.service.StorageUnitNotificationRegistrationService;

/**
 * This class tests various functionality within the storage unit notification registration REST controller.
 */
public class StorageUnitNotificationRegistrationRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private StorageUnitNotificationRegistrationRestController storageUnitNotificationRegistrationRestController;

    @Mock
    private StorageUnitNotificationRegistrationService storageUnitNotificationRegistrationService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateStorageUnitNotificationRegistration()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        StorageUnitNotificationRegistrationCreateRequest request = new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        StorageUnitNotificationRegistration storageUnitNotificationRegistration = new StorageUnitNotificationRegistration(ID, notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        when(storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request)).thenReturn(storageUnitNotificationRegistration);

        // Create a business object data notification.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration =
            storageUnitNotificationRegistrationRestController.createStorageUnitNotificationRegistration(request);

        // Verify the external calls.
        verify(storageUnitNotificationRegistrationService).createStorageUnitNotificationRegistration(request);
        verifyNoMoreInteractions(storageUnitNotificationRegistrationService);

        // Validate the returned object.
        assertEquals(storageUnitNotificationRegistration, resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testDeleteStorageUnitNotificationRegistration()
    {
        NotificationRegistrationKey storageUnitNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        StorageUnitNotificationRegistration storageUnitNotificationRegistration =
            new StorageUnitNotificationRegistration(ID, storageUnitNotificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                    STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
                NotificationRegistrationStatusEntity.ENABLED);

        when(storageUnitNotificationRegistrationService.deleteStorageUnitNotificationRegistration(storageUnitNotificationRegistrationKey))
            .thenReturn(storageUnitNotificationRegistration);

        // Delete this business object data notification.
        StorageUnitNotificationRegistration deletedStorageUnitNotificationRegistration =
            storageUnitNotificationRegistrationRestController.deleteStorageUnitNotification(NAMESPACE, NOTIFICATION_NAME);

        // Verify the external calls.
        verify(storageUnitNotificationRegistrationService).deleteStorageUnitNotificationRegistration(storageUnitNotificationRegistrationKey);
        verifyNoMoreInteractions(storageUnitNotificationRegistrationService);

        // Validate the returned object.
        assertEquals(storageUnitNotificationRegistration, deletedStorageUnitNotificationRegistration);
    }

    @Test
    public void testGetStorageUnitNotificationRegistration()
    {
        NotificationRegistrationKey storageUnitNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        StorageUnitNotificationRegistration storageUnitNotificationRegistration =
            new StorageUnitNotificationRegistration(ID, storageUnitNotificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                    STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
                NotificationRegistrationStatusEntity.ENABLED);

        when(storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistration(storageUnitNotificationRegistrationKey))
            .thenReturn(storageUnitNotificationRegistration);

        // Retrieve the business object data notification.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration =
            storageUnitNotificationRegistrationRestController.getStorageUnitNotificationRegistration(NAMESPACE, NOTIFICATION_NAME);

        // Verify the external calls.
        verify(storageUnitNotificationRegistrationService).getStorageUnitNotificationRegistration(storageUnitNotificationRegistrationKey);
        verifyNoMoreInteractions(storageUnitNotificationRegistrationService);

        // Validate the returned object.
        assertEquals(storageUnitNotificationRegistration, resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNamespace()
    {

        StorageUnitNotificationRegistrationKeys storageUnitNotificationRegistrationKeys =
            new StorageUnitNotificationRegistrationKeys(notificationRegistrationDaoTestHelper.getExpectedNotificationRegistrationKeys());

        when(storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNamespace(NAMESPACE))
            .thenReturn(storageUnitNotificationRegistrationKeys);


        // Retrieve a list of business object data notification registration keys.
        StorageUnitNotificationRegistrationKeys resultStorageUnitNotificationRegistrationKeys =
            storageUnitNotificationRegistrationRestController.getStorageUnitNotificationRegistrationsByNamespace(NAMESPACE);

        // Verify the external calls.
        verify(storageUnitNotificationRegistrationService).getStorageUnitNotificationRegistrationsByNamespace(NAMESPACE);
        verifyNoMoreInteractions(storageUnitNotificationRegistrationService);

        // Validate the returned object.
        assertEquals(storageUnitNotificationRegistrationKeys, resultStorageUnitNotificationRegistrationKeys);
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNotificationFilter()
    {
        // Create a business object data notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        StorageUnitNotificationRegistrationKeys storageUnitNotificationRegistrationKeys =
            new StorageUnitNotificationRegistrationKeys(Arrays.asList(notificationRegistrationKey));

        StorageUnitNotificationFilter storageUnitNotificationFilter =
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, null, null, null);

        when(storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNotificationFilter(storageUnitNotificationFilter))
            .thenReturn(storageUnitNotificationRegistrationKeys);

        // Retrieve a list of business object data notification registration keys.
        StorageUnitNotificationRegistrationKeys resultStorageUnitNotificationRegistrationKeys = storageUnitNotificationRegistrationRestController
            .getStorageUnitNotificationRegistrationsByNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                getServletRequestWithNotificationFilterParameters());

        // Verify the external calls.
        verify(storageUnitNotificationRegistrationService).getStorageUnitNotificationRegistrationsByNotificationFilter(storageUnitNotificationFilter);
        verifyNoMoreInteractions(storageUnitNotificationRegistrationService);

        // Validate the returned object.
        assertEquals(storageUnitNotificationRegistrationKeys, resultStorageUnitNotificationRegistrationKeys);
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNotificationFilterMissingOptionalParameters()
    {
        // Create a business object data notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);
        StorageUnitNotificationRegistrationKeys storageUnitNotificationRegistrationKeys =
            new StorageUnitNotificationRegistrationKeys(Arrays.asList(notificationRegistrationKey));

        StorageUnitNotificationFilter storageUnitNotificationFilter =
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, null, null, null, null);

        when(storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNotificationFilter(storageUnitNotificationFilter))
            .thenReturn(storageUnitNotificationRegistrationKeys);

        // Retrieve a list of business object data notification registration keys by not specifying optional parameters.
        StorageUnitNotificationRegistrationKeys resultStorageUnitNotificationRegistrationKeys = storageUnitNotificationRegistrationRestController
            .getStorageUnitNotificationRegistrationsByNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE,
                getServletRequestWithNotificationFilterParameters());
        // Verify the external calls.
        verify(storageUnitNotificationRegistrationService).getStorageUnitNotificationRegistrationsByNotificationFilter(storageUnitNotificationFilter);
        verifyNoMoreInteractions(storageUnitNotificationRegistrationService);
        // Validate the returned object.
        assertEquals(storageUnitNotificationRegistrationKeys, resultStorageUnitNotificationRegistrationKeys);
    }

    @Test
    public void testUpdateStorageUnitNotificationRegistration()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        StorageUnitNotificationRegistration storageUnitNotificationRegistration = new StorageUnitNotificationRegistration(ID, notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2,
                STORAGE_UNIT_STATUS_3, STORAGE_UNIT_STATUS_4), notificationRegistrationDaoTestHelper.getTestJobActions2(),
            NotificationRegistrationStatusEntity.DISABLED);


        StorageUnitNotificationRegistrationUpdateRequest request =
            new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2,
                    STORAGE_UNIT_STATUS_3, STORAGE_UNIT_STATUS_4), notificationRegistrationDaoTestHelper.getTestJobActions2(),
                NotificationRegistrationStatusEntity.DISABLED);

        when(storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(notificationRegistrationKey, request))
            .thenReturn(storageUnitNotificationRegistration);


        // Update the business object data notification registration.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration =
            storageUnitNotificationRegistrationRestController.updateStorageUnitNotificationRegistration(NAMESPACE, NOTIFICATION_NAME, request);

        // Verify the external calls.
        verify(storageUnitNotificationRegistrationService).updateStorageUnitNotificationRegistration(notificationRegistrationKey, request);
        verifyNoMoreInteractions(storageUnitNotificationRegistrationService);

        // Validate the returned object.
        assertEquals(storageUnitNotificationRegistration, resultStorageUnitNotificationRegistration);

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

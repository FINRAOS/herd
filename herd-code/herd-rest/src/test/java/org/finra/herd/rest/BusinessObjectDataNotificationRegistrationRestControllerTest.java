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
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.mock.web.MockHttpServletRequest;

import org.finra.herd.model.api.xml.BusinessObjectDataNotificationFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistration;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationKeys;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationUpdateRequest;
import org.finra.herd.model.api.xml.JobAction;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.service.BusinessObjectDataNotificationRegistrationService;

/**
 * This class tests various functionality within the business object data notification registration REST controller.
 */
public class BusinessObjectDataNotificationRegistrationRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private BusinessObjectDataNotificationRegistrationRestController businessObjectDataNotificationRegistrationRestController;

    @Mock
    private BusinessObjectDataNotificationRegistrationService businessObjectDataNotificationRegistrationService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistration()
    {
        // Create a notification registration key.
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create a business object data notification filter.
        BusinessObjectDataNotificationFilter businessObjectDataNotificationFilter =
            new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                BDATA_STATUS, BDATA_STATUS_2);

        // Create a list of job actions.
        List<JobAction> jobActions = Arrays.asList(new JobAction(JOB_NAMESPACE, JOB_NAME, CORRELATION_DATA));

        // Create a business object data notification registration create request.
        BusinessObjectDataNotificationRegistrationCreateRequest businessObjectDataNotificationRegistrationCreateRequest =
            new BusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
                businessObjectDataNotificationFilter, jobActions, NOTIFICATION_REGISTRATION_STATUS);

        // Create a business object data notification registration.
        BusinessObjectDataNotificationRegistration businessObjectDataNotificationRegistration =
            new BusinessObjectDataNotificationRegistration(ID, businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
                businessObjectDataNotificationFilter, jobActions, NOTIFICATION_REGISTRATION_STATUS);

        // Mock the external calls.
        when(businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationCreateRequest))
            .thenReturn(businessObjectDataNotificationRegistration);

        // Call the method under test.
        BusinessObjectDataNotificationRegistration result = businessObjectDataNotificationRegistrationRestController
            .createBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationCreateRequest);

        // Verify the external calls.
        verify(businessObjectDataNotificationRegistrationService)
            .createBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationCreateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataNotificationRegistration, result);
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistration()
    {
        // Create a notification registration key.
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create a business object data notification filter.
        BusinessObjectDataNotificationFilter businessObjectDataNotificationFilter =
            new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                BDATA_STATUS, BDATA_STATUS_2);

        // Create a list of job actions.
        List<JobAction> jobActions = Arrays.asList(new JobAction(JOB_NAMESPACE, JOB_NAME, CORRELATION_DATA));

        // Create a business object data notification registration.
        BusinessObjectDataNotificationRegistration businessObjectDataNotificationRegistration =
            new BusinessObjectDataNotificationRegistration(ID, businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
                businessObjectDataNotificationFilter, jobActions, NOTIFICATION_REGISTRATION_STATUS);

        // Mock the external calls.
        when(businessObjectDataNotificationRegistrationService.deleteBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey))
            .thenReturn(businessObjectDataNotificationRegistration);

        // Call the method under test.
        BusinessObjectDataNotificationRegistration result =
            businessObjectDataNotificationRegistrationRestController.deleteBusinessObjectDataNotification(NAMESPACE, NOTIFICATION_NAME);

        // Verify the external calls.
        verify(businessObjectDataNotificationRegistrationService)
            .deleteBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataNotificationRegistration, result);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistration()
    {
        // Create a notification registration key.
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create a business object data notification filter.
        BusinessObjectDataNotificationFilter businessObjectDataNotificationFilter =
            new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                BDATA_STATUS, BDATA_STATUS_2);

        // Create a list of job actions.
        List<JobAction> jobActions = Arrays.asList(new JobAction(JOB_NAMESPACE, JOB_NAME, CORRELATION_DATA));

        // Create a business object data notification registration.
        BusinessObjectDataNotificationRegistration businessObjectDataNotificationRegistration =
            new BusinessObjectDataNotificationRegistration(ID, businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
                businessObjectDataNotificationFilter, jobActions, NOTIFICATION_REGISTRATION_STATUS);

        // Mock the external calls.
        when(businessObjectDataNotificationRegistrationService.getBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey))
            .thenReturn(businessObjectDataNotificationRegistration);

        // Call the method under test.
        BusinessObjectDataNotificationRegistration result =
            businessObjectDataNotificationRegistrationRestController.getBusinessObjectDataNotificationRegistration(NAMESPACE, NOTIFICATION_NAME);

        // Verify the external calls.
        verify(businessObjectDataNotificationRegistrationService).getBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataNotificationRegistration, result);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsByNamespace()
    {
        // Create notification registration keys.
        BusinessObjectDataNotificationRegistrationKeys businessObjectDataNotificationRegistrationKeys =
            new BusinessObjectDataNotificationRegistrationKeys(Arrays.asList(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME)));

        // Mock the external calls.
        when(businessObjectDataNotificationRegistrationService.getBusinessObjectDataNotificationRegistrationsByNamespace(NAMESPACE))
            .thenReturn(businessObjectDataNotificationRegistrationKeys);

        // Call the method under test.
        BusinessObjectDataNotificationRegistrationKeys result =
            businessObjectDataNotificationRegistrationRestController.getBusinessObjectDataNotificationRegistrationsByNamespace(NAMESPACE);

        // Verify the external calls.
        verify(businessObjectDataNotificationRegistrationService).getBusinessObjectDataNotificationRegistrationsByNamespace(NAMESPACE);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataNotificationRegistrationKeys, result);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsByNotificationFilter()
    {
        // Create a business object data notification filter.
        BusinessObjectDataNotificationFilter businessObjectDataNotificationFilter =
            new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                NO_BDATA_STATUS, NO_BDATA_STATUS);

        // Create notification registration keys.
        BusinessObjectDataNotificationRegistrationKeys businessObjectDataNotificationRegistrationKeys =
            new BusinessObjectDataNotificationRegistrationKeys(Arrays.asList(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME)));

        // Create a mock servlet request that contains hard coded business object data notification filter parameters.
        MockHttpServletRequest servletRequest = new MockHttpServletRequest();
        servletRequest.setParameter("businessObjectDefinitionNamespace", BDEF_NAMESPACE);
        servletRequest.setParameter("businessObjectDefinitionName", BDEF_NAME);
        servletRequest.setParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE);
        servletRequest.setParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE);

        // Mock the external calls.
        when(businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistrationsByNotificationFilter(businessObjectDataNotificationFilter))
            .thenReturn(businessObjectDataNotificationRegistrationKeys);

        // Call the method under test.
        BusinessObjectDataNotificationRegistrationKeys result = businessObjectDataNotificationRegistrationRestController
            .getBusinessObjectDataNotificationRegistrationsByNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                servletRequest);

        // Verify the external calls.
        verify(businessObjectDataNotificationRegistrationService)
            .getBusinessObjectDataNotificationRegistrationsByNotificationFilter(businessObjectDataNotificationFilter);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataNotificationRegistrationKeys, result);
    }

    @Test
    public void testUpdateBusinessObjectDataNotificationRegistration()
    {
        // Create a notification registration key.
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create a business object data notification filter.
        BusinessObjectDataNotificationFilter businessObjectDataNotificationFilter =
            new BusinessObjectDataNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                BDATA_STATUS, BDATA_STATUS_2);

        // Create a list of job actions.
        List<JobAction> jobActions = Arrays.asList(new JobAction(JOB_NAMESPACE, JOB_NAME, CORRELATION_DATA));

        // Create a business object data notification registration update request.
        BusinessObjectDataNotificationRegistrationUpdateRequest businessObjectDataNotificationRegistrationUpdateRequest =
            new BusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE, businessObjectDataNotificationFilter, jobActions,
                NOTIFICATION_REGISTRATION_STATUS);

        // Create a business object data notification registration.
        BusinessObjectDataNotificationRegistration businessObjectDataNotificationRegistration =
            new BusinessObjectDataNotificationRegistration(ID, businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
                businessObjectDataNotificationFilter, jobActions, NOTIFICATION_REGISTRATION_STATUS);

        // Mock the external calls.
        when(businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey,
            businessObjectDataNotificationRegistrationUpdateRequest)).thenReturn(businessObjectDataNotificationRegistration);

        // Call the method under test.
        BusinessObjectDataNotificationRegistration result = businessObjectDataNotificationRegistrationRestController
            .updateBusinessObjectDataNotificationRegistration(NAMESPACE, NOTIFICATION_NAME, businessObjectDataNotificationRegistrationUpdateRequest);

        // Verify the external calls.
        verify(businessObjectDataNotificationRegistrationService)
            .updateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey,
                businessObjectDataNotificationRegistrationUpdateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataNotificationRegistration, result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataNotificationRegistrationService);
    }
}

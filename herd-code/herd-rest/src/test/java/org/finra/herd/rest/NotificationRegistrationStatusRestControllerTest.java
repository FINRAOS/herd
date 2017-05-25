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

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.NotificationRegistrationStatusUpdateRequest;
import org.finra.herd.model.api.xml.NotificationRegistrationStatusUpdateResponse;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.service.NotificationRegistrationStatusService;

public class NotificationRegistrationStatusRestControllerTest extends AbstractRestTest
{
    @Mock
    private NotificationRegistrationStatusService notificationRegistrationStatusService;

    @InjectMocks
    private NotificationRegistrationStatusRestController notificationRegistrationStatusRestController;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testUpdateNotificationRegistrationStatusAssertSuccess()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        NotificationRegistrationStatusUpdateRequest request = new NotificationRegistrationStatusUpdateRequest(NotificationRegistrationStatusEntity.DISABLED);
        NotificationRegistrationStatusUpdateResponse response =
            new NotificationRegistrationStatusUpdateResponse(notificationRegistrationKey, NotificationRegistrationStatusEntity.DISABLED);

        when(notificationRegistrationStatusService.updateNotificationRegistrationStatus(NAMESPACE, NOTIFICATION_NAME, request)).thenReturn(response);

        NotificationRegistrationStatusUpdateResponse resultResponse =
            notificationRegistrationStatusRestController.updateNotificationRegistrationStatus(NAMESPACE, NOTIFICATION_NAME, request);

        // Verify the external calls.
        verify(notificationRegistrationStatusService).updateNotificationRegistrationStatus(NAMESPACE, NOTIFICATION_NAME, request);
        verifyNoMoreInteractions(notificationRegistrationStatusService);
        // Validate the returned object.
        assertEquals(response, resultResponse);
    }

}

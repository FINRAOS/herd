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

import org.junit.Test;

import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.NotificationRegistrationStatusUpdateRequest;
import org.finra.herd.model.api.xml.NotificationRegistrationStatusUpdateResponse;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;

public class NotificationRegistrationStatusRestControllerTest extends AbstractRestTest
{
    @Test
    public void testUpdateNotificationRegistrationStatusAssertSuccess()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        notificationRegistrationDaoTestHelper
            .createBusinessObjectDataNotificationRegistrationEntity(notificationRegistrationKey, NOTIFICATION_EVENT_TYPE, NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        NotificationRegistrationStatusUpdateResponse response = notificationRegistrationStatusRestController
            .updateNotificationRegistrationStatus(NAMESPACE, NOTIFICATION_NAME,
                new NotificationRegistrationStatusUpdateRequest(NotificationRegistrationStatusEntity.DISABLED));

        assertEquals(new NotificationRegistrationStatusUpdateResponse(notificationRegistrationKey, NotificationRegistrationStatusEntity.DISABLED), response);
    }
}

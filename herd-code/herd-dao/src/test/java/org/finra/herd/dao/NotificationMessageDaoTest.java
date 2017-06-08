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
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.jpa.NotificationMessageEntity;

public class NotificationMessageDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetOldestNotificationMessage()
    {
        // Create database entries required for testing.
        List<NotificationMessageEntity> notificationMessageEntities = Arrays
            .asList(notificationMessageDaoTestHelper.createNotificationMessageEntity(MESSAGE_TYPE, MESSAGE_DESTINATION, MESSAGE_TEXT),
                notificationMessageDaoTestHelper.createNotificationMessageEntity(MESSAGE_TYPE_2, MESSAGE_DESTINATION_2, MESSAGE_TEXT_2));

        // Retrieve the oldest notification message.
        NotificationMessageEntity result = notificationMessageDao.getOldestNotificationMessage();

        // Validate the results.
        assertEquals(notificationMessageEntities.get(0), result);
    }

    @Test
    public void testGetOldestNotificationMessageQueueIsEmpty()
    {
        // Try to retrieve the oldest notification message from an empty table.
        assertNull(notificationMessageDao.getOldestNotificationMessage());
    }
}

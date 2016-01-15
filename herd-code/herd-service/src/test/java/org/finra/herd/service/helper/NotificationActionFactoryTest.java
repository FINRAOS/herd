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

import org.finra.herd.service.AbstractServiceTest;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * This class tests functionality within the NotificationActionFactory.
 */
public class NotificationActionFactoryTest extends AbstractServiceTest
{
    @Autowired
    NotificationActionFactory notificationActionFactory;
    
    @Test
    public void testCreateBusinessObjectDataNotificationNoHandler() throws Exception
    {
        try
        {
            notificationActionFactory.getNotificationActionHandler("NO_EXIST", "NO_EXIST");
            fail("Should throw an IllegalArgumentException as no supported handler if defined for this notification event type.");
        }
        catch (IllegalArgumentException ex)
        {
            assertTrue(ex.getMessage().startsWith("No supported notification handler found for notificationType"));
        }
    }
}

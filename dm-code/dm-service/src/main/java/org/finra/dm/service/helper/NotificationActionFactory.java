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
package org.finra.dm.service.helper;

import java.util.HashMap;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import org.finra.dm.service.NotificationActionService;

/**
 * Factory class for notification handlers.
 */
@Component
public class NotificationActionFactory implements InitializingBean
{
    @Autowired
    private ApplicationContext applicationContext;

    private Map<String, NotificationActionService> notificationActionHandlerServiceMap;

    /**
     * This method returns the notification action handler for the given notification type and action type.
     *
     * @param notificationType the notification type
     * @param actionType the notification action type
     *
     * @return NotificationActionHandlerService the notification action handler service
     */
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
        justification = "This is a false positive. afterPropertiesSet is called before this method which will ensure notificationActionHandlerServiceMap is " +
            "not null.")
    public NotificationActionService getNotificationActionHandler(String notificationType, String actionType)
    {
        NotificationActionService actionHandlerService = notificationActionHandlerServiceMap.get(notificationType + "|" + actionType);
        if (actionHandlerService == null)
        {
            throw new IllegalArgumentException(
                "No supported notification handler found for notificationType \"" + notificationType + "\" and actionType: \"" + actionType + "\".");
        }
        return actionHandlerService;
    }

    @Override
    public void afterPropertiesSet() throws Exception
    {
        notificationActionHandlerServiceMap = new HashMap<>();

        // Add all the available notification action helpers.
        Map<String, NotificationActionService> handlerBeanMap = applicationContext.getBeansOfType(NotificationActionService.class);
        for (NotificationActionService actionHandler : handlerBeanMap.values())
        {
            notificationActionHandlerServiceMap.put(actionHandler.getNotificationType() + "|" + actionHandler.getNotificationActionType(), actionHandler);
        }
    }
}
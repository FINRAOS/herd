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

import org.finra.herd.model.dto.NotificationEventParamsDto;
import org.finra.herd.service.helper.BusinessObjectDataHelper;

/**
 * The notification action service.
 */
public interface NotificationActionService
{
    /**
     * Return the type of notification type it supports.
     *
     * @return the notification type
     */
    public String getNotificationType();

    /**
     * Return the type of notification action it supports.
     *
     * @return the notification action type.
     */
    public String getNotificationActionType();

    /**
     * Perform the notification action.
     */
    public Object performNotificationAction(NotificationEventParamsDto notificationEventParams) throws Exception;

    /**
     * Gets the identifying information.
     */
    public String getIdentifyingInformation(NotificationEventParamsDto notificationEventParams, BusinessObjectDataHelper businessObjectDataHelper);
}

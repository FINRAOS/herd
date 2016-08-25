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
package org.finra.herd.service.impl;

import org.finra.herd.service.NotificationActionService;

public abstract class NotificationActionServiceImpl implements NotificationActionService
{
    /**
     * The parameters that are sent along with the notification.
     */
    protected static final String PARAM_NAMESPACE = "notification_namespace";

    protected static final String PARAM_NOTIFICATION_NAME = "notification_name";

    protected NotificationActionServiceImpl()
    {
        // Prevent classes from instantiating except sub-classes.
    }
}

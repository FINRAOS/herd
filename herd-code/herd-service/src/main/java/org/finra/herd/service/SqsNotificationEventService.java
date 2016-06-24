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

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.JmsMessage;

public interface SqsNotificationEventService
{
    /**
     * Handles the notification for the business object data status changes to the SQS.
     *
     * @param businessObjectDataKey the business object data key
     * @param newBusinessObjectDataStatus the new business object data status
     * @param oldBusinessObjectDataStatus the old business object data status
     *
     * @return the JMS message that got queued for publishing
     */
    public JmsMessage processBusinessObjectDataStatusChangeNotificationEvent(BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus,
        String oldBusinessObjectDataStatus);

    /**
     * Handles the system monitor event notification.
     *
     * @param systemMonitorRequestPayload the system monitor incoming payload
     *
     * @return the JMS message that got queued for publishing
     */
    public JmsMessage processSystemMonitorNotificationEvent(String systemMonitorRequestPayload);
}

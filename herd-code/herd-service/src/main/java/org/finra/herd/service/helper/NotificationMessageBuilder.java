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

import java.util.List;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.NotificationMessage;

/**
 * The builder that builds notification messages.
 */
public interface NotificationMessageBuilder
{
    /**
     * Builds a list of notification messages for the business object data status change event. The result list might be empty if if no messages should be
     * sent.
     *
     * @param businessObjectDataKey the business object data key for the object whose status changed
     * @param newBusinessObjectDataStatus the new business object data status
     * @param oldBusinessObjectDataStatus the old business object data status
     *
     * @return the list of business object data status change notification messages
     */
    public List<NotificationMessage> buildBusinessObjectDataStatusChangeMessages(BusinessObjectDataKey businessObjectDataKey,
        String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus);

    /**
     * Builds the message for the ESB system monitor response.
     *
     * @param systemMonitorRequestPayload the system monitor request payload
     *
     * @return the outgoing system monitor notification message or null if no message should be sent
     */
    public NotificationMessage buildSystemMonitorResponse(String systemMonitorRequestPayload);
}

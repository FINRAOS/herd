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

import org.finra.herd.model.api.xml.BusinessObjectDataKey;

/**
 * The builder that builds the messages for SQS.
 */
public interface SqsMessageBuilder
{
    /**
     * Builds the message for the business object data status change.
     *
     * @param businessObjectDataKey the business object data key for the object whose status changed.
     * @param newBusinessObjectDataStatus the new business object data status.
     * @param oldBusinessObjectDataStatus the old business object data status.
     *
     * @return the status change message or null if no message should be sent.
     */
    public String buildBusinessObjectDataStatusChangeMessage(BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus,
        String oldBusinessObjectDataStatus);

    /**
     * Builds the message for the ESB system monitor response.
     *
     * @param systemMonitorRequestPayload the system monitor request payload.
     *
     * @return the outgoing system monitor message or null if no message should be sent.
     */
    public String buildSystemMonitorResponse(String systemMonitorRequestPayload);
}

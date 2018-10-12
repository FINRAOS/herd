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
package org.finra.herd.service.helper.notification;

import java.util.HashMap;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.dto.BusinessObjectFormatVersionChangeNotificationEvent;
import org.finra.herd.model.dto.NotificationEvent;

/**
 * The builder that knows how to build Business Object Format Version Change notification messages
 */
@Component
public class BusinessObjectFormatVersionChangeMessageBuilder extends AbstractNotificationMessageBuilder implements NotificationMessageBuilder
{
    @Override
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification =
        "The NotificationEvent is cast to a BusinessObjectFormatVersionChangeNotificationEvent which is always the case since" +
            " we manage the event type to a builder in a map defined in NotificationMessageManager")
    public Map<String, Object> getNotificationMessageVelocityContextMap(NotificationEvent notificationEvent)
    {
        BusinessObjectFormatVersionChangeNotificationEvent event = (BusinessObjectFormatVersionChangeNotificationEvent) notificationEvent;
        Map<String, Object> velocityContextMap = new HashMap<>();

        addObjectPropertyToContext(velocityContextMap, "businessObjectFormatKey", event.getBusinessObjectFormatKey(),
            escapeJsonBusinessObjectFormatKey(event.getBusinessObjectFormatKey()), escapeXmlBusinessObjectFormatKey(event.getBusinessObjectFormatKey()));
        velocityContextMap.put("newBusinessObjectFormatVersion", event.getBusinessObjectFormatKey().getBusinessObjectFormatVersion());
        velocityContextMap.put("oldBusinessObjectFormatVersion", event.getOldBusinessObjectFormatVersion());
        addStringPropertyToContext(velocityContextMap, "namespace", event.getBusinessObjectFormatKey().getNamespace());

        return velocityContextMap;
    }

    /**
     * Creates a JSON escaped copy of the specified business object format key.
     *
     * @param businessObjectFormatKey the business object format key
     *
     * @return the JSON escaped business object format key
     */
    private BusinessObjectFormatKey escapeJsonBusinessObjectFormatKey(final BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Build and return a JSON escaped business object format key.
        return new BusinessObjectFormatKey(escapeJson(businessObjectFormatKey.getNamespace()),
            escapeJson(businessObjectFormatKey.getBusinessObjectDefinitionName()), escapeJson(businessObjectFormatKey.getBusinessObjectFormatUsage()),
            escapeJson(businessObjectFormatKey.getBusinessObjectFormatFileType()), businessObjectFormatKey.getBusinessObjectFormatVersion());
    }

    /**
     * Creates an XML escaped copy of the specified business object format key.
     *
     * @param businessObjectFormatKey the business object format key
     *
     * @return the XML escaped business object format key
     */
    private BusinessObjectFormatKey escapeXmlBusinessObjectFormatKey(final BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Build and return an XML escaped business object format key.
        return new BusinessObjectFormatKey(escapeXml(businessObjectFormatKey.getNamespace()),
            escapeXml(businessObjectFormatKey.getBusinessObjectDefinitionName()), escapeXml(businessObjectFormatKey.getBusinessObjectFormatUsage()),
            escapeXml(businessObjectFormatKey.getBusinessObjectFormatFileType()), businessObjectFormatKey.getBusinessObjectFormatVersion());
    }

}

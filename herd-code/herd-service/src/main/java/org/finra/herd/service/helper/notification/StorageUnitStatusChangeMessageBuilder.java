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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.NotificationEvent;
import org.finra.herd.model.dto.StorageUnitStatusChangeNotificationEvent;
import org.finra.herd.service.helper.notification.AbstractNotificationMessageBuilder;
import org.finra.herd.service.helper.notification.NotificationMessageBuilder;

/**
 * The builder that knows how to build Storage Unit Status Change notification messages
 */
@Component
public class StorageUnitStatusChangeMessageBuilder extends AbstractNotificationMessageBuilder implements NotificationMessageBuilder
{
    @Override
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification =
        "The NotificationEvent is cast to a StorageUnitStatusChangeNotificationEvent which is always the case since" +
            " we manage the event type to a builder in a map defined in NotificationMessageManager")
    public Map<String, Object> getNotificationMessageVelocityContextMap(NotificationEvent notificationEvent)
    {
        StorageUnitStatusChangeNotificationEvent event = (StorageUnitStatusChangeNotificationEvent) notificationEvent;
        Map<String, Object> velocityContextMap = new HashMap<>();

        addObjectPropertyToContext(velocityContextMap, "businessObjectDataKey", event.getBusinessObjectDataKey(),
            escapeJsonBusinessObjectDataKey(event.getBusinessObjectDataKey()), escapeXmlBusinessObjectDataKey(event.getBusinessObjectDataKey()));
        addStringPropertyToContext(velocityContextMap, "storageName", event.getStorageName());
        addStringPropertyToContext(velocityContextMap, "newStorageUnitStatus", event.getNewStorageUnitStatus());
        addStringPropertyToContext(velocityContextMap, "oldStorageUnitStatus", event.getOldStorageUnitStatus());
        addStringPropertyToContext(velocityContextMap, "namespace", event.getBusinessObjectDataKey().getNamespace());

        return velocityContextMap;
    }

    /**
     * Creates a JSON escaped copy of the specified business object data key.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the JSON escaped business object data key
     */
    private BusinessObjectDataKey escapeJsonBusinessObjectDataKey(final BusinessObjectDataKey businessObjectDataKey)
    {
        // Escape sub-partition values, if they are present.
        List<String> escapedSubPartitionValues = null;
        if (businessObjectDataKey.getSubPartitionValues() != null)
        {
            escapedSubPartitionValues = new ArrayList<>();
            for (String subPartitionValue : businessObjectDataKey.getSubPartitionValues())
            {
                escapedSubPartitionValues.add(escapeJson(subPartitionValue));
            }
        }

        // Build and return a JSON escaped business object data key.
        return new BusinessObjectDataKey(escapeJson(businessObjectDataKey.getNamespace()), escapeJson(businessObjectDataKey.getBusinessObjectDefinitionName()),
            escapeJson(businessObjectDataKey.getBusinessObjectFormatUsage()), escapeJson(businessObjectDataKey.getBusinessObjectFormatFileType()),
            businessObjectDataKey.getBusinessObjectFormatVersion(), escapeJson(businessObjectDataKey.getPartitionValue()), escapedSubPartitionValues,
            businessObjectDataKey.getBusinessObjectDataVersion());
    }

    /**
     * Creates an XML escaped copy of the specified business object data key.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the XML escaped business object data key
     */
    private BusinessObjectDataKey escapeXmlBusinessObjectDataKey(final BusinessObjectDataKey businessObjectDataKey)
    {
        // Escape sub-partition values, if they are present.
        List<String> escapedSubPartitionValues = null;
        if (businessObjectDataKey.getSubPartitionValues() != null)
        {
            escapedSubPartitionValues = new ArrayList<>();
            for (String subPartitionValue : businessObjectDataKey.getSubPartitionValues())
            {
                escapedSubPartitionValues.add(escapeXml(subPartitionValue));
            }
        }

        // Build and return an XML escaped business object data key.
        return new BusinessObjectDataKey(escapeXml(businessObjectDataKey.getNamespace()), escapeXml(businessObjectDataKey.getBusinessObjectDefinitionName()),
            escapeXml(businessObjectDataKey.getBusinessObjectFormatUsage()), escapeXml(businessObjectDataKey.getBusinessObjectFormatFileType()),
            businessObjectDataKey.getBusinessObjectFormatVersion(), escapeXml(businessObjectDataKey.getPartitionValue()), escapedSubPartitionValues,
            businessObjectDataKey.getBusinessObjectDataVersion());
    }

}

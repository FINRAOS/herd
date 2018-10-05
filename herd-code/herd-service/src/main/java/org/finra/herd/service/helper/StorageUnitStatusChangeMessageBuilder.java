package org.finra.herd.service.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.NotificationMessage;

/**
 * The builder that knows how to build Storage Unit Status Change notification messages
 */
@Component
public class StorageUnitStatusChangeMessageBuilder extends AbstractNotificationMessageBuilder
{
    /**
     * Builds a list of notification messages for the business object data status change event. The result list might be empty if if no messages should be
     * sent.
     *
     * @param businessObjectDataKey the business object data key for the object whose status changed
     * @param storageName the storage name
     * @param newStorageUnitStatus the new storage unit status
     * @param oldStorageUnitStatus the old storage unit status, may be null
     *
     * @return the list of business object data status change notification messages
     */
    public List<NotificationMessage> buildMessages(BusinessObjectDataKey businessObjectDataKey, String storageName, String newStorageUnitStatus,
        String oldStorageUnitStatus)
    {
        return buildNotificationMessages(new StorageUnitStatusChangeEvent(businessObjectDataKey, storageName, newStorageUnitStatus, oldStorageUnitStatus));
    }

    @Override
    public Map<String, Object> getNotificationMessageVelocityContextMap(NotificationEvent notificationEvent)
    {
        StorageUnitStatusChangeEvent event = (StorageUnitStatusChangeEvent) notificationEvent;
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

    /**
     * The Storage Unit status change event
     */
    public static class StorageUnitStatusChangeEvent extends NotificationEvent
    {
        private BusinessObjectDataKey businessObjectDataKey;

        private String storageName;

        private String newStorageUnitStatus;

        private String oldStorageUnitStatus;

        public StorageUnitStatusChangeEvent(BusinessObjectDataKey businessObjectDataKey, String storageName, String newStorageUnitStatus,
            String oldStorageUnitStatus)
        {
            setMessageDefinitionKey(ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS);
            setEventName("storageUnitStatusChangeEvent");

            this.businessObjectDataKey = businessObjectDataKey;
            this.storageName = storageName;
            this.newStorageUnitStatus = newStorageUnitStatus;
            this.oldStorageUnitStatus = oldStorageUnitStatus;
        }

        public BusinessObjectDataKey getBusinessObjectDataKey()
        {
            return businessObjectDataKey;
        }

        public void setBusinessObjectDataKey(BusinessObjectDataKey businessObjectDataKey)
        {
            this.businessObjectDataKey = businessObjectDataKey;
        }

        public String getStorageName()
        {
            return storageName;
        }

        public void setStorageName(String storageName)
        {
            this.storageName = storageName;
        }

        public String getNewStorageUnitStatus()
        {
            return newStorageUnitStatus;
        }

        public void setNewStorageUnitStatus(String newStorageUnitStatus)
        {
            this.newStorageUnitStatus = newStorageUnitStatus;
        }

        public String getOldStorageUnitStatus()
        {
            return oldStorageUnitStatus;
        }

        public void setOldStorageUnitStatus(String oldStorageUnitStatus)
        {
            this.oldStorageUnitStatus = oldStorageUnitStatus;
        }
    }
}

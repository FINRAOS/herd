package org.finra.herd.service.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.BooleanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;

/**
 * The builder that knows how to build Business Object Data Status Change notification messages
 */
@Component
public class BusinessObjectDataStatusChangeMessageBuilder extends AbstractNotificationMessageBuilder
{

    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    /**
     * Builds a list of notification messages for the business object data status change event. The result list might be empty if if no messages should be
     * sent.
     *
     * @param businessObjectDataKey the business object data key for the object whose status changed
     * @param newBusinessObjectDataStatus the new business object data status
     * @param oldBusinessObjectDataStatus the old business object data status, may be null
     *
     * @return the list of business object data status change notification messages
     */
    public List<NotificationMessage> buildMessages(BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus,
        String oldBusinessObjectDataStatus)
    {
        return buildNotificationMessages(
            new BusinessObjectDataStatusChangeEvent(businessObjectDataKey, newBusinessObjectDataStatus, oldBusinessObjectDataStatus));
    }

    /**
     * Returns Velocity context map of additional keys and values to place in the velocity context.
     *
     * @param event the business object data key
     *
     * @return the Velocity context map
     */
    @Override
    public Map<String, Object> getNotificationMessageVelocityContextMap(NotificationEvent event)
    {
        BusinessObjectDataStatusChangeEvent businessObjectDataStatusChangeEvent = (BusinessObjectDataStatusChangeEvent) event;
        // Create a context map of values that can be used when building the message.
        Map<String, Object> velocityContextMap = new HashMap<>();
        addObjectPropertyToContext(velocityContextMap, "businessObjectDataKey", businessObjectDataStatusChangeEvent.getBusinessObjectDataKey(),
            escapeJsonBusinessObjectDataKey(businessObjectDataStatusChangeEvent.getBusinessObjectDataKey()),
            escapeXmlBusinessObjectDataKey(businessObjectDataStatusChangeEvent.getBusinessObjectDataKey()));
        addStringPropertyToContext(velocityContextMap, "newBusinessObjectDataStatus", businessObjectDataStatusChangeEvent.getNewBusinessObjectDataStatus());
        addStringPropertyToContext(velocityContextMap, "oldBusinessObjectDataStatus", businessObjectDataStatusChangeEvent.getOldBusinessObjectDataStatus());

        // Retrieve business object data entity and business object data id to the context.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataStatusChangeEvent.getBusinessObjectDataKey());
        velocityContextMap.put("businessObjectDataId", businessObjectDataEntity.getId());

        // Load all attribute definitions for this business object data in a map for easy access.
        Map<String, BusinessObjectDataAttributeDefinitionEntity> attributeDefinitionEntityMap =
            businessObjectFormatHelper.getAttributeDefinitionEntities(businessObjectDataEntity.getBusinessObjectFormat());

        // Build an ordered map of business object data attributes that are flagged to be published in notification messages.
        Map<String, String> businessObjectDataAttributes = new LinkedHashMap<>();
        Map<String, String> businessObjectDataAttributesWithJson = new LinkedHashMap<>();
        Map<String, String> businessObjectDataAttributesWithXml = new LinkedHashMap<>();
        if (!attributeDefinitionEntityMap.isEmpty())
        {
            for (BusinessObjectDataAttributeEntity attributeEntity : businessObjectDataEntity.getAttributes())
            {
                if (attributeDefinitionEntityMap.containsKey(attributeEntity.getName().toUpperCase()))
                {
                    BusinessObjectDataAttributeDefinitionEntity attributeDefinitionEntity =
                        attributeDefinitionEntityMap.get(attributeEntity.getName().toUpperCase());

                    if (BooleanUtils.isTrue(attributeDefinitionEntity.getPublish()))
                    {
                        businessObjectDataAttributes.put(attributeEntity.getName(), attributeEntity.getValue());
                        businessObjectDataAttributesWithJson.put(escapeJson(attributeEntity.getName()), escapeJson(attributeEntity.getValue()));
                        businessObjectDataAttributesWithXml.put(escapeXml(attributeEntity.getName()), escapeXml(attributeEntity.getValue()));
                    }
                }
            }
        }

        // Add the map of business object data attributes to the context.
        addObjectPropertyToContext(velocityContextMap, "businessObjectDataAttributes", businessObjectDataAttributes, businessObjectDataAttributesWithJson,
            businessObjectDataAttributesWithXml);

        // Add the namespace Velocity property for the header.
        addStringPropertyToContext(velocityContextMap, "namespace", businessObjectDataStatusChangeEvent.getBusinessObjectDataKey().getNamespace());

        return velocityContextMap;
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
     * The Business Object Data Status change event
     */
    public static class BusinessObjectDataStatusChangeEvent extends NotificationEvent
    {
        private BusinessObjectDataKey businessObjectDataKey;

        private String newBusinessObjectDataStatus;

        private String oldBusinessObjectDataStatus;

        public BusinessObjectDataStatusChangeEvent(BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus,
            String oldBusinessObjectDataStatus)
        {
            setMessageDefinitionKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS);
            setEventName("businessObjectDataStatusChangeEvent");
            this.businessObjectDataKey = businessObjectDataKey;
            this.newBusinessObjectDataStatus = newBusinessObjectDataStatus;
            this.oldBusinessObjectDataStatus = oldBusinessObjectDataStatus;
        }

        public BusinessObjectDataKey getBusinessObjectDataKey()
        {
            return businessObjectDataKey;
        }

        public void setBusinessObjectDataKey(BusinessObjectDataKey businessObjectDataKey)
        {
            this.businessObjectDataKey = businessObjectDataKey;
        }

        public String getNewBusinessObjectDataStatus()
        {
            return newBusinessObjectDataStatus;
        }

        public void setNewBusinessObjectDataStatus(String newBusinessObjectDataStatus)
        {
            this.newBusinessObjectDataStatus = newBusinessObjectDataStatus;
        }

        public String getOldBusinessObjectDataStatus()
        {
            return oldBusinessObjectDataStatus;
        }

        public void setOldBusinessObjectDataStatus(String oldBusinessObjectDataStatus)
        {
            this.oldBusinessObjectDataStatus = oldBusinessObjectDataStatus;
        }
    }
}

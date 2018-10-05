package org.finra.herd.service.helper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.NotificationMessage;

/**
 * The builder that knows how to build Business Object Format Version Change notification messages
 */
@Component
public class BusinessObjectFormatVersionChangeMessageBuilder extends AbstractNotificationMessageBuilder
{

    /**
     * Builds a list of notification messages for the business object format version change event. The result list might be empty if if no messages should be
     * sent.
     *
     * @param businessObjectFormatKey the business object format key for the object whose version changed
     * @param oldBusinessObjectFormatVersion the old business object format version
     *
     * @return the list of business object data status change notification messages
     */
    public List<NotificationMessage> buildMessages(BusinessObjectFormatKey businessObjectFormatKey, String oldBusinessObjectFormatVersion)
    {
        return buildNotificationMessages(new BusinessObjectFormatVersionChangeEvent(businessObjectFormatKey, oldBusinessObjectFormatVersion));
    }

    @Override
    public Map<String, Object> getNotificationMessageVelocityContextMap(NotificationEvent notificationEvent)
    {
        BusinessObjectFormatVersionChangeEvent event = (BusinessObjectFormatVersionChangeEvent) notificationEvent;
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

    /**
     * The Business Object Format Version change event
     */
    public static class BusinessObjectFormatVersionChangeEvent extends NotificationEvent
    {
        private BusinessObjectFormatKey businessObjectFormatKey;

        private String oldBusinessObjectFormatVersion;

        public BusinessObjectFormatVersionChangeEvent(BusinessObjectFormatKey businessObjectFormatKey, String oldBusinessObjectFormatVersion)
        {
            setMessageDefinitionKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS);
            setEventName("businessObjectFormatVersionChangeEvent");

            this.businessObjectFormatKey = businessObjectFormatKey;
            this.oldBusinessObjectFormatVersion = oldBusinessObjectFormatVersion;
        }

        public BusinessObjectFormatKey getBusinessObjectFormatKey()
        {
            return businessObjectFormatKey;
        }

        public void setBusinessObjectFormatKey(BusinessObjectFormatKey businessObjectFormatKey)
        {
            this.businessObjectFormatKey = businessObjectFormatKey;
        }

        public String getOldBusinessObjectFormatVersion()
        {
            return oldBusinessObjectFormatVersion;
        }

        public void setOldBusinessObjectFormatVersion(String oldBusinessObjectFormatVersion)
        {
            this.oldBusinessObjectFormatVersion = oldBusinessObjectFormatVersion;
        }
    }
}

package org.finra.herd.service.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.datatype.XMLGregorianCalendar;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.UserNamespaceAuthorizationDao;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.NamespaceEntity;

/**
 * The builder that knows how to build Business Object Definition Description suggestion Change notification messages
 */
@Component
public class BusinessObjectDefinitionDescriptionSuggestionChangeMessageBuilder extends AbstractNotificationMessageBuilder
{
    @Autowired
    private UserNamespaceAuthorizationDao userNamespaceAuthorizationDao;

    /**
     * Builds a list of notification messages for the business object definition description suggestion change event. The result list might be empty if if no
     * messages should be sent.
     *
     * @param businessObjectDefinitionDescriptionSuggestion the business object definition description suggestion
     * @param lastUpdatedByUserId the User ID of the user who last updated this business object definition description suggestion
     * @param lastUpdatedOn the timestamp when this business object definition description suggestion was last updated on
     * @param namespaceEntity the namespace entity
     *
     * @return the list of notification messages
     */
    public List<NotificationMessage> buildMessages(BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion,
        String lastUpdatedByUserId, XMLGregorianCalendar lastUpdatedOn, NamespaceEntity namespaceEntity)
    {
        return buildNotificationMessages(
            new BusinessObjectDefinitionDescriptionSuggestionChangeEvent(businessObjectDefinitionDescriptionSuggestion, lastUpdatedByUserId, lastUpdatedOn,
                namespaceEntity));
    }

    @Override
    public Map<String, Object> getNotificationMessageVelocityContextMap(NotificationEvent notificationEvent)
    {
        BusinessObjectDefinitionDescriptionSuggestionChangeEvent event = (BusinessObjectDefinitionDescriptionSuggestionChangeEvent) notificationEvent;

        // Create a list of users (User IDs) that need to be notified about this event.
        // Initialize the list with the user who created this business object definition description suggestion.
        List<String> notificationList = new ArrayList<>();
        notificationList.add(event.getBusinessObjectDefinitionDescriptionSuggestion().getCreatedByUserId());

        // Add to the notification list all users that have WRITE or WRITE_DESCRIPTIVE_CONTENT permission on the namespace.
        notificationList.addAll(userNamespaceAuthorizationDao.getUserIdsWithWriteOrWriteDescriptiveContentPermissionsByNamespace(event.getNamespaceEntity()));

        // Create JSON and XML escaped copies of the business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestionWithJson =
            escapeJsonBusinessObjectDefinitionDescriptionSuggestion(event.getBusinessObjectDefinitionDescriptionSuggestion());
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestionWithXml =
            escapeXmlBusinessObjectDefinitionDescriptionSuggestion(event.getBusinessObjectDefinitionDescriptionSuggestion());

        // Create JSON and XML escaped notification lists.
        List<String> notificationListWithJson = new ArrayList<>();
        List<String> notificationListWithXml = new ArrayList<>();
        for (String userId : notificationList)
        {
            notificationListWithJson.add(escapeJson(userId));
            notificationListWithXml.add(escapeXml(userId));
        }

        // Create a context map of values that can be used when building the message.
        Map<String, Object> velocityContextMap = new HashMap<>();
        addObjectPropertyToContext(velocityContextMap, "businessObjectDefinitionDescriptionSuggestion",
            event.getBusinessObjectDefinitionDescriptionSuggestion(), businessObjectDefinitionDescriptionSuggestionWithJson,
            businessObjectDefinitionDescriptionSuggestionWithXml);
        addObjectPropertyToContext(velocityContextMap, "businessObjectDefinitionDescriptionSuggestionKey",
            event.getBusinessObjectDefinitionDescriptionSuggestion().getBusinessObjectDefinitionDescriptionSuggestionKey(),
            businessObjectDefinitionDescriptionSuggestionWithJson.getBusinessObjectDefinitionDescriptionSuggestionKey(),
            businessObjectDefinitionDescriptionSuggestionWithXml.getBusinessObjectDefinitionDescriptionSuggestionKey());
        addStringPropertyToContext(velocityContextMap, "lastUpdatedByUserId", event.getLastUpdatedByUserId());
        velocityContextMap.put("lastUpdatedOn", event.getLastUpdatedOn());
        addObjectPropertyToContext(velocityContextMap, "notificationList", notificationList, notificationListWithJson, notificationListWithXml);
        addStringPropertyToContext(velocityContextMap, "namespace", event.getNamespaceEntity().getCode());

        // Return the Velocity context map.
        return velocityContextMap;
    }

    /**
     * Creates a JSON escaped copy of the specified business object definition description suggestion.
     *
     * @param businessObjectDefinitionDescriptionSuggestion the business object definition description suggestion
     *
     * @return the JSON escaped business object definition description suggestion
     */
    private BusinessObjectDefinitionDescriptionSuggestion escapeJsonBusinessObjectDefinitionDescriptionSuggestion(
        final BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion)
    {
        // Build and return a JSON escaped business object definition description suggestion.
        return new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestion.getId(),
            escapeJsonBusinessObjectDefinitionDescriptionSuggestionKey(
                businessObjectDefinitionDescriptionSuggestion.getBusinessObjectDefinitionDescriptionSuggestionKey()),
            escapeJson(businessObjectDefinitionDescriptionSuggestion.getDescriptionSuggestion()),
            escapeJson(businessObjectDefinitionDescriptionSuggestion.getStatus()),
            escapeJson(businessObjectDefinitionDescriptionSuggestion.getCreatedByUserId()), businessObjectDefinitionDescriptionSuggestion.getCreatedOn());
    }

    /**
     * Creates an XML escaped copy of the specified business object definition description suggestion.
     *
     * @param businessObjectDefinitionDescriptionSuggestion the business object definition description suggestion
     *
     * @return the XML escaped business object definition description suggestion
     */
    private BusinessObjectDefinitionDescriptionSuggestion escapeXmlBusinessObjectDefinitionDescriptionSuggestion(
        final BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion)
    {
        // Build and return an XML escaped business object definition description suggestion.
        return new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestion.getId(),
            escapeXmlBusinessObjectDefinitionDescriptionSuggestionKey(
                businessObjectDefinitionDescriptionSuggestion.getBusinessObjectDefinitionDescriptionSuggestionKey()),
            escapeXml(businessObjectDefinitionDescriptionSuggestion.getDescriptionSuggestion()),
            escapeXml(businessObjectDefinitionDescriptionSuggestion.getStatus()), escapeXml(businessObjectDefinitionDescriptionSuggestion.getCreatedByUserId()),
            businessObjectDefinitionDescriptionSuggestion.getCreatedOn());
    }

    /**
     * Creates an XML escaped copy of the specified business object definition description suggestion key.
     *
     * @param businessObjectDefinitionDescriptionSuggestionKey the business object definition description suggestion key
     *
     * @return the XML escaped business object definition description suggestion key
     */
    private BusinessObjectDefinitionDescriptionSuggestionKey escapeXmlBusinessObjectDefinitionDescriptionSuggestionKey(
        final BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey)
    {
        // Build and return an XML escaped business object definition description suggestion key.
        return new BusinessObjectDefinitionDescriptionSuggestionKey(escapeXml(businessObjectDefinitionDescriptionSuggestionKey.getNamespace()),
            escapeXml(businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName()),
            escapeXml(businessObjectDefinitionDescriptionSuggestionKey.getUserId()));
    }

    /**
     * Creates a JSON escaped copy of the specified business object definition description suggestion key.
     *
     * @param businessObjectDefinitionDescriptionSuggestionKey the business object definition description suggestion key
     *
     * @return the JSON escaped business object definition description suggestion key
     */
    private BusinessObjectDefinitionDescriptionSuggestionKey escapeJsonBusinessObjectDefinitionDescriptionSuggestionKey(
        final BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey)
    {
        // Build and return an JSON escaped business object definition description suggestion key.
        return new BusinessObjectDefinitionDescriptionSuggestionKey(escapeJson(businessObjectDefinitionDescriptionSuggestionKey.getNamespace()),
            escapeJson(businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName()),
            escapeJson(businessObjectDefinitionDescriptionSuggestionKey.getUserId()));
    }

    /**
     * The Business Object Definition Description Suggestion change event
     */
    public static class BusinessObjectDefinitionDescriptionSuggestionChangeEvent extends NotificationEvent
    {
        private BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion;

        private String lastUpdatedByUserId;

        private XMLGregorianCalendar lastUpdatedOn;

        NamespaceEntity namespaceEntity;

        public BusinessObjectDefinitionDescriptionSuggestionChangeEvent(
            BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion, String lastUpdatedByUserId,
            XMLGregorianCalendar lastUpdatedOn, NamespaceEntity namespaceEntity)
        {
            setMessageDefinitionKey(ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS);
            setEventName("businessObjectDefinitionDescriptionSuggestionChangeEvent");
            this.businessObjectDefinitionDescriptionSuggestion = businessObjectDefinitionDescriptionSuggestion;
            this.lastUpdatedByUserId = lastUpdatedByUserId;
            this.lastUpdatedOn = lastUpdatedOn;
            this.namespaceEntity = namespaceEntity;
        }

        public BusinessObjectDefinitionDescriptionSuggestion getBusinessObjectDefinitionDescriptionSuggestion()
        {
            return businessObjectDefinitionDescriptionSuggestion;
        }

        public void setBusinessObjectDefinitionDescriptionSuggestion(
            BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion)
        {
            this.businessObjectDefinitionDescriptionSuggestion = businessObjectDefinitionDescriptionSuggestion;
        }

        public String getLastUpdatedByUserId()
        {
            return lastUpdatedByUserId;
        }

        public void setLastUpdatedByUserId(String lastUpdatedByUserId)
        {
            this.lastUpdatedByUserId = lastUpdatedByUserId;
        }

        public XMLGregorianCalendar getLastUpdatedOn()
        {
            return lastUpdatedOn;
        }

        public void setLastUpdatedOn(XMLGregorianCalendar lastUpdatedOn)
        {
            this.lastUpdatedOn = lastUpdatedOn;
        }

        public NamespaceEntity getNamespaceEntity()
        {
            return namespaceEntity;
        }

        public void setNamespaceEntity(NamespaceEntity namespaceEntity)
        {
            this.namespaceEntity = namespaceEntity;
        }
    }
}

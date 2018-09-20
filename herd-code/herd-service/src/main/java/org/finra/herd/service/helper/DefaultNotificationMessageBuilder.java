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

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.UserNamespaceAuthorizationDao;
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.MessageHeaderDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinitions;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.MessageTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

/**
 * Default implementation of the builder for notification messages. Constructs an ESB message based on given data. To use a different implementation overwrite
 * the bean defined in ServiceSpringModuleConfig.sqsMessageBuilder().
 */
@Component
public class DefaultNotificationMessageBuilder implements NotificationMessageBuilder
{
    private static final String WITH_JSON_SNAKE_CASE = "_with_json";

    private static final String WITH_XML_SNAKE_CASE = "_with_xml";

    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionDaoHelper businessObjectDefinitionDescriptionSuggestionDaoHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private ConfigurationDaoHelper configurationDaoHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    private DocumentBuilder documentBuilder;

    @Autowired
    private HerdDaoSecurityHelper herdDaoSecurityHelper;

    @Autowired
    private JavaPropertiesHelper javaPropertiesHelper;

    @Autowired
    private UserNamespaceAuthorizationDao userNamespaceAuthorizationDao;

    @Autowired
    private VelocityHelper velocityHelper;

    private XPath xpath;

    public DefaultNotificationMessageBuilder() throws ParserConfigurationException
    {
        documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        xpath = XPathFactory.newInstance().newXPath();
    }

    @Override
    public List<NotificationMessage> buildBusinessObjectDataStatusChangeMessages(BusinessObjectDataKey businessObjectDataKey,
        String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus)
    {
        // Create a result list.
        List<NotificationMessage> notificationMessages = new ArrayList<>();

        // Get notification message definitions.
        NotificationMessageDefinitions notificationMessageDefinitions = configurationDaoHelper
            .getXmlClobPropertyAndUnmarshallToObject(NotificationMessageDefinitions.class,
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());

        // Continue processing if notification message definitions are configured.
        if (notificationMessageDefinitions != null && CollectionUtils.isNotEmpty(notificationMessageDefinitions.getNotificationMessageDefinitions()))
        {
            // Create a Velocity context map and initialize it with common keys and values.
            Map<String, Object> velocityContextMap = getBaseVelocityContextMap();

            // Add notification message type specific keys and values to the context map.
            velocityContextMap.putAll(
                getBusinessObjectDataStatusChangeMessageVelocityContextMap(businessObjectDataKey, newBusinessObjectDataStatus, oldBusinessObjectDataStatus));

            // Generate notification message for each notification message definition.
            for (NotificationMessageDefinition notificationMessageDefinition : notificationMessageDefinitions.getNotificationMessageDefinitions())
            {
                // Validate the notification message type.
                if (StringUtils.isBlank(notificationMessageDefinition.getMessageType()))
                {
                    throw new IllegalStateException(String.format("Notification message type must be specified. Please update \"%s\" configuration entry.",
                        ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey()));
                }

                // Validate the notification message destination.
                if (StringUtils.isBlank(notificationMessageDefinition.getMessageDestination()))
                {
                    throw new IllegalStateException(String
                        .format("Notification message destination must be specified. Please update \"%s\" configuration entry.",
                            ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey()));
                }

                // Evaluate the template to generate the message text.
                String messageText = evaluateVelocityTemplate(notificationMessageDefinition.getMessageVelocityTemplate(), velocityContextMap,
                    "businessObjectDataStatusChangeEvent");

                // Build a list of optional message headers.
                List<MessageHeader> messageHeaders = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(notificationMessageDefinition.getMessageHeaderDefinitions()))
                {
                    for (MessageHeaderDefinition messageHeaderDefinition : notificationMessageDefinition.getMessageHeaderDefinitions())
                    {
                        messageHeaders.add(new MessageHeader(messageHeaderDefinition.getKey(),
                            evaluateVelocityTemplate(messageHeaderDefinition.getValueVelocityTemplate(), velocityContextMap,
                                String.format("businessObjectDataStatusChangeEvent_messageHeader_%s", messageHeaderDefinition.getKey()))));
                    }
                }

                // Create a notification message and add it to the result list.
                notificationMessages.add(
                    new NotificationMessage(notificationMessageDefinition.getMessageType(), notificationMessageDefinition.getMessageDestination(), messageText,
                        messageHeaders));
            }
        }

        // Return the results.
        return notificationMessages;
    }

    @Override
    public List<NotificationMessage> buildBusinessObjectDefinitionDescriptionSuggestionChangeMessages(
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion, String lastUpdatedByUserId,
        XMLGregorianCalendar lastUpdatedOn, NamespaceEntity namespaceEntity)
    {
        // Create a result list.
        List<NotificationMessage> notificationMessages = new ArrayList<>();

        // Get notification message definitions.
        NotificationMessageDefinitions notificationMessageDefinitions = configurationDaoHelper
            .getXmlClobPropertyAndUnmarshallToObject(NotificationMessageDefinitions.class,
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey());

        // Continue processing if notification message definitions are configured.
        if (notificationMessageDefinitions != null && CollectionUtils.isNotEmpty(notificationMessageDefinitions.getNotificationMessageDefinitions()))
        {
            // Create a Velocity context map and initialize it with common keys and values.
            Map<String, Object> velocityContextMap = getBaseVelocityContextMap();

            // Add notification message type specific keys and values to the context map.
            velocityContextMap.putAll(
                getBusinessObjectDefinitionDescriptionSuggestionChangeMessageVelocityContextMap(businessObjectDefinitionDescriptionSuggestion,
                    lastUpdatedByUserId, lastUpdatedOn, namespaceEntity));

            // Generate notification message for each notification message definition.
            for (NotificationMessageDefinition notificationMessageDefinition : notificationMessageDefinitions.getNotificationMessageDefinitions())
            {
                // Validate the notification message type.
                if (StringUtils.isBlank(notificationMessageDefinition.getMessageType()))
                {
                    throw new IllegalStateException(String.format("Notification message type must be specified. Please update \"%s\" configuration entry.",
                        ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey()));
                }

                // Validate the notification message destination.
                if (StringUtils.isBlank(notificationMessageDefinition.getMessageDestination()))
                {
                    throw new IllegalStateException(String
                        .format("Notification message destination must be specified. Please update \"%s\" configuration entry.",
                            ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS.getKey()));
                }

                // Evaluate the template to generate the message text.
                String messageText = evaluateVelocityTemplate(notificationMessageDefinition.getMessageVelocityTemplate(), velocityContextMap,
                    "businessObjectDefinitionDescriptionSuggestionChangeEvent");

                // Build a list of optional message headers.
                List<MessageHeader> messageHeaders = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(notificationMessageDefinition.getMessageHeaderDefinitions()))
                {
                    for (MessageHeaderDefinition messageHeaderDefinition : notificationMessageDefinition.getMessageHeaderDefinitions())
                    {
                        messageHeaders.add(new MessageHeader(messageHeaderDefinition.getKey(),
                            evaluateVelocityTemplate(messageHeaderDefinition.getValueVelocityTemplate(), velocityContextMap,
                                String.format("businessObjectDefinitionDescriptionSuggestionChangeEvent_messageHeader_%s", messageHeaderDefinition.getKey()))));
                    }
                }

                // Create a notification message and add it to the result list.
                notificationMessages.add(
                    new NotificationMessage(notificationMessageDefinition.getMessageType(), notificationMessageDefinition.getMessageDestination(), messageText,
                        messageHeaders));
            }
        }

        // Return the results.
        return notificationMessages;
    }

    @Override
    public List<NotificationMessage> buildBusinessObjectFormatVersionChangeMessages(BusinessObjectFormatKey businessObjectFormatKey,
        String oldBusinessObjectFormatVersion)
    {
        // Create a result list.
        List<NotificationMessage> notificationMessages = new ArrayList<>();

        // Get notification message definitions.
        NotificationMessageDefinitions notificationMessageDefinitions = configurationDaoHelper
            .getXmlClobPropertyAndUnmarshallToObject(NotificationMessageDefinitions.class,
                ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey());

        // Continue processing if notification message definitions are configured.
        if (notificationMessageDefinitions != null && CollectionUtils.isNotEmpty(notificationMessageDefinitions.getNotificationMessageDefinitions()))
        {
            // Create a Velocity context map and initialize it with common keys and values.
            Map<String, Object> velocityContextMap = getBaseVelocityContextMap();

            // Add notification message type specific keys and values to the context map.
            velocityContextMap.putAll(getBusinessObjectFormatVersionChangeMessageVelocityContextMap(businessObjectFormatKey, oldBusinessObjectFormatVersion));

            // Generate notification message for each notification message definition.
            for (NotificationMessageDefinition notificationMessageDefinition : notificationMessageDefinitions.getNotificationMessageDefinitions())
            {
                // Validate the notification message type.
                if (StringUtils.isBlank(notificationMessageDefinition.getMessageType()))
                {
                    throw new IllegalStateException(String.format("Notification message type must be specified. Please update \"%s\" configuration entry.",
                        ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey()));
                }

                // Validate the notification message destination.
                if (StringUtils.isBlank(notificationMessageDefinition.getMessageDestination()))
                {
                    throw new IllegalStateException(String
                        .format("Notification message destination must be specified. Please update \"%s\" configuration entry.",
                            ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS.getKey()));
                }

                // Evaluate the template to generate the message text.
                String messageText = evaluateVelocityTemplate(notificationMessageDefinition.getMessageVelocityTemplate(), velocityContextMap,
                    "businessObjectFormatVersionChangeEvent");

                // Build a list of optional message headers.
                List<MessageHeader> messageHeaders = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(notificationMessageDefinition.getMessageHeaderDefinitions()))
                {
                    for (MessageHeaderDefinition messageHeaderDefinition : notificationMessageDefinition.getMessageHeaderDefinitions())
                    {
                        messageHeaders.add(new MessageHeader(messageHeaderDefinition.getKey(),
                            evaluateVelocityTemplate(messageHeaderDefinition.getValueVelocityTemplate(), velocityContextMap,
                                String.format("businessObjectFormatVersionChangeEvent_messageHeader_%s", messageHeaderDefinition.getKey()))));
                    }
                }

                // Create a notification message and add it to the result list.
                notificationMessages.add(
                    new NotificationMessage(notificationMessageDefinition.getMessageType(), notificationMessageDefinition.getMessageDestination(), messageText,
                        messageHeaders));
            }
        }

        // Return the results.
        return notificationMessages;
    }

    @Override
    public List<NotificationMessage> buildStorageUnitStatusChangeMessages(BusinessObjectDataKey businessObjectDataKey, String storageName,
        String newStorageUnitStatus, String oldStorageUnitStatus)
    {
        // Create a result list.
        List<NotificationMessage> notificationMessages = new ArrayList<>();

        // Get notification message definitions.
        NotificationMessageDefinitions notificationMessageDefinitions = configurationDaoHelper
            .getXmlClobPropertyAndUnmarshallToObject(NotificationMessageDefinitions.class,
                ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey());

        // Continue processing if notification message definitions are configured.
        if (notificationMessageDefinitions != null && CollectionUtils.isNotEmpty(notificationMessageDefinitions.getNotificationMessageDefinitions()))
        {
            // Create a Velocity context map and initialize it with common keys and values.
            Map<String, Object> velocityContextMap = getBaseVelocityContextMap();

            // Add notification message type specific keys and values to the context map.
            velocityContextMap
                .putAll(getStorageUnitStatusChangeMessageVelocityContextMap(businessObjectDataKey, storageName, newStorageUnitStatus, oldStorageUnitStatus));

            // Generate notification message for each notification message definition.
            for (NotificationMessageDefinition notificationMessageDefinition : notificationMessageDefinitions.getNotificationMessageDefinitions())
            {
                // Validate the notification message type.
                if (StringUtils.isBlank(notificationMessageDefinition.getMessageType()))
                {
                    throw new IllegalStateException(String.format("Notification message type must be specified. Please update \"%s\" configuration entry.",
                        ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey()));
                }

                // Validate the notification message destination.
                if (StringUtils.isBlank(notificationMessageDefinition.getMessageDestination()))
                {
                    throw new IllegalStateException(String
                        .format("Notification message destination must be specified. Please update \"%s\" configuration entry.",
                            ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS.getKey()));
                }

                // Evaluate the template to generate the message text.
                String messageText =
                    evaluateVelocityTemplate(notificationMessageDefinition.getMessageVelocityTemplate(), velocityContextMap, "storageUnitStatusChangeEvent");

                // Build a list of optional message headers.
                List<MessageHeader> messageHeaders = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(notificationMessageDefinition.getMessageHeaderDefinitions()))
                {
                    for (MessageHeaderDefinition messageHeaderDefinition : notificationMessageDefinition.getMessageHeaderDefinitions())
                    {
                        messageHeaders.add(new MessageHeader(messageHeaderDefinition.getKey(),
                            evaluateVelocityTemplate(messageHeaderDefinition.getValueVelocityTemplate(), velocityContextMap,
                                String.format("storageUnitStatusChangeEvent_messageHeader_%s", messageHeaderDefinition.getKey()))));
                    }
                }

                // Create a notification message and add it to the result list.
                notificationMessages.add(
                    new NotificationMessage(notificationMessageDefinition.getMessageType(), notificationMessageDefinition.getMessageDestination(), messageText,
                        messageHeaders));
            }
        }

        // Return the results.
        return notificationMessages;
    }

    @Override
    public NotificationMessage buildSystemMonitorResponse(String systemMonitorRequestPayload)
    {
        // Get velocity template.
        String velocityTemplate = configurationHelper.getProperty(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE);

        // Continue processing if velocity template is configured.
        if (StringUtils.isNotBlank(velocityTemplate))
        {
            // Create a Velocity context map and initialize it with common keys and values.
            Map<String, Object> velocityContextMap = getBaseVelocityContextMap();

            // Add notification message type specific keys and values to the context map.
            velocityContextMap
                .putAll(getIncomingMessageValueMap(systemMonitorRequestPayload, ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES));

            // Evaluate the template to generate the message text.
            String messageText = evaluateVelocityTemplate(velocityTemplate, velocityContextMap, "systemMonitorResponse");

            // Create a new notification message and return it.
            return new NotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), getSqsQueueName(), messageText, null);
        }
        else
        {
            return null;
        }
    }

    /**
     * Adds string property to the specified context along with the relative JSON and XML escaped copies of the property.
     *
     * @param context the context map
     * @param propertyName the name of the property
     * @param propertyValue the value of the property, maybe null
     * @param jsonEscapedPropertyValue the JSON escaped value of the property, maybe null
     * @param xmlEscapedPropertyValue the XML escaped value of the property, maybe null
     */
    void addObjectPropertyToContext(final Map<String, Object> context, final String propertyName, final Object propertyValue,
        final Object jsonEscapedPropertyValue, final Object xmlEscapedPropertyValue)
    {
        context.put(propertyName, propertyValue);
        context.put(propertyName + "WithJson", jsonEscapedPropertyValue);
        context.put(propertyName + "WithXml", xmlEscapedPropertyValue);
    }

    /**
     * Adds string property to the specified context along with the relative JSON and XML escaped copies of the property.
     *
     * @param context the context map
     * @param propertyName the name of the property
     * @param propertyValue the value of the property, maybe null
     */
    void addStringPropertyToContext(final Map<String, Object> context, final String propertyName, final String propertyValue)
    {
        addObjectPropertyToContext(context, propertyName, propertyValue, escapeJson(propertyValue), escapeXml(propertyValue));
    }

    /**
     * Returns Velocity context map of the keys and values common across all notification message types.
     *
     * @return the Velocity context map
     */
    Map<String, Object> getBaseVelocityContextMap()
    {
        // Create and populate the velocity context with dynamic values. Note that we can't use periods within the context keys since they can't
        // be referenced in the velocity template (i.e. they're used to separate fields with the context object being referenced).
        return getBaseVelocityContextMapHelper(herdDaoSecurityHelper.getCurrentUsername(), ConfigurationValue.HERD_ENVIRONMENT.getKey().replace('.', '_'),
            configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT),
            ConfigurationValue.HERD_NOTIFICATION_SQS_ENVIRONMENT.getKey().replace('.', '_'),
            configurationHelper.getProperty(ConfigurationValue.HERD_NOTIFICATION_SQS_ENVIRONMENT));
    }

    /**
     * Returns Velocity context map of the keys and values common across all notification message types.
     *
     * @param username the username or user id of the logged in user that caused this message to be generated
     * @param herdEnvironmentKey the name of the herd environment property
     * @param herdEnvironmentValue the value of the herd environment property
     * @param herdNotificationSqsEnvironmentKey the name of the herd notification sqs environment property
     * @param herdNotificationSqsEnvironmentValue the value of the herd notification sqs environment property
     *
     * @return the Velocity context map
     */
    Map<String, Object> getBaseVelocityContextMapHelper(String username, String herdEnvironmentKey, String herdEnvironmentValue,
        String herdNotificationSqsEnvironmentKey, String herdNotificationSqsEnvironmentValue)
    {
        Map<String, Object> context = new HashMap<>();
        context.put(herdEnvironmentKey, herdEnvironmentValue);
        context.put(escapeJson(herdEnvironmentKey) + WITH_JSON_SNAKE_CASE, escapeJson(herdEnvironmentValue));
        context.put(escapeXml(herdEnvironmentKey) + WITH_XML_SNAKE_CASE, escapeXml(herdEnvironmentValue));
        context.put(herdNotificationSqsEnvironmentKey, herdNotificationSqsEnvironmentValue);
        context.put(escapeJson(herdNotificationSqsEnvironmentKey) + WITH_JSON_SNAKE_CASE, escapeJson(herdNotificationSqsEnvironmentValue));
        context.put(escapeXml(herdNotificationSqsEnvironmentKey) + WITH_XML_SNAKE_CASE, escapeXml(herdNotificationSqsEnvironmentValue));
        context.put("current_time", HerdDateUtils.now().toString());
        context.put("uuid", UUID.randomUUID().toString());
        addStringPropertyToContext(context, "username", username);
        context.put("StringUtils", StringUtils.class);
        context.put("CollectionUtils", CollectionUtils.class);
        context.put("Collections", Collections.class);

        // Return the message text.
        return context;
    }

    /**
     * Returns Velocity context map of additional keys and values to place in the velocity context.
     *
     * @param businessObjectDataKey the business object data key
     * @param newBusinessObjectDataStatus the new business object data status
     * @param oldBusinessObjectDataStatus the old business object data status
     *
     * @return the Velocity context map
     */
    Map<String, Object> getBusinessObjectDataStatusChangeMessageVelocityContextMap(BusinessObjectDataKey businessObjectDataKey,
        String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus)
    {
        // Create a context map of values that can be used when building the message.
        Map<String, Object> velocityContextMap = new HashMap<>();
        addObjectPropertyToContext(velocityContextMap, "businessObjectDataKey", businessObjectDataKey, escapeJsonBusinessObjectDataKey(businessObjectDataKey),
            escapeXmlBusinessObjectDataKey(businessObjectDataKey));
        addStringPropertyToContext(velocityContextMap, "newBusinessObjectDataStatus", newBusinessObjectDataStatus);
        addStringPropertyToContext(velocityContextMap, "oldBusinessObjectDataStatus", oldBusinessObjectDataStatus);

        // Retrieve business object data entity and business object data id to the context.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);
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
        addStringPropertyToContext(velocityContextMap, "namespace", businessObjectDataKey.getNamespace());

        return velocityContextMap;
    }

    /**
     * Returns Velocity context map of additional keys and values to place in the velocity context.
     *
     * @param businessObjectDefinitionDescriptionSuggestion the business object definition description suggestion
     * @param lastUpdatedByUserId the User ID of the user who last updated this business object definition description suggestion
     * @param lastUpdatedOn the timestamp when this business object definition description suggestion was last updated on
     * @param namespaceEntity the namespace entity
     *
     * @return the Velocity context map
     */
    Map<String, Object> getBusinessObjectDefinitionDescriptionSuggestionChangeMessageVelocityContextMap(
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion, String lastUpdatedByUserId,
        XMLGregorianCalendar lastUpdatedOn, NamespaceEntity namespaceEntity)
    {
        // Create a list of users (User IDs) that need to be notified about this event.
        // Initialize the list with the user who created this business object definition description suggestion.
        List<String> notificationList = new ArrayList<>();
        notificationList.add(businessObjectDefinitionDescriptionSuggestion.getCreatedByUserId());

        // Add to the notification list all users that have WRITE or WRITE_DESCRIPTIVE_CONTENT permission on the namespace.
        notificationList.addAll(userNamespaceAuthorizationDao.getUserIdsWithWriteOrWriteDescriptiveContentPermissionsByNamespace(namespaceEntity));

        // Create JSON and XML escaped copies of the business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestionWithJson =
            escapeJsonBusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestion);
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestionWithXml =
            escapeXmlBusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestion);

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
        addObjectPropertyToContext(velocityContextMap, "businessObjectDefinitionDescriptionSuggestion", businessObjectDefinitionDescriptionSuggestion,
            businessObjectDefinitionDescriptionSuggestionWithJson, businessObjectDefinitionDescriptionSuggestionWithXml);
        addObjectPropertyToContext(velocityContextMap, "businessObjectDefinitionDescriptionSuggestionKey",
            businessObjectDefinitionDescriptionSuggestion.getBusinessObjectDefinitionDescriptionSuggestionKey(),
            businessObjectDefinitionDescriptionSuggestionWithJson.getBusinessObjectDefinitionDescriptionSuggestionKey(),
            businessObjectDefinitionDescriptionSuggestionWithXml.getBusinessObjectDefinitionDescriptionSuggestionKey());
        addStringPropertyToContext(velocityContextMap, "lastUpdatedByUserId", lastUpdatedByUserId);
        velocityContextMap.put("lastUpdatedOn", lastUpdatedOn);
        addObjectPropertyToContext(velocityContextMap, "notificationList", notificationList, notificationListWithJson, notificationListWithXml);
        addStringPropertyToContext(velocityContextMap, "namespace", namespaceEntity.getCode());

        // Return the Velocity context map.
        return velocityContextMap;
    }

    /**
     * Returns Velocity context map of additional keys and values to place in the velocity context.
     *
     * @param businessObjectFormatKey the business object format key
     * @param oldBusinessObjectFormatVersion the old business object format version
     *
     * @return the Velocity context map
     */
    Map<String, Object> getBusinessObjectFormatVersionChangeMessageVelocityContextMap(BusinessObjectFormatKey businessObjectFormatKey,
        String oldBusinessObjectFormatVersion)
    {
        Map<String, Object> velocityContextMap = new HashMap<>();

        addObjectPropertyToContext(velocityContextMap, "businessObjectFormatKey", businessObjectFormatKey,
            escapeJsonBusinessObjectFormatKey(businessObjectFormatKey), escapeXmlBusinessObjectFormatKey(businessObjectFormatKey));
        velocityContextMap.put("newBusinessObjectFormatVersion", businessObjectFormatKey.getBusinessObjectFormatVersion());
        velocityContextMap.put("oldBusinessObjectFormatVersion", oldBusinessObjectFormatVersion);
        addStringPropertyToContext(velocityContextMap, "namespace", businessObjectFormatKey.getNamespace());

        return velocityContextMap;
    }

    /**
     * Returns Velocity context map of additional keys and values to place in the velocity context.
     *
     * @param businessObjectDataKey the business object data key
     * @param storageName the storage name
     * @param newStorageUnitStatus the new storage unit status
     * @param oldStorageUnitStatus the old storage unit status, may be null
     *
     * @return the Velocity context map
     */
    Map<String, Object> getStorageUnitStatusChangeMessageVelocityContextMap(BusinessObjectDataKey businessObjectDataKey, String storageName,
        String newStorageUnitStatus, String oldStorageUnitStatus)
    {
        Map<String, Object> velocityContextMap = new HashMap<>();

        addObjectPropertyToContext(velocityContextMap, "businessObjectDataKey", businessObjectDataKey, escapeJsonBusinessObjectDataKey(businessObjectDataKey),
            escapeXmlBusinessObjectDataKey(businessObjectDataKey));
        addStringPropertyToContext(velocityContextMap, "storageName", storageName);
        addStringPropertyToContext(velocityContextMap, "newStorageUnitStatus", newStorageUnitStatus);
        addStringPropertyToContext(velocityContextMap, "oldStorageUnitStatus", oldStorageUnitStatus);
        addStringPropertyToContext(velocityContextMap, "namespace", businessObjectDataKey.getNamespace());

        return velocityContextMap;
    }

    /**
     * JSON escapes a specified string. This method is null-safe.
     *
     * @param input the input string
     *
     * @return the XML escaped string
     */
    private String escapeJson(final String input)
    {
        if (input == null)
        {
            return null;
        }
        else
        {
            return String.valueOf(JsonStringEncoder.getInstance().quoteAsString(input));
        }
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
     * XML escapes a specified string. This method is null-safe.
     *
     * @param input the input string
     *
     * @return the XML escaped string
     */
    private String escapeXml(final String input)
    {
        return StringEscapeUtils.escapeXml(input);
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
     * Evaluates a velocity template if one is defined for the specified configuration value. If velocity template is null, null will be returned.
     *
     * @param velocityTemplate the optional velocity template, may be null
     * @param contextMap the optional context map of additional keys and values to place in the velocity context. This can be useful if you have values from an
     * incoming request message you want to make available to velocity to use in the building of the outgoing response message.
     * @param velocityTemplateName the velocity template name used when Velocity logs error messages.
     *
     * @return the evaluated velocity template
     */
    private String evaluateVelocityTemplate(String velocityTemplate, Map<String, Object> contextMap, String velocityTemplateName)
    {
        // Initialize the message text to null which will cause a message to not be sent.
        String messageText = null;

        // Process velocity template if it configured.
        if (StringUtils.isNotBlank(velocityTemplate))
        {
            messageText = velocityHelper.evaluate(velocityTemplate, contextMap, velocityTemplateName);
        }

        // Return the message text.
        return messageText;
    }

    /**
     * Gets an incoming message value map from a message payload.
     *
     * @param payload the incoming message payload.
     * @param configurationValue the configuration value for the XPath expression properties.
     *
     * @return the incoming message value map.
     */
    private Map<String, Object> getIncomingMessageValueMap(String payload, ConfigurationValue configurationValue)
    {
        // This method is generic and could be placed in a generic helper, but since it's use is limited to only getting values from an incoming message
        // to produce an outgoing message, it is fine in this class.

        Properties xpathProperties;
        try
        {
            String xpathPropertiesString = configurationHelper.getProperty(configurationValue);
            xpathProperties = javaPropertiesHelper.getProperties(xpathPropertiesString);
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Unable to load XPath properties from configuration with key '" + configurationValue.getKey() + "'", e);
        }

        // Create a map that will house keys that map to values retrieved from the incoming message via the XPath expressions.
        Map<String, Object> incomingMessageValuesMap = new HashMap<>();

        // Evaluate all the XPath expressions on the incoming message and store the results in the map.
        // If validation is desired, an XPath expression can be used to verify that the incoming message contains a valid path in the payload.
        // If no XPath expressions are used, then it is assumed that the message is valid and the message will be processed
        // with no incoming values.
        Document document;
        try
        {
            document = documentBuilder.parse(new InputSource(new StringReader(payload)));
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Payload is not valid XML:\n" + payload, e);
        }

        for (String key : xpathProperties.stringPropertyNames())
        {
            // Get the XPath expression.
            String xpathExpression = xpathProperties.getProperty(key);
            try
            {
                // Evaluate the expression and store the result in the map keyed by the XPath expression key.
                // A document is required here as opposed to an input source since an input source yields a bug when the input XML contains a namespace.
                incomingMessageValuesMap.put(key, xpath.evaluate(xpathExpression, document));
            }
            catch (Exception ex)
            {
                // If any XPath expressions couldn't be evaluated against the incoming payload, throw an exception.
                // If the caller is the incoming JMS processing logic, it will log a debug message because it doesn't know which message it is
                // processing. If this exception is thrown, it assumes the incoming message isn't the one it is processing and moves on to the next message
                // processing routine for a different incoming message.
                // If the XPath expression configured is incorrect (i.e. an internal server error), then the incoming JMS processing logic will eventually
                // find no successful handler and will log an error which can be looked into further. That debug message would then be useful.
                throw new IllegalStateException("XPath expression \"" + xpathExpression + "\" could not be evaluated against payload \"" + payload + "\".", ex);
            }
        }

        return incomingMessageValuesMap;
    }

    /**
     * Returns the SQS queue name. Throws {@link IllegalStateException} if SQS queue name is undefined.
     *
     * @return the sqs queue name
     */
    private String getSqsQueueName()
    {
        String sqsQueueName = configurationHelper.getProperty(ConfigurationValue.HERD_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME);

        if (StringUtils.isBlank(sqsQueueName))
        {
            throw new IllegalStateException(String.format("SQS queue name not found. Ensure the \"%s\" configuration entry is configured.",
                ConfigurationValue.HERD_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME.getKey()));
        }

        return sqsQueueName;
    }
}

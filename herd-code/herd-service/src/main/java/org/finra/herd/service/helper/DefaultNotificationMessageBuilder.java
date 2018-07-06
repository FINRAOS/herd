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

import org.apache.commons.collections4.CollectionUtils;
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
            // Create a context map of values that can be used when building the message.
            Map<String, Object> velocityContextMap =
                getBusinessObjectDataStatusChangeMessageVelocityContextMap(businessObjectDataKey, newBusinessObjectDataStatus, oldBusinessObjectDataStatus);

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
            // Create a context map of values that can be used when building the message.
            Map<String, Object> velocityContextMap =
                getBusinessObjectDefinitionDescriptionSuggestionChangeMessageVelocityContextMap(businessObjectDefinitionDescriptionSuggestion,
                    lastUpdatedByUserId, lastUpdatedOn, namespaceEntity);

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
            // Create a context map of values that can be used when building the message.
            Map<String, Object> velocityContextMap =
                getBusinessObjectFormatVersionChangeMessageVelocityContextMap(businessObjectFormatKey, oldBusinessObjectFormatVersion);

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
            // Create a context map of values that can be used when building the message.
            Map<String, Object> velocityContextMap =
                getStorageUnitStatusChangeMessageVelocityContextMap(businessObjectDataKey, storageName, newStorageUnitStatus, oldStorageUnitStatus);

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
            // Evaluate the template to generate the message text.
            String messageText = evaluateVelocityTemplate(velocityTemplate,
                getIncomingMessageValueMap(systemMonitorRequestPayload, ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES),
                "systemMonitorResponse");

            // Create a new notification message and return it.
            return new NotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), getSqsQueueName(), messageText, null);
        }
        else
        {
            return null;
        }
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
            // Create and populate the velocity context with dynamic values. Note that we can't use periods within the context keys since they can't
            // be referenced in the velocity template (i.e. they're used to separate fields with the context object being referenced).
            Map<String, Object> context = new HashMap<>();
            context.put(ConfigurationValue.HERD_ENVIRONMENT.getKey().replace('.', '_'), configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT));
            context.put(ConfigurationValue.HERD_NOTIFICATION_SQS_ENVIRONMENT.getKey().replace('.', '_'),
                configurationHelper.getProperty(ConfigurationValue.HERD_NOTIFICATION_SQS_ENVIRONMENT));
            context.put("current_time", HerdDateUtils.now().toString());
            context.put("uuid", UUID.randomUUID().toString());
            context.put("username", herdDaoSecurityHelper.getCurrentUsername());
            context.put("StringUtils", StringUtils.class);
            context.put("CollectionUtils", CollectionUtils.class);
            context.put("Collections", Collections.class);

            // Populate the context map entries into the velocity context.
            for (Map.Entry<String, Object> mapEntry : contextMap.entrySet())
            {
                context.put(mapEntry.getKey(), mapEntry.getValue());
            }

            messageText = velocityHelper.evaluate(velocityTemplate, context, velocityTemplateName);
        }

        // Return the message text.
        return messageText;
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
    private Map<String, Object> getBusinessObjectDataStatusChangeMessageVelocityContextMap(BusinessObjectDataKey businessObjectDataKey,
        String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus)
    {
        // Create a context map of values that can be used when building the message.
        Map<String, Object> velocityContextMap = new HashMap<>();
        velocityContextMap.put("businessObjectDataKey", businessObjectDataKey);
        velocityContextMap.put("newBusinessObjectDataStatus", newBusinessObjectDataStatus);
        velocityContextMap.put("oldBusinessObjectDataStatus", oldBusinessObjectDataStatus);

        // Retrieve business object data entity and business object data id to the context.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);
        velocityContextMap.put("businessObjectDataId", businessObjectDataEntity.getId());

        // Load all attribute definitions for this business object data in a map for easy access.
        Map<String, BusinessObjectDataAttributeDefinitionEntity> attributeDefinitionEntityMap =
            businessObjectFormatHelper.getAttributeDefinitionEntities(businessObjectDataEntity.getBusinessObjectFormat());

        // Build an ordered map of business object data attributes that are flagged to be published in notification messages.
        Map<String, String> businessObjectDataAttributes = new LinkedHashMap<>();
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
                    }
                }
            }
        }

        // Add the map of business object data attributes to the context.
        velocityContextMap.put("businessObjectDataAttributes", businessObjectDataAttributes);

        // Add the namespace to the header.
        velocityContextMap.put("namespace", businessObjectDataKey.getNamespace());

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
    private Map<String, Object> getBusinessObjectDefinitionDescriptionSuggestionChangeMessageVelocityContextMap(
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion, String lastUpdatedByUserId,
        XMLGregorianCalendar lastUpdatedOn, NamespaceEntity namespaceEntity)
    {
        // Create a list of users (User IDs) that need to be notified about this event.
        // Initialize the list with the user who created this business object definition description suggestion.
        List<String> notificationList = new ArrayList<>();
        notificationList.add(businessObjectDefinitionDescriptionSuggestion.getCreatedByUserId());

        // Add to the notification list all users that have WRITE or WRITE_DESCRIPTIVE_CONTENT permission on the namespace.
        notificationList.addAll(userNamespaceAuthorizationDao.getUserIdsWithWriteOrWriteDescriptiveContentPermissionsByNamespace(namespaceEntity));

        // Create a context map of values that can be used when building the message.
        Map<String, Object> velocityContextMap = new HashMap<>();
        velocityContextMap.put("businessObjectDefinitionDescriptionSuggestion", businessObjectDefinitionDescriptionSuggestion);
        velocityContextMap.put("businessObjectDefinitionDescriptionSuggestionKey",
            businessObjectDefinitionDescriptionSuggestion.getBusinessObjectDefinitionDescriptionSuggestionKey());
        velocityContextMap.put("lastUpdatedByUserId", lastUpdatedByUserId);
        velocityContextMap.put("lastUpdatedOn", lastUpdatedOn);
        velocityContextMap.put("notificationList", notificationList);
        velocityContextMap.put("namespace", namespaceEntity.getCode());

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
    private Map<String, Object> getBusinessObjectFormatVersionChangeMessageVelocityContextMap(BusinessObjectFormatKey businessObjectFormatKey,
        String oldBusinessObjectFormatVersion)
    {
        Map<String, Object> velocityContextMap = new HashMap<>();

        velocityContextMap.put("businessObjectFormatKey", businessObjectFormatKey);
        velocityContextMap.put("newBusinessObjectFormatVersion", businessObjectFormatKey.getBusinessObjectFormatVersion());
        velocityContextMap.put("oldBusinessObjectFormatVersion", oldBusinessObjectFormatVersion);
        velocityContextMap.put("namespace", businessObjectFormatKey.getNamespace());

        return velocityContextMap;
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
    private Map<String, Object> getStorageUnitStatusChangeMessageVelocityContextMap(BusinessObjectDataKey businessObjectDataKey, String storageName,
        String newStorageUnitStatus, String oldStorageUnitStatus)
    {
        Map<String, Object> velocityContextMap = new HashMap<>();

        velocityContextMap.put("businessObjectDataKey", businessObjectDataKey);
        velocityContextMap.put("storageName", storageName);
        velocityContextMap.put("newStorageUnitStatus", newStorageUnitStatus);
        velocityContextMap.put("oldStorageUnitStatus", oldStorageUnitStatus);
        velocityContextMap.put("namespace", businessObjectDataKey.getNamespace());

        return velocityContextMap;
    }
}

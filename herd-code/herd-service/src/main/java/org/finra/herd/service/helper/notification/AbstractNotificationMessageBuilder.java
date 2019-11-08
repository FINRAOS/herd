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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.MessageHeaderDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinition;
import org.finra.herd.model.api.xml.NotificationMessageDefinitions;
import org.finra.herd.model.dto.BusinessObjectDataStatusChangeNotificationEvent;
import org.finra.herd.model.dto.BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent;
import org.finra.herd.model.dto.BusinessObjectFormatVersionChangeNotificationEvent;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationEvent;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.dto.StorageUnitStatusChangeNotificationEvent;
import org.finra.herd.model.dto.UserNamespaceAuthorizationChangeNotificationEvent;
import org.finra.herd.model.jpa.MessageTypeEntity;
import org.finra.herd.service.helper.ConfigurationDaoHelper;
import org.finra.herd.service.helper.VelocityHelper;

/**
 * This is an abstract class that helps build a notification message.
 */
public abstract class AbstractNotificationMessageBuilder
{
    private static final String WITH_JSON_SNAKE_CASE = "_with_json";

    private static final String WITH_XML_SNAKE_CASE = "_with_xml";

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private ConfigurationDaoHelper configurationDaoHelper;

    @Autowired
    private HerdDaoSecurityHelper herdDaoSecurityHelper;

    @Autowired
    private VelocityHelper velocityHelper;

    private static final Map<Class<?>, ConfigurationValue> eventTypeMessageDefinitionKeyMap = new HashMap<>();

    static
    {
        // Build the event type to message definition key map
        eventTypeMessageDefinitionKeyMap.put(BusinessObjectDataStatusChangeNotificationEvent.class,
            ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS);
        eventTypeMessageDefinitionKeyMap.put(BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent.class,
            ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS);
        eventTypeMessageDefinitionKeyMap.put(BusinessObjectFormatVersionChangeNotificationEvent.class,
            ConfigurationValue.HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS);
        eventTypeMessageDefinitionKeyMap
            .put(StorageUnitStatusChangeNotificationEvent.class, ConfigurationValue.HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS);
        eventTypeMessageDefinitionKeyMap.put(UserNamespaceAuthorizationChangeNotificationEvent.class,
            ConfigurationValue.HERD_NOTIFICATION_USER_NAMESPACE_AUTHORIZATION_CHANGE_MESSAGE_DEFINITIONS);
    }

    /**
     * Get the event type to Message Definition Key map
     *
     * @return the event type to Message Definition Key map
     */
    public Map<Class<?>, ConfigurationValue> getEventTypeMessageDefinitionKeyMap()
    {
        return eventTypeMessageDefinitionKeyMap;
    }

    /**
     * Returns Velocity context map of additional keys and values relating to the notification message body to place in the velocity context.
     *
     * @param notificationEvent the notification event
     *
     * @return the Velocity context map
     */
    public abstract Map<String, Object> getNotificationMessageVelocityContextMap(NotificationEvent notificationEvent);

    /**
     * Builds a list of notification messages for the change event. The result list might be empty if if no messages should be sent.
     *
     * @param notificationEvent the notification event
     *
     * @return the list of notification messages
     */
    public List<NotificationMessage> buildNotificationMessages(NotificationEvent notificationEvent)
    {
        // Create a result list.
        List<NotificationMessage> notificationMessages = new ArrayList<>();

        // Get notification message definitions.
        NotificationMessageDefinitions notificationMessageDefinitions =
            configurationDaoHelper.getXmlClobPropertyAndUnmarshallToObject(NotificationMessageDefinitions.class, getMessageDefinitionKey(notificationEvent));

        // Get notification header key for filter attribute value
        String filterAttributeKey = configurationHelper.getRequiredProperty(ConfigurationValue.MESSAGE_HEADER_KEY_FILTER_ATTRIBUTE_VALUE);

        // Continue processing if notification message definitions are configured.
        if (notificationMessageDefinitions != null && CollectionUtils.isNotEmpty(notificationMessageDefinitions.getNotificationMessageDefinitions()))
        {
            // Create a Velocity context map and initialize it with common keys and values.
            Map<String, Object> velocityContextMap = getBaseVelocityContextMap();

            // Add notification message type specific keys and values to the context map.
            velocityContextMap.putAll(getNotificationMessageVelocityContextMap(notificationEvent));

            // Generate notification message for each notification message definition.
            for (NotificationMessageDefinition notificationMessageDefinition : notificationMessageDefinitions.getNotificationMessageDefinitions())
            {
                // Validate the notification message type.
                if (StringUtils.isBlank(notificationMessageDefinition.getMessageType()))
                {
                    throw new IllegalStateException(String.format("Notification message type must be specified. Please update \"%s\" configuration entry.",
                        getMessageDefinitionKey(notificationEvent)));
                }
                else if (!notificationMessageDefinition.getMessageType().toUpperCase().equals(MessageTypeEntity.MessageEventTypes.SNS.toString()))
                {
                    throw new IllegalStateException(String
                        .format("Only \"%s\" notification message type is supported. Please update \"%s\" configuration entry.",
                            MessageTypeEntity.MessageEventTypes.SNS.toString(), getMessageDefinitionKey(notificationEvent)));
                }

                // Validate the notification message destination.
                if (StringUtils.isBlank(notificationMessageDefinition.getMessageDestination()))
                {
                    throw new IllegalStateException(String
                        .format("Notification message destination must be specified. Please update \"%s\" configuration entry.",
                            getMessageDefinitionKey(notificationEvent)));
                }

                // Evaluate the template to generate the message text.
                String messageText = evaluateVelocityTemplate(notificationMessageDefinition.getMessageVelocityTemplate(), velocityContextMap,
                    notificationEvent.getClass().getCanonicalName());

                // Build a list of optional message headers.
                List<MessageHeader> messageHeaders = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(notificationMessageDefinition.getMessageHeaderDefinitions()))
                {
                    for (MessageHeaderDefinition messageHeaderDefinition : notificationMessageDefinition.getMessageHeaderDefinitions())
                    {
                        messageHeaders.add(new MessageHeader(messageHeaderDefinition.getKey(),
                            evaluateVelocityTemplate(messageHeaderDefinition.getValueVelocityTemplate(), velocityContextMap,
                                String.format("%s_messageHeader_%s", notificationEvent.getClass().getCanonicalName(), messageHeaderDefinition.getKey()))));
                    }
                }

                // If filterAttribute added into context map - add it into notification headers
                if (velocityContextMap.containsKey(filterAttributeKey))
                {
                    messageHeaders.add(new MessageHeader(filterAttributeKey, velocityContextMap.get(filterAttributeKey).toString()));
                }

                // Create a notification message and add it to the result list.
                notificationMessages.add(
                    new NotificationMessage(notificationMessageDefinition.getMessageType(), notificationMessageDefinition.getMessageDestination(), messageText,
                        messageHeaders));
            }
        }
        return notificationMessages;
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
            configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT));
    }

    /**
     * Returns Velocity context map of the keys and values common across all notification message types.
     *
     * @param username the username or user id of the logged in user that caused this message to be generated
     * @param herdEnvironmentKey the name of the herd environment property
     * @param herdEnvironmentValue the value of the herd environment property
     *
     * @return the Velocity context map
     */
    Map<String, Object> getBaseVelocityContextMapHelper(String username, String herdEnvironmentKey, String herdEnvironmentValue)
    {
        Map<String, Object> context = new HashMap<>();
        context.put(herdEnvironmentKey, herdEnvironmentValue);
        context.put(escapeJson(herdEnvironmentKey) + WITH_JSON_SNAKE_CASE, escapeJson(herdEnvironmentValue));
        context.put(escapeXml(herdEnvironmentKey) + WITH_XML_SNAKE_CASE, escapeXml(herdEnvironmentValue));
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
     * JSON escapes a specified string. This method is null-safe.
     *
     * @param input the input string
     *
     * @return the XML escaped string
     */
    protected String escapeJson(final String input)
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
     * XML escapes a specified string. This method is null-safe.
     *
     * @param input the input string
     *
     * @return the XML escaped string
     */
    protected String escapeXml(final String input)
    {
        return StringEscapeUtils.escapeXml(input);
    }

    /**
     * Adds string property to the specified context along with the relative JSON and XML escaped copies of the property.
     *
     * @param context the context map
     * @param propertyName the name of the property
     * @param propertyValue the value of the property, maybe null
     */
    protected void addStringPropertyToContext(final Map<String, Object> context, final String propertyName, final String propertyValue)
    {
        addObjectPropertyToContext(context, propertyName, propertyValue, escapeJson(propertyValue), escapeXml(propertyValue));
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
    protected void addObjectPropertyToContext(final Map<String, Object> context, final String propertyName, final Object propertyValue,
        final Object jsonEscapedPropertyValue, final Object xmlEscapedPropertyValue)
    {
        context.put(propertyName, propertyValue);
        context.put(propertyName + "WithJson", jsonEscapedPropertyValue);
        context.put(propertyName + "WithXml", xmlEscapedPropertyValue);
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
    protected String evaluateVelocityTemplate(String velocityTemplate, Map<String, Object> contextMap, String velocityTemplateName)
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
     * Get the message definition key based on the event type
     *
     * @param notificationEvent the notification event
     *
     * @return the message definition key
     */
    private String getMessageDefinitionKey(NotificationEvent notificationEvent)
    {
        return eventTypeMessageDefinitionKeyMap.get(notificationEvent.getClass()).getKey();
    }

}

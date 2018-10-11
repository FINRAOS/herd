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

import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.NotificationEvent;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.dto.SystemMonitorResponseNotificationEvent;
import org.finra.herd.model.jpa.MessageTypeEntity;
import org.finra.herd.service.helper.notification.AbstractNotificationMessageBuilder;
import org.finra.herd.service.helper.notification.NotificationMessageBuilder;

/**
 * The builder that knows how to build a System Monitor Response message
 */
@Component
public class SystemMonitorResponseMessageBuilder extends AbstractNotificationMessageBuilder implements NotificationMessageBuilder
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private JavaPropertiesHelper javaPropertiesHelper;

    /**
     * Builds the messages for ESB system monitor response. It overrides the method defined in the class {@link AbstractNotificationMessageBuilder}
     *
     * @param notificationEvent the notification event
     *
     * @return the outgoing system monitor notification messages or empty list if no message should be sent
     */
    @Override
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification =
        "The NotificationEvent is cast to a SystemMonitorResponseNotificationEvent which is always the case since" +
            " we manage the event type to a builder in a map defined in NotificationMessageManager")
    public List<NotificationMessage> buildNotificationMessages(NotificationEvent notificationEvent)
    {
        SystemMonitorResponseNotificationEvent event = (SystemMonitorResponseNotificationEvent) notificationEvent;
        NotificationMessage notificationMessage = buildMessage(event.getSystemMonitorRequestPayload());

        // If message is null, send an empty list of notification messages to be processed.
        return notificationMessage == null ? Collections.emptyList() : Collections.singletonList(notificationMessage);
    }

    /**
     * Builds the message for the ESB system monitor response.
     *
     * @param systemMonitorRequestPayload the system monitor request payload
     *
     * @return the outgoing system monitor notification message or null if no message should be sent
     */
    public NotificationMessage buildMessage(String systemMonitorRequestPayload)
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

    @Override
    public Map<String, Object> getNotificationMessageVelocityContextMap(NotificationEvent notificationEvent)
    {
        return null;
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
        DocumentBuilder documentBuilder = null;
        XPath xpath = null;
        try
        {
            documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            xpath = XPathFactory.newInstance().newXPath();
        }
        catch (ParserConfigurationException e)
        {
            throw new RuntimeException(e);
        }

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

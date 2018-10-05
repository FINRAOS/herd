package org.finra.herd.service.helper;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.MessageTypeEntity;

/**
 * The builder that knows how to build a System Monitor Response message
 */
@Component
public class SystemMonitorResponseMessageBuilder extends AbstractNotificationMessageBuilder
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private JavaPropertiesHelper javaPropertiesHelper;

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
            //TODO throw runtime exception
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

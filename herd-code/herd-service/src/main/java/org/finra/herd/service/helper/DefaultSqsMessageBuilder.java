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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

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
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;

/**
 * Default implementation of the builder for SQS messages. Constructs an ESB message based on given data. To use a different implementation overwrite the bean
 * defined in ServiceSpringModuleConfig.sqsMessageBuilder().
 */
@Component
public class DefaultSqsMessageBuilder implements SqsMessageBuilder
{
    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    private DocumentBuilder documentBuilder;

    @Autowired
    private HerdDaoSecurityHelper herdDaoSecurityHelper;

    @Autowired
    private JavaPropertiesHelper javaPropertiesHelper;

    @Autowired
    private VelocityHelper velocityHelper;

    private XPath xpath;

    public DefaultSqsMessageBuilder() throws ParserConfigurationException
    {
        documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        xpath = XPathFactory.newInstance().newXPath();
    }

    @Override
    public String buildBusinessObjectDataStatusChangeMessage(BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus,
        String oldBusinessObjectDataStatus)
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

        // Evaluate the template and return the value.
        return evaluateVelocityTemplate(ConfigurationValue.HERD_NOTIFICATION_SQS_BUSINESS_OBJECT_DATA_STATUS_CHANGE_VELOCITY_TEMPLATE, velocityContextMap,
            "businessObjectDataStatusChangeEvent");
    }

    @Override
    public String buildSystemMonitorResponse(String systemMonitorRequestPayload)
    {
        return evaluateVelocityTemplate(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE,
            getIncomingMessageValueMap(systemMonitorRequestPayload, ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES),
            "systemMonitorResponse");
    }

    /**
     * Evaluates a velocity template if one is defined for the specified configuration value.
     *
     * @param configurationValue the configuration value of the optional velocity template. If the configuration value returned is null, null will be returned.
     * @param contextMap the optional context map of additional keys and values to place in the velocity context. This can be useful if you have values from an
     * incoming request message you want to make available to velocity to use in the building of the outgoing response message.
     * @param velocityTemplateName the velocity template name used when Velocity logs error messages.
     *
     * @return the evaluated velocity template.
     */
    private String evaluateVelocityTemplate(ConfigurationValue configurationValue, Map<String, Object> contextMap, String velocityTemplateName)
    {
        // Initialize the message text to null which will cause a message to not be sent.
        String messageText = null;

        // Get the velocity template and only process it if one is configured.
        String velocityTemplate = configurationHelper.getProperty(configurationValue);
        if (StringUtils.isNotBlank(velocityTemplate))
        {
            // Create and populate the velocity context with dynamic values. Note that we can't use periods within the context keys since they can't
            // be referenced in the velocity template (i.e. they're used to separate fields with the context object being referenced).
            Map<String, Object> context = new HashMap<>();
            context.put(ConfigurationValue.HERD_NOTIFICATION_SQS_ENVIRONMENT.getKey().replace('.', '_'),
                configurationHelper.getProperty(ConfigurationValue.HERD_NOTIFICATION_SQS_ENVIRONMENT));
            context.put("current_time", HerdDateUtils.now().toString());
            context.put("uuid", UUID.randomUUID().toString());
            context.put("username", herdDaoSecurityHelper.getCurrentUsername());
            context.put("StringUtils", StringUtils.class);
            context.put("CollectionUtils", CollectionUtils.class);

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
}

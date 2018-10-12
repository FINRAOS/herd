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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.dto.SystemMonitorResponseNotificationEvent;
import org.finra.herd.model.jpa.MessageTypeEntity;
import org.finra.herd.service.helper.notification.AbstractNotificationMessageBuilderTestHelper;
import org.finra.herd.service.helper.notification.SystemMonitorResponseMessageBuilder;

/**
 * Tests the functionality for SystemMonitorResponseMessageBuilder.
 */
public class SystemMonitorResponseMessageBuilderTest extends AbstractNotificationMessageBuilderTestHelper
{
    @Autowired
    private SystemMonitorResponseMessageBuilder systemMonitorResponseMessageBuilder;

    @Test
    public void testBuildSystemMonitorResponse() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(),
            SYSTEM_MONITOR_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Call the method under test.
            List<NotificationMessage> result = systemMonitorResponseMessageBuilder
                .buildNotificationMessages(new SystemMonitorResponseNotificationEvent(getTestSystemMonitorIncomingMessage()));

            // Validate the results.
            validateSystemMonitorResponseNotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), HERD_OUTGOING_QUEUE, result.get(0));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testBuildSystemMonitorResponseInvalidVelocityTemplate() throws Exception
    {
        // Override the configuration to use an invalid velocity template for building the system monitor response.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(), "#if($missingEndOfIfStatement");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Try to build a system monitor response message when velocity template is invalid.
            systemMonitorResponseMessageBuilder.buildNotificationMessages(new SystemMonitorResponseNotificationEvent(getTestSystemMonitorIncomingMessage()));
            fail();
        }
        catch (ParseErrorException e)
        {
            assertTrue(e.getMessage().startsWith("Encountered \"<EOF>\" at systemMonitorResponse[line 1, column 28]"));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testBuildSystemMonitorResponseInvalidXmlRequestPayload() throws Exception
    {
        // Override the configuration to remove the XPath expressions when building the system monitor response.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(),
            SYSTEM_MONITOR_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Try to get a system monitor response when request payload contains invalid XML.
            systemMonitorResponseMessageBuilder.buildNotificationMessages(new SystemMonitorResponseNotificationEvent(INVALID_VALUE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Payload is not valid XML:\n%s", INVALID_VALUE), e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testBuildSystemMonitorResponseInvalidXpathExpression() throws Exception
    {
        // Override the configuration to use an invalid XPath expression.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(),
            SYSTEM_MONITOR_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML);
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES.getKey(), "key=///");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Try to build a system monitor response message when xpath expression is invalid.
            systemMonitorResponseMessageBuilder.buildNotificationMessages(new SystemMonitorResponseNotificationEvent(getTestSystemMonitorIncomingMessage()));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().startsWith("XPath expression \"///\" could not be evaluated against payload"));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testBuildSystemMonitorResponseInvalidXpathProperties() throws Exception
    {
        // Override configuration to set an invalid XPath properties format (this happens when an invalid unicode character is found).
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(),
            SYSTEM_MONITOR_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML);
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES.getKey(), "key=\\uxxxx");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // When the XPath properties are not a valid format, an IllegalStateException should be thrown.
            systemMonitorResponseMessageBuilder.buildNotificationMessages(new SystemMonitorResponseNotificationEvent(getTestSystemMonitorIncomingMessage()));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Unable to load XPath properties from configuration with key '%s'",
                ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES.getKey()), e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testBuildSystemMonitorResponseNoMessageVelocityTemplate() throws Exception
    {
        // Override the configuration to remove the velocity template for building the system monitor response.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(), null);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // When no velocity template is present, the response messages should be empty.
            assertEquals(0,
                systemMonitorResponseMessageBuilder.buildNotificationMessages(new SystemMonitorResponseNotificationEvent(getTestSystemMonitorIncomingMessage()))
                    .size());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testBuildSystemMonitorResponseNoXPathExpressions() throws Exception
    {
        // Override the configuration to remove the XPath expressions when building the system monitor response.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(),
            SYSTEM_MONITOR_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML);
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES.getKey(), null);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Try to get a system monitor response when XPath expressions are removed.
            // This should throw a MethodInvocationException since velocity template contains an undefined variable.
            systemMonitorResponseMessageBuilder.buildNotificationMessages(new SystemMonitorResponseNotificationEvent(getTestSystemMonitorIncomingMessage()));
            fail();
        }
        catch (MethodInvocationException e)
        {
            assertEquals("Variable $incoming_message_correlation_id has not been set at systemMonitorResponse[line 11, column 29]", e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;

import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * Tests the functionality within the default SQS message builder.
 */
public class DefaultSqsMessageBuilderTest extends AbstractServiceTest
{
    @Autowired
    private SqsMessageBuilder defaultSqsMessageBuilder;

    /**
     * Test message building using 4 sub-partitions and default user.
     */
    @Test
    public void testBuildBusinessObjectDataStatusChangeMessage()
    {
        List<String> subPartitionValues = Arrays.asList("testSubPartitionValueA", "testSubPartitionValueB", "testSubPartitionValueC", "testSubPartitionValueD");
        testBuildBusinessObjectDataStatusChangeMessage(subPartitionValues, HerdDaoSecurityHelper.SYSTEM_USER);
    }

    /**
     * Test message building using null sub-partitions and default user.
     */
    @Test
    public void testBuildBusinessObjectDataStatusChangeMessageNullSubpartitions()
    {
        List<String> subPartitionValues = null;
        testBuildBusinessObjectDataStatusChangeMessage(subPartitionValues, HerdDaoSecurityHelper.SYSTEM_USER);
    }

    /**
     * Test message building using 4 sub-partitions and a non-default user.
     */
    @SuppressWarnings("serial")
    @Test
    public void testBuildBusinessObjectDataStatusChangeMessageUserInContext()
    {
        Authentication originalAuthentication = SecurityContextHolder.getContext().getAuthentication();
        try
        {
            SecurityContextHolder.getContext().setAuthentication(new Authentication()
            {
                @Override
                public String getName()
                {
                    return null;
                }

                @Override
                public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException
                {
                }

                @Override
                public boolean isAuthenticated()
                {
                    return false;
                }

                @Override
                public Object getPrincipal()
                {
                    List<GrantedAuthority> authorities = Collections.emptyList();
                    return new User("testUsername", "", authorities);
                }

                @Override
                public Object getDetails()
                {
                    return null;
                }

                @Override
                public Object getCredentials()
                {
                    return null;
                }

                @Override
                public Collection<? extends GrantedAuthority> getAuthorities()
                {
                    return null;
                }
            });
            List<String> subPartitionValues =
                Arrays.asList("testSubPartitionValueA", "testSubPartitionValueB", "testSubPartitionValueC", "testSubPartitionValueD");
            testBuildBusinessObjectDataStatusChangeMessage(subPartitionValues, "testUsername");
        }
        finally
        {
            // Restore the original authentication.
            SecurityContextHolder.getContext().setAuthentication(originalAuthentication);
        }
    }

    /**
     * Test BuildSystemMonitorResponse. Builds the response based on a test request, then asserts the correct system monitor transformations occur.
     */
    @Test
    public void testBuildSystemMonitorResponse() throws Exception
    {
        validateSystemMonitorResponse(defaultSqsMessageBuilder.buildSystemMonitorResponse(getTestSystemMonitorIncomingMessage()));
    }

    /**
     * Test that no response is built when a velocity template is not configured.
     */
    @Test
    public void testBuildSystemMonitorResponseNoVelocityTemplate() throws Exception
    {
        // Override the configuration to remove the velocity template for building the system monitor response.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(), null);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // When no velocity template is present, the response message should be null.
            assertNull(defaultSqsMessageBuilder.buildSystemMonitorResponse(getTestSystemMonitorIncomingMessage()));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    /**
     * Test that a parsing exception is thrown when an invalid velocity template is configured.
     */
    @Test(expected = ParseErrorException.class)
    public void testBuildSystemMonitorResponseInvalidVelocityTemplate() throws Exception
    {
        // Override the configuration to use an invalid velocity template for building the system monitor response.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE.getKey(), "#if($missingEndOfIfStatement");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // When the velocity template is invalid, a ParseErrorException should be thrown.
            defaultSqsMessageBuilder.buildSystemMonitorResponse(getTestSystemMonitorIncomingMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    /**
     * Test that the resultant system monitor response message works when no XPath expressions are configured.
     */
    @Test
    public void testBuildSystemMonitorResponseNoXPathExpressions() throws Exception
    {
        // Override the configuration to remove the XPath expressions when building the system monitor response.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES.getKey(), null);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Try to get a system monitor response when XPath expressions are removed.
            defaultSqsMessageBuilder.buildSystemMonitorResponse(getTestSystemMonitorIncomingMessage());
            fail("Should throw an MethodInvocationException when the velocity template contains an undefined variable.");
        }
        catch (MethodInvocationException e)
        {
            assertEquals(String.format("Variable $incoming_message_correlation_id has not been set at systemMonitorResponse[line 11, column 29]"),
                e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    /**
     * Test that an IllegalStateException is thrown when an invalid XPath expression is encountered.
     */
    @Test(expected = IllegalStateException.class)
    public void testBuildSystemMonitorResponseInvalidXpathExpression() throws Exception
    {
        // Override the configuration to use an invalid XPath expression.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES.getKey(), "key=///");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // When the XPath expression is invalid, an IllegalStateException should be thrown.
            defaultSqsMessageBuilder.buildSystemMonitorResponse(getTestSystemMonitorIncomingMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    /**
     * Test that an IllegalStateException is thrown when the XPath properties format is invalid.
     */
    @Test(expected = IllegalStateException.class)
    public void testBuildSystemMonitorResponseInvalidXpathProperties() throws Exception
    {
        // Override the configuration to use an invalid XPath properties format. This happens when an invalid unicode character is found.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES.getKey(), "key=\\uxxxx");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // When the XPath properties are not a valid format, an IllegalStateException should be thrown.
            defaultSqsMessageBuilder.buildSystemMonitorResponse(getTestSystemMonitorIncomingMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    /**
     * Builds a message and validates that: - No exceptions are thrown. - Returned message XML is valid and de-serializable. - Returned correlation ID equals
     * expected. - Returned triggered by username equals expected. - Returned business object data key equals expected. - Returned old and new statuses equal
     * expected.
     *
     * @param subPartitionValues customizable expected sub-partition values
     * @param triggeredByUsername customizable expected user name
     */
    private void testBuildBusinessObjectDataStatusChangeMessage(List<String> subPartitionValues, String triggeredByUsername)
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = createTestValidBusinessObjectData(subPartitionValues, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        String newBusinessObjectDataStatus = "testNewBusinessObjectDataStatus";
        String oldBusinessObjectDataStatus = "testOldBusinessObjectDataStatus";

        // Construct the ESB message
        String message = defaultSqsMessageBuilder
            .buildBusinessObjectDataStatusChangeMessage(businessObjectDataKey, newBusinessObjectDataStatus, oldBusinessObjectDataStatus);

        // Validate the message.
        validateBusinessObjectDataStatusChangeMessage(message, businessObjectDataKey, businessObjectDataEntity.getId(), triggeredByUsername,
            newBusinessObjectDataStatus, oldBusinessObjectDataStatus, NO_ATTRIBUTES);
    }
}

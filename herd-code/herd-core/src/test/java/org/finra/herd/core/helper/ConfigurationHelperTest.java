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
package org.finra.herd.core.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.math.BigDecimal;

import org.junit.Test;
import org.springframework.mock.env.MockEnvironment;

import org.finra.herd.core.AbstractCoreTest;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * unit tests for {@link ConfigurationHelper}
 */
public class ConfigurationHelperTest extends AbstractCoreTest
{
    @Test
    public void testGetProperty()
    {
        ConfigurationValue configurationValue = ConfigurationValue.HERD_ENVIRONMENT;
        String expectedValue = "test";

        MockEnvironment environment = new MockEnvironment();
        environment.setProperty(configurationValue.getKey(), expectedValue);

        String value = ConfigurationHelper.getProperty(configurationValue, String.class, environment);

        assertNotNull("value", value);
        assertEquals("value", expectedValue, value);
    }

    @Test
    public void testGetPropertyReturnDefaultWhenPropertyDoesNotExists()
    {
        ConfigurationValue configurationValue = ConfigurationValue.HERD_ENVIRONMENT;
        String expectedValue = (String) configurationValue.getDefaultValue();

        MockEnvironment environment = new MockEnvironment();
        environment.setProperty(configurationValue.getKey(), expectedValue);

        String value = ConfigurationHelper.getProperty(configurationValue, String.class, environment);

        assertNotNull("value", value);
        assertEquals("value", expectedValue, value);
    }

    @Test
    public void testGetPropertyReturnDefaultWhenValueConversionFails() throws Exception
    {
        ConfigurationValue configurationValue = ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_MAX_RESULTS;
        Integer expectedValue = (Integer) configurationValue.getDefaultValue();

        MockEnvironment environment = new MockEnvironment();
        environment.setProperty(configurationValue.getKey(), "NOT_AN_INTEGER");

        executeWithoutLogging(ConfigurationHelper.class, () ->
        {
            Integer value = ConfigurationHelper.getProperty(configurationValue, Integer.class, environment);
            assertNotNull("value", value);
            assertEquals("value", expectedValue, value);
        });
    }

    @Test
    public void testGetPropertyThrowsWhenDefaultTypeDoesNotMatch()
    {
        ConfigurationValue configurationValue = ConfigurationValue.HERD_ENVIRONMENT;
        Class<?> expectedDefaultType = configurationValue.getDefaultValue().getClass();
        Class<?> givenType = Integer.class;

        MockEnvironment environment = new MockEnvironment();

        try
        {
            ConfigurationHelper.getProperty(configurationValue, givenType, environment);
            fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception type", IllegalStateException.class, e.getClass());
            assertEquals("thrown exception message",
                "targetType \"" + givenType + "\" is not assignable from the default value of type \"" + expectedDefaultType + "\" for configuration value " +
                    "\"HERD_ENVIRONMENT\".", e.getMessage());
        }
    }

    @Test
    public void testGetPropertyReturnNullWhenPropertyDoesNotExistsAndNoDefaultsConfigured()
    {
        ConfigurationValue configurationValue = ConfigurationValue.HIBERNATE_DIALECT;

        MockEnvironment environment = new MockEnvironment();

        String value = ConfigurationHelper.getProperty(configurationValue, String.class, environment);

        assertNull("value", value);
    }

    @Test
    public void testGetPropertyValidationThrowsWhenConfigurationValueIsNull()
    {
        MockEnvironment environment = new MockEnvironment();

        try
        {
            ConfigurationHelper.getProperty(null, String.class, environment);
            fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception type", IllegalStateException.class, e.getClass());
            assertEquals("thrown exception message", "configurationValue is required", e.getMessage());
        }
    }

    @Test
    public void testGetPropertyValidationThrowsWhenTargetTypeIsNull()
    {
        MockEnvironment environment = new MockEnvironment();

        try
        {
            ConfigurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT, null, environment);
            fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception type", IllegalStateException.class, e.getClass());
            assertEquals("thrown exception message", "targetType is required", e.getMessage());
        }
    }

    @Test
    public void testGetPropertyNoTargetType()
    {
        ConfigurationValue configurationValue = ConfigurationValue.HERD_ENVIRONMENT;

        MockEnvironment environment = new MockEnvironment();

        String value = ConfigurationHelper.getProperty(configurationValue, environment);

        assertEquals("value", configurationValue.getDefaultValue(), value);
    }

    @Test
    public void testGetPropertyInstance()
    {
        ConfigurationValue configurationValue = ConfigurationValue.HERD_ENVIRONMENT;

        String value = configurationHelper.getProperty(configurationValue, String.class);

        assertEquals("value", configurationValue.getDefaultValue(), value);
    }

    @Test
    public void testGetPropertyInstanceNoTargetType()
    {
        ConfigurationValue configurationValue = ConfigurationValue.HERD_ENVIRONMENT;

        String value = configurationHelper.getProperty(configurationValue);

        assertEquals("value", configurationValue.getDefaultValue(), value);
    }

    @Test
    public void testGetBigDecimalPropertyValue()
    {
        ConfigurationValue configurationValue = ConfigurationValue.EMR_CLUSTER_LOWEST_TOTAL_COST_THRESHOLD_DOLLARS;

        MockEnvironment environment = new MockEnvironment();
        environment.setProperty(configurationValue.getKey(), "0.05");

        assertEquals(new BigDecimal("0.05"), configurationHelper.getBigDecimalProperty(configurationValue, environment));
    }

    @Test
    public void testGetBigDecimalPropertyValueConversionFail()
    {
        ConfigurationValue configurationValue = ConfigurationValue.EMR_CLUSTER_LOWEST_TOTAL_COST_THRESHOLD_DOLLARS;

        MockEnvironment environment = new MockEnvironment();
        environment.setProperty(configurationValue.getKey(), "INVALID_BigDecimal_VALUE");

        try
        {
            configurationHelper.getBigDecimalProperty(configurationValue, environment);
            fail("Should throw an IllegalStatueException when property value is not BigDecimal.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Configuration \"%s\" has an invalid BigDecimal value: \"INVALID_BigDecimal_VALUE\".", configurationValue.getKey()),
                e.getMessage());
        }
    }

    @Test
    public void testGetBigDecimalPropertyValidationThrowsWhenConfigurationValueIsNull()
    {
        try
        {
            configurationHelper.getBigDecimalProperty(null);
            fail("Should throw an IllegalStateException when configuration value is null.");
        }
        catch (IllegalStateException e)
        {
            assertEquals("configurationValue is required", e.getMessage());
        }
    }

    @Test
    public void testGetBooleanPropertyValue()
    {
        ConfigurationValue configurationValue = ConfigurationValue.USER_NAMESPACE_AUTHORIZATION_ENABLED;

        MockEnvironment environment = new MockEnvironment();
        environment.setProperty(configurationValue.getKey(), "true");

        assertEquals(Boolean.TRUE, configurationHelper.getBooleanProperty(configurationValue, environment));
    }

    @Test
    public void testGetBooleanPropertyValueConversionFails()
    {
        ConfigurationValue configurationValue = ConfigurationValue.USER_NAMESPACE_AUTHORIZATION_ENABLED;

        MockEnvironment environment = new MockEnvironment();
        environment.setProperty(configurationValue.getKey(), "NOT_A_BOOLEAN");

        try
        {
            configurationHelper.getBooleanProperty(configurationValue, environment);
            fail("Should throw an IllegalStatueException when property value is not boolean.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Configuration \"%s\" has an invalid boolean value: \"NOT_A_BOOLEAN\".", configurationValue.getKey()), e.getMessage());
        }
    }

    @Test
    public void testGetBooleanPropertyValidationThrowsWhenConfigurationValueIsNull()
    {
        try
        {
            configurationHelper.getBooleanProperty(null);
            fail("Should throw an IllegalStateException when configuration value is null.");
        }
        catch (IllegalStateException e)
        {
            assertEquals("configurationValue is required", e.getMessage());
        }
    }

    @Test
    public void testGetRequiredProperty()
    {
        ConfigurationValue configurationValue = ConfigurationValue.HERD_ENVIRONMENT;

        String value = configurationHelper.getRequiredProperty(configurationValue);

        assertEquals(configurationValue.getDefaultValue(), value);
    }

    @Test
    public void testGetRequiredPropertyIllegalStateException()
    {
        ConfigurationValue configurationValue = ConfigurationValue.HERD_ENVIRONMENT;

        MockEnvironment environment = new MockEnvironment();
        environment.setProperty(configurationValue.getKey(), BLANK_TEXT);

        try
        {
            configurationHelper.getRequiredProperty(configurationValue, environment);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Configuration \"%s\" must have a value.", ConfigurationValue.HERD_ENVIRONMENT.getKey()), e.getMessage());
        }
    }
}

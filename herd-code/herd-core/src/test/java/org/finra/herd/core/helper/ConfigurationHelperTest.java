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

import org.junit.Assert;
import org.junit.Test;
import org.springframework.mock.env.MockEnvironment;

import org.finra.herd.core.AbstractCoreTest;
import org.finra.herd.model.dto.ConfigurationValue;

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

        Assert.assertNotNull("value", value);
        Assert.assertEquals("value", expectedValue, value);
    }

    @Test
    public void testGetPropertyReturnDefaultWhenPropertyDoesNotExists()
    {
        ConfigurationValue configurationValue = ConfigurationValue.HERD_ENVIRONMENT;
        String expectedValue = (String) configurationValue.getDefaultValue();

        MockEnvironment environment = new MockEnvironment();
        environment.setProperty(configurationValue.getKey(), expectedValue);

        String value = ConfigurationHelper.getProperty(configurationValue, String.class, environment);

        Assert.assertNotNull("value", value);
        Assert.assertEquals("value", expectedValue, value);
    }

    @Test
    public void testGetPropertyReturnDefaultWhenValueConversionFails()
    {
        ConfigurationValue configurationValue = ConfigurationValue.EMR_OOZIE_JOBS_TO_INCLUDE_IN_CLUSTER_STATUS;
        Integer expectedValue = (Integer) configurationValue.getDefaultValue();

        MockEnvironment environment = new MockEnvironment();
        environment.setProperty(configurationValue.getKey(), "NOT_AN_INTEGER");

        Integer value = ConfigurationHelper.getProperty(configurationValue, Integer.class, environment);

        Assert.assertNotNull("value", value);
        Assert.assertEquals("value", expectedValue, value);
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
            Assert.fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalStateException.class, e.getClass());
            Assert.assertEquals("thrown exception message",
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

        Assert.assertNull("value", value);
    }

    @Test
    public void testGetPropertyValidationThrowsWhenConfigurationValueIsNull()
    {
        MockEnvironment environment = new MockEnvironment();

        try
        {
            ConfigurationHelper.getProperty(null, String.class, environment);
            Assert.fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalStateException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "configurationValue is required", e.getMessage());
        }
    }

    @Test
    public void testGetPropertyValidationThrowsWhenTargetTypeIsNull()
    {
        MockEnvironment environment = new MockEnvironment();

        try
        {
            ConfigurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT, null, environment);
            Assert.fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalStateException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "targetType is required", e.getMessage());
        }
    }

    @Test
    public void testGetPropertyNoTargetType()
    {
        ConfigurationValue configurationValue = ConfigurationValue.HERD_ENVIRONMENT;

        MockEnvironment environment = new MockEnvironment();

        String value = ConfigurationHelper.getProperty(configurationValue, environment);

        Assert.assertEquals("value", configurationValue.getDefaultValue(), value);
    }

    @Test
    public void testGetPropertyInstance()
    {
        ConfigurationValue configurationValue = ConfigurationValue.HERD_ENVIRONMENT;

        String value = configurationHelper.getProperty(configurationValue, String.class);

        Assert.assertEquals("value", configurationValue.getDefaultValue(), value);
    }

    @Test
    public void testGetPropertyInstanceNoTargetType()
    {
        ConfigurationValue configurationValue = ConfigurationValue.HERD_ENVIRONMENT;

        String value = configurationHelper.getProperty(configurationValue);

        Assert.assertEquals("value", configurationValue.getDefaultValue(), value);
    }
}

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

import java.math.BigDecimal;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.propertyeditors.CustomBooleanEditor;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import org.finra.herd.model.dto.ConfigurationValue;

/**
 * A helper for {@link ConfigurationValue}. Mainly used to facilitate access to current application's {@link Environment} object.
 */
@Component
public class ConfigurationHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationHelper.class);

    @Autowired
    private Environment environment;

    /**
     * Calls {@link #getProperty(ConfigurationValue, Class, Environment)} using String targetType.
     *
     * @param configurationValue {@link ConfigurationValue}
     * @param environment {@link Environment}
     *
     * @return String value
     */
    public static String getProperty(ConfigurationValue configurationValue, Environment environment)
    {
        return getProperty(configurationValue, String.class, environment);
    }

    /**
     * Wrapper for {@link Environment#getProperty(String, Class, Object)} using the given {@link ConfigurationValue} as the key and default value. Optionally,
     * uses the configured default value if the property is not found in the environment. The configured default value must be of the same class, or must be a
     * sub-class of the given targetType.
     *
     * @param <T> The return type
     * @param configurationValue The {@link ConfigurationValue} with property key and default value.
     * @param targetType The returned object's type
     * @param environment The {@link Environment} containing the property
     *
     * @return The property value
     */
    public static <T> T getProperty(ConfigurationValue configurationValue, Class<T> targetType, Environment environment)
    {
        /*
         * Parameter validation
         */
        if (configurationValue == null)
        {
            throw new IllegalStateException("configurationValue is required");
        }

        if (targetType == null)
        {
            throw new IllegalStateException("targetType is required");
        }

        String key = configurationValue.getKey();
        Object genericDefaultValue = configurationValue.getDefaultValue();

        /*
         * Assert that the targetType is of the correct type.
         */
        if (genericDefaultValue != null && !targetType.isAssignableFrom(genericDefaultValue.getClass()))
        {
            throw new IllegalStateException(
                "targetType \"" + targetType + "\" is not assignable from the default value of type \"" + genericDefaultValue.getClass() +
                    "\" for configuration value \"" + configurationValue + "\".");
        }

        @SuppressWarnings("unchecked")
        T defaultValue = (T) genericDefaultValue;

        T value = defaultValue;
        try
        {
            value = environment.getProperty(key, targetType, defaultValue);
        }
        catch (ConversionFailedException conversionFailedException)
        {
            /*
             * If the environment value is not convertible into the targetType, catch it and log a warning.
             * Return the default value.
             */
            LOGGER.warn(
                "Error converting environment property with key '" + key + "' and value '" + environment.getProperty(key) + "' into a '" + targetType + "'.",
                conversionFailedException);
        }
        return value;
    }

    /**
     * Gets a property value as a {@link BigDecimal}.
     *
     * @param configurationValue the {@link BigDecimal} configuration value
     *
     * @return the {@link BigDecimal} property value
     */
    public BigDecimal getBigDecimalRequiredProperty(ConfigurationValue configurationValue)
    {
        return getBigDecimalRequiredProperty(configurationValue, environment);
    }

    /**
     * Gets a property value as a {@link BigDecimal}.
     *
     * @param configurationValue the {@link BigDecimal} configuration value
     * @param environment the environment containing the property
     *
     * @return the {@link BigDecimal} property value
     */
    public BigDecimal getBigDecimalRequiredProperty(ConfigurationValue configurationValue, Environment environment)
    {
        String bigDecimalStringValue = getRequiredProperty(configurationValue, environment);

        BigDecimal bigDecimalValue = null;
        try
        {
            // Converts the string value to BigDecimal
            bigDecimalValue = new BigDecimal(bigDecimalStringValue);
        }
        catch (NumberFormatException numberFormatException)
        {
            logErrorAndThrowIllegalStateException(configurationValue, "BigDecimal", bigDecimalStringValue, numberFormatException);
        }

        return bigDecimalValue;
    }

    /**
     * Gets a property value as a boolean.
     *
     * @param configurationValue the boolean configuration value
     *
     * @return the boolean property value
     */
    public Boolean getBooleanProperty(ConfigurationValue configurationValue)
    {
        return getBooleanProperty(configurationValue, environment);
    }

    /**
     * Gets a property value as a boolean.
     *
     * @param configurationValue the boolean configuration value
     * @param environment the environment containing the property
     *
     * @return the boolean property value
     */
    public Boolean getBooleanProperty(ConfigurationValue configurationValue, Environment environment)
    {
        String booleanStringValue = getProperty(configurationValue, environment);

        // Use custom boolean editor without allowed empty strings to convert the value of the argument to a boolean value.
        CustomBooleanEditor customBooleanEditor = new CustomBooleanEditor(false);
        try
        {
            customBooleanEditor.setAsText(booleanStringValue);
        }
        catch (IllegalArgumentException e)
        {
            logErrorAndThrowIllegalStateException(configurationValue, "boolean", booleanStringValue, e);
        }

        // Return the boolean value.
        return (Boolean) customBooleanEditor.getValue();
    }

    /**
     * Gets a property value as {@link BigDecimal}. Additionally it ensures that the property value is not negative.
     *
     * @param configurationValue the {@link BigDecimal} configuration value
     *
     * @return the non-negative {@link BigDecimal} property value
     */
    public BigDecimal getNonNegativeBigDecimalRequiredProperty(ConfigurationValue configurationValue)
    {
        return getNonNegativeBigDecimalRequiredProperty(configurationValue, environment);
    }


    /**
     * Gets a property value as {@link BigDecimal}. Additionally it ensures that the property value is not negative.
     *
     * @param configurationValue the {@link BigDecimal} configuration value
     * @param environment the environment containing the property
     *
     * @return the non-negative {@link BigDecimal} property value
     */
    public BigDecimal getNonNegativeBigDecimalRequiredProperty(ConfigurationValue configurationValue, Environment environment)
    {
        final BigDecimal nonNegativeBigDecimalPropertyValue = getBigDecimalRequiredProperty(configurationValue, environment);
        if (nonNegativeBigDecimalPropertyValue.signum() == -1)
        {
            throw new IllegalStateException(String
                .format("Configuration \"%s\" has an invalid non-negative BigDecimal value: \"%s\".", configurationValue.getKey(),
                    nonNegativeBigDecimalPropertyValue));
        }

        return nonNegativeBigDecimalPropertyValue;
    }

    /**
     * Calls {@link #getProperty(ConfigurationValue, Class)} where the target type is {@link String}.
     *
     * @param configurationValue {@link ConfigurationValue}
     *
     * @return The string value
     */
    public String getProperty(ConfigurationValue configurationValue)
    {
        return getProperty(configurationValue, String.class);
    }

    /**
     * Calls {@link #getProperty(ConfigurationValue, Class, Environment)}
     *
     * @param configurationValue The {@link ConfigurationValue}
     * @param targetType The return type
     *
     * @return The property value
     */
    public <T> T getProperty(ConfigurationValue configurationValue, Class<T> targetType)
    {
        return getProperty(configurationValue, targetType, environment);
    }

    /**
     * Gets a property value and validates that it is not blank or null.
     *
     * @param configurationValue {@link ConfigurationValue}
     *
     * @return the string value
     */
    public String getRequiredProperty(ConfigurationValue configurationValue)
    {
        return getRequiredProperty(configurationValue, environment);
    }

    /**
     * Gets a property value and validates that it is not blank or null.
     *
     * @param configurationValue {@link ConfigurationValue}
     * @param environment the environment containing the property
     *
     * @return the string value
     */
    public String getRequiredProperty(ConfigurationValue configurationValue, Environment environment)
    {
        String property = getProperty(configurationValue, String.class, environment);

        if (StringUtils.isBlank(property))
        {
            throw new IllegalStateException(String.format("Configuration \"%s\" must have a value.", configurationValue.getKey()));
        }

        return property;
    }

    /**
     * Logs the error message, and then throws {@link IllegalStateException}
     *
     * @param configurationValue - {@link ConfigurationValue}
     * @param targetTypeName - the name of the data type we want to convert the configuration value to(boolean, BigDecimal...etc)
     * @param stringValue - the configuration value in string type
     * @param exception - the exception thrown when converting the configuration value from string type to the target data type
     */
    private void logErrorAndThrowIllegalStateException(ConfigurationValue configurationValue, String targetTypeName, String stringValue, Exception exception)
    {
        // Create an invalid state exception.
        IllegalStateException illegalStateException = new IllegalStateException(
            String.format("Configuration \"%s\" has an invalid %s value: \"%s\".", configurationValue.getKey(), targetTypeName, stringValue), exception);

        // Log the exception.
        LOGGER.error(illegalStateException.getMessage(), illegalStateException);
        // This will produce a 500 HTTP status code error.
        throw illegalStateException;
    }
}

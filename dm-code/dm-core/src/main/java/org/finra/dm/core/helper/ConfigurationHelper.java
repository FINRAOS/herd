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
package org.finra.dm.core.helper;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import org.finra.dm.model.dto.ConfigurationValue;

/**
 * A helper for {@link ConfigurationValue}. Mainly used to facilitate access to current application's {@link Environment} object.
 */
@Component
public class ConfigurationHelper
{
    private static final Logger LOGGER = Logger.getLogger(ConfigurationHelper.class);

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
            throw new IllegalArgumentException("configurationValue is required");
        }

        if (targetType == null)
        {
            throw new IllegalArgumentException("targetType is required");
        }

        String key = configurationValue.getKey();
        Object genericDefaultValue = configurationValue.getDefaultValue();

        /*
         * Assert that the targetType is of the correct type.
         */
        if (genericDefaultValue != null && !targetType.isAssignableFrom(genericDefaultValue.getClass()))
        {
            throw new IllegalArgumentException(
                "targetType '" + targetType + "' is not assignable from the default value of type '" + genericDefaultValue.getClass() + "'.");
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
}

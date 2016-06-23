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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * A helper class for Parameter related code.
 */
@Component
public class ParameterHelper
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * Parses the parameter value as a signed decimal integer.
     *
     * @param parameter the string value to be parsed
     *
     * @return the integer value represented by the parameter value in decimal
     * @throws IllegalArgumentException if the parameter value does not contain a parsable integer
     */
    public int getParameterValueAsInteger(Parameter parameter) throws IllegalArgumentException
    {
        return getParameterValueAsInteger(parameter.getName(), parameter.getValue());
    }

    /**
     * Gets the parameter value if found or defaults to the relative configuration setting value. The parameter value is parsed as a signed decimal integer.
     *
     * @param parameters the map of parameters
     * @param configurationValue the configuration value
     *
     * @return the integer value represented by the parameter value in decimal
     * @throws IllegalArgumentException if the parameter value does not contain a parsable integer
     */
    public int getParameterValueAsInteger(Map<String, String> parameters, ConfigurationValue configurationValue) throws IllegalArgumentException
    {
        return getParameterValueAsInteger(configurationValue.getKey(), getParameterValue(parameters, configurationValue));
    }

    /**
     * Validates that parameter names are there and that there are no duplicate parameter names in case insensitive manner. This method also trims parameter
     * names.
     *
     * @param parameters the list of parameters to be validated
     */
    public void validateParameters(List<Parameter> parameters)
    {
        if (!CollectionUtils.isEmpty(parameters))
        {
            Set<String> parameterNameValidationSet = new HashSet<>();
            for (Parameter parameter : parameters)
            {
                // Validate and trim the parameter name.
                Assert.hasText(parameter.getName(), "A parameter name must be specified.");
                parameter.setName(parameter.getName().trim());

                // Ensure the parameter name isn't a duplicate by using a set with a "lowercase" name as the key for case insensitivity.
                String lowercaseParameterName = parameter.getName().toLowerCase();
                Assert.isTrue(!parameterNameValidationSet.contains(lowercaseParameterName), "Duplicate parameter name found: " + parameter.getName());
                parameterNameValidationSet.add(lowercaseParameterName);
            }
        }
    }

    /**
     * Gets the parameter value if found or defaults to the relative configuration setting value.
     *
     * @param parameters the map of parameters
     * @param configurationValue the configuration value
     *
     * @return the parameter value if found, the relative configuration setting value otherwise
     */
    private String getParameterValue(Map<String, String> parameters, ConfigurationValue configurationValue)
    {
        String parameterName = configurationValue.getKey().toLowerCase();
        String parameterValue;

        if (parameters.containsKey(parameterName))
        {
            parameterValue = parameters.get(parameterName);
        }
        else
        {
            parameterValue = configurationHelper.getProperty(configurationValue);
        }

        return parameterValue;
    }

    /**
     * Parses the parameter value as a signed decimal integer.
     *
     * @param parameterName the parameter name
     * @param parameterValue the string parameter value to be parsed
     *
     * @return the integer value represented by the parameter value in decimal
     * @throws IllegalArgumentException if the parameter value does not contain a parsable integer
     */
    private int getParameterValueAsInteger(String parameterName, String parameterValue) throws IllegalArgumentException
    {
        try
        {
            return Integer.parseInt(parameterValue);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException(String.format("Parameter \"%s\" specifies a non-integer value \"%s\".", parameterName, parameterValue), e);
        }
    }
}

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
package org.finra.dm.dao.helper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.dm.core.helper.ConfigurationHelper;
import org.finra.dm.model.dto.ConfigurationValue;

/**
 * A helper class for general data management code.
 */
@Component
public class DmStringHelper
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * Splits the input String based on the default delimiter
     *
     * @param inputString to be split
     *
     * @return the list of String literals that are split with the delimiter
     */
    public List<String> splitStringWithDefaultDelimiter(String inputString)
    {
        List<String> splitString = new ArrayList<>();
        if (inputString != null)
        {
            StringTokenizer stringTokenizer = new StringTokenizer(inputString, configurationHelper.getProperty(ConfigurationValue.FIELD_DATA_DELIMITER));
            while (stringTokenizer.hasMoreElements())
            {
                splitString.add(stringTokenizer.nextToken());
            }
        }
        return splitString;
    }

    /**
     * Splits the input String based on the default delimiter, and also escapes delimiter.
     *
     * @param inputString to be split
     *
     * @return the list of String literals that are split with the delimiter
     */
    public List<String> splitStringWithDefaultDelimiterEscaped(String inputString)
    {
        String delimiter = configurationHelper.getProperty(ConfigurationValue.FIELD_DATA_DELIMITER);
        String escapeChar = configurationHelper.getProperty(ConfigurationValue.FIELD_DATA_DELIMITER_ESCAPE_CHAR);
        List<String> splitString = new ArrayList<>();
        if (inputString != null)
        {
            // Regex that is used to split, matches on delimiter but does not matches when delimiter is escaped by given escape character
            String regex = "(?<!" + Pattern.quote(escapeChar) + ")" + Pattern.quote(delimiter);

            for (String s : inputString.split(regex))
            {
                // Replace the delimiter when it is escaped with the delimiter value.
                splitString.add(s.replace(escapeChar + delimiter, delimiter));
            }
        }
        return splitString;
    }

    /**
     * Gets the value and verifies that it is not blank for the given configuration option.
     *
     * @param configurationValue the configuration option
     *
     * @return the configuration option value
     */
    public String getRequiredConfigurationValue(ConfigurationValue configurationValue)
    {
        String value = configurationHelper.getProperty(configurationValue);

        if (StringUtils.isBlank(value))
        {
            throw new IllegalStateException(String.format("Missing configuration parameter value for key \"%s\".", configurationValue.getKey()));
        }

        return value;
    }

    /**
     * TODO replace uses of this method with ConfigurationHelper Gets the configuration value as integer.
     *
     * @param configurationValue the configuration value.
     *
     * @return the configuration value as an integer.
     */
    public int getConfigurationValueAsInteger(ConfigurationValue configurationValue)
    {
        return configurationHelper.getProperty(configurationValue, Integer.class);
    }

    /**
     * TODO replace uses of this method with ConfigurationHelper Gets the configuration value as string.
     */
    public String getConfigurationValueAsString(ConfigurationValue configurationValue)
    {
        return configurationHelper.getProperty(configurationValue);
    }

    /**
     * Splits the given string by the given delimiter. Trims each token and ignores if empty.
     * <p/>
     * TODO should be merged with {@link #splitStringWithDefaultDelimiter(String)} and {@link #splitStringWithDefaultDelimiterEscaped(String)}
     * <p/>
     * TODO determine whether we should use {@link String#split(String)} or {@link StringTokenizer}
     *
     * @param string The string to split
     * @param delimiter The delimiter of tokens
     *
     * @return Set of tokens
     */
    public Set<String> splitAndTrim(String string, String delimiter)
    {
        String[] tokens = string.split(delimiter);
        Set<String> resultSet = new HashSet<>();
        for (String token : tokens)
        {
            if (StringUtils.isNotBlank(token))
            {
                resultSet.add(token.trim());
            }
        }
        return resultSet;
    }

    /**
     * Joins the given list of strings, separating each by the given delimiter. The elements are ignored if blank. If the original string contains the
     * delimiter, it will be escaped by prepending it with the given escape sequence. If given list is null, the result is also null.
     *
     * @param list List of strings to join
     * @param delimiter Delimiter to separate each element
     * @param escapeSequence Escape sequence to use to escape delimiters
     *
     * @return Joined string
     */
    public String join(List<String> list, String delimiter, String escapeSequence)
    {
        String result = null;
        if (list != null)
        {
            List<String> filteredAndEscaped = new ArrayList<>();
            for (String element : list)
            {
                if (StringUtils.isNotBlank(element))
                {
                    element = element.replace(delimiter, escapeSequence + delimiter);
                    filteredAndEscaped.add(element);
                }
            }

            result = StringUtils.join(filteredAndEscaped, delimiter);
        }
        return result;
    }
}

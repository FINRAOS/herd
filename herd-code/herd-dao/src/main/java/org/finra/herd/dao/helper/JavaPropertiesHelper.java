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
package org.finra.herd.dao.helper;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.stereotype.Component;

/**
 * A helper which abstracts logic for operations around Java's {@link Properties} class of objects.
 */
@Component
public class JavaPropertiesHelper
{
    /**
     * Converts the given properties into a {@link Map} <{@link String}, {@link Object}>.
     * This conversion is required due to the fact that {@link Properties} is an implementation of {@link Map} <{@link Object}, {@link Object}>.
     * 
     * @param properties {@link Properties}
     * @return {@link Map} <{@link String}, {@link Object}>
     */
    public Map<String, Object> toMap(Properties properties)
    {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<Object, Object> property : properties.entrySet())
        {
            String key = property.getKey().toString();
            Object value = property.getValue();

            map.put(key, value);
        }
        return map;
    }

    /**
     * Parses the given string into a {@link Properties}
     * 
     * @param propertiesString A {@link String}
     * @return {@link Properties}
     */
    public Properties getProperties(String propertiesString)
    {
        Properties properties = new Properties();
        StringReader reader = new StringReader(propertiesString);
        try
        {
            properties.load(reader);
        }
        catch (IOException e)
        {
            // This exception should normally not happen as there are not many cases in which a StringReader would throw an error.
            throw new IllegalArgumentException("Error loading properties from string reader. See cause for details.", e);
        }
        return properties;
    }

    /**
     * Parses the given input stream into a {@link Properties}
     * 
     * @param inputStream {@link InputStream} to parse
     * @return {@link Properties}
     */
    public Properties getProperties(InputStream inputStream)
    {
        Properties properties = new Properties();
        try
        {
            properties.load(inputStream);
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Error loading properties from input stream. See cause for details.", e);
        }
        return properties;
    }
}

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
package org.finra.herd.tools.access.validator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * A helper for {@link java.util.Properties}.
 */
@Component
class PropertiesHelper
{
    static final String AWS_REGION_PROPERTY = "awsRegion";

    static final String AWS_ROLE_ARN_PROPERTY = "awsRoleArn";

    static final String AWS_SQS_QUEUE_URL = "awsSqsQueueUrl";

    static final String BUSINESS_OBJECT_DATA_VERSION_PROPERTY = "businessObjectDataVersion";

    static final String BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY = "businessObjectDefinitionName";

    static final String BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY = "businessObjectFormatFileType";

    static final String BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY = "businessObjectFormatUsage";

    static final String BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY = "businessObjectFormatVersion";

    static final String HERD_BASE_URL_PROPERTY = "herdBaseUrl";

    static final String HERD_PASSWORD_PROPERTY = "herdPassword";

    static final String HERD_USERNAME_PROPERTY = "herdUsername";

    static final String NAMESPACE_PROPERTY = "namespace";

    static final String PRIMARY_PARTITION_VALUE_PROPERTY = "primaryPartitionValue";

    static final String SUB_PARTITION_VALUES_PROPERTY = "subPartitionValues";

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesHelper.class);

    private Properties properties = new Properties();

    /**
     * Searches for the property with the specified key in the property list. The method returns {@code null} if the property is not found.
     *
     * @param key the property key
     *
     * @return the value in this property list with the specified key value
     */
    String getProperty(String key)
    {
        return properties.getProperty(key);
    }

    /**
     * Reads a property list from the specified properties file.
     *
     * @param propertiesFile the properties file
     *
     * @throws IOException if an I/O error was encountered
     */
    void loadProperties(File propertiesFile) throws IOException
    {
        // Load the properties.
        LOGGER.info("Loading properties from \"{}\" file...", propertiesFile);
        try (InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(propertiesFile), "UTF-8"))
        {
            properties.load(inputStreamReader);
        }

        // Log all properties except for password.
        StringBuilder propertiesStringBuilder = new StringBuilder();
        for (Object key : properties.keySet())
        {
            String propertyName = key.toString();
            propertiesStringBuilder.append(propertyName).append('=')
                .append(HERD_PASSWORD_PROPERTY.equalsIgnoreCase(propertyName) ? "***" : properties.getProperty(propertyName)).append('\n');
        }
        LOGGER.info("Successfully loaded properties:%n{}", propertiesStringBuilder.toString());
    }
}

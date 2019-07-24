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
import java.io.FileReader;
import java.io.StringReader;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.finra.herd.core.HerdStringUtils;
import org.finra.herd.sdk.api.ApplicationApi;
import org.finra.herd.sdk.api.BusinessObjectDataApi;
import org.finra.herd.sdk.api.CurrentUserApi;
import org.finra.herd.sdk.invoker.ApiClient;
import org.finra.herd.sdk.model.BusinessObjectData;

/**
 * The controller for the application.
 */
@Component
class AccessValidatorController
{
    private static final String BUSINESS_OBJECT_DATA_VERSION_PROPERTY = "businessObjectDataVersion";

    private static final String BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY = "businessObjectDefinitionName";

    private static final String BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY = "businessObjectFormatFileType";

    private static final String BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY = "businessObjectFormatUsage";

    private static final String BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY = "businessObjectFormatVersion";

    private static final String HERD_BASE_URL_PROPERTY = "herdBaseUrl";

    private static final String HERD_PASSWORD_PROPERTY = "herdPassword";

    private static final String HERD_USERNAME_PROPERTY = "herdUsername";

    private static final Logger LOGGER = LoggerFactory.getLogger(AccessValidatorController.class);

    private static final String NAMESPACE_PROPERTY = "namespace";

    private static final String PRIMARY_PARTITION_VALUE_PROPERTY = "primaryPartitionValue";

    private static final String SUB_PARTITION_VALUES_PROPERTY = "subPartitionValues";

    /**
     * Runs the application with the given command line arguments.
     *
     * @param propertiesFile the properties file
     */
    void execute(File propertiesFile) throws Exception
    {
        // Load properties.
        LOGGER.info("Loading properties from \"{}\" file...", propertiesFile);
        Properties properties = new Properties();
        properties.load(new StringReader(IOUtils.toString(new FileReader(propertiesFile))));

        // Log all properties except for password.
        StringBuilder propertiesStringBuilder = new StringBuilder();
        for (Object key : properties.keySet())
        {
            String propertyName = key.toString();
            propertiesStringBuilder.append(propertyName).append('=')
                .append(HERD_PASSWORD_PROPERTY.equalsIgnoreCase(propertyName) ? "***" : properties.getProperty(propertyName)).append('\n');
        }
        LOGGER.info("Successfully loaded properties:%n{}", propertiesStringBuilder.toString());

        // Create the API client to a specific REST endpoint with proper authentication.
        ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(properties.getProperty(HERD_BASE_URL_PROPERTY));
        apiClient.setUsername(properties.getProperty(HERD_USERNAME_PROPERTY));
        apiClient.setPassword(properties.getProperty(HERD_PASSWORD_PROPERTY));

        // Setup specific API classes.
        ApplicationApi applicationApi = new ApplicationApi(apiClient);
        CurrentUserApi currentUserApi = new CurrentUserApi(apiClient);
        BusinessObjectDataApi businessObjectDataApi = new BusinessObjectDataApi(apiClient);

        // Retrieve build information from the registration server.
        LOGGER.info("Retrieving build information from the registration server...");
        LOGGER.info("{}", applicationApi.applicationGetBuildInfo());

        // Retrieve user information from the registration server.
        LOGGER.info("Retrieving user information from the registration server...");
        LOGGER.info("{}", currentUserApi.currentUserGetCurrentUser());

        // Retrieve business object data from the registration server.
        LOGGER.info("Retrieving business object data information from the registration server...");
        Integer businessObjectFormatVersion = HerdStringUtils.convertStringToInteger(properties.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY), null);
        Integer businessObjectDataVersion = HerdStringUtils.convertStringToInteger(properties.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY), null);
        BusinessObjectData businessObjectData = businessObjectDataApi.businessObjectDataGetBusinessObjectData(properties.getProperty(NAMESPACE_PROPERTY),
            properties.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY), properties.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY),
            properties.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY), null, properties.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY),
            properties.getProperty(SUB_PARTITION_VALUES_PROPERTY), businessObjectFormatVersion, businessObjectDataVersion, null, false, false);
        LOGGER.info("{}", businessObjectData);
    }
}

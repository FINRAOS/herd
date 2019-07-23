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

import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_DATA_VERSION_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_BASE_URL_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_PASSWORD_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_USERNAME_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.NAMESPACE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.PRIMARY_PARTITION_VALUE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.SUB_PARTITION_VALUES_PROPERTY;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.HerdStringUtils;
import org.finra.herd.sdk.api.ApplicationApi;
import org.finra.herd.sdk.api.BusinessObjectDataApi;
import org.finra.herd.sdk.api.CurrentUserApi;
import org.finra.herd.sdk.invoker.ApiClient;
import org.finra.herd.sdk.invoker.ApiException;
import org.finra.herd.sdk.model.BusinessObjectData;

/**
 * The controller for the application.
 */
@Component
class AccessValidatorController
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AccessValidatorController.class);

    @Autowired
    private HerdApiClientOperations herdApiClientOperations;

    @Autowired
    private PropertiesHelper propertiesHelper;

    /**
     * Runs the application with the given command line arguments.
     *
     * @param propertiesFile the properties file
     *
     * @throws IOException if an I/O error was encountered
     * @throws ApiException if a Herd API client error was encountered
     */
    void validateAccess(File propertiesFile) throws IOException, ApiException
    {
        // Load properties.
        propertiesHelper.loadProperties(propertiesFile);

        // Create the API client to a specific REST endpoint with proper authentication.
        ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY));
        apiClient.setUsername(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY));
        apiClient.setPassword(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY));

        // Setup specific API classes.
        ApplicationApi applicationApi = new ApplicationApi(apiClient);
        CurrentUserApi currentUserApi = new CurrentUserApi(apiClient);
        BusinessObjectDataApi businessObjectDataApi = new BusinessObjectDataApi(apiClient);

        // Retrieve build information from the registration server.
        LOGGER.info("Retrieving build information from the registration server...");
        LOGGER.info("{}", herdApiClientOperations.applicationGetBuildInfo(applicationApi));

        // Retrieve user information from the registration server.
        LOGGER.info("Retrieving user information from the registration server...");
        LOGGER.info("{}", herdApiClientOperations.currentUserGetCurrentUser(currentUserApi));

        // Retrieve business object data from the registration server.
        LOGGER.info("Retrieving business object data information from the registration server...");
        Integer businessObjectFormatVersion =
            HerdStringUtils.convertStringToInteger(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY), null);
        Integer businessObjectDataVersion = HerdStringUtils.convertStringToInteger(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY), null);
        BusinessObjectData businessObjectData = herdApiClientOperations
            .businessObjectDataGetBusinessObjectData(businessObjectDataApi, propertiesHelper.getProperty(NAMESPACE_PROPERTY),
                propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY), propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY),
                propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY), null, propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY),
                propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY), businessObjectFormatVersion, businessObjectDataVersion, null, false, false);
        LOGGER.info("{}", businessObjectData);
    }
}

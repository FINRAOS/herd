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

import static org.finra.herd.tools.access.validator.PropertiesHelper.AWS_REGION_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.AWS_ROLE_ARN_PROPERTY;
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
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.UUID;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdStringUtils;
import org.finra.herd.dao.S3Operations;
import org.finra.herd.sdk.api.ApplicationApi;
import org.finra.herd.sdk.api.BusinessObjectDataApi;
import org.finra.herd.sdk.api.CurrentUserApi;
import org.finra.herd.sdk.invoker.ApiClient;
import org.finra.herd.sdk.invoker.ApiException;
import org.finra.herd.sdk.model.Attribute;
import org.finra.herd.sdk.model.BusinessObjectData;
import org.finra.herd.sdk.model.StorageFile;

/**
 * The controller for the application.
 */
@Component
class AccessValidatorController
{
    static final String S3_BUCKET_NAME_ATTRIBUTE = "bucket.name";

    private static final Logger LOGGER = LoggerFactory.getLogger(AccessValidatorController.class);

    @Autowired
    private HerdApiClientOperations herdApiClientOperations;

    @Autowired
    private PropertiesHelper propertiesHelper;

    @Autowired
    private S3Operations s3Operations;

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

        // Create AWS client configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Get AWS region.
        String awsRegion = propertiesHelper.getProperty(AWS_REGION_PROPERTY);

        // Get ARN for the AWS role to assume.
        String awsRoleArn = propertiesHelper.getProperty(AWS_ROLE_ARN_PROPERTY);
        LOGGER.info("Assuming \"{}\" AWS role...", awsRoleArn);
        AWSCredentialsProvider awsCredentialsProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(awsRoleArn, UUID.randomUUID().toString())
            .withStsClient(AWSSecurityTokenServiceClientBuilder.standard().withClientConfiguration(clientConfiguration).withRegion(awsRegion).build()).build();

        // Create AWS S3 client using the assumed role.
        LOGGER.info("Creating AWS S3 client...", awsRoleArn);
        AmazonS3 amazonS3 =
            AmazonS3ClientBuilder.standard().withCredentials(awsCredentialsProvider).withClientConfiguration(clientConfiguration).withRegion(awsRegion).build();

        // Check if retrieved business object data has storage unit registered with it.
        Assert.isTrue(CollectionUtils.isNotEmpty(businessObjectData.getStorageUnits()), "Business object data has no storage unit registered with it.");
        Assert.isTrue(CollectionUtils.isNotEmpty(businessObjectData.getStorageUnits().get(0).getStorageFiles()),
            "No storage files registered with the business object data storage unit.");
        Assert.isTrue(businessObjectData.getStorageUnits().get(0).getStorage() != null, "Business object data storage unit does not have storage information.");

        // Get S3 bucket name.
        String bucketName = null;
        for (Attribute attribute : businessObjectData.getStorageUnits().get(0).getStorage().getAttributes())
        {
            if (StringUtils.equals(attribute.getName(), S3_BUCKET_NAME_ATTRIBUTE))
            {
                bucketName = attribute.getValue();
                break;
            }
        }
        Assert.isTrue(StringUtils.isNotBlank(bucketName), "S3 bucket name is not configured for the storage.");

        // Download S3 files registered with the business object data.
        LOGGER.info("Downloading S3 files registered with the business object data...");
        for (StorageFile storageFile : businessObjectData.getStorageUnits().get(0).getStorageFiles())
        {
            LOGGER.info("Downloading \"{}/{}\" S3 file...", bucketName, storageFile.getFilePath());
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, storageFile.getFilePath());
            S3Object s3Object = s3Operations.getS3Object(getObjectRequest, amazonS3);
            StringWriter stringWriter = new StringWriter();
            IOUtils.copy(s3Object.getObjectContent(), stringWriter, Charset.defaultCharset());
            LOGGER.info("Downloaded S3 file content:{}{}", System.lineSeparator(), stringWriter.toString());
        }
    }
}

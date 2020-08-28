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

import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_NAME;
import static org.finra.herd.dao.AbstractDaoTest.AWS_REGION_NAME_US_EAST_1;
import static org.finra.herd.dao.AbstractDaoTest.AWS_ROLE_ARN;
import static org.finra.herd.dao.AbstractDaoTest.S3_KEY;
import static org.finra.herd.dao.AbstractDaoTest.STORAGE_DIRECTORY_PATH;
import static org.finra.herd.tools.access.validator.AccessValidatorController.S3_BUCKET_NAME_ATTRIBUTE;
import static org.finra.herd.tools.access.validator.PropertiesHelper.AWS_REGION_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.AWS_ROLE_ARN_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.AWS_SQS_QUEUE_URL_PROPERTY;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Collections;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.S3Operations;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.sdk.api.ApplicationApi;
import org.finra.herd.sdk.api.BusinessObjectDataApi;
import org.finra.herd.sdk.api.CurrentUserApi;
import org.finra.herd.sdk.model.Attribute;
import org.finra.herd.sdk.model.BusinessObjectData;
import org.finra.herd.sdk.model.Storage;
import org.finra.herd.sdk.model.StorageDirectory;
import org.finra.herd.sdk.model.StorageFile;
import org.finra.herd.sdk.model.StorageUnit;

public class AccessValidatorControllerTest extends AbstractAccessValidatorTest
{
    private static final int MAX_BYTE_DOWNLOAD = 200;

    @InjectMocks
    private AccessValidatorController accessValidatorController;

    @Mock
    private HerdApiClientOperations herdApiClientOperations;

    @Mock
    private ObjectListing objectListing;

    @Mock
    private PropertiesHelper propertiesHelper;

    @Mock
    private S3Operations s3Operations;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testValidateAccess() throws Exception
    {
        // Create business object data.
        BusinessObjectData businessObjectData = createBusinessObjectData();

        // Create AWS get object request.
        GetObjectRequest getObjectRequest = new GetObjectRequest(S3_BUCKET_NAME, S3_KEY).withRange(0, MAX_BYTE_DOWNLOAD);

        // Create S3 object.
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(new ByteArrayInputStream(RandomStringUtils.randomAlphabetic(MAX_BYTE_DOWNLOAD).getBytes()));

        // Create S3 object metadata.
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(MAX_BYTE_DOWNLOAD);

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(AWS_REGION_PROPERTY)).thenReturn(AWS_REGION_NAME_US_EAST_1);
        when(propertiesHelper.getProperty(AWS_ROLE_ARN_PROPERTY)).thenReturn(AWS_ROLE_ARN);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_VERSION.toString());
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_DATA_VERSION.toString());
        when(propertiesHelper.getProperty(NAMESPACE_PROPERTY)).thenReturn(NAMESPACE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY)).thenReturn(BUSINESS_OBJECT_DEFINITION_NAME);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_USAGE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        when(propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY)).thenReturn(PRIMARY_PARTITION_VALUE);
        when(propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY)).thenReturn(SUB_PARTITION_VALUES);
        when(herdApiClientOperations
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false))).thenReturn(businessObjectData);
        when(s3Operations.getObjectMetadata(any(), any(), any())).thenReturn(objectMetadata);
        when(s3Operations.getS3Object(eq(getObjectRequest), any(AmazonS3.class))).thenReturn(s3Object);

        // Call the method under test with message flag set to "false".
        accessValidatorController.validateAccess(new File(PROPERTIES_FILE_PATH), false);

        // Verify the external calls.
        verify(herdApiClientOperations).checkPropertiesFile(propertiesHelper, false);
        verify(propertiesHelper).loadProperties(new File(PROPERTIES_FILE_PATH));
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
        verify(propertiesHelper).getProperty(AWS_REGION_PROPERTY);
        verify(propertiesHelper).getProperty(AWS_ROLE_ARN_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(NAMESPACE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY);
        verify(propertiesHelper).getProperty(PRIMARY_PARTITION_VALUE_PROPERTY);
        verify(propertiesHelper).getProperty(SUB_PARTITION_VALUES_PROPERTY);
        verify(herdApiClientOperations)
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false));
        verify(s3Operations).getObjectMetadata(any(), any(), any());
        verify(s3Operations).getS3Object(eq(getObjectRequest), any(AmazonS3.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAccessMessageOptionEnabled() throws Exception
    {
        // Create business object data.
        BusinessObjectData businessObjectData = createBusinessObjectData();

        // Get business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE,
                BUSINESS_OBJECT_FORMAT_VERSION, PRIMARY_PARTITION_VALUE, Lists.newArrayList(SUB_PARTITION_VALUES), BUSINESS_OBJECT_DATA_VERSION);

        // Create AWS get object request.
        GetObjectRequest getObjectRequest = new GetObjectRequest(S3_BUCKET_NAME, S3_KEY).withRange(0, MAX_BYTE_DOWNLOAD);

        // Create S3 object.
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(new ByteArrayInputStream(RandomStringUtils.randomAlphabetic(MAX_BYTE_DOWNLOAD).getBytes()));

        // Create S3 object metadata.
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(MAX_BYTE_DOWNLOAD);

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(AWS_REGION_PROPERTY)).thenReturn(AWS_REGION_NAME_US_EAST_1);
        when(propertiesHelper.getProperty(AWS_ROLE_ARN_PROPERTY)).thenReturn(AWS_ROLE_ARN);
        when(propertiesHelper.getProperty(AWS_SQS_QUEUE_URL_PROPERTY)).thenReturn(AWS_SQS_QUEUE_URL);
        when(herdApiClientOperations.getBdataKeySqs(any(AmazonSQS.class), eq(AWS_SQS_QUEUE_URL))).thenReturn(businessObjectDataKey);
        when(herdApiClientOperations
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false))).thenReturn(businessObjectData);
        when(s3Operations.getObjectMetadata(any(), any(), any())).thenReturn(objectMetadata);
        when(s3Operations.getS3Object(eq(getObjectRequest), any(AmazonS3.class))).thenReturn(s3Object);

        // Call the method under test with message flag set to "true".
        accessValidatorController.validateAccess(new File(PROPERTIES_FILE_PATH), true);

        // Verify the external calls.
        verify(herdApiClientOperations).checkPropertiesFile(propertiesHelper, true);
        verify(propertiesHelper).loadProperties(new File(PROPERTIES_FILE_PATH));
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
        verify(propertiesHelper).getProperty(AWS_REGION_PROPERTY);
        verify(propertiesHelper).getProperty(AWS_ROLE_ARN_PROPERTY);
        verify(propertiesHelper).getProperty(AWS_SQS_QUEUE_URL_PROPERTY);
        verify(herdApiClientOperations).getBdataKeySqs(any(AmazonSQS.class), eq(AWS_SQS_QUEUE_URL));
        verify(herdApiClientOperations)
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false));
        verify(s3Operations).getObjectMetadata(any(), any(), any());
        verify(s3Operations).getS3Object(eq(getObjectRequest), any(AmazonS3.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAccessMissingOptionalProperties() throws Exception
    {
        // Create business object data.
        BusinessObjectData businessObjectData = createBusinessObjectData();

        // Create AWS get object request.
        GetObjectRequest getObjectRequest = new GetObjectRequest(S3_BUCKET_NAME, S3_KEY).withRange(0, MAX_BYTE_DOWNLOAD);

        // Create S3 object.
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(new ByteArrayInputStream(RandomStringUtils.randomAlphabetic(MAX_BYTE_DOWNLOAD).getBytes()));

        // Create S3 object metadata.
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(MAX_BYTE_DOWNLOAD);

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(AWS_REGION_PROPERTY)).thenReturn(AWS_REGION_NAME_US_EAST_1);
        when(propertiesHelper.getProperty(AWS_ROLE_ARN_PROPERTY)).thenReturn(AWS_ROLE_ARN);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY)).thenReturn(null);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY)).thenReturn(null);
        when(propertiesHelper.getProperty(NAMESPACE_PROPERTY)).thenReturn(NAMESPACE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY)).thenReturn(BUSINESS_OBJECT_DEFINITION_NAME);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_USAGE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        when(propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY)).thenReturn(PRIMARY_PARTITION_VALUE);
        when(propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY)).thenReturn(null);
        when(herdApiClientOperations
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(null), eq(null), eq(null),
                eq(null), eq(false), eq(false), eq(false))).thenReturn(businessObjectData);
        when(s3Operations.getObjectMetadata(any(), any(), any())).thenReturn(objectMetadata);
        when(s3Operations.getS3Object(eq(getObjectRequest), any(AmazonS3.class))).thenReturn(s3Object);

        // Call the method under test with message flag set to "false".
        accessValidatorController.validateAccess(new File(PROPERTIES_FILE_PATH), false);

        // Verify the external calls.
        verify(herdApiClientOperations).checkPropertiesFile(propertiesHelper, false);
        verify(propertiesHelper).loadProperties(new File(PROPERTIES_FILE_PATH));
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
        verify(propertiesHelper).getProperty(AWS_REGION_PROPERTY);
        verify(propertiesHelper).getProperty(AWS_ROLE_ARN_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(NAMESPACE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY);
        verify(propertiesHelper).getProperty(PRIMARY_PARTITION_VALUE_PROPERTY);
        verify(propertiesHelper).getProperty(SUB_PARTITION_VALUES_PROPERTY);
        verify(herdApiClientOperations)
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(null), eq(null), eq(null),
                eq(null), eq(false), eq(false), eq(false));
        verify(s3Operations).getObjectMetadata(any(), any(), any());
        verify(s3Operations).getS3Object(eq(getObjectRequest), any(AmazonS3.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAccessNoStorage() throws Exception
    {
        // Create business object data without storage information.
        BusinessObjectData businessObjectData = createBusinessObjectData();
        businessObjectData.getStorageUnits().get(0).setStorage(null);

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(AWS_REGION_PROPERTY)).thenReturn(AWS_REGION_NAME_US_EAST_1);
        when(propertiesHelper.getProperty(AWS_ROLE_ARN_PROPERTY)).thenReturn(AWS_ROLE_ARN);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_VERSION.toString());
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_DATA_VERSION.toString());
        when(propertiesHelper.getProperty(NAMESPACE_PROPERTY)).thenReturn(NAMESPACE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY)).thenReturn(BUSINESS_OBJECT_DEFINITION_NAME);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_USAGE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        when(propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY)).thenReturn(PRIMARY_PARTITION_VALUE);
        when(propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY)).thenReturn(SUB_PARTITION_VALUES);
        when(herdApiClientOperations
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false))).thenReturn(businessObjectData);

        // Try to call the method under test.
        try
        {
            accessValidatorController.validateAccess(new File(PROPERTIES_FILE_PATH), false);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object data storage unit does not have storage information.", e.getMessage());
        }

        // Verify the external calls.
        verify(herdApiClientOperations).checkPropertiesFile(propertiesHelper, false);
        verify(propertiesHelper).loadProperties(new File(PROPERTIES_FILE_PATH));
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
        verify(propertiesHelper).getProperty(AWS_REGION_PROPERTY);
        verify(propertiesHelper).getProperty(AWS_ROLE_ARN_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(NAMESPACE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY);
        verify(propertiesHelper).getProperty(PRIMARY_PARTITION_VALUE_PROPERTY);
        verify(propertiesHelper).getProperty(SUB_PARTITION_VALUES_PROPERTY);
        verify(herdApiClientOperations)
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAccessNoStorageFiles() throws Exception
    {
        // Create business object data without any registered storage files, but with storage unit directory path and an actual non zero-byte S3 file.
        BusinessObjectData businessObjectData = createBusinessObjectData();
        businessObjectData.getStorageUnits().get(0).setStorageFiles(null);
        StorageDirectory storageDirectory = new StorageDirectory();
        storageDirectory.setDirectoryPath(STORAGE_DIRECTORY_PATH);
        businessObjectData.getStorageUnits().get(0).setStorageDirectory(storageDirectory);

        // Create AWS list objects response with a non zero-byte S3 file.
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(S3_KEY);
        s3ObjectSummary.setSize(FILE_SIZE_1_KB);
        objectListing = Mockito.mock(ObjectListing.class);
        when(objectListing.getObjectSummaries()).thenReturn(Lists.newArrayList(s3ObjectSummary));

        // Create AWS get object request.
        GetObjectRequest getObjectRequest = new GetObjectRequest(S3_BUCKET_NAME, S3_KEY).withRange(0, MAX_BYTE_DOWNLOAD);

        // Create S3 object.
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(new ByteArrayInputStream(RandomStringUtils.randomAlphabetic(MAX_BYTE_DOWNLOAD).getBytes()));

        // Create S3 object metadata.
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(MAX_BYTE_DOWNLOAD);

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(AWS_REGION_PROPERTY)).thenReturn(AWS_REGION_NAME_US_EAST_1);
        when(propertiesHelper.getProperty(AWS_ROLE_ARN_PROPERTY)).thenReturn(AWS_ROLE_ARN);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_VERSION.toString());
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_DATA_VERSION.toString());
        when(propertiesHelper.getProperty(NAMESPACE_PROPERTY)).thenReturn(NAMESPACE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY)).thenReturn(BUSINESS_OBJECT_DEFINITION_NAME);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_USAGE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        when(propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY)).thenReturn(PRIMARY_PARTITION_VALUE);
        when(propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY)).thenReturn(SUB_PARTITION_VALUES);
        when(herdApiClientOperations
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false))).thenReturn(businessObjectData);
        when(s3Operations.listObjects(any(ListObjectsRequest.class), any(AmazonS3.class))).thenReturn(objectListing);
        when(s3Operations.getS3Object(eq(getObjectRequest), any(AmazonS3.class))).thenReturn(s3Object);

        // Call the method under test with message flag set to "false".
        accessValidatorController.validateAccess(new File(PROPERTIES_FILE_PATH), false);

        // Verify the external calls.
        verify(herdApiClientOperations).checkPropertiesFile(propertiesHelper, false);
        verify(propertiesHelper).loadProperties(new File(PROPERTIES_FILE_PATH));
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
        verify(propertiesHelper).getProperty(AWS_REGION_PROPERTY);
        verify(propertiesHelper).getProperty(AWS_ROLE_ARN_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(NAMESPACE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY);
        verify(propertiesHelper).getProperty(PRIMARY_PARTITION_VALUE_PROPERTY);
        verify(propertiesHelper).getProperty(SUB_PARTITION_VALUES_PROPERTY);
        verify(herdApiClientOperations)
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false));
        verify(s3Operations).listObjects(any(ListObjectsRequest.class), any(AmazonS3.class));
        verify(s3Operations).getS3Object(eq(getObjectRequest), any(AmazonS3.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAccessNoStorageFilesAndDirectoryPathIsBlank() throws Exception
    {
        // Create business object data with storage unit without storage files and with blank directory path.
        BusinessObjectData businessObjectData = createBusinessObjectData();
        businessObjectData.getStorageUnits().get(0).setStorageFiles(null);
        StorageDirectory storageDirectory = new StorageDirectory();
        storageDirectory.setDirectoryPath(BLANK_TEXT);
        businessObjectData.getStorageUnits().get(0).setStorageDirectory(storageDirectory);

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(AWS_REGION_PROPERTY)).thenReturn(AWS_REGION_NAME_US_EAST_1);
        when(propertiesHelper.getProperty(AWS_ROLE_ARN_PROPERTY)).thenReturn(AWS_ROLE_ARN);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_VERSION.toString());
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_DATA_VERSION.toString());
        when(propertiesHelper.getProperty(NAMESPACE_PROPERTY)).thenReturn(NAMESPACE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY)).thenReturn(BUSINESS_OBJECT_DEFINITION_NAME);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_USAGE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        when(propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY)).thenReturn(PRIMARY_PARTITION_VALUE);
        when(propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY)).thenReturn(SUB_PARTITION_VALUES);
        when(herdApiClientOperations
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false))).thenReturn(businessObjectData);

        // Try to call the method under test.
        try
        {
            accessValidatorController.validateAccess(new File(PROPERTIES_FILE_PATH), false);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("No storage files or directory path is registered with the business object data storage unit.", e.getMessage());
        }

        // Verify the external calls.
        verify(herdApiClientOperations).checkPropertiesFile(propertiesHelper, false);
        verify(propertiesHelper).loadProperties(new File(PROPERTIES_FILE_PATH));
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
        verify(propertiesHelper).getProperty(AWS_REGION_PROPERTY);
        verify(propertiesHelper).getProperty(AWS_ROLE_ARN_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(NAMESPACE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY);
        verify(propertiesHelper).getProperty(PRIMARY_PARTITION_VALUE_PROPERTY);
        verify(propertiesHelper).getProperty(SUB_PARTITION_VALUES_PROPERTY);
        verify(herdApiClientOperations)
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAccessNoStorageFilesAndNoStorageDirectory() throws Exception
    {
        // Create business object data with storage unit without storage files and with storage directory set to null.
        BusinessObjectData businessObjectData = createBusinessObjectData();
        businessObjectData.getStorageUnits().get(0).setStorageFiles(null);
        businessObjectData.getStorageUnits().get(0).setStorageDirectory(null);

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(AWS_REGION_PROPERTY)).thenReturn(AWS_REGION_NAME_US_EAST_1);
        when(propertiesHelper.getProperty(AWS_ROLE_ARN_PROPERTY)).thenReturn(AWS_ROLE_ARN);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_VERSION.toString());
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_DATA_VERSION.toString());
        when(propertiesHelper.getProperty(NAMESPACE_PROPERTY)).thenReturn(NAMESPACE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY)).thenReturn(BUSINESS_OBJECT_DEFINITION_NAME);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_USAGE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        when(propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY)).thenReturn(PRIMARY_PARTITION_VALUE);
        when(propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY)).thenReturn(SUB_PARTITION_VALUES);
        when(herdApiClientOperations
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false))).thenReturn(businessObjectData);

        // Try to call the method under test.
        try
        {
            accessValidatorController.validateAccess(new File(PROPERTIES_FILE_PATH), false);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("No storage files or directory path is registered with the business object data storage unit.", e.getMessage());
        }

        // Verify the external calls.
        verify(herdApiClientOperations).checkPropertiesFile(propertiesHelper, false);
        verify(propertiesHelper).loadProperties(new File(PROPERTIES_FILE_PATH));
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
        verify(propertiesHelper).getProperty(AWS_REGION_PROPERTY);
        verify(propertiesHelper).getProperty(AWS_ROLE_ARN_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(NAMESPACE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY);
        verify(propertiesHelper).getProperty(PRIMARY_PARTITION_VALUE_PROPERTY);
        verify(propertiesHelper).getProperty(SUB_PARTITION_VALUES_PROPERTY);
        verify(herdApiClientOperations)
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAccessNoStorageFilesZeroByteS3File() throws Exception
    {
        // Create business object data without any registered storage files, but with storage unit directory path and a zero-byte S3 file.
        BusinessObjectData businessObjectData = createBusinessObjectData();
        businessObjectData.getStorageUnits().get(0).setStorageFiles(null);
        StorageDirectory storageDirectory = new StorageDirectory();
        storageDirectory.setDirectoryPath(STORAGE_DIRECTORY_PATH);
        businessObjectData.getStorageUnits().get(0).setStorageDirectory(storageDirectory);

        // Create AWS list objects response with a zero-byte S3 file.
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(S3_KEY);
        s3ObjectSummary.setSize(FILE_SIZE_0_BYTE);
        objectListing = Mockito.mock(ObjectListing.class);
        when(objectListing.getObjectSummaries()).thenReturn(Lists.newArrayList(s3ObjectSummary));

        // Create AWS get object request.
        GetObjectRequest getObjectRequest = new GetObjectRequest(S3_BUCKET_NAME, S3_KEY).withRange(0, MAX_BYTE_DOWNLOAD);

        // Create S3 object.
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(new ByteArrayInputStream(RandomStringUtils.randomAlphabetic(MAX_BYTE_DOWNLOAD).getBytes()));

        // Create S3 object metadata.
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(MAX_BYTE_DOWNLOAD);

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(AWS_REGION_PROPERTY)).thenReturn(AWS_REGION_NAME_US_EAST_1);
        when(propertiesHelper.getProperty(AWS_ROLE_ARN_PROPERTY)).thenReturn(AWS_ROLE_ARN);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_VERSION.toString());
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_DATA_VERSION.toString());
        when(propertiesHelper.getProperty(NAMESPACE_PROPERTY)).thenReturn(NAMESPACE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY)).thenReturn(BUSINESS_OBJECT_DEFINITION_NAME);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_USAGE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        when(propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY)).thenReturn(PRIMARY_PARTITION_VALUE);
        when(propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY)).thenReturn(SUB_PARTITION_VALUES);
        when(herdApiClientOperations
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false))).thenReturn(businessObjectData);
        when(s3Operations.listObjects(any(ListObjectsRequest.class), any(AmazonS3.class))).thenReturn(objectListing);

        // Call the method under test with message flag set to "false".
        accessValidatorController.validateAccess(new File(PROPERTIES_FILE_PATH), false);

        // Verify the external calls.
        verify(herdApiClientOperations).checkPropertiesFile(propertiesHelper, false);
        verify(propertiesHelper).loadProperties(new File(PROPERTIES_FILE_PATH));
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
        verify(propertiesHelper).getProperty(AWS_REGION_PROPERTY);
        verify(propertiesHelper).getProperty(AWS_ROLE_ARN_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(NAMESPACE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY);
        verify(propertiesHelper).getProperty(PRIMARY_PARTITION_VALUE_PROPERTY);
        verify(propertiesHelper).getProperty(SUB_PARTITION_VALUES_PROPERTY);
        verify(herdApiClientOperations)
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false));
        verify(s3Operations).listObjects(any(ListObjectsRequest.class), any(AmazonS3.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAccessNoStorageUnit() throws Exception
    {
        // Create business object data without any storage units.
        BusinessObjectData businessObjectData = createBusinessObjectData();
        businessObjectData.setStorageUnits(null);

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(AWS_REGION_PROPERTY)).thenReturn(AWS_REGION_NAME_US_EAST_1);
        when(propertiesHelper.getProperty(AWS_ROLE_ARN_PROPERTY)).thenReturn(AWS_ROLE_ARN);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_VERSION.toString());
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_DATA_VERSION.toString());
        when(propertiesHelper.getProperty(NAMESPACE_PROPERTY)).thenReturn(NAMESPACE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY)).thenReturn(BUSINESS_OBJECT_DEFINITION_NAME);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_USAGE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        when(propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY)).thenReturn(PRIMARY_PARTITION_VALUE);
        when(propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY)).thenReturn(SUB_PARTITION_VALUES);
        when(herdApiClientOperations
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false))).thenReturn(businessObjectData);

        // Try to call the method under test.
        try
        {
            accessValidatorController.validateAccess(new File(PROPERTIES_FILE_PATH), false);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object data has no storage unit registered with it.", e.getMessage());
        }

        // Verify the external calls.
        verify(herdApiClientOperations).checkPropertiesFile(propertiesHelper, false);
        verify(propertiesHelper).loadProperties(new File(PROPERTIES_FILE_PATH));
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
        verify(propertiesHelper).getProperty(AWS_REGION_PROPERTY);
        verify(propertiesHelper).getProperty(AWS_ROLE_ARN_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(NAMESPACE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY);
        verify(propertiesHelper).getProperty(PRIMARY_PARTITION_VALUE_PROPERTY);
        verify(propertiesHelper).getProperty(SUB_PARTITION_VALUES_PROPERTY);
        verify(herdApiClientOperations)
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAccessStorageHasNoBucketNameAttribute() throws Exception
    {
        // Create business object data with storage without bucket name attribute.
        BusinessObjectData businessObjectData = createBusinessObjectData();
        businessObjectData.getStorageUnits().get(0).getStorage().setAttributes(Lists.newArrayList(new Attribute()));

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(AWS_REGION_PROPERTY)).thenReturn(AWS_REGION_NAME_US_EAST_1);
        when(propertiesHelper.getProperty(AWS_ROLE_ARN_PROPERTY)).thenReturn(AWS_ROLE_ARN);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_VERSION.toString());
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_DATA_VERSION.toString());
        when(propertiesHelper.getProperty(NAMESPACE_PROPERTY)).thenReturn(NAMESPACE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY)).thenReturn(BUSINESS_OBJECT_DEFINITION_NAME);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_USAGE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        when(propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY)).thenReturn(PRIMARY_PARTITION_VALUE);
        when(propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY)).thenReturn(SUB_PARTITION_VALUES);
        when(herdApiClientOperations
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false))).thenReturn(businessObjectData);

        // Try to call the method under test.
        try
        {
            accessValidatorController.validateAccess(new File(PROPERTIES_FILE_PATH), false);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("S3 bucket name is not configured for the storage.", e.getMessage());
        }

        // Verify the external calls.
        verify(herdApiClientOperations).checkPropertiesFile(propertiesHelper, false);
        verify(propertiesHelper).loadProperties(new File(PROPERTIES_FILE_PATH));
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
        verify(propertiesHelper).getProperty(AWS_REGION_PROPERTY);
        verify(propertiesHelper).getProperty(AWS_ROLE_ARN_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(NAMESPACE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY);
        verify(propertiesHelper).getProperty(PRIMARY_PARTITION_VALUE_PROPERTY);
        verify(propertiesHelper).getProperty(SUB_PARTITION_VALUES_PROPERTY);
        verify(herdApiClientOperations)
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAccessZeroByteS3File() throws Exception
    {
        // Create business object data.
        BusinessObjectData businessObjectData = createBusinessObjectData();

        // Create S3 object metadata.
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(0);

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(AWS_REGION_PROPERTY)).thenReturn(AWS_REGION_NAME_US_EAST_1);
        when(propertiesHelper.getProperty(AWS_ROLE_ARN_PROPERTY)).thenReturn(AWS_ROLE_ARN);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_VERSION.toString());
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY)).thenReturn(BUSINESS_OBJECT_DATA_VERSION.toString());
        when(propertiesHelper.getProperty(NAMESPACE_PROPERTY)).thenReturn(NAMESPACE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY)).thenReturn(BUSINESS_OBJECT_DEFINITION_NAME);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_USAGE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        when(propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY)).thenReturn(PRIMARY_PARTITION_VALUE);
        when(propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY)).thenReturn(SUB_PARTITION_VALUES);
        when(herdApiClientOperations
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false))).thenReturn(businessObjectData);
        when(s3Operations.getObjectMetadata(any(), any(), any())).thenReturn(objectMetadata);

        // Call the method under test with message flag set to "false".
        accessValidatorController.validateAccess(new File(PROPERTIES_FILE_PATH), false);

        // Verify the external calls.
        verify(herdApiClientOperations).checkPropertiesFile(propertiesHelper, false);
        verify(propertiesHelper).loadProperties(new File(PROPERTIES_FILE_PATH));
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
        verify(propertiesHelper).getProperty(AWS_REGION_PROPERTY);
        verify(propertiesHelper).getProperty(AWS_ROLE_ARN_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY);
        verify(propertiesHelper).getProperty(NAMESPACE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY);
        verify(propertiesHelper).getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY);
        verify(propertiesHelper).getProperty(PRIMARY_PARTITION_VALUE_PROPERTY);
        verify(propertiesHelper).getProperty(SUB_PARTITION_VALUES_PROPERTY);
        verify(herdApiClientOperations)
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(SUB_PARTITION_VALUES),
                eq(BUSINESS_OBJECT_FORMAT_VERSION), eq(BUSINESS_OBJECT_DATA_VERSION), eq(null), eq(false), eq(false), eq(false));
        verify(s3Operations).getObjectMetadata(any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    private BusinessObjectData createBusinessObjectData()
    {
        // Create storage with attributes that includes a bucket name attribute.
        Attribute bucketNameAttribute = new Attribute();
        bucketNameAttribute.setName(S3_BUCKET_NAME_ATTRIBUTE);
        bucketNameAttribute.setValue(S3_BUCKET_NAME);
        Attribute otherAttribute = new Attribute();
        otherAttribute.setName(ATTRIBUTE_NAME);
        Storage storage = new Storage();
        storage.setAttributes(Lists.newArrayList(otherAttribute, bucketNameAttribute));

        // Create storage file.
        StorageFile storageFile = new StorageFile();
        storageFile.setFilePath(S3_KEY);

        // Create storage unit with storage and storage file.
        StorageUnit storageUnit = new StorageUnit();
        storageUnit.setStorageFiles(Collections.singletonList(storageFile));
        storageUnit.setStorage(storage);

        // Create and return business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setStorageUnits(Collections.singletonList(storageUnit));

        return businessObjectData;
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(herdApiClientOperations, propertiesHelper, s3Operations);
    }
}

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.S3Operations;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.sdk.api.ApplicationApi;
import org.finra.herd.sdk.api.BusinessObjectDataApi;
import org.finra.herd.sdk.api.CurrentUserApi;
import org.finra.herd.sdk.model.Attribute;
import org.finra.herd.sdk.model.BusinessObjectData;
import org.finra.herd.sdk.model.Storage;
import org.finra.herd.sdk.model.StorageFile;
import org.finra.herd.sdk.model.StorageUnit;

public class AccessValidatorControllerTest extends AbstractAccessValidatorTest
{
    // Create a test properties file path.
    private final File propertiesFile = new File(PROPERTIES_FILE_PATH);

    private BusinessObjectData businessObjectData = new BusinessObjectData();
    private StorageUnit storageUnit = new StorageUnit();
    private StorageFile storageFile = new StorageFile();
    private Storage storage = new Storage();
    private Attribute attribute = new Attribute();
    private Attribute bucketNameAttribute = new Attribute();

    // Create an AWS get object request.
    private GetObjectRequest getObjectRequest = new GetObjectRequest(S3_BUCKET_NAME, S3_KEY);

    private S3Object s3Object = new S3Object();

    @InjectMocks
    private AccessValidatorController accessValidatorController;

    @Mock
    private HerdApiClientOperations herdApiClientOperations;

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
    public void testValidateAccessPropertiesFile() throws Exception
    {
        testValidateAccessHelper(BUSINESS_OBJECT_FORMAT_VERSION, BUSINESS_OBJECT_DATA_VERSION, SUB_PARTITION_VALUES, false);
    }

    @Test
    public void testValidateAccessMissingOptionalProperties() throws Exception
    {
        testValidateAccessHelper(null, null, null, false);
    }

    @Test
    public void testValidateAccessSqsMessage() throws Exception
    {
        testValidateAccessHelper(BUSINESS_OBJECT_FORMAT_VERSION, BUSINESS_OBJECT_DATA_VERSION, SUB_PARTITION_VALUES, true);
    }

    @Test
    public void testValidateAccessSqsMessageMissingOptionalProperties() throws Exception
    {
        testValidateAccessHelper(null, null, null, true);
    }

    @Test
    public void testValidateAccessMultipleSubPartition() throws Exception
    {
        String subpartition = "One|Two|Three";
        testValidateAccessHelper(BUSINESS_OBJECT_FORMAT_VERSION, BUSINESS_OBJECT_DATA_VERSION, subpartition, false);
    }

    @Test
    public void testMapJsontoBdataKey() throws Exception
    {
        String fileName = "sqsMessage.txt";
        String expectedNamespace = "DMFormatNamespace1";

        Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());

        Stream<String> lines = Files.lines(path);
        String messageBody = lines.collect(Collectors.joining("\n")).trim();
        lines.close();

        HerdApiClientOperations apiClient = new HerdApiClientOperations();

        BusinessObjectDataKey bdataKey = apiClient.mapJsontoBdataKey(messageBody).getBusinessObjectDataKey();

        Assert.assertEquals("Did not get the correct namespace", expectedNamespace, bdataKey.getNamespace());
    }

    private void testValidateAccessHelper(Integer businessObjectFormatVersion, Integer businessObjectDataVersion, String subPartitionValues,
        Boolean messageFlag) throws Exception
    {
        setupBusinessDataObject();

        // Mock the external calls.
        when(propertiesHelper.getProperty(HERD_BASE_URL_PROPERTY)).thenReturn(HERD_BASE_URL);
        when(propertiesHelper.getProperty(HERD_USERNAME_PROPERTY)).thenReturn(HERD_USERNAME);
        when(propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY)).thenReturn(HERD_PASSWORD);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_VERSION_PROPERTY))
            .thenReturn(businessObjectFormatVersion != null ? businessObjectFormatVersion.toString() : null);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DATA_VERSION_PROPERTY))
            .thenReturn(businessObjectDataVersion != null ? businessObjectDataVersion.toString() : null);
        when(propertiesHelper.getProperty(NAMESPACE_PROPERTY)).thenReturn(NAMESPACE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY)).thenReturn(BUSINESS_OBJECT_DEFINITION_NAME);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_USAGE);
        when(propertiesHelper.getProperty(BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY)).thenReturn(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        when(propertiesHelper.getProperty(PRIMARY_PARTITION_VALUE_PROPERTY)).thenReturn(PRIMARY_PARTITION_VALUE);
        when(propertiesHelper.getProperty(SUB_PARTITION_VALUES_PROPERTY)).thenReturn(subPartitionValues);
        when(herdApiClientOperations
            .businessObjectDataGetBusinessObjectData(any(BusinessObjectDataApi.class), eq(NAMESPACE), eq(BUSINESS_OBJECT_DEFINITION_NAME),
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(subPartitionValues),
                eq(businessObjectFormatVersion), eq(businessObjectDataVersion), eq(null), eq(false), eq(false))).thenReturn(businessObjectData);
        when(propertiesHelper.getProperty(AWS_REGION_PROPERTY)).thenReturn(AWS_REGION_NAME_US_EAST_1);
        when(propertiesHelper.getProperty(AWS_ROLE_ARN_PROPERTY)).thenReturn(AWS_ROLE_ARN);
        when(s3Operations.getS3Object(eq(getObjectRequest), any(AmazonS3.class))).thenReturn(s3Object);

        if (messageFlag)
        {
            setupSqsTest();
        }

        // Call the method under test.
        accessValidatorController.validateAccess(propertiesFile, messageFlag);

        // Verify the external calls.
        verify(propertiesHelper).loadProperties(propertiesFile);
        verify(propertiesHelper).getProperty(HERD_BASE_URL_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_USERNAME_PROPERTY);
        verify(propertiesHelper).getProperty(HERD_PASSWORD_PROPERTY);
        verify(herdApiClientOperations).applicationGetBuildInfo(any(ApplicationApi.class));
        verify(herdApiClientOperations).currentUserGetCurrentUser(any(CurrentUserApi.class));
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
                eq(BUSINESS_OBJECT_FORMAT_USAGE), eq(BUSINESS_OBJECT_FORMAT_FILE_TYPE), eq(null), eq(PRIMARY_PARTITION_VALUE), eq(subPartitionValues),
                eq(businessObjectFormatVersion), eq(businessObjectDataVersion), eq(null), eq(false), eq(false));
        verify(propertiesHelper).getProperty(AWS_REGION_PROPERTY);
        verify(propertiesHelper).getProperty(AWS_ROLE_ARN_PROPERTY);
        verify(s3Operations).getS3Object(eq(getObjectRequest), any(AmazonS3.class));

        if (messageFlag)
        {
            verifySqsTest();
        }

        verifyNoMoreInteractionsHelper();
    }

    private void setupBusinessDataObject()
    {
        // Create a business object data herd sdk model object.
        businessObjectData.setStorageUnits(Collections.singletonList(storageUnit));
        storageUnit.setStorageFiles(Collections.singletonList(storageFile));
        storageFile.setFilePath(S3_KEY);
        storageUnit.setStorage(storage);
        bucketNameAttribute.setName(S3_BUCKET_NAME_ATTRIBUTE);
        bucketNameAttribute.setValue(S3_BUCKET_NAME);
        attribute.setName(ATTRIBUTE_NAME);
        storage.setAttributes(Lists.newArrayList(attribute, bucketNameAttribute));

        // Create an S3 object with an empty content.
        s3Object.setObjectContent(new ByteArrayInputStream(new byte[] {0}));
    }

    private void setupSqsTest() throws Exception
    {
        BusinessObjectDataKey bdataKey = accessValidatorController.getBdataKeyPropertiesFile();

        when(propertiesHelper.getProperty(AWS_SQS_QUEUE_URL_PROPERTY)).thenReturn(AWS_SQS_QUEUE_URL);
        when(herdApiClientOperations.getBdataKeySqs(any(AmazonSQS.class), eq(AWS_SQS_QUEUE_URL))).thenReturn(bdataKey);
    }

    private void verifySqsTest() throws Exception
    {
        verify(propertiesHelper).getProperty(AWS_SQS_QUEUE_URL_PROPERTY);
        verify(herdApiClientOperations).getBdataKeySqs(any(AmazonSQS.class), eq(AWS_SQS_QUEUE_URL));
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(herdApiClientOperations, propertiesHelper, s3Operations);
    }
}

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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.collections4.IterableUtils;
import org.fusesource.hawtbuf.ByteArrayInputStream;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.dao.impl.MockStsOperationsImpl;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSampleDataFileKey;
import org.finra.herd.model.api.xml.DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest;
import org.finra.herd.model.api.xml.DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationResponse;
import org.finra.herd.model.api.xml.DownloadSingleInitiationResponse;
import org.finra.herd.model.api.xml.SampleDataFile;
import org.finra.herd.model.api.xml.UploadBusinessObjectDefinitionSampleDataFileInitiationRequest;
import org.finra.herd.model.api.xml.UploadBusinessObjectDefinitionSampleDataFileInitiationResponse;
import org.finra.herd.model.api.xml.UploadSingleCredentialExtensionResponse;
import org.finra.herd.model.api.xml.UploadSingleInitiationRequest;
import org.finra.herd.model.api.xml.UploadSingleInitiationResponse;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.impl.UploadDownloadHelperServiceImpl;
import org.finra.herd.service.impl.UploadDownloadServiceImpl;

/**
 * This class tests various functionality within the custom DDL REST controller.
 */
public class UploadDownloadServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "uploadDownloadServiceImpl")
    private UploadDownloadService uploadDownloadServiceImpl;

    @Test
    public void testInitiateUploadSingle()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse =
            uploadDownloadService.initiateUploadSingle(uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest());

        // Validate the returned object.
        uploadDownloadServiceTestHelper
            .validateUploadSingleInitiationResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, businessObjectDefinitionServiceTestHelper.getNewAttributes(), FILE_NAME,
                FILE_SIZE_1_KB, null, resultUploadSingleInitiationResponse);
    }

    @Test
    public void testInitiateUploadSingleMissingRequiredParameters()
    {
        UploadSingleInitiationRequest request;

        // Try to initiate a single file upload when business object format key is not specified.
        request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
        request.setSourceBusinessObjectFormatKey(null);
        try
        {
            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when business object format key is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format key must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when namespace is not specified.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setNamespace(BLANK_TEXT);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when business object definition name is not specified.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setBusinessObjectDefinitionName(BLANK_TEXT);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when business object format usage is not specified.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setBusinessObjectFormatUsage(BLANK_TEXT);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when business object format file type is not specified.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setBusinessObjectFormatFileType(BLANK_TEXT);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when business object format version is not specified.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setBusinessObjectFormatVersion(null);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when business object format key is not specified.
        request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
        request.setTargetBusinessObjectFormatKey(null);
        try
        {
            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when business object format key is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format key must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when namespace is not specified.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setNamespace(BLANK_TEXT);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when business object definition name is not specified.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setBusinessObjectDefinitionName(BLANK_TEXT);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when business object format usage is not specified.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setBusinessObjectFormatUsage(BLANK_TEXT);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when business object format file type is not specified.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setBusinessObjectFormatFileType(BLANK_TEXT);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when business object format version is not specified.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setBusinessObjectFormatVersion(null);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when attribute name is not specified.
        request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
        request.getBusinessObjectDataAttributes().get(0).setName(BLANK_TEXT);
        try
        {
            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when attribute name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An attribute name must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when file information is not specified.
        request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
        request.setFile(null);
        try
        {
            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when file information is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("File information must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when file name is not specified.
        request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
        request.getFile().setFileName(BLANK_TEXT);
        try
        {
            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when file name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A file name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testInitiateUploadSingleMissingOptionalParameters()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload without specifying any of the optional parameters.
        UploadSingleInitiationRequest request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
        request.setBusinessObjectDataAttributes(null);
        request.getFile().setFileSizeBytes(null);
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(request);

        // Validate the returned object.
        uploadDownloadServiceTestHelper
            .validateUploadSingleInitiationResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, NO_ATTRIBUTES, FILE_NAME, null, null, resultUploadSingleInitiationResponse);
    }

    @Test
    public void testInitiateUploadSingleTrimParameters()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload using input parameters with leading and trailing empty spaces.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(uploadDownloadServiceTestHelper
            .createUploadSingleInitiationRequest(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME_2), addWhitespace(FORMAT_USAGE_CODE_2),
                addWhitespace(FORMAT_FILE_TYPE_CODE_2), FORMAT_VERSION_2));

        // Validate the returned object.
        uploadDownloadServiceTestHelper
            .validateUploadSingleInitiationResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, businessObjectDefinitionServiceTestHelper.getNewAttributes(), FILE_NAME,
                FILE_SIZE_1_KB, null, resultUploadSingleInitiationResponse);
    }

    @Test
    public void testInitiateUploadSingleUpperCaseParameters()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload using lower case values.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(uploadDownloadServiceTestHelper
            .createUploadSingleInitiationRequest(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, NAMESPACE.toLowerCase(), BDEF_NAME_2.toLowerCase(), FORMAT_USAGE_CODE_2.toLowerCase(),
                FORMAT_FILE_TYPE_CODE_2.toLowerCase(), FORMAT_VERSION_2));

        // Validate the returned object.
        uploadDownloadServiceTestHelper
            .validateUploadSingleInitiationResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, businessObjectDefinitionServiceTestHelper.getNewAttributes(), FILE_NAME,
                FILE_SIZE_1_KB, null, resultUploadSingleInitiationResponse);
    }

    @Test
    public void testInitiateUploadSingleLowerCaseParameters()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload using upper case values.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(uploadDownloadServiceTestHelper
            .createUploadSingleInitiationRequest(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, NAMESPACE.toUpperCase(), BDEF_NAME_2.toUpperCase(), FORMAT_USAGE_CODE_2.toUpperCase(),
                FORMAT_FILE_TYPE_CODE_2.toUpperCase(), FORMAT_VERSION_2));

        // Validate the returned object.
        uploadDownloadServiceTestHelper
            .validateUploadSingleInitiationResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, businessObjectDefinitionServiceTestHelper.getNewAttributes(), FILE_NAME,
                FILE_SIZE_1_KB, null, resultUploadSingleInitiationResponse);
    }

    @Test
    public void testInitiateUploadSingleInvalidParameters()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        UploadSingleInitiationRequest request;

        // Try to initiate a single file upload using invalid namespace.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setNamespace("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid business object definition name.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setBusinessObjectDefinitionName("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid format usage.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setBusinessObjectFormatUsage("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid format file type.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setBusinessObjectFormatFileType("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid business object format version.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setBusinessObjectFormatVersion(INVALID_FORMAT_VERSION);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid namespace.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setNamespace("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2,
                    FORMAT_VERSION_2), e.getMessage());
        }

        // Try to initiate a single file upload using invalid business object definition name.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setBusinessObjectDefinitionName("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2,
                    FORMAT_VERSION_2), e.getMessage());
        }

        // Try to initiate a single file upload using invalid format usage.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setBusinessObjectFormatUsage("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME_2, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid format file type.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setBusinessObjectFormatFileType("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME_2, FORMAT_USAGE_CODE_2, "I_DO_NOT_EXIST", FORMAT_VERSION_2),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid business object format version.
        try
        {
            request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setBusinessObjectFormatVersion(INVALID_FORMAT_VERSION);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2,
                    INVALID_FORMAT_VERSION), e.getMessage());
        }
    }

    @Test
    public void testInitiateUploadSingleDuplicateAttributes()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        // Try to initiate a single file upload when duplicate attributes are specified.
        UploadSingleInitiationRequest request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
        request.setBusinessObjectDataAttributes(Arrays.asList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_3),
            new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_3)));
        try
        {
            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when duplicate attributes are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate attribute name found: %s", ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase()), e.getMessage());
        }
    }

    @Test
    public void testInitiateUploadSingleRequiredAttribute()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        // Create and persist a business object data attribute definition entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectDataAttributeDefinitionEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE);

        // Initiate a file upload.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse =
            uploadDownloadService.initiateUploadSingle(uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest());

        // Validate the returned object.
        uploadDownloadServiceTestHelper
            .validateUploadSingleInitiationResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, businessObjectDefinitionServiceTestHelper.getNewAttributes(), FILE_NAME,
                FILE_SIZE_1_KB, null, resultUploadSingleInitiationResponse);
    }

    @Test
    public void testInitiateUploadSingleRequiredAttributeMissingValue()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        // Create and persist a business object data attribute definition entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectDataAttributeDefinitionEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE);

        // Try to initiate a single file upload when a required attribute value is not specified.
        UploadSingleInitiationRequest request = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
        request.setBusinessObjectDataAttributes(Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT)));
        try
        {
            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when required attribute value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The business object format has a required attribute \"%s\" which was not specified or has a value which is blank.",
                ATTRIBUTE_NAME_1_MIXED_CASE), e.getMessage());
        }
    }

    /**
     * Asserts that the target business object data that is created is using the target storage name that is specified in the request.
     */
    @Test
    public void testInitiateUploadSingleAssertUseTargetStorageInRequest()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME_3);
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucketName"));
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                "$environment/$namespace/$businessObjectDataPartitionValue"));
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID),
                "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"));

        // Initiate a file upload.
        UploadSingleInitiationRequest uploadSingleInitiationRequest = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
        uploadSingleInitiationRequest.setTargetStorageName(STORAGE_NAME_3);

        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(uploadSingleInitiationRequest);

        // Validate the returned object.
        uploadDownloadServiceTestHelper
            .validateUploadSingleInitiationResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, businessObjectDefinitionServiceTestHelper.getNewAttributes(), FILE_NAME,
                FILE_SIZE_1_KB, STORAGE_NAME_3, resultUploadSingleInitiationResponse);

        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2,
                resultUploadSingleInitiationResponse.getTargetBusinessObjectData().getPartitionValue(), null, 0));

        assertNotNull(targetBusinessObjectDataEntity);
        assertNotNull(targetBusinessObjectDataEntity.getStorageUnits());
        assertEquals(1, targetBusinessObjectDataEntity.getStorageUnits().size());
        StorageUnitEntity storageUnit = IterableUtils.get(targetBusinessObjectDataEntity.getStorageUnits(), 0);
        assertNotNull(storageUnit);
        assertNotNull(storageUnit.getStorage());
        assertEquals(STORAGE_NAME_3, storageUnit.getStorage().getName());
    }

    /**
     * Asserts that error is thrown when target storage's bucket name is not set.
     */
    @Test
    public void testInitiateUploadSingleAssertTargetStorageBucketNameRequired()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME_3);
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                "$environment/$namespace/$businessObjectDataPartitionValue"));
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID),
                "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"));

        // Initiate a file upload.
        UploadSingleInitiationRequest uploadSingleInitiationRequest = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
        uploadSingleInitiationRequest.setTargetStorageName(STORAGE_NAME_3);

        try
        {
            uploadDownloadService.initiateUploadSingle(uploadSingleInitiationRequest);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Attribute \"" + configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME) + "\" for \"" + STORAGE_NAME_3 +
                "\" storage must be configured.", e.getMessage());
        }
    }

    /**
     * Asserts that error is thrown when target storage's kms kms id is not set.
     */
    @Test
    public void testInitiateUploadSingleAssertTargetStorageKmsKeyIdRequired()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME_3);
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucketName"));
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                "$environment/$namespace/$businessObjectDataPartitionValue"));

        // Initiate a file upload.
        UploadSingleInitiationRequest uploadSingleInitiationRequest = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
        uploadSingleInitiationRequest.setTargetStorageName(STORAGE_NAME_3);

        try
        {
            uploadDownloadService.initiateUploadSingle(uploadSingleInitiationRequest);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Attribute \"" + configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID) + "\" for \"" + STORAGE_NAME_3 +
                "\" storage must be configured.", e.getMessage());
        }
    }

    /**
     * Asserts that error is thrown when target storage's prefix template is not set.
     */
    @Test
    public void testInitiateUploadSingleAssertTargetStoragePrefixTemplateRequired()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME_3);
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucketName"));
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID),
                "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"));

        // Initiate a file upload.
        UploadSingleInitiationRequest uploadSingleInitiationRequest = uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest();
        uploadSingleInitiationRequest.setTargetStorageName(STORAGE_NAME_3);

        try
        {
            uploadDownloadService.initiateUploadSingle(uploadSingleInitiationRequest);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Storage \"" + STORAGE_NAME_3 + "\" has no S3 key prefix velocity template configured.", e.getMessage());
        }
    }

    @Test
    public void testPerformCompleteUploadSingleMessage()
    {
        setLogLevel(UploadDownloadServiceImpl.class, LogLevel.OFF);

        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(uploadDownloadServiceTestHelper
            .createUploadSingleInitiationRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, FILE_NAME));

        // Get the file path.
        String filePath = resultUploadSingleInitiationResponse.getTargetBusinessObjectData().getStorageUnits().get(0).getStorageFiles().get(0).getFilePath();

        // Put a 1 KB file in the S3 "loading dock" bucket.
        PutObjectRequest putObjectRequest =
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), filePath, new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]), null);
        s3Operations.putObject(putObjectRequest, null);

        try
        {
            // Complete the upload.
            UploadDownloadServiceImpl.CompleteUploadSingleMessageResult result = uploadDownloadService.performCompleteUploadSingleMessage(filePath);

            // Validate the result object.
            assertEquals(BusinessObjectDataStatusEntity.UPLOADING, result.getSourceOldBusinessObjectDataStatus());
            assertEquals(BusinessObjectDataStatusEntity.DELETED, result.getSourceNewBusinessObjectDataStatus());
            assertEquals(BusinessObjectDataStatusEntity.UPLOADING, result.getTargetOldBusinessObjectDataStatus());
            assertEquals(BusinessObjectDataStatusEntity.VALID, result.getTargetNewBusinessObjectDataStatus());

            // Try to complete the upload the second time. This might happen when a duplicate S3 notification is received for the same uploaded file.
            result = uploadDownloadService.performCompleteUploadSingleMessage(filePath);

            // Validate the result object.
            assertEquals(BusinessObjectDataStatusEntity.DELETED, result.getSourceOldBusinessObjectDataStatus());
            assertNull(result.getSourceNewBusinessObjectDataStatus());
            assertEquals(BusinessObjectDataStatusEntity.VALID, result.getTargetOldBusinessObjectDataStatus());
            assertNull(result.getTargetNewBusinessObjectDataStatus());
        }
        finally
        {
            // Clean up the S3.
            s3Dao.deleteDirectory(
                S3FileTransferRequestParamsDto.builder().s3BucketName(storageDaoTestHelper.getS3LoadingDockBucketName()).s3KeyPrefix(filePath).build());

            s3Operations.rollback();
        }
    }

    @Test
    public void testPerformCompleteUploadSingleMessageStorageFileNoExists()
    {
        setLogLevel(UploadDownloadServiceImpl.class, LogLevel.OFF);

        // Try to complete the upload, when storage file matching the S3 key does not exist in the database.
        UploadDownloadServiceImpl.CompleteUploadSingleMessageResult result = uploadDownloadService.performCompleteUploadSingleMessage("KEY_DOES_NOT_EXIST");

        assertNull(result.getSourceBusinessObjectDataKey());
        assertNull(result.getSourceNewBusinessObjectDataStatus());
        assertNull(result.getSourceOldBusinessObjectDataStatus());
        assertNull(result.getTargetBusinessObjectDataKey());
        assertNull(result.getTargetNewBusinessObjectDataStatus());
        assertNull(result.getTargetOldBusinessObjectDataStatus());
    }

    @Test
    public void testPerformCompleteUploadSingleMessageS3FileNoExists()
    {
        setLogLevel(UploadDownloadServiceImpl.class, LogLevel.OFF);

        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(uploadDownloadServiceTestHelper
            .createUploadSingleInitiationRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, FILE_NAME));

        String filePath = resultUploadSingleInitiationResponse.getTargetBusinessObjectData().getStorageUnits().get(0).getStorageFiles().get(0).getFilePath();

        // Try to complete the upload, when source S3 file does not exist.
        UploadDownloadServiceImpl.CompleteUploadSingleMessageResult result = uploadDownloadService.performCompleteUploadSingleMessage(filePath);

        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, result.getSourceOldBusinessObjectDataStatus());
        assertEquals(BusinessObjectDataStatusEntity.DELETED, result.getSourceNewBusinessObjectDataStatus());

        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, result.getTargetOldBusinessObjectDataStatus());
        assertEquals(BusinessObjectDataStatusEntity.INVALID, result.getTargetNewBusinessObjectDataStatus());
    }

    @Test
    public void testPerformCompleteUploadSingleMessageSourceBusinessObjectDataStatusNotUploading()
    {
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(uploadDownloadServiceTestHelper
            .createUploadSingleInitiationRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, FILE_NAME));

        String filePath = resultUploadSingleInitiationResponse.getTargetBusinessObjectData().getStorageUnits().get(0).getStorageFiles().get(0).getFilePath();

        // Create a business object data status.
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Update the status of the source business object data so it would not be "UPLOADING".
        businessObjectDataStatusService.updateBusinessObjectDataStatus(
            businessObjectDataHelper.getBusinessObjectDataKey(resultUploadSingleInitiationResponse.getSourceBusinessObjectData()),
            new BusinessObjectDataStatusUpdateRequest(BDATA_STATUS));

        // Try to complete the upload, when source business object data status is not "UPLOADING".
        UploadDownloadServiceImpl.CompleteUploadSingleMessageResult result = uploadDownloadService.performCompleteUploadSingleMessage(filePath);

        assertEquals(BDATA_STATUS, result.getSourceOldBusinessObjectDataStatus());
        assertNull(result.getSourceNewBusinessObjectDataStatus());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, result.getTargetOldBusinessObjectDataStatus());
        assertNull(result.getTargetNewBusinessObjectDataStatus());
    }

    @Test
    public void testPerformCompleteUploadSingleMessageTargetBusinessObjectDataStatusNotUploading()
    {
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(uploadDownloadServiceTestHelper
            .createUploadSingleInitiationRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, FILE_NAME));

        String filePath = resultUploadSingleInitiationResponse.getTargetBusinessObjectData().getStorageUnits().get(0).getStorageFiles().get(0).getFilePath();

        // Create a business object data status.
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Update the status of the target business object data so it would not be "UPLOADING".
        businessObjectDataStatusService.updateBusinessObjectDataStatus(
            businessObjectDataHelper.getBusinessObjectDataKey(resultUploadSingleInitiationResponse.getTargetBusinessObjectData()),
            new BusinessObjectDataStatusUpdateRequest(BDATA_STATUS));

        // Try to complete the upload, when target business object data status is not "UPLOADING".
        UploadDownloadServiceImpl.CompleteUploadSingleMessageResult result = uploadDownloadService.performCompleteUploadSingleMessage(filePath);

        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, result.getSourceOldBusinessObjectDataStatus());
        assertNull(result.getSourceNewBusinessObjectDataStatus());
        assertEquals(BDATA_STATUS, result.getTargetOldBusinessObjectDataStatus());
        assertNull(result.getTargetNewBusinessObjectDataStatus());
    }

    @Test
    public void testInitiateDownloadSingle()
    {
        // Create the upload data.
        UploadSingleInitiationResponse uploadSingleInitiationResponse =
            uploadDownloadServiceTestHelper.createUploadedFileData(BusinessObjectDataStatusEntity.VALID);

        // Initiate the download against the uploaded data (i.e. the target business object data).
        DownloadSingleInitiationResponse downloadSingleInitiationResponse = initiateDownload(uploadSingleInitiationResponse.getTargetBusinessObjectData());

        // Validate the download initiation response.
        uploadDownloadServiceTestHelper.validateDownloadSingleInitiationResponse(uploadSingleInitiationResponse, downloadSingleInitiationResponse);
    }

    @Test
    public void testInitiateDownloadSingleBusinessObjectDataNoExists()
    {
        String INVALID_PARTITION_VALUE = "DOES_NOT_EXIST";

        // Create the upload data.
        UploadSingleInitiationResponse uploadSingleInitiationResponse =
            uploadDownloadServiceTestHelper.createUploadedFileData(BusinessObjectDataStatusEntity.VALID);

        // Change the target business object data partition value to something invalid.
        uploadSingleInitiationResponse.getTargetBusinessObjectData().setPartitionValue(INVALID_PARTITION_VALUE);

        // Try to initiate a single file download when business object data does not exist (i.e. the partition value doesn't match).
        try
        {
            // Initiate the download against the uploaded data (i.e. the target business object data).
            initiateDownload(uploadSingleInitiationResponse.getTargetBusinessObjectData());
            fail("Suppose to throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            BusinessObjectData businessObjectData = uploadSingleInitiationResponse.getTargetBusinessObjectData();
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
                    businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
                    businessObjectData.getBusinessObjectFormatVersion(), INVALID_PARTITION_VALUE, NO_SUBPARTITION_VALUES, businessObjectData.getVersion(),
                    null), e.getMessage());
        }
    }

    @Test
    public void testInitiateDownloadSingleMultipleStorageFilesExist()
    {
        // Create the upload data.
        UploadSingleInitiationResponse uploadSingleInitiationResponse =
            uploadDownloadServiceTestHelper.createUploadedFileData(BusinessObjectDataStatusEntity.VALID);

        // Get the target business object data entity.
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDao
            .getBusinessObjectDataByAltKey(businessObjectDataHelper.getBusinessObjectDataKey(uploadSingleInitiationResponse.getTargetBusinessObjectData()));

        // Get the target bushiness object data storage unit.
        StorageUnitEntity targetStorageUnitEntity = IterableUtils.get(targetBusinessObjectDataEntity.getStorageUnits(), 0);

        // Add a second storage file to the target business object data storage unit.
        storageFileDaoTestHelper.createStorageFileEntity(targetStorageUnitEntity, FILE_NAME_2, FILE_SIZE_1_KB, ROW_COUNT_1000);

        // Try to initiate a single file download when business object data has more than one storage file.
        try
        {
            // Initiate the download against the uploaded data (i.e. the target business object data).
            initiateDownload(uploadSingleInitiationResponse.getTargetBusinessObjectData());
            fail("Suppose to throw an IllegalArgumentException when business object has more than one storage file.");
        }
        catch (IllegalArgumentException e)
        {
            BusinessObjectData businessObjectData = uploadSingleInitiationResponse.getTargetBusinessObjectData();
            assertEquals(String.format("Found 2 registered storage files when expecting one in \"%s\" storage for the business object data {%s}.",
                targetStorageUnitEntity.getStorage().getName(), businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataKeyAsString(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectData))), e.getMessage());
        }
    }

    @Test
    public void testInitiateDownloadSingleBusinessObjectDataStatusNotValid()
    {
        // Create the upload data, but leave the target business object data in a "RE-ENCRYPTING" status.
        UploadSingleInitiationResponse uploadSingleInitiationResponse =
            uploadDownloadServiceTestHelper.createUploadedFileData(BusinessObjectDataStatusEntity.RE_ENCRYPTING);

        // Try to initiate a single file download when the business object data is not set to "VALID" which is invalid.
        try
        {
            // Initiate the download against the uploaded data (i.e. the target business object data).
            initiateDownload(uploadSingleInitiationResponse.getTargetBusinessObjectData());
            fail("Suppose to throw an IllegalArgumentException when business object data is not in VALID status.");
        }
        catch (IllegalArgumentException e)
        {
            BusinessObjectData businessObjectData = uploadSingleInitiationResponse.getTargetBusinessObjectData();
            assertEquals(String.format("Business object data status \"%s\" does not match the expected status \"%s\" for the business object data {%s}.",
                uploadSingleInitiationResponse.getTargetBusinessObjectData().getStatus(), BusinessObjectDataStatusEntity.VALID,
                businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataKeyAsString(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
                        businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
                        businessObjectData.getBusinessObjectFormatVersion(), businessObjectData.getPartitionValue(), businessObjectData.getSubPartitionValues(),
                        businessObjectData.getVersion())), e.getMessage());
        }
    }

    @Test
    public void testExtendUploadSingleCredentials() throws InterruptedException
    {
        // Create source and target business object formats database entities which are required to initiate an upload.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload.
        UploadSingleInitiationResponse uploadSingleInitiationResponse =
            uploadDownloadService.initiateUploadSingle(uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest());

        // Sleep a short amount of time to ensure the extended credentials don't return the same expiration as the initial credentials.
        Thread.sleep(10);

        // Initiate the download against the uploaded data (i.e. the target business object data).
        UploadSingleCredentialExtensionResponse uploadSingleCredentialExtensionResponse =
            extendUploadSingleCredentials(uploadSingleInitiationResponse.getSourceBusinessObjectData());

        // Validate the returned object.
        assertNotNull(uploadSingleCredentialExtensionResponse.getAwsAccessKey());
        assertNotNull(uploadSingleCredentialExtensionResponse.getAwsSecretKey());
        assertNotNull(uploadSingleCredentialExtensionResponse.getAwsSessionToken());
        assertNotNull(uploadSingleCredentialExtensionResponse.getAwsSessionExpirationTime());
        assertNotNull(uploadSingleInitiationResponse.getAwsSessionExpirationTime());

        // Ensure the extended credentials are greater than the original set of credentials.
        // We are displaying the values in case there is a problem because this test was acting flaky.
        if (uploadSingleCredentialExtensionResponse.getAwsSessionExpirationTime().toGregorianCalendar().getTimeInMillis() <=
            uploadSingleInitiationResponse.getAwsSessionExpirationTime().toGregorianCalendar().getTimeInMillis())
        {
            fail("Initial expiration time \"" + uploadSingleInitiationResponse.getAwsSessionExpirationTime().toGregorianCalendar().getTimeInMillis() +
                "\" is not > extended expiration time \"" +
                uploadSingleCredentialExtensionResponse.getAwsSessionExpirationTime().toGregorianCalendar().getTimeInMillis() + "\".");
        }
    }

    @Test
    public void testExtendUploadSingleCredentialsBusinessObjectDataStatusNotValid()
    {
        // Create the upload data. This internally calls "complete" which sets the source business object data's status to "DELETED".
        // This is a status where credentials can't be extended (i.e. only UPLOADING statuses).
        UploadSingleInitiationResponse uploadSingleInitiationResponse =
            uploadDownloadServiceTestHelper.createUploadedFileData(BusinessObjectDataStatusEntity.VALID);

        // Try to extend the upload credentials of a source business object data with an invalid status of "DELETED".
        try
        {
            extendUploadSingleCredentials(uploadSingleInitiationResponse.getSourceBusinessObjectData());
            fail("Suppose to throw an IllegalArgumentException when business object data is not in VALID status.");
        }
        catch (IllegalArgumentException e)
        {
            BusinessObjectData businessObjectData = uploadSingleInitiationResponse.getSourceBusinessObjectData();
            assertEquals(String.format("Business object data {%s} has a status of \"%s\" and must be \"%s\" to extend credentials.",
                businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataKeyAsString(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
                        businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
                        businessObjectData.getBusinessObjectFormatVersion(), businessObjectData.getPartitionValue(), NO_SUBPARTITION_VALUES,
                        businessObjectData.getVersion()), BusinessObjectDataStatusEntity.DELETED, BusinessObjectDataStatusEntity.UPLOADING), e.getMessage());
        }
    }

    @Test
    public void testExtendUploadSingleCredentialsBusinessObjectDataNoExists()
    {
        String INVALID_PARTITION_VALUE = "DOES_NOT_EXIST";

        // Create the upload data.
        UploadSingleInitiationResponse uploadSingleInitiationResponse =
            uploadDownloadServiceTestHelper.createUploadedFileData(BusinessObjectDataStatusEntity.VALID);

        // Change the source business object data partition value to something invalid.
        uploadSingleInitiationResponse.getSourceBusinessObjectData().setPartitionValue(INVALID_PARTITION_VALUE);

        // Try to initiate a single file download when business object data does not exist (i.e. the partition value doesn't match).
        try
        {
            extendUploadSingleCredentials(uploadSingleInitiationResponse.getSourceBusinessObjectData());
            fail("Suppose to throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            BusinessObjectData businessObjectData = uploadSingleInitiationResponse.getSourceBusinessObjectData();
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
                    businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
                    businessObjectData.getBusinessObjectFormatVersion(), INVALID_PARTITION_VALUE, NO_SUBPARTITION_VALUES, businessObjectData.getVersion(),
                    null), e.getMessage());
        }
    }

    /**
     * This method is to get coverage for the upload download service method that has an explicit annotation for transaction propagation.
     */
    @Test
    public void testUploadDownloadHelperServiceMethodsNewTransactionPropagation()
    {
        setLogLevel(UploadDownloadHelperServiceImpl.class, LogLevel.OFF);

        uploadDownloadServiceImpl.performCompleteUploadSingleMessage("KEY_DOES_NOT_EXIST");
    }

    /**
     * Initiates a download using the specified business object data.
     *
     * @param businessObjectData the business object data.
     *
     * @return the download single initiation response.
     */
    private DownloadSingleInitiationResponse initiateDownload(BusinessObjectData businessObjectData)
    {
        return uploadDownloadService.initiateDownloadSingle(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
            businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
            businessObjectData.getBusinessObjectFormatVersion(), businessObjectData.getPartitionValue(), businessObjectData.getVersion());
    }

    /**
     * Extends the credentials of an in-progress upload.
     *
     * @param businessObjectData the business object data for the in-progress upload.
     *
     * @return the upload single credential extension response.
     */
    private UploadSingleCredentialExtensionResponse extendUploadSingleCredentials(BusinessObjectData businessObjectData)
    {
        return uploadDownloadService.extendUploadSingleCredentials(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
            businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
            businessObjectData.getBusinessObjectFormatVersion(), businessObjectData.getPartitionValue(), businessObjectData.getVersion());
    }

    @Test
    public void testDownloadBusinessObjectDefinitionSampleFile()
    {
        // Create a test storage.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_DOWNLOAD_ROLE_ARN), DOWNLOADER_ROLE_ARN)));

        // Create and persist a business object definition entity with sample data files.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), Arrays.asList(new SampleDataFile(DIRECTORY_PATH, FILE_NAME)));

        // Initiate download of a sample data file.
        DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest downloadRequest =
            new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest();
        BusinessObjectDefinitionSampleDataFileKey sampleDataFileKey = new BusinessObjectDefinitionSampleDataFileKey();
        sampleDataFileKey.setNamespace(NAMESPACE);
        sampleDataFileKey.setBusinessObjectDefinitionName(BDEF_NAME);
        sampleDataFileKey.setDirectoryPath(DIRECTORY_PATH);
        sampleDataFileKey.setFileName(FILE_NAME);
        downloadRequest.setBusinessObjectDefinitionSampleDataFileKey(sampleDataFileKey);
        DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationResponse downloadResponse =
            uploadDownloadService.initiateDownloadSingleSampleFile(downloadRequest);

        // Validate the response.
        assertNotNull(downloadResponse.getAwsSessionExpirationTime());
        assertNotNull(downloadResponse.getPreSignedUrl());
        assertEquals(new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationResponse(
            new BusinessObjectDefinitionSampleDataFileKey(NAMESPACE, BDEF_NAME, DIRECTORY_PATH, FILE_NAME), S3_BUCKET_NAME,
            MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_ACCESS_KEY, MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SECRET_KEY,
            MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SESSION_TOKEN, downloadResponse.getAwsSessionExpirationTime(), downloadResponse.getPreSignedUrl()),
            downloadResponse);
    }

    @Test
    public void testDownloadBusinessObjectDefinitionSampleFileLowerCaseParameters()
    {
        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles());

        List<SampleDataFile> sampleFileList = businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles();

        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(STORAGE_NAME);
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucketName"));

        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_DOWNLOAD_ROLE_ARN),
                "downloadRole"));

        DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest downloadRequest =
            new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest();

        BusinessObjectDefinitionSampleDataFileKey sampleDataFileKey = new BusinessObjectDefinitionSampleDataFileKey();
        sampleDataFileKey.setBusinessObjectDefinitionName(BDEF_NAME);
        sampleDataFileKey.setNamespace(NAMESPACE);
        sampleDataFileKey.setDirectoryPath(sampleFileList.get(0).getDirectoryPath());
        sampleDataFileKey.setFileName(sampleFileList.get(0).getFileName());

        //use the lower case name space and business definition still return the same response
        BusinessObjectDefinitionSampleDataFileKey sampleDataFileKeyLowerCase = new BusinessObjectDefinitionSampleDataFileKey();
        sampleDataFileKeyLowerCase.setBusinessObjectDefinitionName(BDEF_NAME.toLowerCase());
        sampleDataFileKeyLowerCase.setNamespace(NAMESPACE.toLowerCase());
        sampleDataFileKeyLowerCase.setDirectoryPath(sampleFileList.get(0).getDirectoryPath());
        sampleDataFileKeyLowerCase.setFileName(sampleFileList.get(0).getFileName());
        downloadRequest.setBusinessObjectDefinitionSampleDataFileKey(sampleDataFileKeyLowerCase);

        DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationResponse downloadResponse =
            uploadDownloadService.initiateDownloadSingleSampleFile(downloadRequest);

        assertEquals(downloadResponse.getBusinessObjectDefinitionSampleDataFileKey(), sampleDataFileKey);
        assertNotNull(downloadResponse.getAwsS3BucketName());
        assertNotNull(downloadResponse.getAwsAccessKey());
        assertNotNull(downloadResponse.getAwsSecretKey());
        assertNotNull(downloadResponse.getAwsSessionExpirationTime());
        assertNotNull(downloadResponse.getAwsSessionToken());
        assertNotNull(downloadResponse.getPreSignedUrl());
    }

    @Test
    public void testDownloadBusinessObjectDefinitionSampleFileUpperCaseParameters()
    {
        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles());

        List<SampleDataFile> sampleFileList = businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles();

        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(STORAGE_NAME);
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucketName"));

        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_DOWNLOAD_ROLE_ARN),
                "downloadRole"));

        DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest downloadRequest =
            new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest();

        BusinessObjectDefinitionSampleDataFileKey sampleDataFileKey = new BusinessObjectDefinitionSampleDataFileKey();
        sampleDataFileKey.setBusinessObjectDefinitionName(BDEF_NAME);
        sampleDataFileKey.setNamespace(NAMESPACE);
        sampleDataFileKey.setDirectoryPath(sampleFileList.get(0).getDirectoryPath());
        sampleDataFileKey.setFileName(sampleFileList.get(0).getFileName());

        //use the lower case name space and business definition still return the same response
        BusinessObjectDefinitionSampleDataFileKey sampleDataFileKeyUpperCase = new BusinessObjectDefinitionSampleDataFileKey();
        sampleDataFileKeyUpperCase.setBusinessObjectDefinitionName(BDEF_NAME.toUpperCase());
        sampleDataFileKeyUpperCase.setNamespace(NAMESPACE.toUpperCase());
        sampleDataFileKeyUpperCase.setDirectoryPath(sampleFileList.get(0).getDirectoryPath());
        sampleDataFileKeyUpperCase.setFileName(sampleFileList.get(0).getFileName());
        downloadRequest.setBusinessObjectDefinitionSampleDataFileKey(sampleDataFileKeyUpperCase);

        DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationResponse downloadResponse =
            uploadDownloadService.initiateDownloadSingleSampleFile(downloadRequest);

        assertEquals(downloadResponse.getBusinessObjectDefinitionSampleDataFileKey(), sampleDataFileKey);
        assertNotNull(downloadResponse.getAwsS3BucketName());
        assertNotNull(downloadResponse.getAwsAccessKey());
        assertNotNull(downloadResponse.getAwsSecretKey());
        assertNotNull(downloadResponse.getAwsSessionExpirationTime());
        assertNotNull(downloadResponse.getAwsSessionToken());
        assertNotNull(downloadResponse.getPreSignedUrl());
    }

    @Test
    public void testDownloadBusinessObjectDefinitionSampleFileTrimmedParameters()
    {
        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles());

        List<SampleDataFile> sampleFileList = businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles();

        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(STORAGE_NAME);
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), "testBucketName"));

        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_DOWNLOAD_ROLE_ARN),
                "downloadRole"));

        DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest downloadRequest =
            new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest();

        BusinessObjectDefinitionSampleDataFileKey sampleDataFileKey = new BusinessObjectDefinitionSampleDataFileKey();
        sampleDataFileKey.setBusinessObjectDefinitionName(BDEF_NAME);
        sampleDataFileKey.setNamespace(NAMESPACE);
        sampleDataFileKey.setDirectoryPath(sampleFileList.get(0).getDirectoryPath());
        sampleDataFileKey.setFileName(sampleFileList.get(0).getFileName());

        //use the lower case name space and business definition still return the same response
        BusinessObjectDefinitionSampleDataFileKey sampleDataFileKeyPadded = new BusinessObjectDefinitionSampleDataFileKey();
        sampleDataFileKeyPadded.setBusinessObjectDefinitionName(" " + BDEF_NAME + " ");
        sampleDataFileKeyPadded.setNamespace(" " + NAMESPACE + " ");
        sampleDataFileKeyPadded.setDirectoryPath(sampleFileList.get(0).getDirectoryPath());
        sampleDataFileKeyPadded.setFileName(sampleFileList.get(0).getFileName());
        downloadRequest.setBusinessObjectDefinitionSampleDataFileKey(sampleDataFileKeyPadded);

        DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationResponse downloadResponse =
            uploadDownloadService.initiateDownloadSingleSampleFile(downloadRequest);

        assertEquals(downloadResponse.getBusinessObjectDefinitionSampleDataFileKey(), sampleDataFileKey);
        assertNotNull(downloadResponse.getAwsS3BucketName());
        assertNotNull(downloadResponse.getAwsAccessKey());
        assertNotNull(downloadResponse.getAwsSecretKey());
        assertNotNull(downloadResponse.getAwsSessionExpirationTime());
        assertNotNull(downloadResponse.getAwsSessionToken());
        assertNotNull(downloadResponse.getPreSignedUrl());
    }

    @Test
    public void testDownloadBusinessObjectDefinitionSampleFilesMissingRequiredParameters()
    {
        try
        {
            uploadDownloadService.initiateDownloadSingleSampleFile(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A download business object definition sample data file single initiation request must be specified.", e.getMessage());
        }

        try
        {
            uploadDownloadService.initiateDownloadSingleSampleFile(new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest(null));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition sample data file key must be specified.", e.getMessage());
        }

        try
        {
            uploadDownloadService.initiateDownloadSingleSampleFile(new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest(
                new BusinessObjectDefinitionSampleDataFileKey(BLANK_TEXT, BDEF_NAME, DIRECTORY_PATH, FILE_NAME)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            uploadDownloadService.initiateDownloadSingleSampleFile(new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest(
                new BusinessObjectDefinitionSampleDataFileKey(NAMESPACE, BLANK_TEXT, DIRECTORY_PATH, FILE_NAME)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        try
        {
            uploadDownloadService.initiateDownloadSingleSampleFile(new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest(
                new BusinessObjectDefinitionSampleDataFileKey(NAMESPACE, BDEF_NAME, BLANK_TEXT, FILE_NAME)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A directory path must be specified.", e.getMessage());
        }

        try
        {
            uploadDownloadService.initiateDownloadSingleSampleFile(new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest(
                new BusinessObjectDefinitionSampleDataFileKey(NAMESPACE, BDEF_NAME, DIRECTORY_PATH, BLANK_TEXT)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A file name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDownloadBusinessObjectDefinitionInvalidParameters()
    {
        // Create a test storage.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_DOWNLOAD_ROLE_ARN), DOWNLOADER_ROLE_ARN)));

        // Create and persist a business object definition entity with sample data files.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), Arrays.asList(new SampleDataFile(DIRECTORY_PATH, FILE_NAME)));

        // Try to initiate a download using an invalid namespace.
        try
        {
            uploadDownloadService.initiateDownloadSingleSampleFile(new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest(
                new BusinessObjectDefinitionSampleDataFileKey("I_DO_NOT_EXIST", BDEF_NAME, DIRECTORY_PATH, FILE_NAME)));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME),
                e.getMessage());
        }

        // Try to initiate a download using an invalid business object definition name.
        try
        {
            uploadDownloadService.initiateDownloadSingleSampleFile(new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest(
                new BusinessObjectDefinitionSampleDataFileKey(NAMESPACE, "I_DO_NOT_EXIST", DIRECTORY_PATH, FILE_NAME)));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionNotFoundErrorMessage(NAMESPACE, "I_DO_NOT_EXIST"),
                e.getMessage());
        }

        // Try to initiate a download using an invalid directory path.
        try
        {
            uploadDownloadService.initiateDownloadSingleSampleFile(new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest(
                new BusinessObjectDefinitionSampleDataFileKey(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FILE_NAME)));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format(
                "Business object definition with name \"%s\" and namespace \"%s\" does not have the specified sample file registered with file name \"%s\" in" +
                    " directory path \"%s\"", BDEF_NAME, NAMESPACE, FILE_NAME, "I_DO_NOT_EXIST"), e.getMessage());
        }

        // Try to initiate a download using an invalid file name.
        try
        {
            uploadDownloadService.initiateDownloadSingleSampleFile(new DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest(
                new BusinessObjectDefinitionSampleDataFileKey(NAMESPACE, BDEF_NAME, DIRECTORY_PATH, "I_DO_NOT_EXIST")));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format(
                "Business object definition with name \"%s\" and namespace \"%s\" does not have the specified sample file registered with file name \"%s\" in" +
                    " directory path \"%s\"", BDEF_NAME, NAMESPACE, "I_DO_NOT_EXIST", DIRECTORY_PATH), e.getMessage());
        }
    }
    
    @Test
    public void testUploadBusinessObjectDefinitionSampleFile()
    {
        String s3_velocity_template = "$namespace/$businessObjectDefinitionName";
   
        // Create a test storage.
        storageDaoTestHelper.createStorageEntity(StorageEntity.SAMPLE_DATA_FILE_STORAGE, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN), UPLOADER_ROLE_ARN),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), s3_velocity_template)));

        // Create and persist a business object definition entity with sample data files.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());
        
        UploadBusinessObjectDefinitionSampleDataFileInitiationRequest request = new UploadBusinessObjectDefinitionSampleDataFileInitiationRequest();
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        request.setBusinessObjectDefinitionKey(businessObjectDefinitionKey);

        UploadBusinessObjectDefinitionSampleDataFileInitiationResponse response = uploadDownloadService.initiateUploadSampleFile(request);
        assertEquals(response.getBusinessObjectDefinitionKey(), businessObjectDefinitionKey);
        assertEquals(response.getAwsS3BucketName(), S3_BUCKET_NAME);
        assertEquals(response.getAwsAccessKey(), MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_ACCESS_KEY);
        assertEquals(response.getAwsSecretKey(), MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SECRET_KEY);
        assertEquals(response.getAwsSessionToken(), MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SESSION_TOKEN);

    }
    
    @Test
    public void testUploadBusinessObjectDefinitionSampleFileLowerCase()
    {
        String s3_velocity_template = "$namespace/$businessObjectDefinitionName";
   
        // Create a test storage.
        storageDaoTestHelper.createStorageEntity(StorageEntity.SAMPLE_DATA_FILE_STORAGE, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN), UPLOADER_ROLE_ARN),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), s3_velocity_template)));

        // Create and persist a business object definition entity with sample data files.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());
        
        UploadBusinessObjectDefinitionSampleDataFileInitiationRequest request = new UploadBusinessObjectDefinitionSampleDataFileInitiationRequest();
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        BusinessObjectDefinitionKey businessObjectDefinitionKeyLowerCase = new BusinessObjectDefinitionKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase());
        request.setBusinessObjectDefinitionKey(businessObjectDefinitionKeyLowerCase);

        UploadBusinessObjectDefinitionSampleDataFileInitiationResponse response = uploadDownloadService.initiateUploadSampleFile(request);
        assertEquals(response.getBusinessObjectDefinitionKey(), businessObjectDefinitionKey);
        assertEquals(response.getAwsS3BucketName(), S3_BUCKET_NAME);
        assertEquals(response.getAwsAccessKey(), MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_ACCESS_KEY);
        assertEquals(response.getAwsSecretKey(), MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SECRET_KEY);
        assertEquals(response.getAwsSessionToken(), MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SESSION_TOKEN);

    }
    
    @Test
    public void testUploadBusinessObjectDefinitionSampleFileUpperCase()
    {
        String s3_velocity_template = "$namespace/$businessObjectDefinitionName";
   
        // Create a test storage.
        storageDaoTestHelper.createStorageEntity(StorageEntity.SAMPLE_DATA_FILE_STORAGE, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN), UPLOADER_ROLE_ARN),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), s3_velocity_template)));

        // Create and persist a business object definition entity with sample data files.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());
        
        UploadBusinessObjectDefinitionSampleDataFileInitiationRequest request = new UploadBusinessObjectDefinitionSampleDataFileInitiationRequest();
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        BusinessObjectDefinitionKey businessObjectDefinitionKeyUpperCase = new BusinessObjectDefinitionKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase());
        request.setBusinessObjectDefinitionKey(businessObjectDefinitionKeyUpperCase);

        UploadBusinessObjectDefinitionSampleDataFileInitiationResponse response = uploadDownloadService.initiateUploadSampleFile(request);
        assertEquals(response.getBusinessObjectDefinitionKey(), businessObjectDefinitionKey);
        assertEquals(response.getAwsS3BucketName(), S3_BUCKET_NAME);
        assertEquals(response.getAwsAccessKey(), MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_ACCESS_KEY);
        assertEquals(response.getAwsSecretKey(), MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SECRET_KEY);
        assertEquals(response.getAwsSessionToken(), MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SESSION_TOKEN);
    }
    
    @Test
    public void testUploadBusinessObjectDefinitionSampleFileTrimedParameters()
    {
        String s3_velocity_template = "$namespace/$businessObjectDefinitionName";
   
        // Create a test storage.
        storageDaoTestHelper.createStorageEntity(StorageEntity.SAMPLE_DATA_FILE_STORAGE, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN), UPLOADER_ROLE_ARN),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), s3_velocity_template)));

        // Create and persist a business object definition entity with sample data files.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());
        
        UploadBusinessObjectDefinitionSampleDataFileInitiationRequest request = new UploadBusinessObjectDefinitionSampleDataFileInitiationRequest();
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        BusinessObjectDefinitionKey businessObjectDefinitionKeyWhitespace = new BusinessObjectDefinitionKey("    " + NAMESPACE + " ",  "   " + BDEF_NAME + "  ");
        request.setBusinessObjectDefinitionKey(businessObjectDefinitionKeyWhitespace);

        UploadBusinessObjectDefinitionSampleDataFileInitiationResponse response = uploadDownloadService.initiateUploadSampleFile(request);
        assertEquals(response.getBusinessObjectDefinitionKey(), businessObjectDefinitionKey);
        assertEquals(response.getAwsS3BucketName(), S3_BUCKET_NAME);
        assertEquals(response.getAwsAccessKey(), MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_ACCESS_KEY);
        assertEquals(response.getAwsSecretKey(), MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SECRET_KEY);
        assertEquals(response.getAwsSessionToken(), MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SESSION_TOKEN);
    }
    
    @Test
    public void testUploadBusinessObjectDefinitionSampleFileMissingParameter()
    {
        try
        {
            uploadDownloadService.initiateUploadSampleFile(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An upload initiation request must be specified.", e.getMessage());
        }

        try
        {
            uploadDownloadService.initiateUploadSampleFile(new UploadBusinessObjectDefinitionSampleDataFileInitiationRequest(null));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition key must be specified.", e.getMessage());
        }

        try
        {
            BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey("NAMESPACE", null);
            UploadBusinessObjectDefinitionSampleDataFileInitiationRequest request =
                    new UploadBusinessObjectDefinitionSampleDataFileInitiationRequest(businessObjectDefinitionKey);
            uploadDownloadService.initiateUploadSampleFile(request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        try
        {
            BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(null, "BDEF");
            UploadBusinessObjectDefinitionSampleDataFileInitiationRequest request =
                    new UploadBusinessObjectDefinitionSampleDataFileInitiationRequest(businessObjectDefinitionKey);
            uploadDownloadService.initiateUploadSampleFile(request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }
    
    @Test
    public void testUploadBusinessObjectDefinitionSampleFileInvalidParameter()
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        UploadBusinessObjectDefinitionSampleDataFileInitiationRequest request =
                new UploadBusinessObjectDefinitionSampleDataFileInitiationRequest(businessObjectDefinitionKey);
        try
        {

            uploadDownloadService.initiateUploadSampleFile(request);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionNotFoundErrorMessage(NAMESPACE, BDEF_NAME), e
                    .getMessage());
        }
    }
}

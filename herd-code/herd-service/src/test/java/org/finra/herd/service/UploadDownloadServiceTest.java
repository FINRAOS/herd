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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.DownloadSingleInitiationResponse;
import org.finra.herd.model.api.xml.UploadSingleCredentialExtensionResponse;
import org.finra.herd.model.api.xml.UploadSingleInitiationRequest;
import org.finra.herd.model.api.xml.UploadSingleInitiationResponse;

/**
 * This class tests various functionality within the custom DDL REST controller.
 */
public class UploadDownloadServiceTest extends AbstractServiceTest
{
    @Autowired
    private UploadDownloadService uploadDownloadService;

    @Test
    public void testInitiateUploadSingle()
    {
        // Create database entities required for testing.
        createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(createUploadSingleInitiationRequest());

        // Validate the returned object.
        validateUploadSingleInitiationResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE_CD_2, BOD_NAME_2,
            FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, getNewAttributes(), FILE_NAME, FILE_SIZE_1_KB,
            resultUploadSingleInitiationResponse);
    }

    @Test
    public void testInitiateUploadSingleMissingRequiredParameters()
    {
        UploadSingleInitiationRequest request;

        // Try to initiate a single file upload when business object format key is not specified.
        request = createUploadSingleInitiationRequest();
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
            request = createUploadSingleInitiationRequest();
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
            request = createUploadSingleInitiationRequest();
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
            request = createUploadSingleInitiationRequest();
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
            request = createUploadSingleInitiationRequest();
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
            request = createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setBusinessObjectFormatVersion(null);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when business object format key is not specified.
        request = createUploadSingleInitiationRequest();
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
            request = createUploadSingleInitiationRequest();
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
            request = createUploadSingleInitiationRequest();
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
            request = createUploadSingleInitiationRequest();
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
            request = createUploadSingleInitiationRequest();
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
            request = createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setBusinessObjectFormatVersion(null);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to initiate a single file upload when attribute name is not specified.
        request = createUploadSingleInitiationRequest();
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
        request = createUploadSingleInitiationRequest();
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
        request = createUploadSingleInitiationRequest();
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
        createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload without specifying any of the optional parameters.
        UploadSingleInitiationRequest request = createUploadSingleInitiationRequest();
        request.setBusinessObjectDataAttributes(null);
        request.getFile().setFileSizeBytes(null);
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(request);

        // Validate the returned object.
        validateUploadSingleInitiationResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE_CD_2, BOD_NAME_2,
            FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, NO_ATTRIBUTES, FILE_NAME, null, resultUploadSingleInitiationResponse);
    }

    @Test
    public void testInitiateUploadSingleTrimParameters()
    {
        // Create database entities required for testing.
        createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload using input parameters with leading and trailing empty spaces.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(
            createUploadSingleInitiationRequest(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(NAMESPACE_CD_2), addWhitespace(BOD_NAME_2),
                addWhitespace(FORMAT_USAGE_CODE_2), addWhitespace(FORMAT_FILE_TYPE_CODE_2), FORMAT_VERSION_2));

        // Validate the returned object.
        validateUploadSingleInitiationResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE_CD_2, BOD_NAME_2,
            FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, getNewAttributes(), FILE_NAME, FILE_SIZE_1_KB,
            resultUploadSingleInitiationResponse);
    }

    @Test
    public void testInitiateUploadSingleUpperCaseParameters()
    {
        // Create database entities required for testing.
        createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload using lower case values.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(
            createUploadSingleInitiationRequest(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, NAMESPACE_CD_2.toLowerCase(), BOD_NAME_2.toLowerCase(), FORMAT_USAGE_CODE_2.toLowerCase(),
                FORMAT_FILE_TYPE_CODE_2.toLowerCase(), FORMAT_VERSION_2));

        // Validate the returned object.
        validateUploadSingleInitiationResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE_CD_2, BOD_NAME_2,
            FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, getNewAttributes(), FILE_NAME, FILE_SIZE_1_KB,
            resultUploadSingleInitiationResponse);
    }

    @Test
    public void testInitiateUploadSingleLowerCaseParameters()
    {
        // Create database entities required for testing.
        createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload using upper case values.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(
            createUploadSingleInitiationRequest(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, NAMESPACE_CD_2.toUpperCase(), BOD_NAME_2.toUpperCase(), FORMAT_USAGE_CODE_2.toUpperCase(),
                FORMAT_FILE_TYPE_CODE_2.toUpperCase(), FORMAT_VERSION_2));

        // Validate the returned object.
        validateUploadSingleInitiationResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE_CD_2, BOD_NAME_2,
            FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, getNewAttributes(), FILE_NAME, FILE_SIZE_1_KB,
            resultUploadSingleInitiationResponse);
    }

    @Test
    public void testInitiateUploadSingleInvalidParameters()
    {
        // Create database entities required for testing.
        createDatabaseEntitiesForUploadDownloadTesting();

        UploadSingleInitiationRequest request;

        // Try to initiate a single file upload using invalid namespace.
        try
        {
            request = createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setNamespace("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid business object definition name.
        try
        {
            request = createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setBusinessObjectDefinitionName("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid format usage.
        try
        {
            request = createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setBusinessObjectFormatUsage("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid format file type.
        try
        {
            request = createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setBusinessObjectFormatFileType("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid business object format version.
        try
        {
            request = createUploadSingleInitiationRequest();
            request.getSourceBusinessObjectFormatKey().setBusinessObjectFormatVersion(INVALID_FORMAT_VERSION);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid namespace.
        try
        {
            request = createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setNamespace("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BOD_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2,
                FORMAT_VERSION_2), e.getMessage());
        }

        // Try to initiate a single file upload using invalid business object definition name.
        try
        {
            request = createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setBusinessObjectDefinitionName("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD_2, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2,
                FORMAT_VERSION_2), e.getMessage());
        }

        // Try to initiate a single file upload using invalid format usage.
        try
        {
            request = createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setBusinessObjectFormatUsage("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD_2, BOD_NAME_2, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid format file type.
        try
        {
            request = createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setBusinessObjectFormatFileType("I_DO_NOT_EXIST");

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD_2, BOD_NAME_2, FORMAT_USAGE_CODE_2, "I_DO_NOT_EXIST", FORMAT_VERSION_2),
                e.getMessage());
        }

        // Try to initiate a single file upload using invalid business object format version.
        try
        {
            request = createUploadSingleInitiationRequest();
            request.getTargetBusinessObjectFormatKey().setBusinessObjectFormatVersion(INVALID_FORMAT_VERSION);

            uploadDownloadService.initiateUploadSingle(request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD_2, BOD_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2,
                INVALID_FORMAT_VERSION), e.getMessage());
        }
    }

    @Test
    public void testInitiateUploadSingleDuplicateAttributes()
    {
        // Create database entities required for testing.
        createDatabaseEntitiesForUploadDownloadTesting();

        // Try to initiate a single file upload when duplicate attributes are specified.
        UploadSingleInitiationRequest request = createUploadSingleInitiationRequest();
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
        createDatabaseEntitiesForUploadDownloadTesting();

        // Create and persist a business object data attribute definition entity.
        createBusinessObjectDataAttributeDefinitionEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            ATTRIBUTE_NAME_1_MIXED_CASE);

        // Initiate a file upload.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(createUploadSingleInitiationRequest());

        // Validate the returned object.
        validateUploadSingleInitiationResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE_CD_2, BOD_NAME_2,
            FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, getNewAttributes(), FILE_NAME, FILE_SIZE_1_KB,
            resultUploadSingleInitiationResponse);
    }

    @Test
    public void testInitiateUploadSingleRequiredAttributeMissingValue()
    {
        // Create database entities required for testing.
        createDatabaseEntitiesForUploadDownloadTesting();

        // Create and persist a business object data attribute definition entity.
        createBusinessObjectDataAttributeDefinitionEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            ATTRIBUTE_NAME_1_MIXED_CASE);

        // Try to initiate a single file upload when a required attribute value is not specified.
        UploadSingleInitiationRequest request = createUploadSingleInitiationRequest();
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

    @Test
    public void testInitiateDownloadSingle()
    {
        // Create the upload data.
        UploadSingleInitiationResponse uploadSingleInitiationResponse = createUploadedFileData(BusinessObjectDataStatusEntity.VALID);

        // Initiate the download against the uploaded data (i.e. the target business object data).
        DownloadSingleInitiationResponse downloadSingleInitiationResponse = initiateDownload(uploadSingleInitiationResponse.getTargetBusinessObjectData());

        // Validate the download initiation response.
        validateDownloadSingleInitiationResponse(uploadSingleInitiationResponse, downloadSingleInitiationResponse);
    }

    @Test
    public void testInitiateDownloadSingleBusinessObjectDataStatusNotValid()
    {
        // Create the upload data, but leave the target business object data in a "RE-ENCRYPTING" status.
        UploadSingleInitiationResponse uploadSingleInitiationResponse = createUploadedFileData(BusinessObjectDataStatusEntity.RE_ENCRYPTING);

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
                getExpectedBusinessObjectDataKeyAsString(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
                    businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
                    businessObjectData.getBusinessObjectFormatVersion(), businessObjectData.getPartitionValue(), businessObjectData.getSubPartitionValues(),
                    businessObjectData.getVersion())), e.getMessage());
        }
    }

    @Test
    public void testInitiateDownloadSingleBusinessObjectDataNoExists()
    {
        String INVALID_PARTITION_VALUE = "DOES_NOT_EXIST";

        // Create the upload data.
        UploadSingleInitiationResponse uploadSingleInitiationResponse = createUploadedFileData(BusinessObjectDataStatusEntity.VALID);

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
            assertEquals(
                getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
                    businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
                    businessObjectData.getBusinessObjectFormatVersion(), INVALID_PARTITION_VALUE, NO_SUBPARTITION_VALUES, businessObjectData.getVersion(),
                    null), e.getMessage());
        }
    }

    @Test
    public void testExtendUploadSingleCredentials()
    {
        // Create source and target business object formats database entities which are required to initiate an upload.
        createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload.
        UploadSingleInitiationResponse uploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(createUploadSingleInitiationRequest());

        // Initiate the download against the uploaded data (i.e. the target business object data).
        UploadSingleCredentialExtensionResponse uploadSingleCredentialExtensionResponse =
            extendUploadSingleCredentials(uploadSingleInitiationResponse.getSourceBusinessObjectData());

        // Validate the returned object.
        assertNotNull(uploadSingleCredentialExtensionResponse.getAwsAccessKey());
        assertNotNull(uploadSingleCredentialExtensionResponse.getAwsSecretKey());
        assertNotNull(uploadSingleCredentialExtensionResponse.getAwsSessionToken());

        // Ensure the extended credentials are greater than the original set of credentials.
        assertTrue(uploadSingleCredentialExtensionResponse.getAwsSessionExpirationTime().toGregorianCalendar().getTimeInMillis() >
            uploadSingleInitiationResponse.getAwsSessionExpirationTime().toGregorianCalendar().getTimeInMillis());
    }

    @Test
    public void testExtendUploadSingleCredentialsBusinessObjectDataStatusNotValid()
    {
        // Create the upload data. This internally calls "complete" which sets the source business object data's status to "DELETED".
        // This is a status where credentials can't be extended (i.e. only UPLOADING statuses).
        UploadSingleInitiationResponse uploadSingleInitiationResponse = createUploadedFileData(BusinessObjectDataStatusEntity.VALID);

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
                getExpectedBusinessObjectDataKeyAsString(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
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
        UploadSingleInitiationResponse uploadSingleInitiationResponse = createUploadedFileData(BusinessObjectDataStatusEntity.VALID);

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
            assertEquals(
                getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
                    businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
                    businessObjectData.getBusinessObjectFormatVersion(), INVALID_PARTITION_VALUE, NO_SUBPARTITION_VALUES, businessObjectData.getVersion(),
                    null), e.getMessage());
        }
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
}

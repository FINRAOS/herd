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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSampleDataFileKey;
import org.finra.herd.model.api.xml.DownloadBusinesObjectDefinitionSampleDataFileSingleInitiationRequest;
import org.finra.herd.model.api.xml.DownloadBusinesObjectDefinitionSampleDataFileSingleInitiationResponse;
import org.finra.herd.model.api.xml.DownloadSingleInitiationResponse;
import org.finra.herd.model.api.xml.SampleDataFile;
import org.finra.herd.model.api.xml.UploadSingleCredentialExtensionResponse;
import org.finra.herd.model.api.xml.UploadSingleInitiationResponse;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.StorageEntity;

/**
 * This class tests various functionality within the custom DDL REST controller.
 */
public class UploadDownloadRestControllerTest extends AbstractRestTest
{
    @Test
    public void testInitiateUploadSingle()
    {
        // Create database entities required for testing.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse =
            uploadDownloadRestController.initiateUploadSingle(uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest());

        // Validate the returned object.
        uploadDownloadServiceTestHelper
            .validateUploadSingleInitiationResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, businessObjectDefinitionServiceTestHelper.getNewAttributes(), FILE_NAME,
                FILE_SIZE_1_KB, null, resultUploadSingleInitiationResponse);
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
    public void testExtendUploadSingleCredentials() throws InterruptedException
    {
        // Create source and target business object formats database entities which are required to initiate an upload.
        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload.
        UploadSingleInitiationResponse uploadSingleInitiationResponse =
            uploadDownloadRestController.initiateUploadSingle(uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest());

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

    /**
     * Initiates a download using the specified business object data.
     *
     * @param businessObjectData the business object data.
     *
     * @return the download single initiation response.
     */
    private DownloadSingleInitiationResponse initiateDownload(BusinessObjectData businessObjectData)
    {
        return uploadDownloadRestController.initiateDownloadSingle(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
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
        return uploadDownloadRestController
            .extendUploadSingleCredentials(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
                businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
                businessObjectData.getBusinessObjectFormatVersion(), businessObjectData.getPartitionValue(), businessObjectData.getVersion());
    }
    
    @Test
    public void testDownloadSampleDataFile()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                        businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles());

        List<SampleDataFile> sampleFileList = businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles();

        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(STORAGE_NAME);
        storageEntity.getAttributes().add(
                storageDaoTestHelper
                        .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                                "testBucketName"));

        storageEntity.getAttributes().add(
                storageDaoTestHelper
                        .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_DOWNLOAD_ROLE_ARN),
                                "downloadRole"));

        DownloadBusinesObjectDefinitionSampleDataFileSingleInitiationRequest downloadRequest =
                new DownloadBusinesObjectDefinitionSampleDataFileSingleInitiationRequest();
        BusinessObjectDefinitionSampleDataFileKey sampleDataFileKey = new BusinessObjectDefinitionSampleDataFileKey();
        sampleDataFileKey.setBusinessObjectDefinitionName(BDEF_NAME);
        sampleDataFileKey.setNamespace(NAMESPACE);
        sampleDataFileKey.setDirectoryPath(sampleFileList.get(0).getDirectoryPath());
        sampleDataFileKey.setFileName(sampleFileList.get(0).getFileName());

        downloadRequest.setBusinessObjectDefinitionSampleDataFileKey(sampleDataFileKey);

        DownloadBusinesObjectDefinitionSampleDataFileSingleInitiationResponse downloadResponse =
                uploadDownloadRestController.initiateDownloadSingleSampleFile(downloadRequest);

        assertEquals(downloadResponse.getBusinessObjectDefinitionSampleDataFileKey(), sampleDataFileKey);
        assertNotNull(downloadResponse.getAwsAccessKey());
        assertNotNull(downloadResponse.getAwsSecretKey());
        assertNotNull(downloadResponse.getAwsSessionExpirationTime());
        assertNotNull(downloadResponse.getAwsSessionToken());
        assertNotNull(downloadResponse.getPreSignedUrl());
    }
}

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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.DownloadSingleInitiationResponse;
import org.finra.herd.model.api.xml.UploadSingleCredentialExtensionResponse;
import org.finra.herd.model.api.xml.UploadSingleInitiationResponse;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;

/**
 * This class tests various functionality within the custom DDL REST controller.
 */
public class UploadDownloadRestControllerTest extends AbstractRestTest
{
    @Test
    public void testInitiateUploadSingle()
    {
        // Create database entities required for testing.
        createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse =
            uploadDownloadRestController.initiateUploadSingle(createUploadSingleInitiationRequest());

        // Validate the returned object.
        validateUploadSingleInitiationResponse(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
            FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, getNewAttributes(), FILE_NAME, FILE_SIZE_1_KB, null,
            resultUploadSingleInitiationResponse);
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
    public void testExtendUploadSingleCredentials()
    {
        // Create source and target business object formats database entities which are required to initiate an upload.
        createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload.
        UploadSingleInitiationResponse uploadSingleInitiationResponse =
            uploadDownloadRestController.initiateUploadSingle(createUploadSingleInitiationRequest());

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
        assertTrue(uploadSingleCredentialExtensionResponse.getAwsSessionExpirationTime().toGregorianCalendar().getTimeInMillis() >
            uploadSingleInitiationResponse.getAwsSessionExpirationTime().toGregorianCalendar().getTimeInMillis());
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
}

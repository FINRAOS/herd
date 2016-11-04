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

import org.finra.herd.model.api.xml.DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest;
import org.finra.herd.model.api.xml.DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationResponse;
import org.finra.herd.model.api.xml.DownloadSingleInitiationResponse;
import org.finra.herd.model.api.xml.UploadSingleCredentialExtensionResponse;
import org.finra.herd.model.api.xml.UploadSingleInitiationRequest;
import org.finra.herd.model.api.xml.UploadSingleInitiationResponse;
import org.finra.herd.service.impl.UploadDownloadServiceImpl.CompleteUploadSingleMessageResult;

/**
 * The upload download service.
 */
public interface UploadDownloadService
{
    /**
     * Initiates a single file upload capability by creating the relative business object data instance in UPLOADING state and allowing write access to a
     * specific location in S3_MANAGED_LOADING_DOCK storage.
     *
     * @param uploadSingleInitiationRequest the information needed to initiate a file upload
     *
     * @return the upload single initiation response
     */
    public UploadSingleInitiationResponse initiateUploadSingle(UploadSingleInitiationRequest uploadSingleInitiationRequest);

    /**
     * Performs the completion of upload single file. Runs in new transaction and logs the error if an error occurs.
     *
     * @param objectKey the object key.
     * 
     * @return CompleteUploadSingleMessageResult 
     */
    public CompleteUploadSingleMessageResult performCompleteUploadSingleMessage(String objectKey);

    /**
     * Returns information required to download object from S3 for the object registered against the given parameters.
     *
     * @param namespace - business object definition namespace
     * @param businessObjectDefinitionName - business object definition name
     * @param businessObjectFormatUsage - business object format usage code
     * @param businessObjectFormatFileType - business object format file type
     * @param businessObjectFormatVersion - business object format version
     * @param partitionValue - business object data partition value
     * @param businessObjectDataVersion - business object data version
     *
     * @return {@link DownloadSingleInitiationResponse}
     */
    public DownloadSingleInitiationResponse initiateDownloadSingle(String namespace, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue, Integer businessObjectDataVersion);

    /**
     * Returns extended credentials for an existing upload. This is needed because the temporary STS credentials that were returned in the
     * initiateUploadSingle method can be no longer than what STS allows (> 15 minutes and < 60 minutes).
     *
     * @param namespace - business object definition namespace
     * @param businessObjectDefinitionName - business object definition name
     * @param businessObjectFormatUsage - business object format usage code
     * @param businessObjectFormatFileType - business object format file type
     * @param businessObjectFormatVersion - business object format version
     * @param partitionValue - business object data partition value
     * @param businessObjectDataVersion - business object data version
     *
     * @return {@link UploadSingleCredentialExtensionResponse}
     */
    public UploadSingleCredentialExtensionResponse extendUploadSingleCredentials(String namespace, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue,
        Integer businessObjectDataVersion);
    
    /**
     * Returns information required to download object from S3 for business object definition sample file
     * @param downloadRequest download request for single sample file
     * @return download response
     */
    public DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationResponse initiateDownloadSingleSampleFile(
        DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest downloadRequest);

}

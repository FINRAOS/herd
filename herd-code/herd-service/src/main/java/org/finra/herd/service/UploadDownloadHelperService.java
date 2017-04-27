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

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.CompleteUploadSingleParamsDto;

public interface UploadDownloadHelperService
{
    /**
     * Prepares to move an S3 file from the source bucket to the target bucket. On success, both the target and source business object data statuses are set to
     * "RE-ENCRYPTING" and the DTO is updated accordingly.
     *
     * @param objectKey the object key (i.e. filename)
     * @param completeUploadSingleParamsDto the DTO to be initialized with parameters required for complete upload single message processing
     */
    public void prepareForFileMove(String objectKey, CompleteUploadSingleParamsDto completeUploadSingleParamsDto);

    /**
     * Moves an S3 file from the source bucket to the target bucket. Updates the target business object data status in the DTO based on the result of S3 copy
     * operation. If S3 copy fails, the target business object data status in the DTO is set to "INVALID", otherwise it is set to "VALID".
     *
     * @param completeUploadSingleParamsDto the DTO that contains complete upload single message parameters
     */
    public void performFileMove(CompleteUploadSingleParamsDto completeUploadSingleParamsDto);

    /**
     * Executes the steps required to complete the processing of complete upload single message following a successful S3 file move operation. The method also
     * updates the DTO that contains complete upload single message parameters.
     *
     * @param completeUploadSingleParamsDto the DTO that contains complete upload single message parameters
     */
    public void executeFileMoveAfterSteps(CompleteUploadSingleParamsDto completeUploadSingleParamsDto);

    /**
     * delete the source file from S3
     * 
     * @param completeUploadSingleParamsDto the DTO that contains complete upload single message parameters
     */
    void deleteSourceFileFromS3(CompleteUploadSingleParamsDto completeUploadSingleParamsDto);

    /**
     * Updates the business object data status in a new transaction.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataStatus the business object data status
     */
    public void updateBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus);

    /**
     * Asserts that the S3 object in the specified bucket and key does not exist.
     *
     * @param bucketName The S3 bucket name
     * @param key The S3 key
     */
    public void assertS3ObjectKeyDoesNotExist(String bucketName, String key);
}

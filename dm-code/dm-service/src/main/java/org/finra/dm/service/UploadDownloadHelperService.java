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
package org.finra.dm.service;

import org.finra.dm.model.dto.AwsParamsDto;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;

public interface UploadDownloadHelperService
{
    /**
     * Synchronously moves the file to target bucket from source bucket and updates the source business object data status to DELETED and target business object
     * data status to VALID. If S3 copy fails, marks the target BData to INVALID and source BData to DELETED.
     *
     * @param sourceBusinessObjectDataKey, the source business object data key
     * @param targetBusinessObjectDataKey, the target business object data key
     * @param sourceBucketName, the source bucket name
     * @param targetBucketName, the target bucket name
     * @param filePath, the file path
     * @param kmsKeyId, the KMS id for target bucket
     * @param awsParams, the aws parameters
     *
     * @return Array containing status updates to source and target business object data, null if no changes happened.
     */
    public String[] performFileMoveSync(BusinessObjectDataKey sourceBusinessObjectDataKey, BusinessObjectDataKey targetBusinessObjectDataKey,
        String sourceBucketName, String targetBucketName, String filePath, String kmsKeyId, AwsParamsDto awsParams);

    /**
     * Updates the business object data status in a new transaction.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataStatus the business object data status
     */
    public void updateBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus);
}

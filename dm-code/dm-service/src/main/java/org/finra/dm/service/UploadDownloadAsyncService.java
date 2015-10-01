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

import java.util.concurrent.Future;

import org.finra.dm.model.dto.AwsParamsDto;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;

public interface UploadDownloadAsyncService
{
    /**
     * Asynchronously moves the file to target bucket from source bucket and updates the source business object data status to DELETED and target business
     * object data status to VALID.
     * If S3 copy fails, marks the target BData to INVALID and source BData to DELETED.
     *
     * @param sourceBusinessObjectDataKey, the source business object data key
     * @param targetBusinessObjectDataKey, the target business object data key
     * @param sourceBucketName, the source bucket name
     * @param destinationBucketName, the destination bucket name
     * @param filePath, the file path
     * @param kmsKeyId, the AWS KMS id for the destination bucket
     * @param awsParams, the aws parameters
     *
     * @return the result of an asynchronous call
     */
    public Future<Void> performFileMoveAsync(BusinessObjectDataKey sourceBusinessObjectDataKey, BusinessObjectDataKey targetBusinessObjectDataKey,
            String sourceBucketName, String destinationBucketName, String filePath, String kmsKeyId, AwsParamsDto awsParams);
}

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
package org.finra.dm.service.impl;

import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.dto.AwsParamsDto;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.service.UploadDownloadAsyncService;
import org.finra.dm.service.UploadDownloadHelperService;

/**
 * A service class for UploadDownloadService asynchronous functions.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
public class UploadDownloadAsyncServiceImpl implements UploadDownloadAsyncService
{
    @Autowired
    private UploadDownloadHelperService uploadDownloadHelperService;

    /**
     * {@inheritDoc}
     */
    @Override
    @Async
    public Future<Void> performFileMoveAsync(BusinessObjectDataKey sourceBusinessObjectDataKey, BusinessObjectDataKey targetBusinessObjectDataKey,
        String sourceBucketName, String destinationBucketName, String filePath, String kmsKeyId, AwsParamsDto awsParams)
    {
        performFileMoveSync(sourceBusinessObjectDataKey, targetBusinessObjectDataKey, sourceBucketName, destinationBucketName, filePath, kmsKeyId, awsParams);

        // Return an AsyncResult so callers will know the future is "done". They can call "isDone" to know when this method has completed and they
        // can call "get" to see if any exceptions were thrown.
        return new AsyncResult<>(null);
    }

    /**
     * Performs a synchronous file move.
     *
     * @param sourceBusinessObjectDataKey the source business object data key.
     * @param targetBusinessObjectDataKey the target business object data key.
     * @param sourceBucketName the source bucket name.
     * @param destinationBucketName the destination bucket name.
     * @param filePath the file path to move.
     * @param kmsKeyId the target KMS key Id.
     * @param awsParams the AWS parameters.
     */
    protected void performFileMoveSync(BusinessObjectDataKey sourceBusinessObjectDataKey, BusinessObjectDataKey targetBusinessObjectDataKey,
        String sourceBucketName, String destinationBucketName, String filePath, String kmsKeyId, AwsParamsDto awsParams)
    {
        uploadDownloadHelperService
            .performFileMoveSync(sourceBusinessObjectDataKey, targetBusinessObjectDataKey, sourceBucketName, destinationBucketName, filePath, kmsKeyId,
                awsParams);
    }
}

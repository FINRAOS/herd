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
package org.finra.herd.model.dto;

/**
 * A DTO that holds various parameters for making an S3 file copy request.
 */
public class S3FileCopyRequestParamsDto extends S3FileTransferRequestParamsDto
{
    /**
     * The source S3 bucket name.
     */
    private String sourceBucketName;

    /**
     * The target S3 bucket name.
     */
    private String targetBucketName;

    /**
     * The source S3 object key
     */
    private String sourceObjectKey;

    /**
     * The target S3 object key
     */
    private String targetObjectKey;

    public String getSourceBucketName()
    {
        return sourceBucketName;
    }

    public void setSourceBucketName(String sourceBucketName)
    {
        this.sourceBucketName = sourceBucketName;
    }

    public String getTargetBucketName()
    {
        return targetBucketName;
    }

    public void setTargetBucketName(String targetBucketName)
    {
        this.targetBucketName = targetBucketName;
    }

    public String getSourceObjectKey()
    {
        return sourceObjectKey;
    }

    public void setSourceObjectKey(String sourceObjectKey)
    {
        this.sourceObjectKey = sourceObjectKey;
    }

    public String getTargetObjectKey()
    {
        return targetObjectKey;
    }

    public void setTargetObjectKey(String targetObjectKey)
    {
        this.targetObjectKey = targetObjectKey;
    }
}

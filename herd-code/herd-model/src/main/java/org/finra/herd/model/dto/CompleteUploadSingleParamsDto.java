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

import org.finra.herd.model.api.xml.BusinessObjectDataKey;

/**
 * A DTO that holds various parameters needed to perform a complete upload single message processing.
 */
public class CompleteUploadSingleParamsDto
{
    /**
     * Default no-arg constructor.
     */
    public CompleteUploadSingleParamsDto()
    {
        // This is intentionally empty, nothing needed here.
    }

    /**
     * Fully-initialising value constructor.
     *
     * @param sourceBusinessObjectDataKey the source business object data key
     * @param sourceBucketName the source bucket name
     * @param sourceFilePath the source file path
     * @param sourceOldStatus the old status of the source business object data
     * @param sourceNewStatus the new status of the source business object data
     * @param targetBusinessObjectDataKey the target business object data key
     * @param targetBucketName the target bucket name
     * @param targetFilePath the target file path
     * @param targetOldStatus the old status of the target business object data
     * @param targetNewStatus the new status of the target business object data
     * @param kmsKeyId the KMS ID for the target bucket
     * @param awsParams the AWS parameters
     */
    public CompleteUploadSingleParamsDto(final BusinessObjectDataKey sourceBusinessObjectDataKey, final String sourceBucketName, final String sourceFilePath,
        final String sourceOldStatus, final String sourceNewStatus, final BusinessObjectDataKey targetBusinessObjectDataKey, final String targetBucketName,
        final String targetFilePath, final String targetOldStatus, final String targetNewStatus, final String kmsKeyId, final AwsParamsDto awsParams)
    {
        this.sourceBusinessObjectDataKey = sourceBusinessObjectDataKey;
        this.sourceBucketName = sourceBucketName;
        this.sourceFilePath = sourceFilePath;
        this.sourceOldStatus = sourceOldStatus;
        this.sourceNewStatus = sourceNewStatus;
        this.targetBusinessObjectDataKey = targetBusinessObjectDataKey;
        this.targetBucketName = targetBucketName;
        this.targetFilePath = targetFilePath;
        this.targetOldStatus = targetOldStatus;
        this.targetNewStatus = targetNewStatus;
        this.kmsKeyId = kmsKeyId;
        this.awsParams = awsParams;
    }

    /**
     * The source business object data key.
     */
    private BusinessObjectDataKey sourceBusinessObjectDataKey;

    /**
     * The source bucket name.
     */
    private String sourceBucketName;

    /**
     * The source file path.
     */
    private String sourceFilePath;

    /**
     * The old status of the source business object data.
     */
    private String sourceOldStatus;

    /**
     * The new status of the source business object data.
     */
    private String sourceNewStatus;

    /**
     * The target business object data key.
     */
    private BusinessObjectDataKey targetBusinessObjectDataKey;

    /**
     * The target bucket name.
     */
    private String targetBucketName;

    /**
     * The target file path.
     */
    private String targetFilePath;

    /**
     * The old status of the target business object data.
     */
    private String targetOldStatus;

    /**
     * The new status of the target business object data.
     */
    private String targetNewStatus;

    /**
     * The KMS ID for the target bucket.
     */
    private String kmsKeyId;

    /**
     * The AWS parameters.
     */
    private AwsParamsDto awsParams;

    public BusinessObjectDataKey getSourceBusinessObjectDataKey()
    {
        return sourceBusinessObjectDataKey;
    }

    public void setSourceBusinessObjectDataKey(BusinessObjectDataKey sourceBusinessObjectDataKey)
    {
        this.sourceBusinessObjectDataKey = sourceBusinessObjectDataKey;
    }

    public String getSourceBucketName()
    {
        return sourceBucketName;
    }

    public void setSourceBucketName(String sourceBucketName)
    {
        this.sourceBucketName = sourceBucketName;
    }

    public String getSourceFilePath()
    {
        return sourceFilePath;
    }

    public void setSourceFilePath(String sourceFilePath)
    {
        this.sourceFilePath = sourceFilePath;
    }

    public String getSourceOldStatus()
    {
        return sourceOldStatus;
    }

    public void setSourceOldStatus(String sourceOldStatus)
    {
        this.sourceOldStatus = sourceOldStatus;
    }

    public String getSourceNewStatus()
    {
        return sourceNewStatus;
    }

    public void setSourceNewStatus(String sourceNewStatus)
    {
        this.sourceNewStatus = sourceNewStatus;
    }

    public BusinessObjectDataKey getTargetBusinessObjectDataKey()
    {
        return targetBusinessObjectDataKey;
    }

    public void setTargetBusinessObjectDataKey(BusinessObjectDataKey targetBusinessObjectDataKey)
    {
        this.targetBusinessObjectDataKey = targetBusinessObjectDataKey;
    }

    public String getTargetBucketName()
    {
        return targetBucketName;
    }

    public void setTargetBucketName(String targetBucketName)
    {
        this.targetBucketName = targetBucketName;
    }

    public String getTargetFilePath()
    {
        return targetFilePath;
    }

    public void setTargetFilePath(String targetFilePath)
    {
        this.targetFilePath = targetFilePath;
    }

    public String getTargetOldStatus()
    {
        return targetOldStatus;
    }

    public void setTargetOldStatus(String targetOldStatus)
    {
        this.targetOldStatus = targetOldStatus;
    }

    public String getTargetNewStatus()
    {
        return targetNewStatus;
    }

    public void setTargetNewStatus(String targetNewStatus)
    {
        this.targetNewStatus = targetNewStatus;
    }

    public String getKmsKeyId()
    {
        return kmsKeyId;
    }

    public void setKmsKeyId(String kmsKeyId)
    {
        this.kmsKeyId = kmsKeyId;
    }

    public AwsParamsDto getAwsParams()
    {
        return awsParams;
    }

    public void setAwsParams(AwsParamsDto awsParams)
    {
        this.awsParams = awsParams;
    }
}

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
    
    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (object == null || getClass() != object.getClass())
        {
            return false;
        }
        if (!super.equals(object))
        {
            return false;
        }

        S3FileCopyRequestParamsDto that = (S3FileCopyRequestParamsDto) object;

        if (sourceBucketName != null ? !sourceBucketName.equals(that.sourceBucketName) : that.sourceBucketName != null)
        {
            return false;
        }
        if (sourceObjectKey != null ? !sourceObjectKey.equals(that.sourceObjectKey) : that.sourceObjectKey != null)
        {
            return false;
        }
        if (targetBucketName != null ? !targetBucketName.equals(that.targetBucketName) : that.targetBucketName != null)
        {
            return false;
        }
        if (targetObjectKey != null ? !targetObjectKey.equals(that.targetObjectKey) : that.targetObjectKey != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (sourceBucketName != null ? sourceBucketName.hashCode() : 0);
        result = 31 * result + (targetBucketName != null ? targetBucketName.hashCode() : 0);
        result = 31 * result + (sourceObjectKey != null ? sourceObjectKey.hashCode() : 0);
        result = 31 * result + (targetObjectKey != null ? targetObjectKey.hashCode() : 0);
        return result;
    }
}

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
import org.finra.herd.model.api.xml.StoragePolicyKey;

/**
 * A DTO that holds various parameters needed to perform a business object data retry storage policy transition.
 */
public class BusinessObjectDataRetryStoragePolicyTransitionDto
{
    /**
     * The business object data key.
     */
    private BusinessObjectDataKey businessObjectDataKey;

    /**
     * The Glacier AWS S3 bucket name.
     */
    private String glacierBucketName;

    /**
     * The Glacier S3 key prefix.
     */
    private String glacierS3KeyPrefix;

    /**
     * The Glacier storage name.
     */
    private String glacierStorageName;

    /**
     * SQS queue name for storage policy selector.
     */
    private String sqsQueueName;

    /**
     * The storage policy key.
     */
    private StoragePolicyKey storagePolicyKey;

    /**
     * The storage policy version.
     */
    private Integer storagePolicyVersion;

    /**
     * Default no-arg constructor.
     */
    public BusinessObjectDataRetryStoragePolicyTransitionDto()
    {
        // This is intentionally empty, nothing needed here.
    }

    /**
     * Fully-initialising value constructor.
     *
     * @param businessObjectDataKey the business object data key
     * @param storagePolicyKey the storage policy key
     * @param storagePolicyVersion the storage policy version
     * @param glacierStorageName the Glacier storage name
     * @param glacierBucketName the Glacier S3 bucket name
     * @param glacierS3KeyPrefix the Glacier S3 key prefix
     * @param sqsQueueName the SQS queue name
     */
    public BusinessObjectDataRetryStoragePolicyTransitionDto(final BusinessObjectDataKey businessObjectDataKey, final StoragePolicyKey storagePolicyKey,
        final Integer storagePolicyVersion, final String glacierStorageName, final String glacierBucketName, final String glacierS3KeyPrefix,
        final String sqsQueueName)
    {
        this.businessObjectDataKey = businessObjectDataKey;
        this.storagePolicyKey = storagePolicyKey;
        this.storagePolicyVersion = storagePolicyVersion;
        this.glacierStorageName = glacierStorageName;
        this.glacierBucketName = glacierBucketName;
        this.glacierS3KeyPrefix = glacierS3KeyPrefix;
        this.sqsQueueName = sqsQueueName;
    }

    public BusinessObjectDataKey getBusinessObjectDataKey()
    {
        return businessObjectDataKey;
    }

    public void setBusinessObjectDataKey(BusinessObjectDataKey businessObjectDataKey)
    {
        this.businessObjectDataKey = businessObjectDataKey;
    }

    public String getGlacierBucketName()
    {
        return glacierBucketName;
    }

    public void setGlacierBucketName(String glacierBucketName)
    {
        this.glacierBucketName = glacierBucketName;
    }

    public String getGlacierS3KeyPrefix()
    {
        return glacierS3KeyPrefix;
    }

    public void setGlacierS3KeyPrefix(String glacierS3KeyPrefix)
    {
        this.glacierS3KeyPrefix = glacierS3KeyPrefix;
    }

    public String getGlacierStorageName()
    {
        return glacierStorageName;
    }

    public void setGlacierStorageName(String glacierStorageName)
    {
        this.glacierStorageName = glacierStorageName;
    }

    public String getSqsQueueName()
    {
        return sqsQueueName;
    }

    public void setSqsQueueName(String sqsQueueName)
    {
        this.sqsQueueName = sqsQueueName;
    }

    public StoragePolicyKey getStoragePolicyKey()
    {
        return storagePolicyKey;
    }

    public void setStoragePolicyKey(StoragePolicyKey storagePolicyKey)
    {
        this.storagePolicyKey = storagePolicyKey;
    }

    public Integer getStoragePolicyVersion()
    {
        return storagePolicyVersion;
    }

    public void setStoragePolicyVersion(Integer storagePolicyVersion)
    {
        this.storagePolicyVersion = storagePolicyVersion;
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (!(object instanceof BusinessObjectDataRetryStoragePolicyTransitionDto))
        {
            return false;
        }

        BusinessObjectDataRetryStoragePolicyTransitionDto that = (BusinessObjectDataRetryStoragePolicyTransitionDto) object;

        if (businessObjectDataKey != null ? !businessObjectDataKey.equals(that.businessObjectDataKey) : that.businessObjectDataKey != null)
        {
            return false;
        }
        if (glacierBucketName != null ? !glacierBucketName.equals(that.glacierBucketName) : that.glacierBucketName != null)
        {
            return false;
        }
        if (glacierS3KeyPrefix != null ? !glacierS3KeyPrefix.equals(that.glacierS3KeyPrefix) : that.glacierS3KeyPrefix != null)
        {
            return false;
        }
        if (glacierStorageName != null ? !glacierStorageName.equals(that.glacierStorageName) : that.glacierStorageName != null)
        {
            return false;
        }
        if (sqsQueueName != null ? !sqsQueueName.equals(that.sqsQueueName) : that.sqsQueueName != null)
        {
            return false;
        }
        if (storagePolicyKey != null ? !storagePolicyKey.equals(that.storagePolicyKey) : that.storagePolicyKey != null)
        {
            return false;
        }
        if (storagePolicyVersion != null ? !storagePolicyVersion.equals(that.storagePolicyVersion) : that.storagePolicyVersion != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = businessObjectDataKey != null ? businessObjectDataKey.hashCode() : 0;
        result = 31 * result + (glacierBucketName != null ? glacierBucketName.hashCode() : 0);
        result = 31 * result + (glacierS3KeyPrefix != null ? glacierS3KeyPrefix.hashCode() : 0);
        result = 31 * result + (glacierStorageName != null ? glacierStorageName.hashCode() : 0);
        result = 31 * result + (sqsQueueName != null ? sqsQueueName.hashCode() : 0);
        result = 31 * result + (storagePolicyKey != null ? storagePolicyKey.hashCode() : 0);
        result = 31 * result + (storagePolicyVersion != null ? storagePolicyVersion.hashCode() : 0);
        return result;
    }
}

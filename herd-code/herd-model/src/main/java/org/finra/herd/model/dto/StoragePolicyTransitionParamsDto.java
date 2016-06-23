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

import java.util.List;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;

/**
 * A DTO that holds various parameters needed to perform a storage policy transition.
 */
public class StoragePolicyTransitionParamsDto
{
    /**
     * Default no-arg constructor.
     */
    public StoragePolicyTransitionParamsDto()
    {
        // This is intentionally empty, nothing needed here.
    }

    /**
     * Fully-initialising value constructor.
     *
     * @param businessObjectDataKey the business object data key
     * @param sourceStorageName the source storage name
     * @param sourceBucketName the source S3 bucket name
     * @param sourceS3KeyPrefix the source S3 key prefix
     * @param sourceStorageFiles the list of source storage files
     * @param destinationStorageName the destination storage name
     * @param destinationBucketName the destination S3 bucket name
     * @param destinationS3KeyBasePrefix the destination S3 key base prefix
     */
    public StoragePolicyTransitionParamsDto(final BusinessObjectDataKey businessObjectDataKey, final String sourceStorageName, final String sourceBucketName,
        final String sourceS3KeyPrefix, final List<StorageFile> sourceStorageFiles, final String destinationStorageName, final String destinationBucketName,
        final String destinationS3KeyBasePrefix)
    {
        this.businessObjectDataKey = businessObjectDataKey;
        this.sourceStorageName = sourceStorageName;
        this.sourceBucketName = sourceBucketName;
        this.sourceS3KeyPrefix = sourceS3KeyPrefix;
        this.sourceStorageFiles = sourceStorageFiles;
        this.destinationStorageName = destinationStorageName;
        this.destinationBucketName = destinationBucketName;
        this.destinationS3KeyBasePrefix = destinationS3KeyBasePrefix;
    }

    /**
     * The business object data key.
     */
    private BusinessObjectDataKey businessObjectDataKey;

    /**
     * The source storage name.
     */
    private String sourceStorageName;

    /**
     * The source AWS S3 bucket name.
     */
    private String sourceBucketName;

    /**
     * The source S3 key prefix.
     */
    private String sourceS3KeyPrefix;

    /**
     * The source storage files.
     */
    private List<StorageFile> sourceStorageFiles;

    /**
     * The destination storage name.
     */
    private String destinationStorageName;

    /**
     * The destination AWS S3 bucket name.
     */
    private String destinationBucketName;

    /**
     * The destination S3 key base prefix. To build the actual destination S3 key prefix, please concatenate this base prefix with the source S3 key prefix
     * using "/" character as a separator.
     */
    private String destinationS3KeyBasePrefix;

    public BusinessObjectDataKey getBusinessObjectDataKey()
    {
        return businessObjectDataKey;
    }

    public void setBusinessObjectDataKey(BusinessObjectDataKey businessObjectDataKey)
    {
        this.businessObjectDataKey = businessObjectDataKey;
    }

    public String getSourceStorageName()
    {
        return sourceStorageName;
    }

    public void setSourceStorageName(String sourceStorageName)
    {
        this.sourceStorageName = sourceStorageName;
    }

    public String getSourceBucketName()
    {
        return sourceBucketName;
    }

    public void setSourceBucketName(String sourceBucketName)
    {
        this.sourceBucketName = sourceBucketName;
    }

    public String getSourceS3KeyPrefix()
    {
        return sourceS3KeyPrefix;
    }

    public void setSourceS3KeyPrefix(String sourceS3KeyPrefix)
    {
        this.sourceS3KeyPrefix = sourceS3KeyPrefix;
    }

    public List<StorageFile> getSourceStorageFiles()
    {
        return sourceStorageFiles;
    }

    public void setSourceStorageFiles(List<StorageFile> sourceStorageFiles)
    {
        this.sourceStorageFiles = sourceStorageFiles;
    }

    public String getDestinationStorageName()
    {
        return destinationStorageName;
    }

    public void setDestinationStorageName(String destinationStorageName)
    {
        this.destinationStorageName = destinationStorageName;
    }

    public String getDestinationBucketName()
    {
        return destinationBucketName;
    }

    public void setDestinationBucketName(String destinationBucketName)
    {
        this.destinationBucketName = destinationBucketName;
    }

    public String getDestinationS3KeyBasePrefix()
    {
        return destinationS3KeyBasePrefix;
    }

    public void setDestinationS3KeyBasePrefix(String destinationS3KeyBasePrefix)
    {
        this.destinationS3KeyBasePrefix = destinationS3KeyBasePrefix;
    }
}

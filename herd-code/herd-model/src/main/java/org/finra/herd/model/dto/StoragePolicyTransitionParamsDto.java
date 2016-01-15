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
     * @param sourceStorageUnitId the source storage unit id
     * @param sourceS3KeyPrefix the source S3 key prefix
     * @param sourceStorageFiles the list of source storage files
     * @param sourceStorageFilesSizeBytes the total size in bytes of source storage files
     * @param destinationStorageName the destination storage name
     * @param destinationVaultName the destination Glacier vault name
     * @param destinationStorageFile the destination storage file
     */
    public StoragePolicyTransitionParamsDto(final BusinessObjectDataKey businessObjectDataKey, final String sourceStorageName, final String sourceBucketName,
        final Integer sourceStorageUnitId, final String sourceS3KeyPrefix, final List<StorageFile> sourceStorageFiles, final long sourceStorageFilesSizeBytes,
        final String destinationStorageName, final String destinationVaultName, final StorageFile destinationStorageFile)
    {
        this.businessObjectDataKey = businessObjectDataKey;
        this.sourceStorageName = sourceStorageName;
        this.sourceBucketName = sourceBucketName;
        this.sourceStorageUnitId = sourceStorageUnitId;
        this.sourceS3KeyPrefix = sourceS3KeyPrefix;
        this.sourceStorageFiles = sourceStorageFiles;
        this.sourceStorageFilesSizeBytes = sourceStorageFilesSizeBytes;
        this.destinationStorageName = destinationStorageName;
        this.destinationVaultName = destinationVaultName;
        this.destinationStorageFile = destinationStorageFile;
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
     * The source storage unit id.
     */
    private Integer sourceStorageUnitId;

    /**
     * The source S3 key prefix.
     */
    private String sourceS3KeyPrefix;

    /**
     * The source storage files.
     */
    private List<StorageFile> sourceStorageFiles;

    /**
     * The total size in bytes of source storage files.
     */
    private long sourceStorageFilesSizeBytes;

    /**
     * The destination storage name.
     */
    private String destinationStorageName;

    /**
     * The destination AWS Glacier vault name.
     */
    private String destinationVaultName;

    /**
     * The destination storage file.
     */
    private StorageFile destinationStorageFile;

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

    public Integer getSourceStorageUnitId()
    {
        return sourceStorageUnitId;
    }

    public void setSourceStorageUnitId(Integer sourceStorageUnitId)
    {
        this.sourceStorageUnitId = sourceStorageUnitId;
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

    public long getSourceStorageFilesSizeBytes()
    {
        return sourceStorageFilesSizeBytes;
    }

    public void setSourceStorageFilesSizeBytes(long sourceStorageFilesSizeBytes)
    {
        this.sourceStorageFilesSizeBytes = sourceStorageFilesSizeBytes;
    }

    public String getDestinationStorageName()
    {
        return destinationStorageName;
    }

    public void setDestinationStorageName(String destinationStorageName)
    {
        this.destinationStorageName = destinationStorageName;
    }

    public String getDestinationVaultName()
    {
        return destinationVaultName;
    }

    public void setDestinationVaultName(String destinationVaultName)
    {
        this.destinationVaultName = destinationVaultName;
    }

    public StorageFile getDestinationStorageFile()
    {
        return destinationStorageFile;
    }

    public void setDestinationStorageFile(StorageFile destinationStorageFile)
    {
        this.destinationStorageFile = destinationStorageFile;
    }
}

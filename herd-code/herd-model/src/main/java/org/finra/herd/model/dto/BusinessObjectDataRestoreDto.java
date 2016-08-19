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
 * A DTO that holds various parameters needed to perform a business object data restore.
 */
public class BusinessObjectDataRestoreDto
{
    /**
     * Default no-arg constructor.
     */
    public BusinessObjectDataRestoreDto()
    {
        // This is intentionally empty, nothing needed here.
    }

    /**
     * Fully-initialising value constructor.
     *
     * @param businessObjectDataKey the business object data key
     * @param originStorageName the origin storage name
     * @param originBucketName the origin S3 bucket name
     * @param originS3KeyPrefix the origin S3 key prefix
     * @param oldOriginStorageUnitStatus the old (previous) status of the origin storage unit
     * @param newOriginStorageUnitStatus the new status of the origin storage unit
     * @param originStorageFiles the list of origin storage files
     * @param glacierStorageName the Glacier storage name
     * @param glacierBucketName the Glacier S3 bucket name
     * @param glacierS3KeyBasePrefix the Glacier S3 key base prefix
     * @param glacierS3KeyPrefix the Glacier S3 key prefix
     * @param exception the exception
     */
    public BusinessObjectDataRestoreDto(final BusinessObjectDataKey businessObjectDataKey, final String originStorageName, final String originBucketName,
        final String originS3KeyPrefix, final String oldOriginStorageUnitStatus, final String newOriginStorageUnitStatus,
        final List<StorageFile> originStorageFiles, final String glacierStorageName, final String glacierBucketName, final String glacierS3KeyBasePrefix,
        final String glacierS3KeyPrefix, final Exception exception)
    {
        this.businessObjectDataKey = businessObjectDataKey;
        this.originStorageName = originStorageName;
        this.originBucketName = originBucketName;
        this.originS3KeyPrefix = originS3KeyPrefix;
        this.oldOriginStorageUnitStatus = oldOriginStorageUnitStatus;
        this.newOriginStorageUnitStatus = newOriginStorageUnitStatus;
        this.originStorageFiles = originStorageFiles;
        this.glacierStorageName = glacierStorageName;
        this.glacierBucketName = glacierBucketName;
        this.glacierS3KeyBasePrefix = glacierS3KeyBasePrefix;
        this.glacierS3KeyPrefix = glacierS3KeyPrefix;
        this.exception = exception;
    }

    /**
     * The business object data key.
     */
    private BusinessObjectDataKey businessObjectDataKey;

    /**
     * The origin storage name.
     */
    private String originStorageName;

    /**
     * The origin AWS S3 bucket name.
     */
    private String originBucketName;

    /**
     * The origin S3 key prefix.
     */
    private String originS3KeyPrefix;

    /**
     * The old status of the origin storage unit.
     */
    private String oldOriginStorageUnitStatus;

    /**
     * The new status of the origin storage unit.
     */
    private String newOriginStorageUnitStatus;

    /**
     * The origin storage files.
     */
    private List<StorageFile> originStorageFiles;

    /**
     * The Glacier storage name.
     */
    private String glacierStorageName;

    /**
     * The Glacier AWS S3 bucket name.
     */
    private String glacierBucketName;

    /**
     * The Glacier S3 key base prefix. To build the full Glacier S3 key prefix, please concatenate the base prefix with the origin S3 key prefix using "/"
     * character as a separator.
     */
    private String glacierS3KeyBasePrefix;

    /**
     * The Glacier S3 key prefix.
     */
    private String glacierS3KeyPrefix;

    /**
     * This field points to an exception that could be thrown when executing the initiate a business object data restore request.
     */
    private Exception exception;

    public BusinessObjectDataKey getBusinessObjectDataKey()
    {
        return businessObjectDataKey;
    }

    public void setBusinessObjectDataKey(BusinessObjectDataKey businessObjectDataKey)
    {
        this.businessObjectDataKey = businessObjectDataKey;
    }

    public String getOriginStorageName()
    {
        return originStorageName;
    }

    public void setOriginStorageName(String originStorageName)
    {
        this.originStorageName = originStorageName;
    }

    public String getOriginBucketName()
    {
        return originBucketName;
    }

    public void setOriginBucketName(String originBucketName)
    {
        this.originBucketName = originBucketName;
    }

    public String getOriginS3KeyPrefix()
    {
        return originS3KeyPrefix;
    }

    public void setOriginS3KeyPrefix(String originS3KeyPrefix)
    {
        this.originS3KeyPrefix = originS3KeyPrefix;
    }

    public String getOldOriginStorageUnitStatus()
    {
        return oldOriginStorageUnitStatus;
    }

    public void setOldOriginStorageUnitStatus(String oldOriginStorageUnitStatus)
    {
        this.oldOriginStorageUnitStatus = oldOriginStorageUnitStatus;
    }

    public String getNewOriginStorageUnitStatus()
    {
        return newOriginStorageUnitStatus;
    }

    public void setNewOriginStorageUnitStatus(String newOriginStorageUnitStatus)
    {
        this.newOriginStorageUnitStatus = newOriginStorageUnitStatus;
    }

    public List<StorageFile> getOriginStorageFiles()
    {
        return originStorageFiles;
    }

    public void setOriginStorageFiles(List<StorageFile> originStorageFiles)
    {
        this.originStorageFiles = originStorageFiles;
    }

    public String getGlacierStorageName()
    {
        return glacierStorageName;
    }

    public void setGlacierStorageName(String glacierStorageName)
    {
        this.glacierStorageName = glacierStorageName;
    }

    public String getGlacierBucketName()
    {
        return glacierBucketName;
    }

    public void setGlacierBucketName(String glacierBucketName)
    {
        this.glacierBucketName = glacierBucketName;
    }

    public String getGlacierS3KeyBasePrefix()
    {
        return glacierS3KeyBasePrefix;
    }

    public void setGlacierS3KeyBasePrefix(String glacierS3KeyBasePrefix)
    {
        this.glacierS3KeyBasePrefix = glacierS3KeyBasePrefix;
    }

    public String getGlacierS3KeyPrefix()
    {
        return glacierS3KeyPrefix;
    }

    public void setGlacierS3KeyPrefix(String glacierS3KeyPrefix)
    {
        this.glacierS3KeyPrefix = glacierS3KeyPrefix;
    }

    public Exception getException()
    {
        return exception;
    }

    public void setException(Exception exception)
    {
        this.exception = exception;
    }
}

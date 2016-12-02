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
package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.StoragePolicyDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;

/**
 * Helper for storage policy related operations which require DAO.
 */
@Component
public class StoragePolicyDaoHelper
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StoragePolicyDao storagePolicyDao;

    /**
     * Gets a storage policy entity based on the key and makes sure that it exists.
     *
     * @param storagePolicyKey the storage policy key
     *
     * @return the storage policy entity
     * @throws ObjectNotFoundException if the entity doesn't exist
     */
    public StoragePolicyEntity getStoragePolicyEntityByKey(StoragePolicyKey storagePolicyKey) throws ObjectNotFoundException
    {
        StoragePolicyEntity storagePolicyEntity = storagePolicyDao.getStoragePolicyByAltKey(storagePolicyKey);

        if (storagePolicyEntity == null)
        {
            throw new ObjectNotFoundException(String
                .format("Storage policy with name \"%s\" does not exist for \"%s\" namespace.", storagePolicyKey.getStoragePolicyName(),
                    storagePolicyKey.getNamespace()));
        }

        return storagePolicyEntity;
    }

    /**
     * Gets a storage policy entity based on the key and version and makes sure that it exists.
     *
     * @param storagePolicyKey the storage policy key
     * @param storagePolicyVersion the storage policy version
     *
     * @return the storage policy entity
     * @throws ObjectNotFoundException if the entity doesn't exist
     */
    public StoragePolicyEntity getStoragePolicyEntityByKeyAndVersion(StoragePolicyKey storagePolicyKey, Integer storagePolicyVersion)
        throws ObjectNotFoundException
    {
        StoragePolicyEntity storagePolicyEntity = storagePolicyDao.getStoragePolicyByAltKeyAndVersion(storagePolicyKey, storagePolicyVersion);

        if (storagePolicyEntity == null)
        {
            throw new ObjectNotFoundException(String
                .format("Storage policy with name \"%s\" and version \"%d\" does not exist for \"%s\" namespace.", storagePolicyKey.getStoragePolicyName(),
                    storagePolicyVersion, storagePolicyKey.getNamespace()));
        }

        return storagePolicyEntity;
    }

    /**
     * Validates the destination storage.
     *
     * @param storageEntity the destination storage entity
     */
    public void validateDestinationStorage(StorageEntity storageEntity)
    {
        Assert.isTrue(StoragePlatformEntity.GLACIER.equals(storageEntity.getStoragePlatform().getName()),
            String.format("Storage platform for destination storage with name \"%s\" is not \"%s\".", storageEntity.getName(), StoragePlatformEntity.GLACIER));

        // Validate that storage policy transition destination storage has S3 bucket name configured for the "archive" S3 bucket.
        // The "archive" S3 bucket is an S3 bucket that has an object lifecycle rule that moves data to Glacier after 0 days.
        // Please note that since S3 bucket name attribute value is required we pass a "true" flag.
        storageHelper.getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageEntity, true);
    }

    /**
     * Validates the source storage.
     *
     * @param storageEntity the storage entity
     */
    public void validateSourceStorage(StorageEntity storageEntity)
    {
        // Validate that storage platform is S3 for the storage policy filter storage.
        Assert.isTrue(StoragePlatformEntity.S3.equals(storageEntity.getStoragePlatform().getName()),
            String.format("Storage platform for storage with name \"%s\" is not \"%s\".", storageEntity.getName(), StoragePlatformEntity.S3));

        // Validate that storage policy filter storage has S3 bucket name configured.
        // Please note that since S3 bucket name attribute value is required we pass a "true" flag.
        storageHelper.getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageEntity, true);

        // Validate that storage policy filter storage has the S3 path prefix validation enabled.
        if (!storageHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true))
        {
            throw new IllegalStateException(String.format("Path prefix validation must be enabled on \"%s\" storage.", storageEntity.getName()));
        }

        // Validate that storage policy filter storage has the S3 file existence validation enabled.
        if (!storageHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true))
        {
            throw new IllegalStateException(String.format("File existence validation must be enabled on \"%s\" storage.", storageEntity.getName()));
        }
    }
}

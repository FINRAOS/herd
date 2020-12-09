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

import java.util.List;

import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.dao.StoragePolicyDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.NamespaceEntity;
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

    @Autowired
    private NamespaceDao namespaceDao;

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
     * Validates the storage policy filter storage.
     *
     * @param storageEntity the storage entity
     */
    public void validateStoragePolicyFilterStorage(StorageEntity storageEntity)
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

    /**
     * Gets a list of keys for all storage policy keys defined in the system for the specified namespace.
     *
     * @param namespace the name space
     *
     * @return list of storage policy keys
     */
    public List<StoragePolicyKey> getStoragePolicyKeys(String namespace)
    {
        // Try to retrieve the relative namespace entity.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(namespace);

        // If namespace entity exists, retrieve storage policy keys by namespace entity.
        if (namespaceEntity != null)
        {
            return storagePolicyDao.getStoragePolicyKeysByNamespace(namespaceEntity);
        }
        else
        {
            return Lists.newArrayList();
        }
    }
}

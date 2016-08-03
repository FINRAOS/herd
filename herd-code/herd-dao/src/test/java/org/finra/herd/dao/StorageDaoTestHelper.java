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
package org.finra.herd.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.StorageAttributeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;

@Component
public class StorageDaoTestHelper
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private StorageDao storageDao;

    @Autowired
    private StoragePlatformDao storagePlatformDao;

    @Autowired
    private StoragePlatformDaoTestHelper storagePlatformDaoTestHelper;

    /**
     * Creates and persists a new storage attribute entity.
     *
     * @param storageEntity the storage entity to add the attribute to
     * @param attributeName the attribute name
     * @param attributeValue the attribute value
     *
     * @return the newly created storage attribute entity.
     */
    public StorageAttributeEntity createStorageAttributeEntity(StorageEntity storageEntity, String attributeName, String attributeValue)
    {
        StorageAttributeEntity storageAttributeEntity = new StorageAttributeEntity();
        storageAttributeEntity.setStorage(storageEntity);
        storageAttributeEntity.setName(attributeName);
        storageAttributeEntity.setValue(attributeValue);
        return storageDao.saveAndRefresh(storageAttributeEntity);
    }

    /**
     * Creates and persists a new storage entity with a random name and no attributes.
     *
     * @return the newly created storage entity.
     */
    public StorageEntity createStorageEntity()
    {
        return createStorageEntity("StorageTest" + AbstractDaoTest.getRandomSuffix());
    }

    /**
     * Creates and persists a new storage entity of S3 storage platform with no attributes.
     *
     * @param storageName the storage name
     *
     * @return the newly created storage entity.
     */
    public StorageEntity createStorageEntity(String storageName)
    {
        return createStorageEntity(storageName, StoragePlatformEntity.S3);
    }

    /**
     * Creates and persists a new storage entity of S3 storage platform with the specified attributes.
     *
     * @param storageName the storage name
     * @param attributes the storage attributes.
     *
     * @return the newly created storage entity.
     */
    public StorageEntity createStorageEntity(String storageName, List<Attribute> attributes)
    {
        return createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);
    }

    /**
     * Creates and persists a new storage entity with no attributes.
     *
     * @param storageName the storage name
     * @param storagePlatformCode the storage platform code
     *
     * @return the newly created storage entity.
     */
    public StorageEntity createStorageEntity(String storageName, String storagePlatformCode)
    {
        return createStorageEntity(storageName, storagePlatformCode, null);
    }

    /**
     * Creates and persists a new storage entity with an attribute.
     *
     * @param storageName the storage name
     * @param storagePlatformCode the storage platform code
     * @param attributeName the attribute name
     * @param attributeValue the attribute value
     *
     * @return the newly created storage entity.
     */
    public StorageEntity createStorageEntity(String storageName, String storagePlatformCode, String attributeName, String attributeValue)
    {
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(attributeName, attributeValue));
        return createStorageEntity(storageName, storagePlatformCode, attributes);
    }

    /**
     * Creates and persists a new storage entity with the specified attributes.
     *
     * @param storageName the storage name
     * @param storagePlatformCode the storage platform code
     * @param attributes the attributes.
     *
     * @return the newly created storage entity.
     */
    public StorageEntity createStorageEntity(String storageName, String storagePlatformCode, List<Attribute> attributes)
    {
        // Create storage platform entity if it does not exist.
        StoragePlatformEntity storagePlatformEntity = storagePlatformDao.getStoragePlatformByName(storagePlatformCode);
        if (storagePlatformEntity == null)
        {
            storagePlatformEntity = storagePlatformDaoTestHelper.createStoragePlatformEntity(storagePlatformCode);
        }
        return createStorageEntity(storageName, storagePlatformEntity, attributes);
    }

    /**
     * Creates and persists a new storage entity based on the specified storage name and storage platform entity and no attributes.
     *
     * @param storageName the storage name
     * @param storagePlatformEntity the storage platform entity
     *
     * @return the newly created storage entity.
     */
    public StorageEntity createStorageEntity(String storageName, StoragePlatformEntity storagePlatformEntity)
    {
        return createStorageEntity(storageName, storagePlatformEntity, null);
    }

    /**
     * Creates and persists a new storage entity.
     *
     * @param storageName the storage name
     * @param storagePlatformEntity the storage platform entity
     *
     * @return the newly created storage entity.
     */
    public StorageEntity createStorageEntity(String storageName, StoragePlatformEntity storagePlatformEntity, List<Attribute> attributes)
    {
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(storageName);
        storageEntity.setStoragePlatform(storagePlatformEntity);

        // Create the attributes if they are specified.
        if (!CollectionUtils.isEmpty(attributes))
        {
            List<StorageAttributeEntity> attributeEntities = new ArrayList<>();
            storageEntity.setAttributes(attributeEntities);
            for (Attribute attribute : attributes)
            {
                StorageAttributeEntity attributeEntity = new StorageAttributeEntity();
                attributeEntities.add(attributeEntity);
                attributeEntity.setStorage(storageEntity);
                attributeEntity.setName(attribute.getName());
                attributeEntity.setValue(attribute.getValue());
            }
        }

        return storageDao.saveAndRefresh(storageEntity);
    }

    /**
     * Creates and persists a new storage entity with a random name with the specified attributes.
     *
     * @param attributeName the attribute name.
     * @param attributeValue the attribute value.
     *
     * @return the newly created storage entity.
     */
    public StorageEntity createStorageEntityWithAttributes(String attributeName, String attributeValue)
    {
        return createStorageEntity("StorageTest" + AbstractDaoTest.getRandomSuffix(), StoragePlatformEntity.S3, attributeName, attributeValue);
    }

    /**
     * Gets the bucket name for the S3 external storage.
     *
     * @return the bucket name.
     */
    public String getS3ExternalBucketName()
    {
        return getBucketNameFromStorage(StorageEntity.MANAGED_EXTERNAL_STORAGE);
    }

    /**
     * Gets the bucket name for the S3 loading dock storage.
     *
     * @return the bucket name.
     */
    public String getS3LoadingDockBucketName()
    {
        return getBucketNameFromStorage(StorageEntity.MANAGED_LOADING_DOCK_STORAGE);
    }

    /**
     * Gets the bucket name for the S3 managed storage.
     *
     * @return the bucket name.
     */
    public String getS3ManagedBucketName()
    {
        return getBucketNameFromStorage(StorageEntity.MANAGED_STORAGE);
    }

    /**
     * Returns a list of test storage keys.
     *
     * @return the list of test storage keys
     */
    public List<StorageKey> getTestStorageKeys()
    {
        // Get a list of test storage keys.
        return Arrays.asList(new StorageKey(AbstractDaoTest.STORAGE_NAME), new StorageKey(AbstractDaoTest.STORAGE_NAME_2));
    }

    /**
     * Returns the bucket name of the specified storage name.
     * <p/>
     * Gets the storage with specified name and finds and returns the value of the attribute for the bucket name.
     *
     * @param storageName the name of the storage to get the bucket name for.
     *
     * @return S3 bucket name
     * @throws IllegalStateException when either the storage or attribute is not found.
     */
    private String getBucketNameFromStorage(String storageName)
    {
        String s3BucketName = null;
        StorageEntity storageEntity = storageDao.getStorageByName(storageName);

        if (storageEntity == null)
        {
            throw new IllegalStateException("storageEntity \"" + storageName + "\" not found");
        }

        for (StorageAttributeEntity storageAttributeEntity : storageEntity.getAttributes())
        {
            if (configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME).equals(storageAttributeEntity.getName()))
            {
                s3BucketName = storageAttributeEntity.getValue();
                break;
            }
        }

        if (s3BucketName == null)
        {
            throw new IllegalStateException(
                "storageAttributeEntity with name " + configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME) +
                    " not found for storage \"" + storageName + "\".");
        }

        return s3BucketName;
    }
}

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
package org.finra.herd.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.StorageDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageAttributesUpdateRequest;
import org.finra.herd.model.api.xml.StorageCreateRequest;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.api.xml.StorageKeys;
import org.finra.herd.model.api.xml.StorageUpdateRequest;
import org.finra.herd.model.jpa.StorageAttributeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.service.StorageService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.AttributeHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StoragePlatformHelper;

/**
 * The storage service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class StorageServiceImpl implements StorageService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private AttributeHelper attributeHelper;

    @Autowired
    private StorageDao storageDao;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private StoragePlatformHelper storagePlatformHelper;

    @Override
    public Storage createStorage(StorageCreateRequest storageCreateRequest)
    {
        // Perform validation and trim.
        validateAndTrimStorageCreateRequest(storageCreateRequest);

        // Retrieve storage platform.
        StoragePlatformEntity storagePlatformEntity = storagePlatformHelper.getStoragePlatformEntity(storageCreateRequest.getStoragePlatformName());

        // See if a storage with the specified name already exists.
        StorageEntity storageEntity = storageDao.getStorageByName(storageCreateRequest.getName());
        if (storageEntity != null)
        {
            throw new AlreadyExistsException(String.format("Storage with name \"%s\" already exists.", storageCreateRequest.getName()));
        }

        // Create a storage entity.
        storageEntity = new StorageEntity();
        storageEntity.setName(storageCreateRequest.getName());
        storageEntity.setStoragePlatform(storagePlatformEntity);

        // Create attributes if they are specified.
        if (!CollectionUtils.isEmpty(storageCreateRequest.getAttributes()))
        {
            List<StorageAttributeEntity> attributeEntities = new ArrayList<>();
            storageEntity.setAttributes(attributeEntities);
            for (Attribute attribute : storageCreateRequest.getAttributes())
            {
                StorageAttributeEntity attributeEntity = new StorageAttributeEntity();
                attributeEntities.add(attributeEntity);
                attributeEntity.setStorage(storageEntity);
                attributeEntity.setName(attribute.getName());
                attributeEntity.setValue(attribute.getValue());
            }
        }

        // Persist the storage entity.
        storageEntity = storageDao.saveAndRefresh(storageEntity);

        // Return the storage information.
        return createStorageFromEntity(storageEntity);
    }

    @Override
    public Storage deleteStorage(StorageKey storageKey)
    {
        // Perform validation and trim.
        validateAndTrimStorageKey(storageKey);

        // Retrieve and ensure that a storage exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageKey);

        // Delete the storage.
        storageDao.delete(storageEntity);

        // Return the storage information.
        return createStorageFromEntity(storageEntity);
    }

    @Override
    public StorageKeys getAllStorage()
    {
        StorageKeys storageKeys = new StorageKeys();
        storageKeys.getStorageKeys().addAll(storageDao.getAllStorage());
        return storageKeys;
    }

    @Override
    public Storage getStorage(StorageKey storageKey)
    {
        // Perform validation and trim.
        validateAndTrimStorageKey(storageKey);

        // Retrieve and ensure that a storage exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageKey);

        // Return the storage information.
        return createStorageFromEntity(storageEntity);
    }

    @Override
    public Storage updateStorage(StorageKey storageKey, StorageUpdateRequest storageUpdateRequest)
    {
        // Perform validation and trim.
        validateAndTrimStorageKey(storageKey);

        // Retrieve and ensure that a storage exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageKey);

        // TODO: Add in code to update storageEntity as needed from storageUpdateRequest attributes.

        // Update and persist the storage entity.
        storageEntity = storageDao.saveAndRefresh(storageEntity);

        // Return the storage information.
        return createStorageFromEntity(storageEntity);
    }

    @Override
    public Storage updateStorageAttributes(StorageKey storageKey, StorageAttributesUpdateRequest storageAttributesUpdateRequest)
    {
        // Perform validation and trim the storage key parameters.
        validateAndTrimStorageKey(storageKey);

        // Validate storage attributes update request.
        validateAndTrimStorageAttributesUpdateRequest(storageAttributesUpdateRequest);

        // Retrieve and ensure that a storage exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageKey);

        // Update storage attributes.
        updateStorageAttributesHelper(storageEntity, storageAttributesUpdateRequest.getAttributes(), storageKey);

        // Persist and refresh the entity.
        storageEntity = storageDao.saveAndRefresh(storageEntity);

        // Return the storage information.
        return createStorageFromEntity(storageEntity);
    }

    /**
     * Creates a storage from it's entity object.
     *
     * @param storageEntity the storage entity
     *
     * @return the storage
     */
    private Storage createStorageFromEntity(StorageEntity storageEntity)
    {
        // Create a list of attributes.
        List<Attribute> attributes = new ArrayList<>();
        for (StorageAttributeEntity attributeEntity : storageEntity.getAttributes())
        {
            attributes.add(new Attribute(attributeEntity.getName(), attributeEntity.getValue()));
        }

        return new Storage(storageEntity.getName(), storageEntity.getStoragePlatform().getName(), attributes);
    }

    /**
     * Updates storage attributes.
     *
     * @param storageEntity the storage entity
     * @param attributes the list of attributes
     * @param storageKey the storage key
     */
    private void updateStorageAttributesHelper(StorageEntity storageEntity, List<Attribute> attributes, StorageKey storageKey)
    {
        // Load all existing attribute entities in a map with a "lowercase" attribute name as the key for case insensitivity.
        Map<String, StorageAttributeEntity> existingAttributeEntities = new HashMap<>();
        for (StorageAttributeEntity attributeEntity : storageEntity.getAttributes())
        {
            String mapKey = attributeEntity.getName().toLowerCase();
            if (existingAttributeEntities.containsKey(mapKey))
            {
                throw new IllegalStateException(
                    String.format("Found duplicate attribute with name \"%s\" for \"%s\" storage.", mapKey, storageKey.getStorageName()));
            }
            existingAttributeEntities.put(mapKey, attributeEntity);
        }

        // Process the list of attributes to determine that storage attribute entities should be created, updated, or deleted.
        List<StorageAttributeEntity> createdAttributeEntities = new ArrayList<>();
        List<StorageAttributeEntity> retainedAttributeEntities = new ArrayList<>();
        if (!CollectionUtils.isEmpty(attributes))
        {
            for (Attribute attribute : attributes)
            {
                // Use a "lowercase" attribute name for case insensitivity.
                String lowercaseAttributeName = attribute.getName().toLowerCase();
                if (existingAttributeEntities.containsKey(lowercaseAttributeName))
                {
                    // Check if the attribute value needs to be updated.
                    StorageAttributeEntity attributeEntity = existingAttributeEntities.get(lowercaseAttributeName);
                    if (!StringUtils.equals(attribute.getValue(), attributeEntity.getValue()))
                    {
                        // Update the attribute entity.
                        attributeEntity.setValue(attribute.getValue());
                    }

                    // Add this entity to the list of attribute entities to be retained.
                    retainedAttributeEntities.add(attributeEntity);
                }
                else
                {
                    // Create a new attribute entity.
                    StorageAttributeEntity attributeEntity = new StorageAttributeEntity();
                    storageEntity.getAttributes().add(attributeEntity);
                    attributeEntity.setStorage(storageEntity);
                    attributeEntity.setName(attribute.getName());
                    attributeEntity.setValue(attribute.getValue());

                    // Add this entity to the list of the newly created attribute entities.
                    createdAttributeEntities.add(attributeEntity);
                }
            }
        }

        // Remove any of the currently existing attribute entities that did not get onto the retained entities list.
        storageEntity.getAttributes().retainAll(retainedAttributeEntities);

        // Add all of the newly created attribute entities.
        storageEntity.getAttributes().addAll(createdAttributeEntities);
    }

    /**
     * Validates storage update request. This method also trims request parameters.
     *
     * @param storageAttributesUpdateRequest the storage attributes update request
     */
    private void validateAndTrimStorageAttributesUpdateRequest(StorageAttributesUpdateRequest storageAttributesUpdateRequest)
    {
        // Validate storage attributes update request.
        Assert.notNull(storageAttributesUpdateRequest, "A storage attributes update request is required.");
        Assert.notNull(storageAttributesUpdateRequest.getAttributes(), "A storage attributes list is required.");

        // Validate optional attributes. This is also going to trim the attribute names.
        attributeHelper.validateAttributes(storageAttributesUpdateRequest.getAttributes());
    }

    /**
     * Validates storage create request. This method also trims request parameters.
     *
     * @param storageCreateRequest the storage create request
     */
    private void validateAndTrimStorageCreateRequest(StorageCreateRequest storageCreateRequest)
    {
        // Validate storage attributes update request.
        Assert.notNull(storageCreateRequest, "A storage create request is required.");
        storageCreateRequest
            .setStoragePlatformName(alternateKeyHelper.validateStringParameter("storage platform name", storageCreateRequest.getStoragePlatformName()));
        storageCreateRequest.setName(alternateKeyHelper.validateStringParameter("storage name", storageCreateRequest.getName()));

        // Validate optional attributes. This is also going to trim the attribute names.
        attributeHelper.validateAttributes(storageCreateRequest.getAttributes());
    }

    /**
     * Validates storage key. This method also trims storage key parameters.
     *
     * @param storageKey the storage key
     */
    private void validateAndTrimStorageKey(StorageKey storageKey)
    {
        storageKey.setStorageName(alternateKeyHelper.validateStringParameter("storage name", storageKey.getStorageName()));
    }
}

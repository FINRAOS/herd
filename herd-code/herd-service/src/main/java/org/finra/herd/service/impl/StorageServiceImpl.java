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
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.StorageDao;
import org.finra.herd.dao.StorageUploadStatsDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStats;
import org.finra.herd.model.api.xml.StorageCreateRequest;
import org.finra.herd.model.api.xml.StorageDailyUploadStats;
import org.finra.herd.model.api.xml.StorageKeys;
import org.finra.herd.model.api.xml.StorageUpdateRequest;
import org.finra.herd.model.dto.DateRangeDto;
import org.finra.herd.model.dto.StorageAlternateKeyDto;
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

    @Autowired
    private StorageUploadStatsDao storageUploadStatsDao;

    /**
     * Creates a new storage.
     *
     * @param storageCreateRequest the storage request
     *
     * @return the created storage information
     */
    @Override
    public Storage createStorage(StorageCreateRequest storageCreateRequest)
    {
        // Perform validation and trim.
        validateStorageCreateRequest(storageCreateRequest);

        // Retrieve storage platform.
        StoragePlatformEntity storagePlatformEntity = storagePlatformHelper.getStoragePlatformEntity(storageCreateRequest.getStoragePlatformName());

        // See if a storage with the specified name already exists.
        StorageEntity storageEntity = storageDao.getStorageByName(storageCreateRequest.getName());
        if (storageEntity != null)
        {
            throw new AlreadyExistsException(String.format("Storage with name \"%s\" already exists.", storageCreateRequest.getName()));
        }

        // Create and persist the storage entity.
        storageEntity = new StorageEntity();
        storageEntity.setName(storageCreateRequest.getName());
        storageEntity.setStoragePlatform(storagePlatformEntity);

        // Create the attributes if they are specified.
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

        storageEntity = storageDao.saveAndRefresh(storageEntity);

        // Return the storage information.
        return createStorageFromEntity(storageEntity);
    }

    /**
     * Updates an existing storage.
     *
     * @param storageAlternateKey the storage alternate key (case-insensitive)
     * @param storageUpdateRequest the storage update request
     *
     * @return the updated storage information
     */
    @Override
    public Storage updateStorage(StorageAlternateKeyDto storageAlternateKey, StorageUpdateRequest storageUpdateRequest)
    {
        // Perform validation and trim.
        validateStorageAlternateKey(storageAlternateKey);

        // Retrieve and ensure that a storage with the specified alternate key exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageAlternateKey);

        // TODO: Add in code to update storageEntity as needed from storageUpdateRequest attributes.

        // Update and persist the storage entity.
        storageEntity = storageDao.saveAndRefresh(storageEntity);

        // Return the storage information.
        return createStorageFromEntity(storageEntity);
    }

    /**
     * Gets a storage for the specified storage name.
     *
     * @param storageAlternateKey the storage alternate key (case-insensitive)
     *
     * @return the storage
     */
    @Override
    public Storage getStorage(StorageAlternateKeyDto storageAlternateKey)
    {
        // Perform validation and trim.
        validateStorageAlternateKey(storageAlternateKey);

        // Retrieve the storage with the specified alternate key.
        return createStorageFromEntity(storageDaoHelper.getStorageEntity(storageAlternateKey));
    }

    /**
     * Deletes a storage for the specified storage alternate key.
     *
     * @param storageAlternateKey the storage alternate key (case-insensitive)
     *
     * @return the storage that was deleted
     */
    @Override
    public Storage deleteStorage(StorageAlternateKeyDto storageAlternateKey)
    {
        // Perform validation and trim.
        validateStorageAlternateKey(storageAlternateKey);

        // Retrieve and ensure that a storage with the specified alternate key exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageAlternateKey);

        // Delete the storage.
        storageDao.delete(storageEntity);

        // Return the storage that got deleted.
        return createStorageFromEntity(storageEntity);
    }

    @Override
    public StorageKeys getStorages()
    {
        StorageKeys storageKeys = new StorageKeys();
        storageKeys.getStorageKeys().addAll(storageDao.getStorages());
        return storageKeys;
    }

    /**
     * Retrieves cumulative daily upload statistics for the storage for the specified upload date.  If the upload date is not specified, returns the upload
     * stats for the past 7 calendar days plus today (8 days total).
     *
     * @param storageAlternateKey the storage alternate key (case-insensitive)
     * @param uploadDate the upload date (optional)
     *
     * @return the upload statistics
     */
    @Override
    public StorageDailyUploadStats getStorageUploadStats(StorageAlternateKeyDto storageAlternateKey, Date uploadDate)
    {
        // Perform validation and trim.
        validateStorageAlternateKey(storageAlternateKey);
        validateStorageEntityExists(storageAlternateKey);

        // If the upload date is not specified, retrieve upload stats for the past 7 calendar days plus today (8 days total).
        DateRangeDto dateRange = uploadDate == null ? getLastNDaysDateRange(7) : getOneDayDateRange(uploadDate);

        return storageUploadStatsDao.getStorageUploadStats(storageAlternateKey, dateRange);
    }

    /**
     * Retrieves daily upload statistics for the storage by business object definition for the specified upload date.  If the upload date is not specified,
     * returns the upload stats for the past 7 calendar days plus today (8 days total).
     *
     * @param storageAlternateKey the storage alternate key (case-insensitive)
     * @param uploadDate the upload date (optional)
     *
     * @return the upload statistics
     */
    @Override
    public StorageBusinessObjectDefinitionDailyUploadStats getStorageUploadStatsByBusinessObjectDefinition(StorageAlternateKeyDto storageAlternateKey,
        Date uploadDate)
    {
        // Perform validation and trim.
        validateStorageAlternateKey(storageAlternateKey);
        validateStorageEntityExists(storageAlternateKey);

        // If the upload date is not specified, retrieve upload stats for the past 7 calendar days plus today (8 days total).
        DateRangeDto dateRange = uploadDate == null ? getLastNDaysDateRange(7) : getOneDayDateRange(uploadDate);

        return storageUploadStatsDao.getStorageUploadStatsByBusinessObjectDefinition(storageAlternateKey, dateRange);
    }

    /**
     * Checks if storage exists.
     *
     * @param storageAlternateKey the storage alternate key (case-insensitive)
     *
     * @throws ObjectNotFoundException if storage with this alternate key does not exist
     */
    private void validateStorageEntityExists(StorageAlternateKeyDto storageAlternateKey) throws ObjectNotFoundException
    {
        storageDaoHelper.getStorageEntity(storageAlternateKey);
    }

    /**
     * Validates the storage create request. This method also trims request parameters.
     *
     * @param request the request.
     *
     * @throws IllegalArgumentException if any validation errors were found.
     */
    private void validateStorageCreateRequest(StorageCreateRequest request)
    {
        request.setStoragePlatformName(alternateKeyHelper.validateStringParameter("storage platform name", request.getStoragePlatformName()));
        request.setName(alternateKeyHelper.validateStringParameter("storage name", request.getName()));
        attributeHelper.validateAttributes(request.getAttributes());
    }

    /**
     * Validates the storage alternate key. This method also trims the alternate key parameters.
     *
     * @param key the storage alternate key
     */
    private void validateStorageAlternateKey(StorageAlternateKeyDto key)
    {
        key.setStorageName(alternateKeyHelper.validateStringParameter("storage name", key.getStorageName()));
    }

    /**
     * Creates a storage from it's entity object.
     *
     * @param storageEntity the storage entity.
     *
     * @return the storage.
     */
    private Storage createStorageFromEntity(StorageEntity storageEntity)
    {
        // Create the base storage.
        Storage storage = new Storage();
        storage.setName(storageEntity.getName());
        storage.setStoragePlatformName(storageEntity.getStoragePlatform().getName());

        // Add in the attributes.
        List<Attribute> attributes = new ArrayList<>();
        storage.setAttributes(attributes);
        for (StorageAttributeEntity attributeEntity : storageEntity.getAttributes())
        {
            Attribute attribute = new Attribute();
            attributes.add(attribute);
            attribute.setName(attributeEntity.getName());
            attribute.setValue(attributeEntity.getValue());
        }

        return storage;
    }

    /**
     * Returns a date range that contains only one day.
     *
     * @param date the date to be included in the date range
     *
     * @return the date range that contains only the specified day
     */
    private DateRangeDto getOneDayDateRange(Date date)
    {
        return DateRangeDto.builder().lowerDate(date).upperDate(date).build();
    }

    /**
     * Returns a date range for the past N calendar days plus today.
     *
     * @param numberOfDays the number of past days to be included in the date range
     *
     * @return the date range that includes the past N days plus today
     */
    private DateRangeDto getLastNDaysDateRange(int numberOfDays)
    {
        // Get current date without a time part set.
        Date currentDate = HerdDateUtils.getCurrentCalendarNoTime().getTime();

        // Create the date range.
        return DateRangeDto.builder().lowerDate(HerdDateUtils.addDays(currentDate, -numberOfDays)).upperDate(currentDate).build();
    }
}

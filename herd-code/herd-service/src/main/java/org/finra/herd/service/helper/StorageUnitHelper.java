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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitStatusChangeEvent;
import org.finra.herd.model.dto.StorageUnitAvailabilityDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageAttributeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusHistoryEntity;

/**
 * A helper class for storage unit related code.
 */
@Component
public class StorageUnitHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private StorageFileHelper storageFileHelper;

    /**
     * Creates a business object data storage unit key from business object data key and storage name.
     *
     * @param businessObjectDataKey the business object data key
     * @param storageName the storage name
     *
     * @return the storage unit key
     */
    public BusinessObjectDataStorageUnitKey createBusinessObjectDataStorageUnitKey(BusinessObjectDataKey businessObjectDataKey, String storageName)
    {
        return new BusinessObjectDataStorageUnitKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(), businessObjectDataKey.getSubPartitionValues(),
            businessObjectDataKey.getBusinessObjectDataVersion(), storageName);
    }

    /**
     * Creates a storage unit key from a business object data key and a storage name.
     *
     * @param businessObjectDataKey the business object data key
     * @param storageName the storage name
     *
     * @return the storage unit key
     */
    public BusinessObjectDataStorageUnitKey createStorageUnitKey(BusinessObjectDataKey businessObjectDataKey, String storageName)
    {
        return new BusinessObjectDataStorageUnitKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(), businessObjectDataKey.getSubPartitionValues(),
            businessObjectDataKey.getBusinessObjectDataVersion(), storageName);
    }

    /**
     * Creates a storage unit key from a storage unit entity.
     *
     * @param storageUnitEntity the storage unit entity
     *
     * @return the storage unit key
     */
    public BusinessObjectDataStorageUnitKey createStorageUnitKeyFromEntity(StorageUnitEntity storageUnitEntity)
    {
        // Get the business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = storageUnitEntity.getBusinessObjectData();

        // Create and initialize the storage unit key.
        BusinessObjectDataStorageUnitKey storageUnitKey = new BusinessObjectDataStorageUnitKey();
        storageUnitKey.setNamespace(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode());
        storageUnitKey.setBusinessObjectDefinitionName(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName());
        storageUnitKey.setBusinessObjectFormatUsage(businessObjectDataEntity.getBusinessObjectFormat().getUsage());
        storageUnitKey.setBusinessObjectFormatFileType(businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode());
        storageUnitKey.setBusinessObjectFormatVersion(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion());
        storageUnitKey.setPartitionValue(businessObjectDataEntity.getPartitionValue());
        storageUnitKey.setSubPartitionValues(businessObjectDataHelper.getSubPartitionValues(businessObjectDataEntity));
        storageUnitKey.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());
        storageUnitKey.setStorageName(storageUnitEntity.getStorage().getName());

        return storageUnitKey;
    }

    /**
     * Creates a list of storage units from the list of storage unit entities.
     *
     * @param storageUnitEntities the storage unit entities
     * @param includeStorageUnitStatusHistory specifies to include storage unit status history for each storage unit in the response
     * @param excludeBusinessObjectDataStorageFiles specifies to exclude storage files in the response
     *
     * @return the list of storage units
     */
    List<StorageUnit> createStorageUnitsFromEntities(Collection<StorageUnitEntity> storageUnitEntities, Boolean includeStorageUnitStatusHistory,
        Boolean excludeBusinessObjectDataStorageFiles)
    {
        List<StorageUnit> storageUnits = new ArrayList<>();

        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            StorageUnit storageUnit = new StorageUnit();
            storageUnits.add(storageUnit);

            Storage storage = new Storage();
            storageUnit.setStorage(storage);

            StorageEntity storageEntity = storageUnitEntity.getStorage();
            storage.setName(storageEntity.getName());
            storage.setStoragePlatformName(storageEntity.getStoragePlatform().getName());

            // Add the storage attributes.
            if (!CollectionUtils.isEmpty(storageEntity.getAttributes()))
            {
                List<Attribute> storageAttributes = new ArrayList<>();
                storage.setAttributes(storageAttributes);
                for (StorageAttributeEntity storageAttributeEntity : storageEntity.getAttributes())
                {
                    Attribute attribute = new Attribute();
                    storageAttributes.add(attribute);
                    attribute.setName(storageAttributeEntity.getName());
                    attribute.setValue(storageAttributeEntity.getValue());
                }
            }

            // Add the storage directory.
            if (storageUnitEntity.getDirectoryPath() != null)
            {
                StorageDirectory storageDirectory = new StorageDirectory();
                storageUnit.setStorageDirectory(storageDirectory);
                storageDirectory.setDirectoryPath(storageUnitEntity.getDirectoryPath());
            }

            // Add the storage files, is they are not excluded and present in the database.
            if (BooleanUtils.isNotTrue(excludeBusinessObjectDataStorageFiles) && CollectionUtils.isNotEmpty(storageUnitEntity.getStorageFiles()))
            {
                List<StorageFile> storageFiles = new ArrayList<>();
                storageUnit.setStorageFiles(storageFiles);

                for (StorageFileEntity storageFileEntity : storageUnitEntity.getStorageFiles())
                {
                    storageFiles.add(storageFileHelper.createStorageFileFromEntity(storageFileEntity, storageUnitEntity.getDirectoryPath()));
                }
            }

            // Set the storage unit status.
            storageUnit.setStorageUnitStatus(storageUnitEntity.getStatus().getCode());

            // If specified, add storage unit status history.
            if (BooleanUtils.isTrue(includeStorageUnitStatusHistory))
            {
                List<StorageUnitStatusChangeEvent> storageUnitStatusChangeEvents = new ArrayList<>();
                storageUnit.setStorageUnitStatusHistory(storageUnitStatusChangeEvents);
                for (StorageUnitStatusHistoryEntity storageUnitStatusHistoryEntity : storageUnitEntity.getHistoricalStatuses())
                {
                    storageUnitStatusChangeEvents.add(new StorageUnitStatusChangeEvent(storageUnitStatusHistoryEntity.getStatus().getCode(),
                        HerdDateUtils.getXMLGregorianCalendarValue(storageUnitStatusHistoryEntity.getCreatedOn()),
                        storageUnitStatusHistoryEntity.getCreatedBy()));
                }
            }

            // Set the number of failed attempts to execute a storage policy transition.
            storageUnit.setStoragePolicyTransitionFailedAttempts(storageUnitEntity.getStoragePolicyTransitionFailedAttempts());

            if (storageUnitEntity.getRestoreExpirationOn() != null)
            {
                storageUnit.setRestoreExpirationOn(HerdDateUtils.getXMLGregorianCalendarValue(storageUnitEntity.getRestoreExpirationOn()));
            }
        }

        return storageUnits;
    }

    /**
     * Excludes storage unit availability DTOs from the list with business object data status matching to the specified value.
     *
     * @param storageUnitAvailabilityDtos the list of storage unit availability DTOs
     * @param excludedBusinessObjectDataStatus the business object data status to be excluded
     *
     * @return the updated list of storage unit availability DTOs
     */
    public List<StorageUnitAvailabilityDto> excludeBusinessObjectDataStatus(List<StorageUnitAvailabilityDto> storageUnitAvailabilityDtos,
        String excludedBusinessObjectDataStatus)
    {
        List<StorageUnitAvailabilityDto> result = new ArrayList<>();

        for (StorageUnitAvailabilityDto storageUnitAvailabilityDto : storageUnitAvailabilityDtos)
        {
            if (!storageUnitAvailabilityDto.getBusinessObjectDataStatus().equalsIgnoreCase(excludedBusinessObjectDataStatus))
            {
                result.add(storageUnitAvailabilityDto);
            }
        }

        return result;
    }

    /**
     * Excludes storage unit availability DTOs from the list with primary and sub-partition values matching one of the excluded partitions.
     *
     * @param storageUnitAvailabilityDtos the list of storage unit availability DTOs
     * @param excludedPartitions list of excluded partitions, where each partition consists of primary and optional sub-partition values
     *
     * @return the updated list of storage unit availability DTOs
     */
    public List<StorageUnitAvailabilityDto> excludePartitions(List<StorageUnitAvailabilityDto> storageUnitAvailabilityDtos,
        List<List<String>> excludedPartitions)
    {
        List<StorageUnitAvailabilityDto> result = new ArrayList<>();

        for (StorageUnitAvailabilityDto storageUnitAvailabilityDto : storageUnitAvailabilityDtos)
        {
            if (!excludedPartitions.contains(businessObjectDataHelper.getPrimaryAndSubPartitionValues(storageUnitAvailabilityDto.getBusinessObjectDataKey())))
            {
                result.add(storageUnitAvailabilityDto);
            }
        }

        return result;
    }

    /**
     * Gets a business object data key from a storage unit key.
     *
     * @param businessObjectDataStorageUnitKey the storage unit key
     *
     * @return the business object data key
     */
    public BusinessObjectDataKey getBusinessObjectDataKey(BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey)
    {
        return new BusinessObjectDataKey(businessObjectDataStorageUnitKey.getNamespace(), businessObjectDataStorageUnitKey.getBusinessObjectDefinitionName(),
            businessObjectDataStorageUnitKey.getBusinessObjectFormatUsage(), businessObjectDataStorageUnitKey.getBusinessObjectFormatFileType(),
            businessObjectDataStorageUnitKey.getBusinessObjectFormatVersion(), businessObjectDataStorageUnitKey.getPartitionValue(),
            businessObjectDataStorageUnitKey.getSubPartitionValues(), businessObjectDataStorageUnitKey.getBusinessObjectDataVersion());
    }

    /**
     * Creates a list of storage unit ids from a list of storage unit availability DTOs.
     *
     * @param storageUnitAvailabilityDtos the list of storage unit availability DTOs
     *
     * @return the list of storage unit ids
     */
    public List<Integer> getStorageUnitIds(List<StorageUnitAvailabilityDto> storageUnitAvailabilityDtos)
    {
        List<Integer> storageUnitIds = new ArrayList<>(storageUnitAvailabilityDtos.size());

        for (StorageUnitAvailabilityDto storageUnitAvailabilityDto : storageUnitAvailabilityDtos)
        {
            storageUnitIds.add(storageUnitAvailabilityDto.getStorageUnitId());
        }

        return storageUnitIds;
    }

    /**
     * Validates the business object data storage unit key. This method also trims the request parameters.
     *
     * @param key the business object data storage unit create request
     */
    public void validateBusinessObjectDataStorageUnitKey(BusinessObjectDataStorageUnitKey key)
    {
        Assert.notNull(key, "A business object data storage unit key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", key.getBusinessObjectDefinitionName()));
        key.setBusinessObjectFormatUsage(alternateKeyHelper.validateStringParameter("business object format usage", key.getBusinessObjectFormatUsage()));
        key.setBusinessObjectFormatFileType(
            alternateKeyHelper.validateStringParameter("business object format file type", key.getBusinessObjectFormatFileType()));
        Assert.notNull(key.getBusinessObjectFormatVersion(), "A business object format version must be specified.");
        key.setPartitionValue(alternateKeyHelper.validateStringParameter("partition value", key.getPartitionValue()));
        businessObjectDataHelper.validateSubPartitionValues(key.getSubPartitionValues());
        Assert.notNull(key.getBusinessObjectDataVersion(), "A business object data version must be specified.");
        key.setStorageName(alternateKeyHelper.validateStringParameter("storage name", key.getStorageName()));
    }
}

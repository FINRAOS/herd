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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitCreateResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.BusinessObjectDataStorageUnitService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageFileHelper;

/**
 * The business object data storage unit service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDataStorageUnitServiceImpl implements BusinessObjectDataStorageUnitService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private StorageFileHelper storageFileHelper;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @NamespacePermission(fields = "#request.businessObjectDataStorageUnitKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataStorageUnitCreateResponse createBusinessObjectDataStorageUnit(BusinessObjectDataStorageUnitCreateRequest request)
    {
        return createBusinessObjectDataStorageUnitImpl(request);
    }

    /**
     * Creates and populates a business object data storage unit create response.
     *
     * @param storageUnitEntity the storage unit entity
     *
     * @return the business object data storage unit create response
     */
    protected BusinessObjectDataStorageUnitCreateResponse createBusinessObjectDataStorageUnitCreateResponse(StorageUnitEntity storageUnitEntity)
    {
        // Get business object data key from the business object data entity.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.createBusinessObjectDataKeyFromEntity(storageUnitEntity.getBusinessObjectData());

        // Create a business object data storage unit create response.
        BusinessObjectDataStorageUnitCreateResponse response = new BusinessObjectDataStorageUnitCreateResponse();

        // Add storage key.
        response.setBusinessObjectDataStorageUnitKey(createBusinessObjectDataStorageUnitKey(businessObjectDataKey, storageUnitEntity.getStorage().getName()));

        // Add storage directory.
        if (storageUnitEntity.getDirectoryPath() != null)
        {
            response.setStorageDirectory(new StorageDirectory(storageUnitEntity.getDirectoryPath()));
        }

        // Add storage files.
        if (CollectionUtils.isNotEmpty(storageUnitEntity.getStorageFiles()))
        {
            response.setStorageFiles(storageFileHelper.createStorageFilesFromEntities(storageUnitEntity.getStorageFiles()));
        }

        // Return the response.
        return response;
    }

    /**
     * Creates new storage unit for a given business object data and storage.
     *
     * @param request the create business object data storage unit create request
     *
     * @return the create business object data storage unit create response
     */
    protected BusinessObjectDataStorageUnitCreateResponse createBusinessObjectDataStorageUnitImpl(BusinessObjectDataStorageUnitCreateRequest request)
    {
        // Validate the request.
        validateBusinessObjectDataStorageUnitCreateRequest(request);

        // Retrieve and validate that business object data exists.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(getBusinessObjectDataKey(request.getBusinessObjectDataStorageUnitKey()));

        // Retrieve and validate that storage exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(request.getBusinessObjectDataStorageUnitKey().getStorageName());

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = businessObjectDataDaoHelper
            .createStorageUnitEntity(businessObjectDataEntity, storageEntity, request.getStorageDirectory(), request.getStorageFiles(),
                request.isDiscoverStorageFiles());

        // Construct and return the response.
        return createBusinessObjectDataStorageUnitCreateResponse(storageUnitEntity);
    }

    /**
     * Validates the business object definition column create request. This method also trims the request parameters.
     *
     * @param request the business object data storage unit create request
     */
    protected void validateBusinessObjectDataStorageUnitCreateRequest(BusinessObjectDataStorageUnitCreateRequest request)
    {
        Assert.notNull(request, "A business object data storage unit create request must be specified.");

        validateBusinessObjectDataStorageUnitKey(request.getBusinessObjectDataStorageUnitKey());

        if (BooleanUtils.isTrue(request.isDiscoverStorageFiles()))
        {
            // The auto-discovery of storage files is enabled, thus a storage directory is required and storage files cannot be specified.
            Assert.isTrue(request.getStorageDirectory() != null, "A storage directory must be specified when discovery of storage files is enabled.");
            Assert.isTrue(CollectionUtils.isEmpty(request.getStorageFiles()), "Storage files cannot be specified when discovery of storage files is enabled.");
        }
        else
        {
            // Since auto-discovery is disabled, a storage directory or at least one storage file are required for each storage unit.
            Assert.isTrue(request.getStorageDirectory() != null || CollectionUtils.isNotEmpty(request.getStorageFiles()),
                "A storage directory or at least one storage file must be specified when discovery of storage files is not enabled.");
        }

        // If storageDirectory element is present in the request, we require it to contain a non-empty directoryPath element.
        if (request.getStorageDirectory() != null)
        {
            Assert.hasText(request.getStorageDirectory().getDirectoryPath(), "A storage directory path must be specified.");
            request.getStorageDirectory().setDirectoryPath(request.getStorageDirectory().getDirectoryPath().trim());
        }

        // Validate a list of storage files, if specified.
        if (CollectionUtils.isNotEmpty(request.getStorageFiles()))
        {
            storageFileHelper.validateCreateRequestStorageFiles(request.getStorageFiles());
        }
    }

    /**
     * Validates the business object data storage unit key. This method also trims the request parameters.
     *
     * @param key the business object data storage unit create request
     */
    protected void validateBusinessObjectDataStorageUnitKey(BusinessObjectDataStorageUnitKey key)
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

    /**
     * Creates a business object data storage unit key from business object data key and storage name.
     *
     * @param businessObjectDataKey the business object data key
     * @param storageName the storage name
     *
     * @return the storage unit key
     */
    private BusinessObjectDataStorageUnitKey createBusinessObjectDataStorageUnitKey(BusinessObjectDataKey businessObjectDataKey, String storageName)
    {
        return new BusinessObjectDataStorageUnitKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(), businessObjectDataKey.getSubPartitionValues(),
            businessObjectDataKey.getBusinessObjectDataVersion(), storageName);
    }

    /**
     * Gets a business object data key from a storage unit key.
     *
     * @param storageUnitKey the storage unit key
     *
     * @return the business object data key
     */
    private BusinessObjectDataKey getBusinessObjectDataKey(BusinessObjectDataStorageUnitKey storageUnitKey)
    {
        return new BusinessObjectDataKey(storageUnitKey.getNamespace(), storageUnitKey.getBusinessObjectDefinitionName(),
            storageUnitKey.getBusinessObjectFormatUsage(), storageUnitKey.getBusinessObjectFormatFileType(), storageUnitKey.getBusinessObjectFormatVersion(),
            storageUnitKey.getPartitionValue(), storageUnitKey.getSubPartitionValues(), storageUnitKey.getBusinessObjectDataVersion());
    }
}

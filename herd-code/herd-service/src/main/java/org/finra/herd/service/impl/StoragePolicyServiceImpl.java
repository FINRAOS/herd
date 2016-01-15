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

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.StoragePolicy;
import org.finra.herd.model.api.xml.StoragePolicyCreateRequest;
import org.finra.herd.model.api.xml.StoragePolicyFilter;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.api.xml.StoragePolicyRule;
import org.finra.herd.model.api.xml.StoragePolicyTransition;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.service.StoragePolicyService;
import org.finra.herd.service.helper.HerdDaoHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StoragePolicyHelper;

/**
 * The storage policy service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class StoragePolicyServiceImpl implements StoragePolicyService
{
    @Autowired
    private HerdDao herdDao;

    @Autowired
    private HerdDaoHelper herdDaoHelper;

    @Autowired
    protected ConfigurationHelper configurationHelper;

    @Autowired
    protected StoragePolicyHelper storagePolicyHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    /**
     * Creates a new storage policy.
     *
     * @param request the information needed to create a storage policy
     *
     * @return the newly created storage policy
     */
    @Override
    public StoragePolicy createStoragePolicy(StoragePolicyCreateRequest request)
    {
        // Validate and trim the request parameters.
        validateStoragePolicyCreateRequest(request);

        // Get the storage policy key.
        StoragePolicyKey storagePolicyKey = request.getStoragePolicyKey();

        // Ensure a storage policy with the specified name doesn't already exist for the specified namespace.
        StoragePolicyEntity storagePolicyEntity = herdDao.getStoragePolicyByAltKey(storagePolicyKey);
        if (storagePolicyEntity != null)
        {
            throw new AlreadyExistsException(String.format("Unable to create storage policy with name \"%s\" because it already exists for namespace \"%s\".",
                storagePolicyKey.getStoragePolicyName(), storagePolicyKey.getNamespace()));
        }

        // Retrieve and ensure that namespace exists with the specified storage policy namespace code.
        NamespaceEntity namespaceEntity = herdDaoHelper.getNamespaceEntity(storagePolicyKey.getNamespace());

        // Retrieve and ensure that storage policy type exists.
        StoragePolicyRuleTypeEntity storagePolicyRuleTypeEntity = storageDaoHelper.getStoragePolicyRuleTypeEntity(request.getStoragePolicyRule().getRuleType());

        // Get the storage policy filter.
        StoragePolicyFilter storagePolicyFilter = request.getStoragePolicyFilter();

        // If specified, retrieve and ensure that the business object definition exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = null;
        if (StringUtils.isNotBlank(storagePolicyFilter.getBusinessObjectDefinitionName()))
        {
            businessObjectDefinitionEntity = herdDaoHelper.getBusinessObjectDefinitionEntity(
                new BusinessObjectDefinitionKey(storagePolicyFilter.getNamespace(), storagePolicyFilter.getBusinessObjectDefinitionName()));
        }

        // If specified, retrieve and ensure that file type exists.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(storagePolicyFilter.getBusinessObjectFormatFileType()))
        {
            fileTypeEntity = herdDaoHelper.getFileTypeEntity(storagePolicyFilter.getBusinessObjectFormatFileType());
        }

        // Retrieve and ensure that storage policy filter storage exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storagePolicyFilter.getStorageName());

        // Validate that storage platform is S3 for the storage policy filter storage.
        Assert.isTrue(StoragePlatformEntity.S3.equals(storageEntity.getStoragePlatform().getName()),
            String.format("Storage platform for storage with name \"%s\" is not \"%s\".", storageEntity.getName(), StoragePlatformEntity.S3));

        // Validate that storage policy filter storage has S3 bucket name configured.
        // Please note that since S3 bucket name attribute value is required we pass a "true" flag.
        storageDaoHelper.getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageEntity, true);

        // Validate that storage policy filter storage has the S3 path prefix validation enabled.
        if (!storageDaoHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true))
        {
            throw new IllegalStateException(String.format("Path prefix validation must be enabled on \"%s\" storage.", storageEntity.getName()));
        }

        // Validate that storage policy filter storage has the S3 file existence validation enabled.
        if (!storageDaoHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true))
        {
            throw new IllegalStateException(String.format("File existence validation must be enabled on \"%s\" storage.", storageEntity.getName()));
        }

        // Retrieve and ensure that destination storage exists.
        StorageEntity destinationStorageEntity = storageDaoHelper.getStorageEntity(request.getStoragePolicyTransition().getDestinationStorageName());

        // Validate that storage platform is GLACIER for the destination storage.
        Assert.isTrue(StoragePlatformEntity.GLACIER.equals(destinationStorageEntity.getStoragePlatform().getName()), String
            .format("Storage platform for destination storage with name \"%s\" is not \"%s\".", destinationStorageEntity.getName(),
                StoragePlatformEntity.GLACIER));

        // Validate that storage policy transition destination storage has Glacier vault name configured.
        // Please note that since Glacier vault name attribute value is required we pass a "true" flag.
        storageDaoHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.GLACIER_ATTRIBUTE_NAME_VAULT_NAME), destinationStorageEntity,
                true);

        // Create and persist a new storage policy entity from the request information.
        storagePolicyEntity = createStoragePolicyEntity(request, namespaceEntity, storageEntity, destinationStorageEntity, storagePolicyRuleTypeEntity,
            businessObjectDefinitionEntity, fileTypeEntity);

        // Create and return the storage policy object from the persisted entity.
        return createStoragePolicyFromEntity(storagePolicyEntity);
    }

    /**
     * Gets an existing storage policy by key.
     *
     * @param storagePolicyKey the storage policy registration key
     *
     * @return the storage policy information
     */
    @Override
    public StoragePolicy getStoragePolicy(StoragePolicyKey storagePolicyKey)
    {
        // Validate and trim the key.
        storagePolicyHelper.validateStoragePolicyKey(storagePolicyKey);

        // Retrieve and ensure that a storage policy exists with the specified key.
        StoragePolicyEntity storagePolicyEntity = storageDaoHelper.getStoragePolicyEntity(storagePolicyKey);

        // Create and return the storage policy object from the persisted entity.
        return createStoragePolicyFromEntity(storagePolicyEntity);
    }

    /**
     * Validates the storage policy create request. This method also trims the request parameters.
     *
     * @param request the storage policy create request
     */
    private void validateStoragePolicyCreateRequest(StoragePolicyCreateRequest request)
    {
        Assert.notNull(request, "A storage policy create request must be specified.");

        storagePolicyHelper.validateStoragePolicyKey(request.getStoragePolicyKey());
        validateStoragePolicyRule(request.getStoragePolicyRule());
        validateStoragePolicyFilter(request.getStoragePolicyFilter());
        validateStoragePolicyTransition(request.getStoragePolicyTransition());
    }

    /**
     * Validates the storage policy rule. This method also trims the key parameters.
     *
     * @param storagePolicyRule the storage policy rule
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateStoragePolicyRule(StoragePolicyRule storagePolicyRule) throws IllegalArgumentException
    {
        Assert.notNull(storagePolicyRule, "A storage policy rule must be specified.");

        Assert.hasText(storagePolicyRule.getRuleType(), "A storage policy rule type must be specified.");
        storagePolicyRule.setRuleType(storagePolicyRule.getRuleType().trim());

        Assert.notNull(storagePolicyRule.getRuleValue(), "A storage policy rule value must be specified.");

        // Ensure that storage policy rule value is not negative.
        Assert.isTrue(storagePolicyRule.getRuleValue() >= 0, "Storage policy rule value must be a positive integer or zero.");
    }

    /**
     * Validates the storage policy filter. This method also trims the filter parameters.
     *
     * @param storagePolicyFilter the storage policy filter
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateStoragePolicyFilter(StoragePolicyFilter storagePolicyFilter) throws IllegalArgumentException
    {
        Assert.notNull(storagePolicyFilter, "A storage policy filter must be specified.");

        if (storagePolicyFilter.getNamespace() != null)
        {
            storagePolicyFilter.setNamespace(storagePolicyFilter.getNamespace().trim());
        }

        if (storagePolicyFilter.getBusinessObjectDefinitionName() != null)
        {
            storagePolicyFilter.setBusinessObjectDefinitionName(storagePolicyFilter.getBusinessObjectDefinitionName().trim());
        }

        // Validate that business object definition namespace and name are specified together.
        Assert.isTrue(
            (StringUtils.isNotBlank(storagePolicyFilter.getNamespace()) && StringUtils.isNotBlank(storagePolicyFilter.getBusinessObjectDefinitionName())) ||
                (StringUtils.isBlank(storagePolicyFilter.getNamespace()) && StringUtils.isBlank(storagePolicyFilter.getBusinessObjectDefinitionName())),
            "Business object definition name and namespace must be specified together.");

        if (storagePolicyFilter.getBusinessObjectFormatUsage() != null)
        {
            storagePolicyFilter.setBusinessObjectFormatUsage(storagePolicyFilter.getBusinessObjectFormatUsage().trim());
        }

        if (storagePolicyFilter.getBusinessObjectFormatFileType() != null)
        {
            storagePolicyFilter.setBusinessObjectFormatFileType(storagePolicyFilter.getBusinessObjectFormatFileType().trim());
        }

        // Validate that business object format usage and file type are specified together.
        Assert.isTrue((StringUtils.isNotBlank(storagePolicyFilter.getBusinessObjectFormatUsage()) &&
            StringUtils.isNotBlank(storagePolicyFilter.getBusinessObjectFormatFileType())) ||
            (StringUtils.isBlank(storagePolicyFilter.getBusinessObjectFormatUsage()) &&
                StringUtils.isBlank(storagePolicyFilter.getBusinessObjectFormatFileType())),
            "Business object format usage and file type must be specified together.");

        Assert.hasText(storagePolicyFilter.getStorageName(), "A storage name must be specified.");
        storagePolicyFilter.setStorageName(storagePolicyFilter.getStorageName().trim());
    }

    /**
     * Validates the storage policy transition. This method also trims the filter parameters.
     *
     * @param storagePolicyTransition the storage policy transition
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateStoragePolicyTransition(StoragePolicyTransition storagePolicyTransition) throws IllegalArgumentException
    {
        Assert.notNull(storagePolicyTransition, "A storage policy transition must be specified.");

        Assert.hasText(storagePolicyTransition.getDestinationStorageName(), "A destination storage name must be specified.");
        storagePolicyTransition.setDestinationStorageName(storagePolicyTransition.getDestinationStorageName().trim());
    }

    /**
     * Creates and persists a new storage policy registration entity from the request information.
     *
     * @param storagePolicyCreateRequest the storage policy create request
     * @param namespaceEntity the namespace entity
     * @param storageEntity the storage entity
     * @param destinationStorageEntity the destination storage entity
     * @param storagePolicyRuleTypeEntity the storage policy rule type entity
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param fileTypeEntity the file type entity
     *
     * @return the newly created storage policy entity
     */
    private StoragePolicyEntity createStoragePolicyEntity(StoragePolicyCreateRequest storagePolicyCreateRequest, NamespaceEntity namespaceEntity,
        StorageEntity storageEntity, StorageEntity destinationStorageEntity, StoragePolicyRuleTypeEntity storagePolicyRuleTypeEntity,
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity, FileTypeEntity fileTypeEntity)
    {
        StoragePolicyEntity storagePolicyEntity = new StoragePolicyEntity();

        storagePolicyEntity.setNamespace(namespaceEntity);
        storagePolicyEntity.setName(storagePolicyCreateRequest.getStoragePolicyKey().getStoragePolicyName());
        storagePolicyEntity.setStorage(storageEntity);
        storagePolicyEntity.setDestinationStorage(destinationStorageEntity);
        storagePolicyEntity.setStoragePolicyRuleType(storagePolicyRuleTypeEntity);
        storagePolicyEntity.setStoragePolicyRuleValue(storagePolicyCreateRequest.getStoragePolicyRule().getRuleValue());
        storagePolicyEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        if (StringUtils.isNotBlank(storagePolicyCreateRequest.getStoragePolicyFilter().getBusinessObjectFormatUsage()))
        {
            storagePolicyEntity.setUsage(storagePolicyCreateRequest.getStoragePolicyFilter().getBusinessObjectFormatUsage());
        }
        storagePolicyEntity.setFileType(fileTypeEntity);

        return herdDao.saveAndRefresh(storagePolicyEntity);
    }

    /**
     * Creates the storage policy registration from the persisted entity.
     *
     * @param storagePolicyEntity the storage policy registration entity
     *
     * @return the storage policy registration
     */
    private StoragePolicy createStoragePolicyFromEntity(StoragePolicyEntity storagePolicyEntity)
    {
        StoragePolicy storagePolicy = new StoragePolicy();

        storagePolicy.setId(storagePolicyEntity.getId());

        StoragePolicyKey storagePolicyKey = new StoragePolicyKey();
        storagePolicy.setStoragePolicyKey(storagePolicyKey);
        storagePolicyKey.setNamespace(storagePolicyEntity.getNamespace().getCode());
        storagePolicyKey.setStoragePolicyName(storagePolicyEntity.getName());

        StoragePolicyRule storagePolicyRule = new StoragePolicyRule();
        storagePolicy.setStoragePolicyRule(storagePolicyRule);
        storagePolicyRule.setRuleType(storagePolicyEntity.getStoragePolicyRuleType().getCode());
        storagePolicyRule.setRuleValue(storagePolicyEntity.getStoragePolicyRuleValue());

        StoragePolicyFilter storagePolicyFilter = new StoragePolicyFilter();
        storagePolicy.setStoragePolicyFilter(storagePolicyFilter);
        storagePolicyFilter.setNamespace(
            storagePolicyEntity.getBusinessObjectDefinition() != null ? storagePolicyEntity.getBusinessObjectDefinition().getNamespace().getCode() : null);
        storagePolicyFilter.setBusinessObjectDefinitionName(
            storagePolicyEntity.getBusinessObjectDefinition() != null ? storagePolicyEntity.getBusinessObjectDefinition().getName() : null);
        storagePolicyFilter.setBusinessObjectFormatUsage(storagePolicyEntity.getUsage());
        storagePolicyFilter.setBusinessObjectFormatFileType(storagePolicyEntity.getFileType() != null ? storagePolicyEntity.getFileType().getCode() : null);
        storagePolicyFilter.setStorageName(storagePolicyEntity.getStorage().getName());

        StoragePolicyTransition storagePolicyTransition = new StoragePolicyTransition();
        storagePolicy.setStoragePolicyTransition(storagePolicyTransition);
        storagePolicyTransition.setDestinationStorageName(storagePolicyEntity.getDestinationStorage().getName());

        return storagePolicy;
    }
}

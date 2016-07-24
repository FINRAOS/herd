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

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;

@Component
public class StoragePolicyDaoTestHelper
{
    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDaoTestHelper businessObjectDefinitionDaoTestHelper;

    @Autowired
    private FileTypeDao fileTypeDao;

    @Autowired
    private FileTypeDaoTestHelper fileTypeDaoTestHelper;

    @Autowired
    private NamespaceDao namespaceDao;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    @Autowired
    private StorageDao storageDao;

    @Autowired
    private StorageDaoTestHelper storageDaoTestHelper;

    @Autowired
    private StoragePolicyDao storagePolicyDao;

    @Autowired
    private StoragePolicyRuleTypeDao storagePolicyRuleTypeDao;

    @Autowired
    private StoragePolicyRuleTypeDaoTestHelper storagePolicyRuleTypeDaoTestHelper;

    @Autowired
    private StoragePolicyStatusDao storagePolicyStatusDao;

    /**
     * Creates and persists a storage policy entity.
     *
     * @param storagePolicyKey the storage policy key
     * @param storagePolicyRuleType the storage policy rule type
     * @param storagePolicyRuleValue the storage policy rule value
     * @param businessObjectDefinitionNamespace the business object definition namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object usage
     * @param businessObjectFormatFileType the business object format file type
     * @param storageName the storage name
     * @param destinationStorageName the destination storage name
     * @param storagePolicyStatus the storage policy status
     * @param storagePolicyVersion the storage policy version
     * @param storagePolicyLatestVersion specifies if this storage policy is flagged as latest version or not
     *
     * @return the newly created storage policy entity
     */
    public StoragePolicyEntity createStoragePolicyEntity(StoragePolicyKey storagePolicyKey, String storagePolicyRuleType, Integer storagePolicyRuleValue,
        String businessObjectDefinitionNamespace, String businessObjectDefinitionName, String businessObjectFormatUsage, String businessObjectFormatFileType,
        String storageName, String destinationStorageName, String storagePolicyStatus, Integer storagePolicyVersion, Boolean storagePolicyLatestVersion)
    {
        // Create a storage policy namespace entity if needed.
        NamespaceEntity storagePolicyNamespaceEntity = namespaceDao.getNamespaceByCd(storagePolicyKey.getNamespace());
        if (storagePolicyNamespaceEntity == null)
        {
            storagePolicyNamespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(storagePolicyKey.getNamespace());
        }

        // Create a storage policy rule type type entity if needed.
        StoragePolicyRuleTypeEntity storagePolicyRuleTypeEntity = storagePolicyRuleTypeDao.getStoragePolicyRuleTypeByCode(storagePolicyRuleType);
        if (storagePolicyRuleTypeEntity == null)
        {
            storagePolicyRuleTypeEntity =
                storagePolicyRuleTypeDaoTestHelper.createStoragePolicyRuleTypeEntity(storagePolicyRuleType, AbstractDaoTest.DESCRIPTION);
        }

        // Create a business object definition entity if needed.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = null;
        if (StringUtils.isNotBlank(businessObjectDefinitionName))
        {
            businessObjectDefinitionEntity = businessObjectDefinitionDao
                .getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(businessObjectDefinitionNamespace, businessObjectDefinitionName));
            if (businessObjectDefinitionEntity == null)
            {
                // Create a business object definition.
                businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
                    .createBusinessObjectDefinitionEntity(businessObjectDefinitionNamespace, businessObjectDefinitionName, AbstractDaoTest.DATA_PROVIDER_NAME,
                        AbstractDaoTest.BDEF_DESCRIPTION);
            }
        }

        // Create a business object format file type entity if needed.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(businessObjectFormatFileType))
        {
            fileTypeEntity = fileTypeDao.getFileTypeByCode(businessObjectFormatFileType);
            if (fileTypeEntity == null)
            {
                fileTypeEntity = fileTypeDaoTestHelper.createFileTypeEntity(businessObjectFormatFileType);
            }
        }

        // Create a storage entity of S3 storage platform type if needed.
        StorageEntity storageEntity = storageDao.getStorageByName(storageName);
        if (storageEntity == null)
        {
            storageEntity = storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3);
        }

        // Create a destination storage entity of GLACIER storage platform type if needed.
        StorageEntity destinationStorageEntity = storageDao.getStorageByName(destinationStorageName);
        if (destinationStorageEntity == null)
        {
            destinationStorageEntity = storageDaoTestHelper.createStorageEntity(destinationStorageName, StoragePlatformEntity.GLACIER);
        }

        // Create a storage entity, if not exists.
        StoragePolicyStatusEntity storagePolicyStatusEntity = storagePolicyStatusDao.getStoragePolicyStatusByCode(storagePolicyStatus);
        if (storagePolicyStatusEntity == null)
        {
            storagePolicyStatusEntity = createStoragePolicyStatusEntity(storagePolicyStatus);
        }

        // Create a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = new StoragePolicyEntity();

        storagePolicyEntity.setNamespace(storagePolicyNamespaceEntity);
        storagePolicyEntity.setName(storagePolicyKey.getStoragePolicyName());
        storagePolicyEntity.setStoragePolicyRuleType(storagePolicyRuleTypeEntity);
        storagePolicyEntity.setStoragePolicyRuleValue(storagePolicyRuleValue);
        storagePolicyEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        storagePolicyEntity.setUsage(businessObjectFormatUsage);
        storagePolicyEntity.setFileType(fileTypeEntity);
        storagePolicyEntity.setStorage(storageEntity);
        storagePolicyEntity.setDestinationStorage(destinationStorageEntity);
        storagePolicyEntity.setStatus(storagePolicyStatusEntity);
        storagePolicyEntity.setVersion(storagePolicyVersion);
        storagePolicyEntity.setLatestVersion(storagePolicyLatestVersion);

        return storagePolicyDao.saveAndRefresh(storagePolicyEntity);
    }

    /**
     * Creates and persists a new storage policy status entity.
     *
     * @param statusCode the code of the storage policy status
     *
     * @return the newly created storage policy status entity
     */
    public StoragePolicyStatusEntity createStoragePolicyStatusEntity(String statusCode)
    {
        return createStoragePolicyStatusEntity(statusCode, AbstractDaoTest.DESCRIPTION);
    }

    /**
     * Creates and persists a new storage policy status entity.
     *
     * @param statusCode the code of the storage policy status
     * @param description the description of the status code
     *
     * @return the newly created storage policy status entity
     */
    public StoragePolicyStatusEntity createStoragePolicyStatusEntity(String statusCode, String description)
    {
        StoragePolicyStatusEntity storagePolicyStatusEntity = new StoragePolicyStatusEntity();
        storagePolicyStatusEntity.setCode(statusCode);
        storagePolicyStatusEntity.setDescription(description);
        return storagePolicyStatusDao.saveAndRefresh(storagePolicyStatusEntity);
    }
}

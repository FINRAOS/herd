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
package org.finra.herd.service;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectDefinitionDaoTestHelper;
import org.finra.herd.dao.FileTypeDaoTestHelper;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.dao.NamespaceDaoTestHelper;
import org.finra.herd.dao.StorageDao;
import org.finra.herd.dao.StorageDaoTestHelper;
import org.finra.herd.dao.StoragePolicyRuleTypeDao;
import org.finra.herd.dao.StoragePolicyRuleTypeDaoTestHelper;
import org.finra.herd.dao.StoragePolicyTransitionTypeDao;
import org.finra.herd.dao.StoragePolicyTransitionTypeDaoTestHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.StoragePolicyCreateRequest;
import org.finra.herd.model.api.xml.StoragePolicyFilter;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.api.xml.StoragePolicyRule;
import org.finra.herd.model.api.xml.StoragePolicyTransition;
import org.finra.herd.model.api.xml.StoragePolicyUpdateRequest;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;

@Component
public class StoragePolicyServiceTestHelper
{
    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDaoTestHelper businessObjectDefinitionDaoTestHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

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
    private StoragePolicyRuleTypeDao storagePolicyRuleTypeDao;

    @Autowired
    private StoragePolicyRuleTypeDaoTestHelper storagePolicyRuleTypeDaoTestHelper;

    @Autowired
    private StoragePolicyTransitionTypeDao storagePolicyTransitionTypeDao;

    @Autowired
    private StoragePolicyTransitionTypeDaoTestHelper storagePolicyTransitionTypeDaoTestHelper;

    /**
     * Create and persist database entities required for storage policy service testing.
     */
    public void createDatabaseEntitiesForStoragePolicyTesting()
    {
        createDatabaseEntitiesForStoragePolicyTesting(AbstractServiceTest.STORAGE_POLICY_NAMESPACE_CD,
            Arrays.asList(AbstractServiceTest.STORAGE_POLICY_RULE_TYPE), AbstractServiceTest.BDEF_NAMESPACE, AbstractServiceTest.BDEF_NAME,
            Arrays.asList(AbstractServiceTest.FORMAT_FILE_TYPE_CODE), Arrays.asList(AbstractServiceTest.STORAGE_NAME),
            Arrays.asList(AbstractServiceTest.STORAGE_POLICY_TRANSITION_TYPE));
    }

    /**
     * Create and persist database entities required for storage policy service testing.
     *
     * @param storagePolicyNamespace the storage policy namespace
     * @param storagePolicyRuleTypes the list of storage policy rule types
     * @param businessObjectDefinitionNamespace the business object definition namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param fileTypes the list of file types
     * @param storageNames the list of storage names
     * @param storagePolicyTransitionTypes the list of storage policy transition types
     */
    public void createDatabaseEntitiesForStoragePolicyTesting(String storagePolicyNamespace, List<String> storagePolicyRuleTypes,
        String businessObjectDefinitionNamespace, String businessObjectDefinitionName, List<String> fileTypes, List<String> storageNames,
        List<String> storagePolicyTransitionTypes)
    {
        // Create a storage policy namespace entity, if not exists.
        NamespaceEntity storagePolicyNamespaceEntity = namespaceDao.getNamespaceByCd(storagePolicyNamespace);
        if (storagePolicyNamespaceEntity == null)
        {
            namespaceDaoTestHelper.createNamespaceEntity(storagePolicyNamespace);
        }

        // Create specified storage policy rule types, if not exist.
        if (!CollectionUtils.isEmpty(storagePolicyRuleTypes))
        {
            for (String storagePolicyRuleType : storagePolicyRuleTypes)
            {
                if (storagePolicyRuleTypeDao.getStoragePolicyRuleTypeByCode(storagePolicyRuleType) == null)
                {
                    storagePolicyRuleTypeDaoTestHelper.createStoragePolicyRuleTypeEntity(storagePolicyRuleType, AbstractServiceTest.DESCRIPTION);
                }
            }
        }

        // Create specified business object definition entity, if not exist.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity;
        if (StringUtils.isNotBlank(businessObjectDefinitionName))
        {
            businessObjectDefinitionEntity = businessObjectDefinitionDao
                .getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(businessObjectDefinitionNamespace, businessObjectDefinitionName));
            if (businessObjectDefinitionEntity == null)
            {
                // Create a business object definition.
                businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(businessObjectDefinitionNamespace, businessObjectDefinitionName,
                    AbstractServiceTest.DATA_PROVIDER_NAME, AbstractServiceTest.BDEF_DESCRIPTION);
            }
        }

        // Create specified file type entities, if not exist.
        if (!CollectionUtils.isEmpty(fileTypes))
        {
            for (String businessObjectFormatFileType : fileTypes)
            {
                fileTypeDaoTestHelper.createFileTypeEntity(businessObjectFormatFileType);
            }
        }

        // Create specified storage entities of S3 storage platform type, if not exist.
        if (!CollectionUtils.isEmpty(storageNames))
        {
            for (String storageName : storageNames)
            {
                if (storageDao.getStorageByName(storageName) == null)
                {
                    // Create S3 storage with the relative attributes.
                    storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, Arrays.asList(
                        new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), AbstractServiceTest.S3_BUCKET_NAME),
                        new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                            AbstractServiceTest.S3_KEY_PREFIX_VELOCITY_TEMPLATE),
                        new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.TRUE.toString()),
                        new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.TRUE.toString())));
                }
            }
        }

        // Create specified storage policy transition type entities, if not exist.
        if (!CollectionUtils.isEmpty(storagePolicyTransitionTypes))
        {
            for (String storagePolicyTransitionType : storagePolicyTransitionTypes)
            {
                if (storagePolicyTransitionTypeDao.getStoragePolicyTransitionTypeByCode(storagePolicyTransitionType) == null)
                {
                    storagePolicyTransitionTypeDaoTestHelper.createStoragePolicyTransitionTypeEntity(storagePolicyTransitionType);
                }
            }
        }
    }

    /**
     * Creates a storage policy create request.
     *
     * @param storagePolicyKey the storage policy key
     * @param storagePolicyRuleType the storage policy rule type
     * @param storagePolicyRuleValue the storage policy rule value
     * @param businessObjectDefinitionNamespace the business object definition namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object usage
     * @param businessObjectFormatFileType the business object format file type
     * @param storageName the storage name
     * @param storagePolicyTransitionType the storage policy transition type
     * @param storagePolicyStatus the storage policy status
     *
     * @return the newly created storage policy create request
     */
    public StoragePolicyCreateRequest createStoragePolicyCreateRequest(StoragePolicyKey storagePolicyKey, String storagePolicyRuleType,
        Integer storagePolicyRuleValue, String businessObjectDefinitionNamespace, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, String storageName, String storagePolicyTransitionType, String storagePolicyStatus)
    {
        StoragePolicyCreateRequest request = new StoragePolicyCreateRequest();

        request.setStoragePolicyKey(storagePolicyKey);

        StoragePolicyRule storagePolicyRule = new StoragePolicyRule();
        request.setStoragePolicyRule(storagePolicyRule);
        storagePolicyRule.setRuleType(storagePolicyRuleType);
        storagePolicyRule.setRuleValue(storagePolicyRuleValue);

        StoragePolicyFilter storagePolicyFilter = new StoragePolicyFilter();
        request.setStoragePolicyFilter(storagePolicyFilter);
        storagePolicyFilter.setNamespace(businessObjectDefinitionNamespace);
        storagePolicyFilter.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        storagePolicyFilter.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        storagePolicyFilter.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        storagePolicyFilter.setStorageName(storageName);

        StoragePolicyTransition storagePolicyTransition = new StoragePolicyTransition();
        request.setStoragePolicyTransition(storagePolicyTransition);
        storagePolicyTransition.setTransitionType(storagePolicyTransitionType);

        request.setStatus(storagePolicyStatus);

        return request;
    }

    /**
     * Creates a storage policy update request.
     *
     * @param storagePolicyRuleType the storage policy rule type
     * @param storagePolicyRuleValue the storage policy rule value
     * @param businessObjectDefinitionNamespace the business object definition namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object usage
     * @param businessObjectFormatFileType the business object format file type
     * @param storageName the storage name
     * @param storagePolicyTransitionType the storage policy transition type
     * @param storagePolicyStatus the storage policy status
     *
     * @return the newly created storage policy create request
     */
    public StoragePolicyUpdateRequest createStoragePolicyUpdateRequest(String storagePolicyRuleType, Integer storagePolicyRuleValue,
        String businessObjectDefinitionNamespace, String businessObjectDefinitionName, String businessObjectFormatUsage, String businessObjectFormatFileType,
        String storageName, String storagePolicyTransitionType, String storagePolicyStatus)
    {
        StoragePolicyUpdateRequest request = new StoragePolicyUpdateRequest();

        StoragePolicyRule storagePolicyRule = new StoragePolicyRule();
        request.setStoragePolicyRule(storagePolicyRule);
        storagePolicyRule.setRuleType(storagePolicyRuleType);
        storagePolicyRule.setRuleValue(storagePolicyRuleValue);

        StoragePolicyFilter storagePolicyFilter = new StoragePolicyFilter();
        request.setStoragePolicyFilter(storagePolicyFilter);
        storagePolicyFilter.setNamespace(businessObjectDefinitionNamespace);
        storagePolicyFilter.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        storagePolicyFilter.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        storagePolicyFilter.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        storagePolicyFilter.setStorageName(storageName);

        StoragePolicyTransition storagePolicyTransition = new StoragePolicyTransition();
        request.setStoragePolicyTransition(storagePolicyTransition);
        storagePolicyTransition.setTransitionType(storagePolicyTransitionType);

        request.setStatus(storagePolicyStatus);

        return request;
    }

    /**
     * Returns an expected string representation of the specified storage policy key and version.
     *
     * @param storagePolicyKey the storage policy key
     * @param storagePolicyVersion the storage policy version
     *
     * @return the string representation of the specified storage policy key
     */
    public String getExpectedStoragePolicyKeyAndVersionAsString(StoragePolicyKey storagePolicyKey, Integer storagePolicyVersion)
    {
        return String.format("namespace: \"%s\", storagePolicyName: \"%s\", storagePolicyVersion: \"%d\"", storagePolicyKey.getNamespace(),
            storagePolicyKey.getStoragePolicyName(), storagePolicyVersion);
    }

    /**
     * Returns an expected storage policy not found error message.
     *
     * @param storagePolicyKey the storage policy key
     *
     * @return the storage policy not found error message
     */
    public String getExpectedStoragePolicyNotFoundErrorMessage(StoragePolicyKey storagePolicyKey)
    {
        return String.format("Storage policy with name \"%s\" does not exist for \"%s\" namespace.", storagePolicyKey.getStoragePolicyName(),
            storagePolicyKey.getNamespace());
    }
}

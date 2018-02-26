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


import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_CREATE;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.BusinessObjectFormatService;
import org.finra.herd.service.MessageNotificationEventService;
import org.finra.herd.service.RelationalTableRegistrationService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectDataStatusDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.DataProviderDaoHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.SearchIndexUpdateHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageUnitStatusDaoHelper;

/**
 * The relational table registration service implementation
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class RelationalTableRegistrationServiceImpl implements RelationalTableRegistrationService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private DataProviderDaoHelper dataProviderDaoHelper;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectFormatService businessObjectFormatService;

    @Autowired
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectDataStatusDaoHelper businessObjectDataStatusDaoHelper;

    @Autowired
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private MessageNotificationEventService sqsNotificationEventService;

    @PublishNotificationMessages
    @NamespacePermission(fields = "#relationalTableRegistrationCreateRequest.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectData createRelationalTableRegistration(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest)
    {
        return createRelationalTableRegistrationImpl(relationalTableRegistrationCreateRequest);
    }

    /**
     * Create relational table registration in Herd
     * Includes create business object definition, business object format and business object data
     *
     * @param createRequest relational table registration create request
     *
     * @return business object data
     */
    private BusinessObjectData createRelationalTableRegistrationImpl(RelationalTableRegistrationCreateRequest createRequest)
    {
        String relationalTableBusinessObjectFormatAttributeName =
            configurationHelper.getProperty(ConfigurationValue.RELATIONAL_TABLE_BUSINESS_OBJECT_FORMAT_ATTRIBUTE_NAME, String.class);
        validateAndTrimRelationalTableRegistrationCreateRequest(createRequest);

        DataProviderEntity dataProviderEntity = dataProviderDaoHelper.getDataProviderEntity(createRequest.getDataProviderName());
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(createRequest.getNamespace());
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(createRequest.getStorageName());
        Assert.isTrue(storageEntity.getStoragePlatform().getName().equals(StoragePlatformEntity.RELATIONAL),
            String.format("Only %s storage platform is supported.", StoragePlatformEntity.RELATIONAL));

        // Create Business Object Definition
        BusinessObjectDefinitionCreateRequest businessObjectDefinitionCreateRequest = new BusinessObjectDefinitionCreateRequest();
        businessObjectDefinitionCreateRequest.setNamespace(namespaceEntity.getCode());
        businessObjectDefinitionCreateRequest.setDataProviderName(dataProviderEntity.getName());
        businessObjectDefinitionCreateRequest.setBusinessObjectDefinitionName(createRequest.getBusinessObjectDefinitionName());
        businessObjectDefinitionCreateRequest.setDisplayName(createRequest.getBusinessObjectDefinitionDisplayName());
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.createBusinessObjectDefinitionEntity(businessObjectDefinitionCreateRequest);

        // Notify the search index that a business object definition must be created.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_CREATE);

        // Create Business Object Format
        BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest = new BusinessObjectFormatCreateRequest();
        businessObjectFormatCreateRequest.setNamespace(namespaceEntity.getCode());
        businessObjectFormatCreateRequest.setBusinessObjectDefinitionName(businessObjectDefinitionEntity.getName());
        businessObjectFormatCreateRequest.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        businessObjectFormatCreateRequest.setBusinessObjectFormatUsage(createRequest.getBusinessObjectFormatUsage());
        businessObjectFormatCreateRequest.setPartitionKey(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY);
        businessObjectFormatCreateRequest
            .setAttributes(Arrays.asList(new Attribute(relationalTableBusinessObjectFormatAttributeName, createRequest.getRelationalTableName())));
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatCreateRequest);

        BusinessObjectFormatEntity businessObjectFormatEntity =
            businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormat));

        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(BusinessObjectDataStatusEntity.VALID);

        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setVersion(0);
        businessObjectDataEntity.setLatestVersion(true);
        businessObjectDataEntity.setPartitionValue(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE);

        // Get the storage unit status entity for the ENABLED status.
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.ENABLED);
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        businessObjectDataEntity.setStorageUnits(Arrays.asList(storageUnitEntity));

        // Persist the new entity.
        businessObjectDataEntity = businessObjectDataDao.saveAndRefresh(businessObjectDataEntity);

        // Create a status change notification to be sent on create business object data event.
        sqsNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity),
                businessObjectDataStatusEntity.getCode(), null);

        return businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity);
    }

    /**
     * Validate and trim relational table registration create request
     *
     * @param createRequest relational table registration create request
     */
    private void validateAndTrimRelationalTableRegistrationCreateRequest(RelationalTableRegistrationCreateRequest createRequest)
    {
        Assert.notNull(createRequest, "A relational table registration create request must be specified.");
        createRequest.setNamespace(alternateKeyHelper.validateStringParameter("namespace", createRequest.getNamespace()));
        createRequest.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", createRequest.getBusinessObjectDefinitionName()));

        createRequest.setRelationalTableName(alternateKeyHelper.validateStringParameter("relational table name", createRequest.getRelationalTableName()));

        createRequest.setDataProviderName(alternateKeyHelper.validateStringParameter("data provider name", createRequest.getDataProviderName()));
        createRequest.setStorageName(alternateKeyHelper.validateStringParameter("storage name", createRequest.getStorageName()));
        createRequest.setBusinessObjectFormatUsage(
            alternateKeyHelper.validateStringParameter("business object format usage", createRequest.getBusinessObjectFormatUsage()));

        if (createRequest.getBusinessObjectDefinitionDisplayName() != null)
        {
            createRequest.setBusinessObjectDefinitionDisplayName(createRequest.getBusinessObjectDefinitionDisplayName().trim());
        }
    }
}

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
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.model.api.xml.RelationalTableRegistrationDeleteResponse;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.RelationalStorageAttributesDto;
import org.finra.herd.model.dto.RelationalTableRegistrationDto;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.RelationalTableRegistrationHelperService;
import org.finra.herd.service.RelationalTableRegistrationService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.StorageUnitHelper;

/**
 * An implementation of the relational table registration service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class RelationalTableRegistrationServiceImpl implements RelationalTableRegistrationService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RelationalTableRegistrationServiceImpl.class);

    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private RelationalTableRegistrationHelperService relationalTableRegistrationHelperService;

    @Autowired
    private StorageUnitDao storageUnitDao;

    @Autowired
    private StorageUnitHelper storageUnitHelper;

    @PublishNotificationMessages
    @NamespacePermission(fields = "#relationalTableRegistrationCreateRequest.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public BusinessObjectData createRelationalTableRegistration(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest,
        Boolean appendToExistingBusinessObjectDefinition)
    {
        return createRelationalTableRegistrationImpl(relationalTableRegistrationCreateRequest, appendToExistingBusinessObjectDefinition);
    }

    @NamespacePermission(fields = "#businessObjectFormatKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public RelationalTableRegistrationDeleteResponse deleteRelationalTableRegistration(BusinessObjectFormatKey businessObjectFormatKey)
    {
        return deleteRelationalTableRegistrationImpl(businessObjectFormatKey);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<BusinessObjectDataStorageUnitKey> getRelationalTableRegistrationsForSchemaUpdate()
    {
        return getRelationalTableRegistrationsForSchemaUpdateImpl();
    }

    @PublishNotificationMessages
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public BusinessObjectData processRelationalTableRegistrationForSchemaUpdate(BusinessObjectDataStorageUnitKey storageUnitKey)
    {
        return processRelationalTableRegistrationForSchemaUpdateImpl(storageUnitKey);
    }

    /**
     * Creates a new relational table registration.
     *
     * @param relationalTableRegistrationCreateRequest the relational table registration create request
     * @param appendToExistingBusinessObjectDefinition boolean flag that determines if the format should be appended to an existing business object definition
     *
     * @return the information for the newly created business object data
     */
    BusinessObjectData createRelationalTableRegistrationImpl(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest,
        Boolean appendToExistingBusinessObjectDefinition)
    {
        // Validate the relational table registration create request.
        relationalTableRegistrationHelperService.validateAndTrimRelationalTableRegistrationCreateRequest(relationalTableRegistrationCreateRequest);

        // Validate database entities per specified relational table registration create request.
        // This method also gets storage attributes required to perform relation table registration.
        RelationalStorageAttributesDto relationalStorageAttributesDto = relationalTableRegistrationHelperService
            .prepareForRelationalTableRegistration(relationalTableRegistrationCreateRequest, appendToExistingBusinessObjectDefinition);

        // Retrieve a list of actual schema columns for the specified relational table.
        // This method uses actual JDBC connection to retrieve a description of table columns.
        List<SchemaColumn> schemaColumns = relationalTableRegistrationHelperService
            .retrieveRelationalTableColumns(relationalStorageAttributesDto, relationalTableRegistrationCreateRequest.getRelationalSchemaName(),
                relationalTableRegistrationCreateRequest.getRelationalTableName());

        // Create a new relational table registration and return the information for the newly created business object data.
        return relationalTableRegistrationHelperService
            .registerRelationalTable(relationalTableRegistrationCreateRequest, schemaColumns, appendToExistingBusinessObjectDefinition);
    }

    /**
     * Deletes a relational table registration.
     *
     * @param businessObjectFormatKey the business object format key
     *
     * @return the relational table registration delete response
     */
    RelationalTableRegistrationDeleteResponse deleteRelationalTableRegistrationImpl(BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Perform validations and trim.  Business object format version is expected to be null, so we exclude it from validation.
        businessObjectFormatHelper.validateBusinessObjectFormatKey(businessObjectFormatKey, false);

        // Create business object definition key from the business object format key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey =
            new BusinessObjectDefinitionKey(businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName());

        // Get the existing business object definition
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Get the business object format entities for this business object definition
        Collection<BusinessObjectFormatEntity> businessObjectFormatEntities = businessObjectDefinitionEntity.getBusinessObjectFormats();

        // Create a list to hold the filtered business object format entities.
        List<BusinessObjectFormatEntity> filteredBusinessObjectFormatEntities = new ArrayList<>();

        // Filter the list of business object format entities.
        for (BusinessObjectFormatEntity businessObjectFormatEntity : businessObjectFormatEntities)
        {
            // If this format entity matches the format usage and file type code. Please note that we use equalsIgnoreCase()
            // for business object format usage values, since business object format usage is not a database lookup value.
            if (businessObjectFormatEntity.getUsage().equalsIgnoreCase(businessObjectFormatKey.getBusinessObjectFormatUsage()) &&
                businessObjectFormatEntity.getFileTypeCode().equals(businessObjectFormatKey.getBusinessObjectFormatFileType()))
            {
                filteredBusinessObjectFormatEntities.add(businessObjectFormatEntity);
            }
        }

        // Create a list of business object data that are deleted.
        List<BusinessObjectData> deletedBusinessObjectData = new ArrayList<>();

        // For each business object format entity, delete the associated business object data and then delete the business object format.
        for (BusinessObjectFormatEntity businessObjectFormatEntity : filteredBusinessObjectFormatEntities)
        {
            // Get the associated Business Object Data entity.
            List<BusinessObjectDataKey> businessObjectDataKeys =
                businessObjectDataDao.getBusinessObjectDataByBusinessObjectFormat(businessObjectFormatEntity, null);

            // Delete the business object data associated with this business object format.
            for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys)
            {
                BusinessObjectData businessObjectData = businessObjectDataHelper.deleteBusinessObjectData(businessObjectDataKey, false);
                LOGGER.info("Deleting business object data. businessObjectData={}", jsonHelper.objectToJson(businessObjectData));

                // Add the business object data to the deleted list.
                deletedBusinessObjectData.add(businessObjectData);
            }

            // Delete the business object format.
            // This service call will also update the Elasticsearch index.
            BusinessObjectFormat businessObjectFormat =
                businessObjectFormatHelper.deleteBusinessObjectFormat(businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormatEntity));

            LOGGER.info("Deleting business object format. businessObjectFormat={}", jsonHelper.objectToJson(businessObjectFormat));
        }

        // If specified Business Object Format is the last Business Object Format in the Business Object Definition,
        // then the Business Object Definition will be deleted as well.
        if (businessObjectFormatEntities.size() == filteredBusinessObjectFormatEntities.size())
        {
            // Delete the business object definition.
            // This service call will also update the Elasticsearch index.
            BusinessObjectDefinition businessObjectDefinition = businessObjectDefinitionHelper.deleteBusinessObjectDefinition(businessObjectDefinitionKey);

            LOGGER.info("Deleting business object definition. businessObjectDefinition={}", jsonHelper.objectToJson(businessObjectDefinition));
        }

        return new RelationalTableRegistrationDeleteResponse(deletedBusinessObjectData);
    }

    /**
     * Returns all relational tables registered in the system.
     *
     * @return the list of relational table registrations
     */
    List<BusinessObjectDataStorageUnitKey> getRelationalTableRegistrationsForSchemaUpdateImpl()
    {
        // Create a result list.
        List<BusinessObjectDataStorageUnitKey> storageUnitKeys = new ArrayList<>();

        // Get all latest version storage units registered in the system that belong to storage
        // of the RELATIONAL storage platform type and business object format of the RELATIONAL_TABLE file type.
        List<StorageUnitEntity> storageUnitEntities = storageUnitDao
            .getLatestVersionStorageUnitsByStoragePlatformAndFileType(StoragePlatformEntity.RELATIONAL, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);

        // Populate the result list per selected storage unit entities.
        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            storageUnitKeys.add(storageUnitHelper.createStorageUnitKeyFromEntity(storageUnitEntity));
        }

        return storageUnitKeys;
    }

    /**
     * Updates relational table schema, if changes are detected, for an already existing relational table registration.
     *
     * @param storageUnitKey the storage unit key for relational table registration
     *
     * @return the information for the newly created business object data, if schema was updated; null otherwise
     */
    BusinessObjectData processRelationalTableRegistrationForSchemaUpdateImpl(BusinessObjectDataStorageUnitKey storageUnitKey)
    {
        // Validate database entities per specified relational table registration create request.
        // This method also gets storage attributes required to perform relation table registration.
        RelationalTableRegistrationDto relationalTableRegistrationDto =
            relationalTableRegistrationHelperService.prepareForRelationalTableSchemaUpdate(storageUnitKey);

        // Retrieve a list of actual schema columns for the specified relational table.
        // This method uses actual JDBC connection to retrieve a description of table columns.
        List<SchemaColumn> schemaColumns = relationalTableRegistrationHelperService
            .retrieveRelationalTableColumns(relationalTableRegistrationDto.getRelationalStorageAttributes(),
                relationalTableRegistrationDto.getRelationalSchemaName(), relationalTableRegistrationDto.getRelationalTableName());

        // Check if we need to update the relational table schema.
        if (!CollectionUtils.isEqualCollection(schemaColumns, relationalTableRegistrationDto.getBusinessObjectFormat().getSchema().getColumns()))
        {
            return relationalTableRegistrationHelperService.updateRelationalTableSchema(relationalTableRegistrationDto, schemaColumns);
        }
        else
        {
            return null;
        }
    }
}

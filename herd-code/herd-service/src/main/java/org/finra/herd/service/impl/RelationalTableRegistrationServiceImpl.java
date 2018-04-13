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
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.RelationalStorageAttributesDto;
import org.finra.herd.model.dto.RelationalTableRegistrationDto;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.RelationalTableRegistrationHelperService;
import org.finra.herd.service.RelationalTableRegistrationService;
import org.finra.herd.service.helper.StorageUnitHelper;

/**
 * An implementation of the relational table registration service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class RelationalTableRegistrationServiceImpl implements RelationalTableRegistrationService
{
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

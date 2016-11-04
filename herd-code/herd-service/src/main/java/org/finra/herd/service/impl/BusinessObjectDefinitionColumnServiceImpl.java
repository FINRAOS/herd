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

import java.util.Collection;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.IterableUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.BusinessObjectDefinitionColumnDao;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.SchemaColumnDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumn;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionColumnEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;
import org.finra.herd.service.BusinessObjectDefinitionColumnService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionColumnDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;

/**
 * The business object definition column service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDefinitionColumnServiceImpl implements BusinessObjectDefinitionColumnService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDefinitionColumnDao businessObjectDefinitionColumnDao;

    @Autowired
    private BusinessObjectDefinitionColumnDaoHelper businessObjectDefinitionColumnDaoHelper;

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Autowired
    private SchemaColumnDao schemaColumnDao;

    @Override
    public BusinessObjectDefinitionColumn createBusinessObjectDefinitionColumn(BusinessObjectDefinitionColumnCreateRequest request)
    {
        // Validate and trim the business object definition column create request.
        validateBusinessObjectDefinitionColumnCreateRequest(request);

        // Get the business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey =
            businessObjectDefinitionHelper.getBusinessObjectDefinitionKey(request.getBusinessObjectDefinitionColumnKey());

        // Get the business object definition and ensure it exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Ensure a business object definition column with the specified name doesn't already exist for the business object definition.
        if (businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByBusinessObjectDefinitionColumnName(businessObjectDefinitionEntity,
            request.getBusinessObjectDefinitionColumnKey().getBusinessObjectDefinitionColumnName()) != null)
        {
            throw new AlreadyExistsException(String.format(
                "Unable to create business object definition column with name \"%s\" because it already exists for the business object definition {%s}.",
                request.getBusinessObjectDefinitionColumnKey().getBusinessObjectDefinitionColumnName(),
                businessObjectDefinitionHelper.businessObjectDefinitionKeyToString(businessObjectDefinitionKey)));
        }

        // Retrieve schema column entities from all format instances for the business object definition that match the specified schema column name.
        Collection<SchemaColumnEntity> schemaColumnEntities = schemaColumnDao.getSchemaColumns(businessObjectDefinitionEntity, request.getSchemaColumnName());

        // Ensure that exists at least one schema column that matches the specified schema column name.
        if (CollectionUtils.isEmpty(schemaColumnEntities))
        {
            if (businessObjectDefinitionEntity.getDescriptiveBusinessObjectFormat() == null)
            {
                throw new ObjectNotFoundException(String.format("Unable to create business object definition column because there are no format schema " +
                        "columns with name \"%s\" for the business object definition {%s}.", request.getSchemaColumnName(),
                    businessObjectDefinitionHelper.businessObjectDefinitionKeyToString(businessObjectDefinitionKey)));
            }
            else
            {
                throw new ObjectNotFoundException(String.format("Unable to create business object definition column because there are no format schema " +
                        "columns with name \"%s\" in the descriptive business object format for the business object definition {%s}.",
                    request.getSchemaColumnName(), businessObjectDefinitionHelper.businessObjectDefinitionKeyToString(businessObjectDefinitionKey)));
            }
        }

        // Ensure a business object definition column with the specified schema column name doesn't already exist for this business object definition.
        for (SchemaColumnEntity schemaColumnEntity : schemaColumnEntities)
        {
            if (schemaColumnEntity.getBusinessObjectDefinitionColumn() != null)
            {
                throw new AlreadyExistsException(String.format(
                    "Unable to create business object definition column because a business object definition column " +
                        "with schema column name \"%s\" already exists for the business object definition {%s}.", request.getSchemaColumnName(),
                    businessObjectDefinitionHelper.businessObjectDefinitionKeyToString(businessObjectDefinitionKey)));
            }
        }

        // Create a business object definition column entity from the request information.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionEntity, request);

        // Link schema columns with the business object definition column.
        for (SchemaColumnEntity schemaColumnEntity : schemaColumnEntities)
        {
            schemaColumnEntity.setBusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity);
        }

        // Persist the new entity.
        businessObjectDefinitionColumnEntity = businessObjectDefinitionColumnDao.saveAndRefresh(businessObjectDefinitionColumnEntity);

        // Create and return the business object definition column object from the persisted entity.
        return createBusinessObjectDefinitionColumnFromEntity(businessObjectDefinitionColumnEntity);
    }

    @Override
    public BusinessObjectDefinitionColumn deleteBusinessObjectDefinitionColumn(BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey)
    {
        // Validate and trim the business object definition column key.
        validateBusinessObjectDefinitionColumnKey(businessObjectDefinitionColumnKey);

        // Retrieve and ensure that a business object definition column exists with the business object definition.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoHelper.getBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey);

        // Unlink schema columns from the business object definition column.
        for (SchemaColumnEntity schemaColumnEntity : businessObjectDefinitionColumnEntity.getSchemaColumns())
        {
            schemaColumnEntity.setBusinessObjectDefinitionColumn(null);
        }

        // Delete the business object definition column.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionColumnEntity.getBusinessObjectDefinition();
        businessObjectDefinitionEntity.getColumns().remove(businessObjectDefinitionColumnEntity);
        businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);

        // Create and return the business object definition column object from the deleted entity.
        return createBusinessObjectDefinitionColumnFromEntity(businessObjectDefinitionColumnEntity);
    }

    @Override
    public BusinessObjectDefinitionColumn getBusinessObjectDefinitionColumn(BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey)
    {
        // Validate and trim the business object definition column key.
        validateBusinessObjectDefinitionColumnKey(businessObjectDefinitionColumnKey);

        // Retrieve and ensure that a business object definition column exists with the business object definition.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoHelper.getBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey);

        // Create and return the business object definition column object from the persisted entity.
        return createBusinessObjectDefinitionColumnFromEntity(businessObjectDefinitionColumnEntity);
    }

    @Override
    public BusinessObjectDefinitionColumnKeys getBusinessObjectDefinitionColumns(BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        // Validate and trim the business object definition key.
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);

        // Retrieve the business object definition and ensure it exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Create and populate a list of business object definition column keys.
        BusinessObjectDefinitionColumnKeys businessObjectDefinitionColumnKeys = new BusinessObjectDefinitionColumnKeys();
        for (BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity : businessObjectDefinitionEntity.getColumns())
        {
            businessObjectDefinitionColumnKeys.getBusinessObjectDefinitionColumnKeys()
                .add(getBusinessObjectDefinitionColumnKey(businessObjectDefinitionColumnEntity));
        }

        return businessObjectDefinitionColumnKeys;
    }

    @Override
    public BusinessObjectDefinitionColumn updateBusinessObjectDefinitionColumn(BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey,
        BusinessObjectDefinitionColumnUpdateRequest request)
    {
        // Validate and trim the business object definition column key.
        validateBusinessObjectDefinitionColumnKey(businessObjectDefinitionColumnKey);

        // Validate and trim the business object definition column update request.
        validateBusinessObjectDefinitionColumnUpdateRequest(request);

        // Retrieve and ensure that a business object definition column exists with the business object definition.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoHelper.getBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey);

        // Update the entity with the new values.
        businessObjectDefinitionColumnEntity.setDescription(request.getDescription());

        // Persist the entity.
        businessObjectDefinitionColumnEntity = businessObjectDefinitionColumnDao.saveAndRefresh(businessObjectDefinitionColumnEntity);

        // Create and return the business object definition column object from the persisted entity.
        return createBusinessObjectDefinitionColumnFromEntity(businessObjectDefinitionColumnEntity);
    }

    /**
     * Creates a new business object definition column entity from the business object definition entity and the request information.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param request the business object definition column create request
     *
     * @return the newly created business object definition column entity
     */
    private BusinessObjectDefinitionColumnEntity createBusinessObjectDefinitionColumnEntity(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        BusinessObjectDefinitionColumnCreateRequest request)
    {
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity = new BusinessObjectDefinitionColumnEntity();

        businessObjectDefinitionColumnEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionColumnEntity.setName(request.getBusinessObjectDefinitionColumnKey().getBusinessObjectDefinitionColumnName());
        businessObjectDefinitionColumnEntity.setDescription(request.getDescription());

        return businessObjectDefinitionColumnEntity;
    }

    /**
     * Creates a business object definition column from the persisted entity.
     *
     * @param businessObjectDefinitionColumnEntity the business object definition column entity
     *
     * @return the business object definition column
     */
    private BusinessObjectDefinitionColumn createBusinessObjectDefinitionColumnFromEntity(
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity)
    {
        BusinessObjectDefinitionColumn businessObjectDefinitionColumn = new BusinessObjectDefinitionColumn();

        businessObjectDefinitionColumn.setId(businessObjectDefinitionColumnEntity.getId());
        businessObjectDefinitionColumn.setBusinessObjectDefinitionColumnKey(getBusinessObjectDefinitionColumnKey(businessObjectDefinitionColumnEntity));
        businessObjectDefinitionColumn.setDescription(businessObjectDefinitionColumnEntity.getDescription());

        if (CollectionUtils.isNotEmpty(businessObjectDefinitionColumnEntity.getSchemaColumns()))
        {
            businessObjectDefinitionColumn.setSchemaColumnName(IterableUtils.get(businessObjectDefinitionColumnEntity.getSchemaColumns(), 0).getName());
        }

        return businessObjectDefinitionColumn;
    }

    /**
     * Creates a business object definition column key from the entity.
     *
     * @param businessObjectDefinitionColumnEntity the business object definition entity
     *
     * @return the business object definition column key
     */
    private BusinessObjectDefinitionColumnKey getBusinessObjectDefinitionColumnKey(BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity)
    {
        return new BusinessObjectDefinitionColumnKey(businessObjectDefinitionColumnEntity.getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectDefinitionColumnEntity.getBusinessObjectDefinition().getName(), businessObjectDefinitionColumnEntity.getName());
    }

    /**
     * Validates the business object definition column create request. This method also trims the request parameters.
     *
     * @param request the business object definition column create request
     */
    private void validateBusinessObjectDefinitionColumnCreateRequest(BusinessObjectDefinitionColumnCreateRequest request)
    {
        Assert.notNull(request, "A business object definition column create request must be specified.");

        validateBusinessObjectDefinitionColumnKey(request.getBusinessObjectDefinitionColumnKey());

        Assert.hasText(request.getSchemaColumnName(), "A schema column name must be specified.");
        request.setSchemaColumnName(request.getSchemaColumnName().trim());
    }

    /**
     * Validates the business object definition column key. This method also trims the key parameters.
     *
     * @param key the business object definition column key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectDefinitionColumnKey(BusinessObjectDefinitionColumnKey key) throws IllegalArgumentException
    {
        Assert.notNull(key, "A business object data attribute key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", key.getBusinessObjectDefinitionName()));
        key.setBusinessObjectDefinitionColumnName(
            alternateKeyHelper.validateStringParameter("business object definition column name", key.getBusinessObjectDefinitionColumnName()));
    }

    /**
     * Validates the business object definition column update request. This method also trims the request parameters.
     *
     * @param request the business object definition column update request
     */
    private void validateBusinessObjectDefinitionColumnUpdateRequest(BusinessObjectDefinitionColumnUpdateRequest request)
    {
        Assert.notNull(request, "A business object definition column update request must be specified.");
    }
}

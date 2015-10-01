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
package org.finra.dm.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.dm.dao.DmDao;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.dm.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.CustomDdlEntity;
import org.finra.dm.model.jpa.FileTypeEntity;
import org.finra.dm.model.jpa.PartitionKeyGroupEntity;
import org.finra.dm.model.jpa.SchemaColumnEntity;
import org.finra.dm.model.api.xml.AttributeDefinition;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.dm.model.api.xml.BusinessObjectFormat;
import org.finra.dm.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.dm.model.api.xml.BusinessObjectFormatDdl;
import org.finra.dm.model.api.xml.BusinessObjectFormatDdlCollectionRequest;
import org.finra.dm.model.api.xml.BusinessObjectFormatDdlCollectionResponse;
import org.finra.dm.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.dm.model.api.xml.BusinessObjectFormatKey;
import org.finra.dm.model.api.xml.BusinessObjectFormatKeys;
import org.finra.dm.model.api.xml.BusinessObjectFormatUpdateRequest;
import org.finra.dm.model.api.xml.CustomDdlKey;
import org.finra.dm.model.api.xml.Schema;
import org.finra.dm.model.api.xml.SchemaColumn;
import org.finra.dm.service.BusinessObjectFormatService;
import org.finra.dm.service.helper.BusinessObjectFormatHelper;
import org.finra.dm.service.helper.DdlGeneratorFactory;
import org.finra.dm.service.helper.DmDaoHelper;
import org.finra.dm.service.helper.DmHelper;

/**
 * The business object format service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectFormatServiceImpl implements BusinessObjectFormatService
{
    @Autowired
    private DmHelper dmHelper;

    @Autowired
    private DmDao dmDao;

    @Autowired
    private DmDaoHelper dmDaoHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private DdlGeneratorFactory ddlGeneratorFactory;

    /**
     * Creates a new business object format.
     *
     * @param request the business object format create request.
     *
     * @return the created business object format.
     */
    @Override
    public BusinessObjectFormat createBusinessObjectFormat(BusinessObjectFormatCreateRequest request)
    {
        // Perform the validation of the request parameters, except for the alternate key.
        validateBusinessObjectFormatCreateRequest(request);

        // Get business object format key from the request.
        BusinessObjectFormatKey businessObjectFormatKey = getBusinessObjectFormatKey(request);

        // Get the business object definition and ensure it exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity;
        if (StringUtils.isBlank(businessObjectFormatKey.getNamespace()))
        {
            // If namespace is not specified, retrieve the legacy business object definition by it's name only.
            businessObjectDefinitionEntity = dmDaoHelper.getLegacyBusinessObjectDefinitionEntity(request.getBusinessObjectDefinitionName());

            // Update the business object key with the retrieved namespace.
            businessObjectFormatKey.setNamespace(businessObjectDefinitionEntity.getNamespace().getCode());
        }
        else
        {
            // Since namespace is specified, retrieve a business object definition by it's key.
            businessObjectDefinitionEntity = dmDaoHelper.getBusinessObjectDefinitionEntity(
                new BusinessObjectDefinitionKey(businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName()));
        }

        // Get business object format file type and ensure it exists.
        FileTypeEntity fileTypeEntity = dmDaoHelper.getFileTypeEntity(request.getBusinessObjectFormatFileType());

        // Get the latest format version for this business format, if it exists.
        BusinessObjectFormatEntity latestVersionBusinessObjectFormatEntity = dmDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey);

        // Create a business object format entity from the request information.
        Integer businessObjectFormatVersion =
            latestVersionBusinessObjectFormatEntity == null ? 0 : latestVersionBusinessObjectFormatEntity.getBusinessObjectFormatVersion() + 1;
        BusinessObjectFormatEntity newBusinessObjectFormatEntity =
            createBusinessObjectFormatEntity(request, businessObjectDefinitionEntity, fileTypeEntity, businessObjectFormatVersion);

        // If the latest version exists, perform the additive schema validation and update the latest entity.
        if (latestVersionBusinessObjectFormatEntity != null)
        {
            // Get the latest version business object format model object.
            BusinessObjectFormat latestVersionBusinessObjectFormat =
                businessObjectFormatHelper.createBusinessObjectFormatFromEntity(latestVersionBusinessObjectFormatEntity);

            // If the latest version format has schema, check the new format version schema is "additive" to the previous format version.
            if (latestVersionBusinessObjectFormat.getSchema() != null)
            {
                validateNewSchemaIsAdditiveToOldSchema(request.getSchema(), latestVersionBusinessObjectFormat.getSchema());
            }

            // Update the latest entity.
            latestVersionBusinessObjectFormatEntity.setLatestVersion(false);
            dmDao.saveAndRefresh(latestVersionBusinessObjectFormatEntity);
        }

        // Persist the new entity.
        newBusinessObjectFormatEntity = dmDao.saveAndRefresh(newBusinessObjectFormatEntity);

        // Create and return the business object format object from the persisted entity.
        return businessObjectFormatHelper.createBusinessObjectFormatFromEntity(newBusinessObjectFormatEntity);
    }

    /**
     * Updates a business object format.
     *
     * @param businessObjectFormatKey the business object format key
     * @param request the business object format update request
     *
     * @return the updated business object format.
     */
    @Override
    public BusinessObjectFormat updateBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey, BusinessObjectFormatUpdateRequest request)
    {
        // Perform validation and trim the alternate key parameters.
        validateBusinessObjectFormatKey(businessObjectFormatKey, true);

        // If namespace is not specified, get the namespace code by locating the legacy business object definition.
        businessObjectFormatHelper.populateLegacyNamespace(businessObjectFormatKey);

        // Retrieve and ensure that a business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = dmDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);

        // Update business object format description.
        businessObjectFormatEntity.setDescription(request.getDescription());

        // Validate optional schema information.  This is also going to trim the relative schema column field values.
        validateBusinessObjectFormatSchema(request.getSchema(), businessObjectFormatEntity.getPartitionKey());

        // Get business object format model object.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);

        // Check if we need to update business object format schema information.
        if ((request.getSchema() != null && !request.getSchema().equals(businessObjectFormat.getSchema())) ||
            (request.getSchema() == null && businessObjectFormat.getSchema() != null))
        {
            // TODO: Check if we are allowed to update schema information for this business object format.
            //if (businessObjectFormat.getSchema() != null && dmDao.getBusinessObjectDataCount(businessObjectFormatKey) > 0L)
            //{
            //    throw new IllegalArgumentException(String
            //        .format("Can not update schema information for a business object format that has an existing schema " +
            //            "defined and business object data associated with it. Business object format: {%s}",
            //            dmDaoHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
            //}

            // Update schema information by clearing and setting the relative business object
            // format entity fields and by re-creating schema column entities.  Please note that
            // this approach results in changing all schema column Id's which could have
            // ramifications down the road if other entities have relations to schema columns.
            // Also, performance will be slightly slower doing a bunch of deletes followed by a bunch
            // of inserts for what could otherwise be a single SQL statement if only one column was changed.
            // Nevertheless, the below approach results in a simpler code.

            // Removes business object format schema information from the business object format entity.
            clearBusinessObjectFormatSchema(businessObjectFormatEntity);

            // In order to avoid INSERT-then-DELETE, we need to flush the session before we add new schema column entities.
            dmDao.saveAndRefresh(businessObjectFormatEntity);

            // Populates schema information within the business object format entity.
            populateBusinessObjectFormatSchema(businessObjectFormatEntity, request.getSchema());
        }

        // Persist and refresh the entity.
        businessObjectFormatEntity = dmDao.saveAndRefresh(businessObjectFormatEntity);

        // Create and return the business object format object from the persisted entity.
        return businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
    }

    /**
     * Gets a business object format.
     *
     * @param businessObjectFormatKey the business object format alternate key
     *
     * @return the business object format
     */
    @Override
    public BusinessObjectFormat getBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Perform validation and trim the alternate key parameters.
        validateBusinessObjectFormatKey(businessObjectFormatKey, false);

        // If namespace is not specified, get the namespace code by locating the legacy business object definition.
        businessObjectFormatHelper.populateLegacyNamespace(businessObjectFormatKey);

        // Retrieve and ensure that a business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = dmDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);

        // Create and return the business object format object from the persisted entity.
        return businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
    }

    /**
     * Deletes a business object format.
     *
     * @param businessObjectFormatKey the business object format alternate key
     *
     * @return the business object format that was deleted
     */
    @Override
    public BusinessObjectFormat deleteBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Perform validation and trim the alternate key parameters.
        validateBusinessObjectFormatKey(businessObjectFormatKey, true);

        // If namespace is not specified, get the namespace code by locating the legacy business object definition.
        businessObjectFormatHelper.populateLegacyNamespace(businessObjectFormatKey);

        // Retrieve and ensure that a business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = dmDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);

        // Check if we are allowed to delete this business object format.
        if (dmDao.getBusinessObjectDataCount(businessObjectFormatKey) > 0L)
        {
            throw new IllegalArgumentException(String
                .format("Can not delete a business object format that has business object data associated with it. Business object format: {%s}",
                    dmDaoHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
        }

        // Delete this business object format.
        dmDao.delete(businessObjectFormatEntity);

        // If this business object format version is the latest, set the latest flag on the previous version of this object format, if it exists.
        if (businessObjectFormatEntity.getLatestVersion())
        {
            // Get the maximum version for this business object format, if it exists.
            Integer maxBusinessObjectFormatVersion = dmDao.getBusinessObjectFormatMaxVersion(businessObjectFormatKey);

            if (maxBusinessObjectFormatVersion != null)
            {
                // Retrieve the previous version business object format entity. Since we successfully got the maximum
                // version for this business object format, the retrieved entity is not expected to be null.
                BusinessObjectFormatEntity previousVersionBusinessObjectFormatEntity = dmDao.getBusinessObjectFormatByAltKey(
                    new BusinessObjectFormatKey(businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(),
                        businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType(),
                        maxBusinessObjectFormatVersion));

                // Update the previous version business object format entity.
                previousVersionBusinessObjectFormatEntity.setLatestVersion(true);
                dmDao.saveAndRefresh(previousVersionBusinessObjectFormatEntity);
            }
        }

        // Create and return the business object format object from the deleted entity.
        return businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
    }

    /**
     * Gets a list of business object formats for the specified business object definition name.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param latestBusinessObjectFormatVersion specifies is only the latest (maximum) versions of the business object formats are returned
     *
     * @return the list of business object formats.
     */
    @Override
    public BusinessObjectFormatKeys getBusinessObjectFormats(BusinessObjectDefinitionKey businessObjectDefinitionKey, boolean latestBusinessObjectFormatVersion)
    {
        // Perform validation and trim.
        dmHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);

        // Ensure that a business object definition already exists with the specified name.
        if (StringUtils.isBlank(businessObjectDefinitionKey.getNamespace()))
        {
            // If namespace is not specified, retrieve the legacy business object definition by it's name only.
            BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
                dmDaoHelper.getLegacyBusinessObjectDefinitionEntity(businessObjectDefinitionKey.getBusinessObjectDefinitionName());

            // Update the business object definition key with the retrieved namespace.
            businessObjectDefinitionKey.setNamespace(businessObjectDefinitionEntity.getNamespace().getCode());
        }
        else
        {
            // Since namespace is specified, retrieve a business object definition by it's key.
            dmDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);
        }

        // Gets the list of keys and return them.
        BusinessObjectFormatKeys businessObjectFormatKeys = new BusinessObjectFormatKeys();
        businessObjectFormatKeys.getBusinessObjectFormatKeys()
            .addAll(dmDao.getBusinessObjectFormats(businessObjectDefinitionKey, latestBusinessObjectFormatVersion));
        return businessObjectFormatKeys;
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system (e.g. Hive) by creating a table for the requested business object format. This
     * method starts a new transaction.
     *
     * @param request the business object format DDL request
     *
     * @return the business object format DDL information
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectFormatDdl generateBusinessObjectFormatDdl(BusinessObjectFormatDdlRequest request)
    {
        return generateBusinessObjectFormatDdlImpl(request, false);
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system (e.g. Hive) by creating tables for a collection of business object formats.
     * This method starts a new transaction.
     *
     * @param request the business object format DDL collection request
     *
     * @return the business object format DDL information
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectFormatDdlCollectionResponse generateBusinessObjectFormatDdlCollection(BusinessObjectFormatDdlCollectionRequest request)
    {
        return generateBusinessObjectFormatDdlCollectionImpl(request);
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system (e.g. Hive) by creating a table for the requested business object format.
     *
     * @param request the business object format DDL request
     * @param skipRequestValidation specifies whether to skip the request validation and trimming
     *
     * @return the business object format DDL information
     */
    protected BusinessObjectFormatDdl generateBusinessObjectFormatDdlImpl(BusinessObjectFormatDdlRequest request, boolean skipRequestValidation)
    {
        // Perform the validation.
        if (!skipRequestValidation)
        {
            validateBusinessObjectFormatDdlRequest(request);
        }

        // Get the business object format entity for the specified parameters and make sure it exists.
        // Please note that when format version is not specified, we should get back the latest format version.
        BusinessObjectFormatEntity businessObjectFormatEntity = dmDaoHelper.getBusinessObjectFormatEntity(
            new BusinessObjectFormatKey(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()));

        // Get business object format key.
        BusinessObjectFormatKey businessObjectFormatKey = dmDaoHelper.getBusinessObjectFormatKey(businessObjectFormatEntity);

        // Validate that format has schema information.
        Assert.notEmpty(businessObjectFormatEntity.getSchemaColumns(), String.format(
            "Business object format with namespace \"%s\", business object definition name \"%s\", format usage \"%s\", format file type \"%s\"," +
                " and format version \"%s\" doesn't have schema information.", businessObjectFormatKey.getNamespace(),
            businessObjectFormatKey.getBusinessObjectDefinitionName(), businessObjectFormatKey.getBusinessObjectFormatUsage(),
            businessObjectFormatKey.getBusinessObjectFormatFileType(), businessObjectFormatKey.getBusinessObjectFormatVersion()));

        // If it was specified, retrieve the custom DDL and ensure it exists.
        CustomDdlEntity customDdlEntity = null;
        if (StringUtils.isNotBlank(request.getCustomDdlName()))
        {
            customDdlEntity = dmDaoHelper.getCustomDdlEntity(
                new CustomDdlKey(businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(),
                    businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType(),
                    businessObjectFormatKey.getBusinessObjectFormatVersion(), request.getCustomDdlName()));
        }

        // Create business object format DDL object instance.
        BusinessObjectFormatDdl businessObjectFormatDdl = new BusinessObjectFormatDdl();
        businessObjectFormatDdl.setNamespace(businessObjectFormatKey.getNamespace());
        businessObjectFormatDdl.setBusinessObjectDefinitionName(businessObjectFormatKey.getBusinessObjectDefinitionName());
        businessObjectFormatDdl.setBusinessObjectFormatUsage(businessObjectFormatKey.getBusinessObjectFormatUsage());
        businessObjectFormatDdl.setBusinessObjectFormatFileType(businessObjectFormatKey.getBusinessObjectFormatFileType());
        businessObjectFormatDdl.setBusinessObjectFormatVersion(businessObjectFormatKey.getBusinessObjectFormatVersion());
        businessObjectFormatDdl.setOutputFormat(request.getOutputFormat());
        businessObjectFormatDdl.setTableName(request.getTableName());
        businessObjectFormatDdl.setCustomDdlName(customDdlEntity != null ? customDdlEntity.getCustomDdlName() : request.getCustomDdlName());
        businessObjectFormatDdl.setDdl(
            ddlGeneratorFactory.getDdlGenerator(request.getOutputFormat()).generateCreateTableDdl(request, businessObjectFormatEntity, customDdlEntity));

        // Return business object format DDL.
        return businessObjectFormatDdl;
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system (e.g. Hive) by creating tables for a collection of business object formats.
     *
     * @param businessObjectFormatDdlCollectionRequest the business object format DDL collection request
     *
     * @return the business object format DDL information
     */
    protected BusinessObjectFormatDdlCollectionResponse generateBusinessObjectFormatDdlCollectionImpl(
        BusinessObjectFormatDdlCollectionRequest businessObjectFormatDdlCollectionRequest)
    {
        // Perform the validation of the entire request, before we start processing the individual requests that requires the database access.
        validateBusinessObjectFormatDdlCollectionRequest(businessObjectFormatDdlCollectionRequest);

        // Process the individual requests and build the response.
        BusinessObjectFormatDdlCollectionResponse businessObjectFormatDdlCollectionResponse = new BusinessObjectFormatDdlCollectionResponse();
        List<BusinessObjectFormatDdl> businessObjectFormatDdlResponses = new ArrayList<>();
        businessObjectFormatDdlCollectionResponse.setBusinessObjectFormatDdlResponses(businessObjectFormatDdlResponses);
        List<String> ddls = new ArrayList<>();
        for (BusinessObjectFormatDdlRequest request : businessObjectFormatDdlCollectionRequest.getBusinessObjectFormatDdlRequests())
        {
            // Please note that when calling to process individual ddl requests, we ask to skip the request validation and trimming step.
            BusinessObjectFormatDdl businessObjectFormatDdl = generateBusinessObjectFormatDdlImpl(request, true);
            businessObjectFormatDdlResponses.add(businessObjectFormatDdl);
            ddls.add(businessObjectFormatDdl.getDdl());
        }
        businessObjectFormatDdlCollectionResponse.setDdlCollection(StringUtils.join(ddls, "\n\n"));

        return businessObjectFormatDdlCollectionResponse;
    }

    /**
     * Validates the business object format create request, except for the alternate key values. This method also trims request parameters.
     *
     * @param request the business object format create request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectFormatCreateRequest(BusinessObjectFormatCreateRequest request)
    {
        // Extract business object format key from the create request.
        BusinessObjectFormatKey businessObjectFormatKey = getBusinessObjectFormatKey(request);

        // Perform validation and trim the business object format key parameters, except for a business object format version.
        validateBusinessObjectFormatKey(businessObjectFormatKey, false);

        // Update the request object instance with the alternate key parameters.
        updateBusinessObjectFormatAlternateKeyOnCreateRequest(request, businessObjectFormatKey);

        // Perform validation.
        Assert.hasText(request.getPartitionKey(), "A business object format partition key must be specified.");

        // Remove leading and trailing spaces.
        request.setPartitionKey(request.getPartitionKey().trim());

        // Validate attribute definitions if they are specified.
        if (!CollectionUtils.isEmpty(request.getAttributeDefinitions()))
        {
            Map<String, AttributeDefinition> attributeNameValidationMap = new HashMap<>();
            for (AttributeDefinition attributeDefinition : request.getAttributeDefinitions())
            {
                Assert.hasText(attributeDefinition.getName(), "An attribute definition name must be specified.");
                attributeDefinition.setName(attributeDefinition.getName().trim());

                // Ensure the attribute key isn't a duplicate by using a map with a "lowercase" name as the key for case insensitivity.
                String lowercaseAttributeDefinitionName = attributeDefinition.getName().toLowerCase();
                if (attributeNameValidationMap.containsKey(lowercaseAttributeDefinitionName))
                {
                    throw new IllegalArgumentException(String.format("Duplicate attribute definition name \"%s\" found.", attributeDefinition.getName()));
                }
                attributeNameValidationMap.put(lowercaseAttributeDefinitionName, attributeDefinition);
            }
        }

        // Validate optional schema information.
        validateBusinessObjectFormatSchema(request.getSchema(), request.getPartitionKey());
    }

    /**
     * Validates the business object format key. This method also trims the alternate key parameters.
     *
     * @param businessObjectFormatKey the business object format key
     * @param businessObjectFormatVersionRequired specifies if business object format version parameter is required or not
     */
    private void validateBusinessObjectFormatKey(BusinessObjectFormatKey businessObjectFormatKey, Boolean businessObjectFormatVersionRequired)
    {
        // Perform validation.
        Assert.hasText(businessObjectFormatKey.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        Assert.hasText(businessObjectFormatKey.getBusinessObjectFormatUsage(), "A business object format usage must be specified.");
        Assert.hasText(businessObjectFormatKey.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        if (businessObjectFormatVersionRequired)
        {
            Assert.notNull(businessObjectFormatKey.getBusinessObjectFormatVersion(), "A business object format version must be specified.");
        }

        // Remove leading and trailing spaces.
        if (businessObjectFormatKey.getNamespace() != null)
        {
            businessObjectFormatKey.setNamespace(businessObjectFormatKey.getNamespace().trim());
        }
        businessObjectFormatKey.setBusinessObjectDefinitionName(businessObjectFormatKey.getBusinessObjectDefinitionName().trim());
        businessObjectFormatKey.setBusinessObjectFormatUsage(businessObjectFormatKey.getBusinessObjectFormatUsage().trim());
        businessObjectFormatKey.setBusinessObjectFormatFileType(businessObjectFormatKey.getBusinessObjectFormatFileType().trim());
    }

    /**
     * Validates the business object format schema. This method also trims some of the schema column attributes parameters.
     *
     * @param schema the business object format schema
     * @param partitionKey the business object format partition key
     *
     * @throws IllegalArgumentException if any validation errors were found.
     */
    private void validateBusinessObjectFormatSchema(Schema schema, String partitionKey)
    {
        // Validate optional schema information.
        if (schema != null)
        {
            // Perform validation.
            Assert.notNull(schema.getNullValue(), "A schema null value can not be null.");

            // Remove leading and trailing spaces.
            // Note that we don't trim the row option characters since they could contain white space or non-ASCII characters which would get removed if
            // "trim"ed.
            schema.setPartitionKeyGroup(schema.getPartitionKeyGroup() == null ? null : schema.getPartitionKeyGroup().trim());

            Assert.isTrue(!CollectionUtils.isEmpty(schema.getColumns()), "A schema must have at least one column.");

            // Validates the schema columns. We are creating a map here since it is used across both data and partition columns.
            LinkedHashMap<String, SchemaColumn> schemaEqualityValidationMap = new LinkedHashMap<>();
            validateSchemaColumns(schema.getColumns(), schemaEqualityValidationMap);
            validateSchemaColumns(schema.getPartitions(), schemaEqualityValidationMap);

            // Ensure the partition key matches the first schema column name if a single partition column is present.
            // TODO: We are doing this check since the primary partition is specified in 2 places and we want to keep them in sync. In the future,
            // the partition key will go away and this check will be removed.
            if (!CollectionUtils.isEmpty(schema.getPartitions()))
            {
                SchemaColumn schemaColumn = schema.getPartitions().get(0);
                if (!partitionKey.equalsIgnoreCase(schemaColumn.getName()))
                {
                    throw new IllegalArgumentException(String
                        .format("Partition key \"%s\" does not match the first schema partition column name \"%s\".", partitionKey, schemaColumn.getName()));
                }
            }
        }
    }

    /**
     * Validates a list of schema columns.
     *
     * @param schemaColumns the list of schema columns.
     * @param schemaEqualityValidationMap a map of schema column names to their schema column. This is used to check equality across all data schema columns as
     * well as partition schema columns.
     */
    private void validateSchemaColumns(List<SchemaColumn> schemaColumns, LinkedHashMap<String, SchemaColumn> schemaEqualityValidationMap)
    {
        // Validate schema columns if they are specified.
        if (!CollectionUtils.isEmpty(schemaColumns))
        {
            // Create a schema column name map that we will use to check for duplicate
            // columns for the specified list of schema columns (i.e. data or partition).
            LinkedHashMap<String, SchemaColumn> schemaColumnNameValidationMap = new LinkedHashMap<>();

            // Loop through each schema column in the list.
            for (SchemaColumn schemaColumn : schemaColumns)
            {
                // Perform validation.
                Assert.hasText(schemaColumn.getName(), "A schema column name must be specified.");
                Assert.hasText(schemaColumn.getType(), "A schema column data type must be specified.");

                // Remove leading and trailing spaces.
                schemaColumn.setName(schemaColumn.getName().trim());
                schemaColumn.setType(schemaColumn.getType().trim());
                schemaColumn.setSize(schemaColumn.getSize() == null ? null : schemaColumn.getSize().trim());
                schemaColumn.setDefaultValue(schemaColumn.getDefaultValue() == null ? null : schemaColumn.getDefaultValue().trim());

                // Ensure the column name isn't a duplicate within this list only by using a map.
                String lowercaseSchemaColumnName = schemaColumn.getName().toLowerCase();
                if (schemaColumnNameValidationMap.containsKey(lowercaseSchemaColumnName))
                {
                    throw new IllegalArgumentException(String.format("Duplicate schema column name \"%s\" found.", schemaColumn.getName()));
                }
                schemaColumnNameValidationMap.put(lowercaseSchemaColumnName, schemaColumn);

                // Ensure a partition column and a data column are equal (i.e. contain the same configuration).
                SchemaColumn existingSchemaColumn = schemaEqualityValidationMap.get(lowercaseSchemaColumnName);
                if ((existingSchemaColumn != null) && !schemaColumn.equals(existingSchemaColumn))
                {
                    throw new IllegalArgumentException("Schema data and partition column configurations with name \"" + schemaColumn.getName() +
                        "\" have conflicting values. All column values are case sensitive and must be identical.");
                }
                schemaEqualityValidationMap.put(lowercaseSchemaColumnName, schemaColumn);
            }
        }
    }

    /**
     * Validates that the new business object format version schema is additive to the old one.
     *
     * @param newSchema the new schema
     * @param oldSchema the old schema
     */
    private void validateNewSchemaIsAdditiveToOldSchema(Schema newSchema, Schema oldSchema)
    {
        String mainErrorMessage = "New format version schema is not \"additive\" to the previous format version schema.";

        // Validate that new version format schema is specified.
        Assert.notNull(newSchema, String.format("%s New format version schema is not specified.", mainErrorMessage));

        // Validate that there are no changes to the schema null value, which is a required parameter.
        // Please note that NULL value in the database represents an empty string.
        Assert.isTrue(oldSchema.getNullValue() == null ? newSchema.getNullValue().isEmpty() : oldSchema.getNullValue().equals(newSchema.getNullValue()),
            String.format("%s New format version null value does not match to the previous format version null value.", mainErrorMessage));

        // Validate that there are no changes to the delimiter character, which is a an optional parameter.
        // Please note that null and an empty string values are both stored in the database as NULL.
        Assert.isTrue(oldSchema.getDelimiter() == null ? newSchema.getDelimiter() == null || newSchema.getDelimiter().isEmpty() :
            oldSchema.getDelimiter().equals(newSchema.getDelimiter()),
            String.format("%s New format version delimiter character does not match to the previous format version delimiter character.", mainErrorMessage));

        // Validate that there are no changes to the escape character, which is a an optional parameter.
        // Please note that null and an empty string values are both stored in the database as NULL.
        Assert.isTrue(oldSchema.getEscapeCharacter() == null ? newSchema.getEscapeCharacter() == null || newSchema.getEscapeCharacter().isEmpty() :
            oldSchema.getEscapeCharacter().equals(newSchema.getEscapeCharacter()),
            String.format("%s New format version escape character does not match to the previous format version escape character.", mainErrorMessage));

        // Validate that there are no changes to partition columns.
        Assert.isTrue(oldSchema.getPartitions() == null ? newSchema.getPartitions() == null : oldSchema.getPartitions().equals(newSchema.getPartitions()),
            String.format("%s New format version partition columns do not match to the previous format version partition columns.", mainErrorMessage));

        // Validate that there are no changes to the previous format version regular columns.
        Assert.isTrue(oldSchema.getColumns().size() <= newSchema.getColumns().size() &&
            oldSchema.getColumns().equals(newSchema.getColumns().subList(0, oldSchema.getColumns().size())),
            String.format("%s Changes detected to the previously defined regular (non-partitioning) columns.", mainErrorMessage));
    }

    /**
     * Validates a business object format DDL collection request. This method also trims appropriate request parameters.
     *
     * @param businessObjectFormatDdlCollectionRequest the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectFormatDdlCollectionRequest(BusinessObjectFormatDdlCollectionRequest businessObjectFormatDdlCollectionRequest)
    {
        Assert.notNull(businessObjectFormatDdlCollectionRequest, "A business object format DDL collection request must be specified.");

        Assert.isTrue(!CollectionUtils.isEmpty(businessObjectFormatDdlCollectionRequest.getBusinessObjectFormatDdlRequests()),
            "At least one business object format DDL request must be specified.");

        for (BusinessObjectFormatDdlRequest request : businessObjectFormatDdlCollectionRequest.getBusinessObjectFormatDdlRequests())
        {
            validateBusinessObjectFormatDdlRequest(request);
        }
    }

    /**
     * Validates the business object format DDL request. This method also trims appropriate request parameters.
     *
     * @param request the business object format DDL request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectFormatDdlRequest(BusinessObjectFormatDdlRequest request)
    {
        Assert.notNull(request, "A business object format DDL request must be specified.");

        // Validate and trim the request parameters.
        Assert.hasText(request.getNamespace(), "A namespace must be specified.");
        request.setNamespace(request.getNamespace().trim());

        Assert.hasText(request.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().trim());

        Assert.hasText(request.getBusinessObjectFormatUsage(), "A business object format usage name must be specified.");
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().trim());

        Assert.hasText(request.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().trim());

        Assert.notNull(request.getOutputFormat(), "An output format must be specified.");

        Assert.hasText(request.getTableName(), "A table name must be specified.");
        request.setTableName(request.getTableName().trim());

        if (request.getCustomDdlName() != null)
        {
            request.setCustomDdlName(request.getCustomDdlName().trim());
        }
    }

    /**
     * Creates a business object format key from the relative values in the business object format create request.  Please note that the key will contain a null
     * for the business object format version value.
     *
     * @param businessObjectFormatCreateRequest the business object format create request
     *
     * @return the business object format key
     */
    private BusinessObjectFormatKey getBusinessObjectFormatKey(BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest)
    {
        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey();

        businessObjectFormatKey.setNamespace(businessObjectFormatCreateRequest.getNamespace());
        businessObjectFormatKey.setBusinessObjectDefinitionName(businessObjectFormatCreateRequest.getBusinessObjectDefinitionName());
        businessObjectFormatKey.setBusinessObjectFormatUsage(businessObjectFormatCreateRequest.getBusinessObjectFormatUsage());
        businessObjectFormatKey.setBusinessObjectFormatFileType(businessObjectFormatCreateRequest.getBusinessObjectFormatFileType());

        return businessObjectFormatKey;
    }

    /**
     * Sets the relative fields in the business object format create request per specified business object format alternate key.
     *
     * @param businessObjectFormatCreateRequest the business object format create request
     * @param businessObjectFormatKey the business object format alternate key
     */
    private void updateBusinessObjectFormatAlternateKeyOnCreateRequest(BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest,
        BusinessObjectFormatKey businessObjectFormatKey)
    {
        businessObjectFormatCreateRequest.setNamespace(businessObjectFormatKey.getNamespace());
        businessObjectFormatCreateRequest.setBusinessObjectDefinitionName(businessObjectFormatKey.getBusinessObjectDefinitionName());
        businessObjectFormatCreateRequest.setBusinessObjectFormatUsage(businessObjectFormatKey.getBusinessObjectFormatUsage());
        businessObjectFormatCreateRequest.setBusinessObjectFormatFileType(businessObjectFormatKey.getBusinessObjectFormatFileType());
    }

    /**
     * Creates a new business object format entity from the request information.
     *
     * @param request the request.
     *
     * @return the newly created business object format entity.
     */
    private BusinessObjectFormatEntity createBusinessObjectFormatEntity(BusinessObjectFormatCreateRequest request,
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity, FileTypeEntity fileTypeEntity, Integer businessObjectFormatVersion)
    {
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setUsage(request.getBusinessObjectFormatUsage());
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectFormatEntity.setLatestVersion(Boolean.TRUE);
        businessObjectFormatEntity.setPartitionKey(request.getPartitionKey());
        businessObjectFormatEntity.setDescription(request.getDescription());
        businessObjectFormatEntity.setAttributeDefinitions(createAttributeDefinitionEntities(request.getAttributeDefinitions(), businessObjectFormatEntity));

        // Add optional schema information.
        populateBusinessObjectFormatSchema(businessObjectFormatEntity, request.getSchema());

        return businessObjectFormatEntity;
    }

    /**
     * Creates a list of attribute definition entities.
     *
     * @param attributeDefinitions the list of attribute definitions
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return the newly created list of attribute definition entities
     */
    private List<BusinessObjectDataAttributeDefinitionEntity> createAttributeDefinitionEntities(List<AttributeDefinition> attributeDefinitions,
        BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        List<BusinessObjectDataAttributeDefinitionEntity> attributeDefinitionEntities = new ArrayList<>();

        if (!CollectionUtils.isEmpty(attributeDefinitions))
        {
            for (AttributeDefinition attributeDefinition : attributeDefinitions)
            {
                BusinessObjectDataAttributeDefinitionEntity attributeDefinitionEntity = new BusinessObjectDataAttributeDefinitionEntity();
                attributeDefinitionEntities.add(attributeDefinitionEntity);
                attributeDefinitionEntity.setBusinessObjectFormat(businessObjectFormatEntity);
                attributeDefinitionEntity.setName(attributeDefinition.getName());
            }
        }

        return attributeDefinitionEntities;
    }

    /**
     * Adds business object schema information to the business object format entity.
     *
     * @param businessObjectFormatEntity the business object format entity
     * @param schema the schema from the business object format create request
     */
    private void populateBusinessObjectFormatSchema(BusinessObjectFormatEntity businessObjectFormatEntity, Schema schema)
    {
        // Add optional schema information.
        if (schema != null)
        {
            // If optional partition key group value is specified, get the partition key group entity and ensure it exists.
            PartitionKeyGroupEntity partitionKeyGroupEntity = null;
            if (StringUtils.isNotBlank(schema.getPartitionKeyGroup()))
            {
                partitionKeyGroupEntity = dmDaoHelper.getPartitionKeyGroupEntity(schema.getPartitionKeyGroup());
            }

            businessObjectFormatEntity.setNullValue(schema.getNullValue());
            businessObjectFormatEntity.setDelimiter(schema.getDelimiter());
            businessObjectFormatEntity.setEscapeCharacter(schema.getEscapeCharacter());
            businessObjectFormatEntity.setPartitionKeyGroup(partitionKeyGroupEntity);

            // Create a schema column entities collection, if needed.
            Collection<SchemaColumnEntity> schemaColumnEntities = businessObjectFormatEntity.getSchemaColumns();
            if (schemaColumnEntities == null)
            {
                schemaColumnEntities = new ArrayList<>();
                businessObjectFormatEntity.setSchemaColumns(schemaColumnEntities);
            }

            // Create a map that will easily let us keep track of schema column entities we're creating by their column name.
            Map<String, SchemaColumnEntity> schemaColumnMap = new HashMap<>();

            // Create the schema columns for both the data columns and then the partition columns.
            // Since both lists share the same schema column entity list, we will use a common method to process them in the same way.
            createSchemaColumnEntities(schema.getColumns(), false, schemaColumnEntities, schemaColumnMap, businessObjectFormatEntity);
            createSchemaColumnEntities(schema.getPartitions(), true, schemaColumnEntities, schemaColumnMap, businessObjectFormatEntity);
        }
    }

    /**
     * Creates the schema column entities.
     *
     * @param schemaColumns the list of schema columns (for either the data columns or the partition columns).
     * @param isPartitionList A flag to specify whether the list of schema columns is a data column or a partition column list.
     * @param schemaColumnEntityList the list of schema column entities we're creating. This method will add a new one to the list if it hasn't been created
     * before.
     * @param schemaColumnEntityMap a map of schema column entity names to the schema column entity. This is used so we don't have to keep searching the
     * schemaColumnEntityList which is less efficient.
     * @param businessObjectFormatEntity the business object format entity to associated each newly created schema column entity with.
     */
    private void createSchemaColumnEntities(List<SchemaColumn> schemaColumns, boolean isPartitionList, Collection<SchemaColumnEntity> schemaColumnEntityList,
        Map<String, SchemaColumnEntity> schemaColumnEntityMap, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        // Create the relative database entities if schema columns are specified.
        if (!CollectionUtils.isEmpty(schemaColumns))
        {
            // Initialize a position counter since the order we encounter the schema columns is the order we will set on the entity.
            int position = 1;

            // Loop through each schema column in the list.
            for (SchemaColumn schemaColumn : schemaColumns)
            {
                // See if we created the schema column already. If not, create it and add it to the map and the collection.
                SchemaColumnEntity schemaColumnEntity = schemaColumnEntityMap.get(schemaColumn.getName());
                if (schemaColumnEntity == null)
                {
                    schemaColumnEntity = createSchemaColumnEntity(schemaColumn, businessObjectFormatEntity);
                    schemaColumnEntityList.add(schemaColumnEntity);
                    schemaColumnEntityMap.put(schemaColumn.getName(), schemaColumnEntity);
                }

                // Set the position or partition level depending on the type of object we're processing.
                if (isPartitionList)
                {
                    schemaColumnEntity.setPartitionLevel(position++);
                }
                else
                {
                    schemaColumnEntity.setPosition(position++);
                }
            }
        }
    }

    /**
     * Creates a new schema column entity from the schema column.
     *
     * @param schemaColumn the schema column.
     * @param businessObjectFormatEntity the business object format entity to associated each newly created schema column entity with.
     *
     * @return the newly created schema column entity.
     */
    private SchemaColumnEntity createSchemaColumnEntity(SchemaColumn schemaColumn, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();

        schemaColumnEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        schemaColumnEntity.setName(schemaColumn.getName());
        schemaColumnEntity.setType(schemaColumn.getType());
        schemaColumnEntity.setSize(schemaColumn.getSize());
        schemaColumnEntity.setRequired(schemaColumn.isRequired());
        schemaColumnEntity.setDefaultValue(schemaColumn.getDefaultValue());
        schemaColumnEntity.setDescription(schemaColumn.getDescription());

        return schemaColumnEntity;
    }

    /**
     * Deletes business object format schema information for the specified business object format entity.
     *
     * @param businessObjectFormatEntity the business object format entity
     */
    private void clearBusinessObjectFormatSchema(BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        businessObjectFormatEntity.setNullValue(null);
        businessObjectFormatEntity.setDelimiter(null);
        businessObjectFormatEntity.setEscapeCharacter(null);
        businessObjectFormatEntity.setPartitionKeyGroup(null);
        businessObjectFormatEntity.getSchemaColumns().clear();
    }
}

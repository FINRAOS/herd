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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.exception.CredStashGetCredentialFailedException;
import org.finra.herd.dao.helper.CredStashHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.model.api.xml.Schema;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.RelationalStorageAttributesDto;
import org.finra.herd.model.dto.RelationalTableRegistrationDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.BusinessObjectFormatService;
import org.finra.herd.service.MessageNotificationEventService;
import org.finra.herd.service.RelationalTableRegistrationHelperService;
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
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;
import org.finra.herd.service.helper.StorageUnitStatusDaoHelper;

/**
 * An implementation of the helper service class for the relational table registration service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class RelationalTableRegistrationHelperServiceImpl implements RelationalTableRegistrationHelperService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RelationalTableRegistrationHelperServiceImpl.class);

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectDataStatusDaoHelper businessObjectDataStatusDaoHelper;

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private BusinessObjectFormatService businessObjectFormatService;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private CredStashHelper credStashHelper;

    @Autowired
    private DataProviderDaoHelper dataProviderDaoHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private MessageNotificationEventService messageNotificationEventService;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Autowired
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public RelationalStorageAttributesDto prepareForRelationalTableRegistration(
        RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest, Boolean appendToExistingBusinessObjectDefinition)
    {
        return prepareForRelationalTableRegistrationImpl(relationalTableRegistrationCreateRequest, appendToExistingBusinessObjectDefinition);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public RelationalTableRegistrationDto prepareForRelationalTableSchemaUpdate(BusinessObjectDataStorageUnitKey storageUnitKey)
    {
        return prepareForRelationalTableSchemaUpdateImpl(storageUnitKey);
    }

    @PublishNotificationMessages
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectData registerRelationalTable(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest,
        List<SchemaColumn> schemaColumns, Boolean appendToExistingBusinessObjectDefinition)
    {
        return registerRelationalTableImpl(relationalTableRegistrationCreateRequest, schemaColumns, appendToExistingBusinessObjectDefinition);
    }

    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public List<SchemaColumn> retrieveRelationalTableColumns(RelationalStorageAttributesDto relationalStorageAttributesDto, String relationalSchemaName,
        String relationalTableName)
    {
        return retrieveRelationalTableColumnsImpl(relationalStorageAttributesDto, relationalSchemaName, relationalTableName);
    }

    @PublishNotificationMessages
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectData updateRelationalTableSchema(RelationalTableRegistrationDto relationalTableRegistrationDto, List<SchemaColumn> schemaColumns)
    {
        return updateRelationalTableSchemaImpl(relationalTableRegistrationDto, schemaColumns);
    }

    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void validateAndTrimRelationalTableRegistrationCreateRequest(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest)
    {
        validateAndTrimRelationalTableRegistrationCreateRequestImpl(relationalTableRegistrationCreateRequest);
    }

    /**
     * Gets a password value that should be used to connect to the relational storage.
     *
     * @param relationalStorageAttributesDto the relational storage attributes DTO
     *
     * @return the password value
     */
    String getPassword(RelationalStorageAttributesDto relationalStorageAttributesDto)
    {
        // Default password value to null.
        String password = null;

        // Check if we need to get a password value from the credstash.
        if (StringUtils.isNotBlank(relationalStorageAttributesDto.getJdbcUserCredentialName()))
        {
            final String credStashEncryptionContext = configurationHelper.getProperty(ConfigurationValue.CREDSTASH_RELATIONAL_STORAGE_ENCRYPTION_CONTEXT);

            try
            {
                password = credStashHelper.getCredentialFromCredStash(credStashEncryptionContext, relationalStorageAttributesDto.getJdbcUserCredentialName());
            }
            catch (CredStashGetCredentialFailedException e)
            {
                throw new IllegalStateException(e);
            }
        }

        return password;
    }

    /**
     * Prepares for relational table registration by validating database entities per specified relational table registration create request. This method
     * returns storage attributes required to perform relation table registration.
     *
     * @param relationalTableRegistrationCreateRequest the relational table registration create request
     * @param appendToExistingBusinessObjectDefinition boolean flag that determines if the format should be appended to an existing business object definition
     *
     * @return the relational storage attributes DTO
     */
    RelationalStorageAttributesDto prepareForRelationalTableRegistrationImpl(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest,
        Boolean appendToExistingBusinessObjectDefinition)
    {
        // Validate that specified namespace exists.
        namespaceDaoHelper.getNamespaceEntity(relationalTableRegistrationCreateRequest.getNamespace());

        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(relationalTableRegistrationCreateRequest.getNamespace(),
            relationalTableRegistrationCreateRequest.getBusinessObjectDefinitionName());

        // Ensure that a business object definition with the specified key doesn't already exist.
        if (BooleanUtils.isNotTrue(appendToExistingBusinessObjectDefinition) &&
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey) != null)
        {
            throw new AlreadyExistsException(String.format("Business object definition with name \"%s\" already exists for namespace \"%s\".",
                businessObjectDefinitionKey.getBusinessObjectDefinitionName(), businessObjectDefinitionKey.getNamespace()));
        }

        // Get the latest format version for this business format, if it exists.
        BusinessObjectFormatEntity latestVersionBusinessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(relationalTableRegistrationCreateRequest.getNamespace(),
                relationalTableRegistrationCreateRequest.getBusinessObjectDefinitionName(),
                relationalTableRegistrationCreateRequest.getBusinessObjectFormatUsage(), FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, null));

        // If the latest version exists, fail with an already exists exception.
        if (latestVersionBusinessObjectFormatEntity != null)
        {
            throw new AlreadyExistsException(String
                .format("Format with file type \"%s\" and usage \"%s\" already exists for business object definition \"%s\".",
                    latestVersionBusinessObjectFormatEntity.getFileType().getCode(), latestVersionBusinessObjectFormatEntity.getUsage(),
                    latestVersionBusinessObjectFormatEntity.getBusinessObjectDefinition().getName()));
        }

        // Validate that specified data provider exists.
        dataProviderDaoHelper.getDataProviderEntity(relationalTableRegistrationCreateRequest.getDataProviderName());

        // Get the storage.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(relationalTableRegistrationCreateRequest.getStorageName());

        // Only RELATIONAL storage platform is supported for the relational table registration feature.
        Assert.isTrue(storageEntity.getStoragePlatform().getName().equals(StoragePlatformEntity.RELATIONAL), String.format(
            "Cannot register relational table in \"%s\" storage of %s storage platform type. Only %s storage platform type is supported by this feature.",
            storageEntity.getName(), storageEntity.getStoragePlatform().getName(), StoragePlatformEntity.RELATIONAL));

        // Get and return the relational storage attributes required to access relation table schema.
        return getRelationalStorageAttributes(storageEntity);
    }

    /**
     * Prepares for relational table schema update by validating database entities per specified storage unit key. This method returns a relational table
     * registration DTO which contains attributes required to retrieve the current relation table schema.
     *
     * @param storageUnitKey the storage unit key for the relational table registration
     *
     * @return the relational table registration DTO
     */
    RelationalTableRegistrationDto prepareForRelationalTableSchemaUpdateImpl(BusinessObjectDataStorageUnitKey storageUnitKey)
    {
        // Log the business object format create request.
        LOGGER.info("Checking relational table registration for schema update... storageUnitKey={}", jsonHelper.objectToJson(storageUnitKey));

        // Get the storage unit.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntityByKey(storageUnitKey);

        // Get the business object format entity for this relation table registration.
        BusinessObjectFormatEntity businessObjectFormatEntity = storageUnitEntity.getBusinessObjectData().getBusinessObjectFormat();

        // Get relational schema name from the business object format. This business object format attribute is required and must have a non-blank value.
        String relationalSchemaName = businessObjectFormatDaoHelper.getBusinessObjectFormatAttributeValueByName(
            configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_FORMAT_ATTRIBUTE_NAME_RELATIONAL_SCHEMA_NAME), businessObjectFormatEntity, true,
            true);

        // Get relational table name from the business object format. This business object format attribute is required and must have a non-blank value.
        String relationalTableName = businessObjectFormatDaoHelper.getBusinessObjectFormatAttributeValueByName(
            configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_FORMAT_ATTRIBUTE_NAME_RELATIONAL_TABLE_NAME), businessObjectFormatEntity, true,
            true);

        // Get relational storage attributes required to access relation table schema.
        RelationalStorageAttributesDto relationalStorageAttributesDto = getRelationalStorageAttributes(storageUnitEntity.getStorage());

        // Get business object format for the relational table registration.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);

        // Create and return a relational storage attributes DTO.
        return new RelationalTableRegistrationDto(storageUnitKey, relationalStorageAttributesDto, relationalSchemaName, relationalTableName,
            businessObjectFormat);
    }

    /**
     * Creates a new relational table registration. The relation table registration includes creation of the following entities: <ul> <li>a business object
     * definition</li> <li>a business object format with the specified schema columns</li> <li>a business object data</li> <li>a storage unit that links
     * together the business object data with the storage specified in the create request</li> </ul>
     *
     * @param relationalTableRegistrationCreateRequest the relational table registration create request
     * @param schemaColumns the list of schema columns
     * @param appendToExistingBusinessObjectDefinition boolean flag that determines if the format should be appended to an existing business object definition
     *
     * @return the information for the newly created business object data
     */
    BusinessObjectData registerRelationalTableImpl(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest,
        List<SchemaColumn> schemaColumns, Boolean appendToExistingBusinessObjectDefinition)
    {
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity;

        // If we are appending the new business object format to an already existing business object definition,
        // then get the business object definition with the information in the relational table registration create request,
        // else create a new business object definition with the information in the relational table registration create request.
        if (BooleanUtils.isTrue(appendToExistingBusinessObjectDefinition))
        {
            // Get the existing business object definition
            businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(
                new BusinessObjectDefinitionKey(relationalTableRegistrationCreateRequest.getNamespace(),
                    relationalTableRegistrationCreateRequest.getBusinessObjectDefinitionName()));
        }
        else
        {
            // Create a business object definition.
            businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper.createBusinessObjectDefinitionEntity(
                new BusinessObjectDefinitionCreateRequest(relationalTableRegistrationCreateRequest.getNamespace(),
                    relationalTableRegistrationCreateRequest.getBusinessObjectDefinitionName(), relationalTableRegistrationCreateRequest.getDataProviderName(),
                    null, relationalTableRegistrationCreateRequest.getBusinessObjectDefinitionDisplayName(), null));
        }

        // Notify the search index that a business object definition is created.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_CREATE);

        // Build a business object format create request.
        // Store the relational table name as a business object format attribute per attribute name configured in the system.
        BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest = new BusinessObjectFormatCreateRequest();
        businessObjectFormatCreateRequest.setNamespace(relationalTableRegistrationCreateRequest.getNamespace());
        businessObjectFormatCreateRequest.setBusinessObjectDefinitionName(relationalTableRegistrationCreateRequest.getBusinessObjectDefinitionName());
        businessObjectFormatCreateRequest.setBusinessObjectFormatUsage(relationalTableRegistrationCreateRequest.getBusinessObjectFormatUsage());
        businessObjectFormatCreateRequest.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        businessObjectFormatCreateRequest.setPartitionKey(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY);
        businessObjectFormatCreateRequest.setAttributes(Arrays.asList(
            new Attribute(configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_FORMAT_ATTRIBUTE_NAME_RELATIONAL_SCHEMA_NAME),
                relationalTableRegistrationCreateRequest.getRelationalSchemaName()),
            new Attribute(configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_FORMAT_ATTRIBUTE_NAME_RELATIONAL_TABLE_NAME),
                relationalTableRegistrationCreateRequest.getRelationalTableName())));
        businessObjectFormatCreateRequest.setSchema(new Schema(schemaColumns, null, "", null, null, null));

        // Log the business object format create request.
        LOGGER.info("Registering relational table... businessObjectFormatCreateRequest={}", jsonHelper.objectToJson(businessObjectFormatCreateRequest));

        // Create a business object format.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatCreateRequest);

        // Retrieve the newly created business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormat));

        // Allow non-backwards-compatible schema changes.
        businessObjectFormatEntity.setAllowNonBackwardsCompatibleChanges(true);
        businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);

        // Get a business object data status entity for the VALID status.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(BusinessObjectDataStatusEntity.VALID);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setVersion(0);
        businessObjectDataEntity.setLatestVersion(true);
        businessObjectDataEntity.setPartitionValue(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE);

        // Get a storage unit status entity for the ENABLED status.
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.ENABLED);

        // Get the storage.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(relationalTableRegistrationCreateRequest.getStorageName());

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitDaoHelper.setStorageUnitStatus(storageUnitEntity, storageUnitStatusEntity);
        businessObjectDataEntity.setStorageUnits(Collections.singletonList(storageUnitEntity));

        // Persist the newly created business object data entity.
        businessObjectDataEntity = businessObjectDataDao.saveAndRefresh(businessObjectDataEntity);

        // Create a status change notification to be sent on create business object data event.
        messageNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity),
                businessObjectDataStatusEntity.getCode(), null);

        // Create and return business object data information from the newly created business object data entity.
        return businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity);
    }

    /**
     * Retrieves a list of actual schema columns for the specified relational table. This method uses actual JDBC connection to retrieve a description of table
     * columns.
     *
     * @param relationalStorageAttributesDto the relational storage attributes DTO
     * @param relationalSchemaName the name of the relational database schema
     * @param relationalTableName the name of the relational table
     *
     * @return the list of schema columns for the specified relational table
     */
    List<SchemaColumn> retrieveRelationalTableColumnsImpl(RelationalStorageAttributesDto relationalStorageAttributesDto, String relationalSchemaName,
        String relationalTableName)
    {
        // Get the JDBC password value.
        String password = getPassword(relationalStorageAttributesDto);

        // Create and initialize a driver manager data source (a simple implementation of the standard JDBC interface).
        // We only support PostgreSQL database type.
        DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource();
        driverManagerDataSource.setUrl(relationalStorageAttributesDto.getJdbcUrl());
        driverManagerDataSource.setUsername(relationalStorageAttributesDto.getJdbcUsername());
        driverManagerDataSource.setPassword(password);
        driverManagerDataSource.setDriverClassName(JdbcServiceImpl.DRIVER_POSTGRES);

        // Create an empty result list.
        List<SchemaColumn> schemaColumns = new ArrayList<>();

        // Connect to the database and retrieve the relational table columns.
        try (Connection connection = driverManagerDataSource.getConnection())
        {
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            // Check if the specified relational table exists in the database.
            try (ResultSet tables = databaseMetaData.getTables(null, relationalSchemaName, relationalTableName, null))
            {
                Assert.isTrue(tables.next(), String
                    .format("Relational table with \"%s\" name not found under \"%s\" schema at jdbc.url=\"%s\" for jdbc.username=\"%s\".", relationalTableName,
                        relationalSchemaName, driverManagerDataSource.getUrl(), driverManagerDataSource.getUsername()));
            }

            // Retrieve the relational table columns.
            try (ResultSet columns = databaseMetaData.getColumns(null, relationalSchemaName, relationalTableName, null))
            {
                while (columns.next())
                {
                    SchemaColumn schemaColumn = new SchemaColumn();
                    schemaColumn.setName(columns.getString("COLUMN_NAME"));
                    schemaColumn.setType(columns.getString("TYPE_NAME"));
                    schemaColumn.setSize(columns.getString("COLUMN_SIZE"));
                    schemaColumn.setRequired(columns.getInt("NULLABLE") == 0);
                    schemaColumn.setDefaultValue(columns.getString("COLUMN_DEF"));
                    schemaColumns.add(schemaColumn);
                }
            }
        }
        catch (SQLException e)
        {
            throw new IllegalArgumentException(String.format("Failed to retrieve description of a relational table with \"%s\" name under \"%s\" schema " +
                    "at jdbc.url=\"%s\" using jdbc.username=\"%s\". Reason: %s", relationalTableName, relationalSchemaName, driverManagerDataSource.getUrl(),
                driverManagerDataSource.getUsername(), e.getMessage()), e);
        }

        return schemaColumns;
    }

    /**
     * Updates relational table schema for an already existing relational table registration.
     *
     * @param relationalTableRegistrationDto the relational table registration DTO
     * @param schemaColumns the new relational table schema
     *
     * @return the information for the business object data created for the updated relational table registration
     */
    BusinessObjectData updateRelationalTableSchemaImpl(RelationalTableRegistrationDto relationalTableRegistrationDto, List<SchemaColumn> schemaColumns)
    {
        // Build a create request for a new version of the business object format.
        BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest = new BusinessObjectFormatCreateRequest();
        businessObjectFormatCreateRequest.setNamespace(relationalTableRegistrationDto.getBusinessObjectFormat().getNamespace());
        businessObjectFormatCreateRequest
            .setBusinessObjectDefinitionName(relationalTableRegistrationDto.getBusinessObjectFormat().getBusinessObjectDefinitionName());
        businessObjectFormatCreateRequest.setBusinessObjectFormatUsage(relationalTableRegistrationDto.getBusinessObjectFormat().getBusinessObjectFormatUsage());
        businessObjectFormatCreateRequest
            .setBusinessObjectFormatFileType(relationalTableRegistrationDto.getBusinessObjectFormat().getBusinessObjectFormatFileType());
        businessObjectFormatCreateRequest.setPartitionKey(relationalTableRegistrationDto.getBusinessObjectFormat().getPartitionKey());
        businessObjectFormatCreateRequest.setAttributes(relationalTableRegistrationDto.getBusinessObjectFormat().getAttributes());
        businessObjectFormatCreateRequest.setSchema(new Schema(schemaColumns, null, "", null, null, null));

        // Log the relational table registration DTO along with the business object format create request.
        LOGGER.info("Updating relational table schema... relationalTableRegistrationDto={} businessObjectFormatCreateRequest={}",
            jsonHelper.objectToJson(relationalTableRegistrationDto), jsonHelper.objectToJson(businessObjectFormatCreateRequest));

        // Create a new version of the business object format.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatCreateRequest);

        // Retrieve the newly created business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormat));

        // Get a business object data status entity for the VALID status.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(BusinessObjectDataStatusEntity.VALID);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setVersion(0);
        businessObjectDataEntity.setLatestVersion(true);
        businessObjectDataEntity.setPartitionValue(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE);

        // Get a storage unit status entity for the ENABLED status.
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.ENABLED);

        // Get the storage.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(relationalTableRegistrationDto.getStorageUnitKey().getStorageName());

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitDaoHelper.setStorageUnitStatus(storageUnitEntity, storageUnitStatusEntity);
        businessObjectDataEntity.setStorageUnits(Collections.singletonList(storageUnitEntity));

        // Persist the newly created business object data entity.
        businessObjectDataEntity = businessObjectDataDao.saveAndRefresh(businessObjectDataEntity);

        // Create a status change notification to be sent on create business object data event.
        messageNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity),
                businessObjectDataStatusEntity.getCode(), null);

        // Create and return business object data information from the newly created business object data entity.
        return businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity);
    }

    /**
     * Validates a relational table registration create request. This method also trims the request parameters.
     *
     * @param relationalTableRegistrationCreateRequest the relational table registration create request
     */
    void validateAndTrimRelationalTableRegistrationCreateRequestImpl(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest)
    {
        Assert.notNull(relationalTableRegistrationCreateRequest, "A relational table registration create request must be specified.");

        // Validate and trim required parameters.
        relationalTableRegistrationCreateRequest
            .setNamespace(alternateKeyHelper.validateStringParameter("namespace", relationalTableRegistrationCreateRequest.getNamespace()));
        relationalTableRegistrationCreateRequest.setBusinessObjectDefinitionName(alternateKeyHelper
            .validateStringParameter("business object definition name", relationalTableRegistrationCreateRequest.getBusinessObjectDefinitionName()));
        relationalTableRegistrationCreateRequest.setBusinessObjectFormatUsage(alternateKeyHelper
            .validateStringParameter("business object format usage", relationalTableRegistrationCreateRequest.getBusinessObjectFormatUsage()));
        relationalTableRegistrationCreateRequest.setDataProviderName(
            alternateKeyHelper.validateStringParameter("data provider name", relationalTableRegistrationCreateRequest.getDataProviderName()));
        relationalTableRegistrationCreateRequest.setRelationalSchemaName(
            alternateKeyHelper.validateStringParameter("relational schema name", relationalTableRegistrationCreateRequest.getRelationalSchemaName()));
        relationalTableRegistrationCreateRequest.setRelationalTableName(
            alternateKeyHelper.validateStringParameter("relational table name", relationalTableRegistrationCreateRequest.getRelationalTableName()));
        relationalTableRegistrationCreateRequest
            .setStorageName(alternateKeyHelper.validateStringParameter("storage name", relationalTableRegistrationCreateRequest.getStorageName()));

        // Trim optional business object definition display name, if specified.
        if (relationalTableRegistrationCreateRequest.getBusinessObjectDefinitionDisplayName() != null)
        {
            relationalTableRegistrationCreateRequest
                .setBusinessObjectDefinitionDisplayName(relationalTableRegistrationCreateRequest.getBusinessObjectDefinitionDisplayName().trim());
        }
    }

    /**
     * Returns storage attributes required to access relation table schema.
     *
     * @param storageEntity the storage entity
     *
     * @return the relational storage attributes DTO
     */
    private RelationalStorageAttributesDto getRelationalStorageAttributes(StorageEntity storageEntity)
    {
        // Get JDBC URL for this storage. This storage attribute is required and must have a non-blank value.
        String jdbcUrl = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.STORAGE_ATTRIBUTE_NAME_JDBC_URL), storageEntity, true, true);

        // Get JDBC username for this storage. This storage attribute is not required and it is allowed to have a blank value.
        String jdbcUsername = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.STORAGE_ATTRIBUTE_NAME_JDBC_USERNAME), storageEntity, false,
                false);

        // Get JDBC user credential name for this storage. This storage attribute is not required and it is allowed to have a blank value.
        String jdbcUserCredentialName = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.STORAGE_ATTRIBUTE_NAME_JDBC_USER_CREDENTIAL_NAME), storageEntity,
                false, false);

        // Create and return a relational storage attributes DTO.
        return new RelationalStorageAttributesDto(jdbcUrl, jdbcUsername, jdbcUserCredentialName);
    }
}

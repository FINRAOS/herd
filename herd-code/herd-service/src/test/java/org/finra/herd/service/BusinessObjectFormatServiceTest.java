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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import javax.persistence.PersistenceException;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.core.Command;
import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.AllowedAttributeValueDao;
import org.finra.herd.dao.GlobalAttributeDefinitionDaoTestHelper;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeDefinition;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptiveInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatAttributeDefinitionsUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatAttributesUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdl;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKeys;
import org.finra.herd.model.api.xml.BusinessObjectFormatParentsUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatUpdateRequest;
import org.finra.herd.model.api.xml.DescriptiveBusinessObjectFormat;
import org.finra.herd.model.api.xml.DescriptiveBusinessObjectFormatUpdateRequest;
import org.finra.herd.model.api.xml.Schema;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.jpa.AllowedAttributeValueEntity;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.helper.Hive13DdlGenerator;
import org.finra.herd.service.impl.BusinessObjectFormatServiceImpl;

public class BusinessObjectFormatServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "businessObjectFormatServiceImpl")
    private BusinessObjectFormatServiceImpl businessObjectFormatServiceImpl;

    @Autowired
    private GlobalAttributeDefinitionDaoTestHelper globalAttributeDefinitionDaoTestHelper;

    @Autowired
    private AllowedAttributeValueDao allowedAttributeValueDao;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * This method is to get the coverage for the business object format service method that starts the new transaction.
     */
    @Test
    public void testBusinessObjectDataServiceMethodsNewTx() throws Exception
    {
        try
        {
            BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey();
            businessObjectFormatServiceImpl.getBusinessObjectFormat(businessObjectFormatKey);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = new BusinessObjectFormatDdlRequest();
            businessObjectFormatServiceImpl.generateBusinessObjectFormatDdl(businessObjectFormatDdlRequest);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            BusinessObjectFormatDdlCollectionRequest businessObjectFormatDdlCollectionRequest = new BusinessObjectFormatDdlCollectionRequest();
            businessObjectFormatServiceImpl.generateBusinessObjectFormatDdlCollection(businessObjectFormatDdlCollectionRequest);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one business object format DDL request must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormat()
    {
        // Create an initial version of a business object format.
        BusinessObjectFormat businessObjectFormat =
            businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema(),
                businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatCreateSecondVersion()
    {
        // Create an initial version of a business object format.
        businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat();

        // Create a second version of the business object format.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
                PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema(),
                resultBusinessObjectFormat);

        // Check if we now have only one latest format version - our first version is not marked as latest anymore.
        // Please note that we do not have to validate if the first format version is not marked as "latest" now, since having more that one
        // format versions with the latestVersion flag set to Yes produces exception in herdDao.getBusinessObjectFormatByAltKey() method.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao
            .getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));
        assertEquals(SECOND_FORMAT_VERSION, businessObjectFormatEntity.getBusinessObjectFormatVersion());
        assertEquals(true, businessObjectFormatEntity.getLatestVersion());
    }

    @Test
    public void testCreateBusinessObjectFormatCreateSecondVersionNoPartitioningColumns()
    {
        // Create and persist an initial version of a business object format with schema without any partitioning columns.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY, PARTITION_KEY_GROUP,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(RANDOM_SUFFIX), null);

        // Create a second version of the business object format without partitioning columns.
        Schema testSchema = businessObjectFormatServiceTestHelper.getTestSchema();
        testSchema.setPartitions(null);
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), testSchema);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema();
        expectedSchema.setPartitions(null);
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
                PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), expectedSchema, resultBusinessObjectFormat);

        // Check if we now have only one latest format version - our first version is not marked as latest anymore.
        // Please note that we do not have to validate if the first format version is not marked as "latest" now, since having more that one
        // format versions with the latestVersion flag set to Yes produces exception in herdDao.getBusinessObjectFormatByAltKey() method.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao
            .getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));
        assertEquals(SECOND_FORMAT_VERSION, businessObjectFormatEntity.getBusinessObjectFormatVersion());
        assertEquals(true, businessObjectFormatEntity.getLatestVersion());
    }

    @Test
    public void testCreateBusinessObjectFormatDuplicateAttributeDefinitions()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Add in duplicate attribute definition.
        AttributeDefinition attributeDefinition = new AttributeDefinition();
        attributeDefinition.setName(ATTRIBUTE_NAME_1_MIXED_CASE);
        request.getAttributeDefinitions().add(attributeDefinition);

        // Create the business object format which is invalid since duplicate attribute definitions are present.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Expecting an Illegal Argument Exception to be thrown which wasn't since duplicate attribute definitions are present.");
        }
        catch (IllegalArgumentException ex)
        {
            // We are expecting this exception so do nothing.
        }
    }

    @Test
    public void testCreateBusinessObjectFormatDuplicateAttributes()
    {
        // Try to create a business object format instance when duplicate attributes are specified.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, Arrays.asList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_3),
                        new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_3)),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema()));
            fail("Should throw an IllegalArgumentException when duplicate attributes are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate attribute name found: %s", ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase()), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatDuplicateColumns()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Add in duplicate schema columns.
        for (String columnName : Arrays.asList("duplicate_column_name", "DUPLICATE_COLUMN_NAME"))
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            request.getSchema().getColumns().add(schemaColumn);
            schemaColumn.setName(columnName);
            schemaColumn.setType("TYPE");
        }

        // Try to create a business object format which has duplicate column names.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when duplicate column names are present.");
        }
        catch (IllegalArgumentException e)
        {
            String expectedErrorMessage = String.format("Duplicate schema column name \"DUPLICATE_COLUMN_NAME\" found.");
            assertEquals(expectedErrorMessage, e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatDuplicatePartitionColumns()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Add in duplicate schema columns.
        for (String columnName : Arrays.asList("duplicate_column_name", "DUPLICATE_COLUMN_NAME"))
        {
            SchemaColumn partitionColumn = new SchemaColumn();
            request.getSchema().getPartitions().add(partitionColumn);
            partitionColumn.setName(columnName);
            partitionColumn.setType("TYPE");
        }

        // Try to create a business object format which has duplicate column names.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when duplicate partition column names are present.");
        }
        catch (IllegalArgumentException e)
        {
            String expectedErrorMessage = String.format("Duplicate schema column name \"DUPLICATE_COLUMN_NAME\" found.");
            assertEquals(expectedErrorMessage, e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatIncorrectLatestVersion() throws Exception
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a first version of the format with the latest flag set to false.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, false, PARTITION_KEY);

        try
        {
            // Try to create a new format version for this format.
            final BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest = businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
            executeWithoutLogging(SqlExceptionHelper.class, new Command()
            {
                @Override
                public void execute()
                {
                    businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatCreateRequest);
                    fail("Should throw a PersistenceException since the latest flag does not identify the maximum format version.");
                }
            });
        }
        catch (PersistenceException e)
        {
            assertTrue(e.getMessage().contains("ConstraintViolationException: could not execute statement"));
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsAsDescriptiveFormatForBusinessObjectDefinition()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);

        // Create and persist two versions of the business object format.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

        DescriptiveBusinessObjectFormatUpdateRequest descriptiveBusinessObjectFormatUpdateRequest =
            new DescriptiveBusinessObjectFormatUpdateRequest(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE);

        // Perform an update by changing the description and updating the attributes.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    descriptiveBusinessObjectFormatUpdateRequest));

        DescriptiveBusinessObjectFormat descriptiveBusinessObjectFormat =
            new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2, null,
            BDEF_DISPLAY_NAME_2, NO_ATTRIBUTES, descriptiveBusinessObjectFormat, NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(),
            businessObjectDefinitionEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()),
            NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS), updatedBusinessObjectDefinition);

        //create a new version of the business object format, the associated bdef descriptive format should be updated to the new version
        businessObjectFormatService.createBusinessObjectFormat(
            new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_DESCRIPTION, PARTITION_KEY,
                FORMAT_DOCUMENT_SCHEMA, null, null, null));
        descriptiveBusinessObjectFormat = new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION);
        updatedBusinessObjectDefinition = businessObjectDefinitionService
            .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);
        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2, null,
            BDEF_DISPLAY_NAME_2, NO_ATTRIBUTES, descriptiveBusinessObjectFormat, NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(),
            businessObjectDefinitionEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()),
            NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS), updatedBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaAdditiveSchemaChangesColumnDescriptionUpdated()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial format schema.
        Schema initialSchema = new Schema(
            Arrays.asList(new SchemaColumn(COLUMN_NAME, COLUMN_DATA_TYPE, COLUMN_SIZE, NO_COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION)),
            Arrays.asList(new SchemaColumn(COLUMN_NAME_2, COLUMN_DATA_TYPE_2, COLUMN_SIZE, NO_COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION_2)),
            SCHEMA_NULL_VALUE_BACKSLASH_N, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, PARTITION_KEY_GROUP);

        // Create the updated format schema having modified column descriptions for both regular and partition columns.
        Schema updatedSchema = (Schema) initialSchema.clone();
        updatedSchema.getColumns().get(0).setDescription(COLUMN_DESCRIPTION_3);
        updatedSchema.getPartitions().get(0).setDescription(COLUMN_DESCRIPTION_4);

        // Create an initial version of the business object format.
        BusinessObjectFormat initialBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(
            new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, COLUMN_NAME_2, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, initialSchema));

        // Create a second version of the business object format with the schema columns having updated descriptions.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(
            new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, COLUMN_NAME_2, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, updatedSchema));

        // Validate the returned object.
        BusinessObjectFormat expectedBusinessObjectFormat = (BusinessObjectFormat) initialBusinessObjectFormat.clone();
        expectedBusinessObjectFormat.setId(resultBusinessObjectFormat.getId());
        expectedBusinessObjectFormat.setBusinessObjectFormatVersion(SECOND_FORMAT_VERSION);
        expectedBusinessObjectFormat.setSchema(updatedSchema);
        assertEquals(expectedBusinessObjectFormat, resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaAdditiveSchemaChangesColumnSizeIncreased()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        int i = 0;
        for (String columnDataType : Arrays.asList("CHAR", "VARCHAR", "VARCHAR2", "char", "varchar", "varchar2"))
        {
            // Create an initial format schema.
            Schema initialSchema = new Schema(
                Arrays.asList(new SchemaColumn(COLUMN_NAME, columnDataType, COLUMN_SIZE, NO_COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION)),
                Arrays.asList(new SchemaColumn(COLUMN_NAME_2, columnDataType, COLUMN_SIZE, NO_COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION_2)),
                SCHEMA_NULL_VALUE_BACKSLASH_N, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, PARTITION_KEY_GROUP);

            // Create the updated format schema having increased column sizes for both regular and partition columns.
            Schema updatedSchema = (Schema) initialSchema.clone();
            updatedSchema.getColumns().get(0).setSize(COLUMN_SIZE_2);
            updatedSchema.getPartitions().get(0).setSize(COLUMN_SIZE_2);

            // We need to specify a unique format usage for each data type being tested to avoid an already exists exception.
            String formatUsage = String.format("%s_%d", FORMAT_USAGE_CODE, i++);

            // Create an initial version of the business object format.
            BusinessObjectFormat initialBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(
                new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, formatUsage, FORMAT_FILE_TYPE_CODE, COLUMN_NAME_2, FORMAT_DESCRIPTION,
                    NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, initialSchema));

            // Create a second version of the business object format having increased column sizes for both regular and partition columns.
            BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(
                new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, formatUsage, FORMAT_FILE_TYPE_CODE, COLUMN_NAME_2, FORMAT_DESCRIPTION,
                    NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, updatedSchema));

            // Validate the returned object.
            BusinessObjectFormat expectedBusinessObjectFormat = (BusinessObjectFormat) initialBusinessObjectFormat.clone();
            expectedBusinessObjectFormat.setId(resultBusinessObjectFormat.getId());
            expectedBusinessObjectFormat.setBusinessObjectFormatVersion(SECOND_FORMAT_VERSION);
            expectedBusinessObjectFormat.setSchema(updatedSchema);
            assertEquals(expectedBusinessObjectFormat, resultBusinessObjectFormat);
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaAdditiveSchemaChangesNewColumnAdded()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format with a schema.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        businessObjectFormatService.createBusinessObjectFormat(request);

        // Create a second version of the business object format with a new schema that is additive to the previous format version schema.
        Schema newFormatVersionSchema = businessObjectFormatServiceTestHelper.getTestSchema();
        SchemaColumn newSchemaColumn = new SchemaColumn();
        newFormatVersionSchema.getColumns().add(newSchemaColumn);
        newSchemaColumn.setName("NEW_COLUMN");
        newSchemaColumn.setType("TYPE");
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), newFormatVersionSchema);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
                PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), newFormatVersionSchema, resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaHavingNullRowFormatValuesNoSchemaChanges()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        BusinessObjectFormatCreateRequest request;
        BusinessObjectFormat resultBusinessObjectFormat;

        // Create an initial version of the business object format with a schema having null row format values.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        request.getSchema().setDelimiter(null);
        request.getSchema().setEscapeCharacter(null);
        businessObjectFormatService.createBusinessObjectFormat(request);

        // Get business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
        assertNotNull(businessObjectFormatEntity);

        // Schema null value is a required parameter, so we update business object format entity directly to set it to null.
        businessObjectFormatEntity.setNullValue(null);
        businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);

        // Create a second version of the business object format with a schema that is identical to the initial version schema.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        request.getSchema().setNullValue(EMPTY_STRING);   // This is a required parameter, so it cannot be set to null.
        request.getSchema().setDelimiter(null);
        request.getSchema().setEscapeCharacter(null);
        resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        // Please note that the escape character which was passed as an empty string gets returned as null.
        Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema();
        expectedSchema.setNullValue(null);
        expectedSchema.setDelimiter(null);
        expectedSchema.setEscapeCharacter(null);
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(resultBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                SECOND_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                expectedSchema, resultBusinessObjectFormat);

        // Create a third version of the business object format with a schema that is identical to the initial version schema,
        // except that we now pass empty string values for all three row format parameters.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        request.getSchema().setNullValue(EMPTY_STRING);
        request.getSchema().setDelimiter(EMPTY_STRING);
        request.getSchema().setEscapeCharacter(EMPTY_STRING);
        resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(resultBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                THIRD_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                expectedSchema, resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaHavingNullRowFormatValuesNonAdditiveSchemaChanges()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format with a schema having null row format values.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        request.getSchema().setDelimiter(null);
        request.getSchema().setEscapeCharacter(null);
        businessObjectFormatService.createBusinessObjectFormat(request);

        // Get business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
        assertNotNull(businessObjectFormatEntity);

        // Schema null value is a required parameter, so we update business object format entity directly to set it to null.
        businessObjectFormatEntity.setNullValue(null);
        businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);

        Schema newSchema;

        // Try to create a second version of the business object format with a schema that has a different null value.
        try
        {
            newSchema = businessObjectFormatServiceTestHelper.getTestSchema();
            newSchema.setNullValue(SCHEMA_NULL_VALUE_NULL_WORD);
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), newSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "New format version null value does not match to the previous format version null value.", e.getMessage());
        }

        // Try to create a second version of the business object format with a schema that has a different schema delimiter.
        try
        {
            newSchema = businessObjectFormatServiceTestHelper.getTestSchema();
            newSchema.setNullValue(EMPTY_STRING);   // This is a required parameter, so it cannot be set to null.
            newSchema.setDelimiter(SCHEMA_DELIMITER_COMMA);
            newSchema.setEscapeCharacter(null);
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), newSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "New format version delimiter character does not match to the previous format version delimiter character.", e.getMessage());
        }

        // Try to create a second version of the business object format with a schema that has a different schema escape character.
        try
        {
            newSchema = businessObjectFormatServiceTestHelper.getTestSchema();
            newSchema.setNullValue(EMPTY_STRING);   // This is a required parameter, so it cannot be set to null.
            newSchema.setDelimiter(null);
            newSchema.setEscapeCharacter(SCHEMA_ESCAPE_CHARACTER_TILDE);
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), newSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "New format version escape character does not match to the previous format version escape character.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaNoSchemaChanges()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format with a schema.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        businessObjectFormatService.createBusinessObjectFormat(request);

        // Create a second version of the business object format with a schema that is identical to the initial version schema.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
                PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema(),
                resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaNonAdditiveSchemaChanges()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of a business object format with a schema.
        businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema()));

        Schema newSchema;

        // Try to create a second version of the business object format without a schema.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), NO_SCHEMA));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. New format version schema is not specified.",
                e.getMessage());
        }

        // Try to create a second version of the business object format with a schema that has a different null value.
        try
        {
            newSchema = businessObjectFormatServiceTestHelper.getTestSchema();
            newSchema.setNullValue(SCHEMA_NULL_VALUE_NULL_WORD);
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), newSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "New format version null value does not match to the previous format version null value.", e.getMessage());
        }

        // Try to create a second version of the business object format with a schema that has a different schema delimiter.
        try
        {
            newSchema = businessObjectFormatServiceTestHelper.getTestSchema();
            newSchema.setDelimiter(SCHEMA_DELIMITER_COMMA);
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), newSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "New format version delimiter character does not match to the previous format version delimiter character.", e.getMessage());
        }

        // Try to create a second version of the business object format with a schema that has a different schema escape character.
        try
        {
            newSchema = businessObjectFormatServiceTestHelper.getTestSchema();
            newSchema.setEscapeCharacter(SCHEMA_ESCAPE_CHARACTER_TILDE);
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), newSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "New format version escape character does not match to the previous format version escape character.", e.getMessage());
        }

        // Try to create a second version of the business object format with a schema that has no partition column.
        try
        {
            newSchema = businessObjectFormatServiceTestHelper.getTestSchema();
            newSchema.setPartitions(null);
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), newSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "Non-additive changes detected to the previously defined partition columns.", e.getMessage());
        }

        // Try to create a second version of the business object format with a schema that has one less partition column.
        try
        {
            newSchema = businessObjectFormatServiceTestHelper.getTestSchema();
            newSchema.setPartitions(newSchema.getPartitions().subList(0, newSchema.getPartitions().size() - 1));
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), newSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "Non-additive changes detected to the previously defined partition columns.", e.getMessage());
        }

        // Try to create a second version of the business object format with a schema that has different set of partition columns.
        try
        {
            newSchema = businessObjectFormatServiceTestHelper.getTestSchema();
            newSchema.setPartitions(businessObjectFormatServiceTestHelper.getTestSchema2().getPartitions());
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), newSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "Non-additive changes detected to the previously defined partition columns.", e.getMessage());
        }

        // Try to create a second version of the business object format with a schema that has one less regular column.
        try
        {
            newSchema = businessObjectFormatServiceTestHelper.getTestSchema();
            newSchema.setColumns(newSchema.getColumns().subList(0, newSchema.getColumns().size() - 1));
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), newSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "Non-additive changes detected to the previously defined regular (non-partitioning) columns.", e.getMessage());
        }

        // Try to create a second version of the business object format with a schema that has different set of regular columns.
        try
        {
            newSchema = businessObjectFormatServiceTestHelper.getTestSchema();
            newSchema.setColumns(businessObjectFormatServiceTestHelper.getTestSchema2().getColumns());
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), newSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "Non-additive changes detected to the previously defined regular (non-partitioning) columns.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaNonAdditiveSchemaChangesColumnSizeDecreased()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial format schema.
        Schema initialSchema = new Schema(
            Arrays.asList(new SchemaColumn(COLUMN_NAME, COLUMN_DATA_TYPE_CHAR, COLUMN_SIZE_2, NO_COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION)),
            NO_PARTITION_COLUMNS, SCHEMA_NULL_VALUE_BACKSLASH_N, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, PARTITION_KEY_GROUP);

        // Create an initial version of the business object format.
        businessObjectFormatService.createBusinessObjectFormat(
            new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, initialSchema));

        // Create an updated schema having a regular column size decreased.
        Schema updatedSchema = (Schema) initialSchema.clone();
        updatedSchema.getColumns().get(0).setSize(COLUMN_SIZE);

        try
        {
            // Try to create a second version of the business object format with a new schema having regular column size decreased.
            businessObjectFormatService.createBusinessObjectFormat(
                new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, updatedSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "Non-additive changes detected to the previously defined regular (non-partitioning) columns.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaNonAdditiveSchemaChangesColumnSizeIncreasedForNotAllowedDataType()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial format schema.
        Schema initialSchema = new Schema(
            Arrays.asList(new SchemaColumn(COLUMN_NAME, COLUMN_DATA_TYPE, COLUMN_SIZE, NO_COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION)),
            NO_PARTITION_COLUMNS, SCHEMA_NULL_VALUE_BACKSLASH_N, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, PARTITION_KEY_GROUP);

        // Create an initial version of the business object format.
        businessObjectFormatService.createBusinessObjectFormat(
            new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, initialSchema));

        // Create an updated schema having a regular column size increased.
        Schema updatedSchema = (Schema) initialSchema.clone();
        updatedSchema.getColumns().get(0).setSize(COLUMN_SIZE_2);

        try
        {
            // Try to create a second version of the business object format with a new schema
            // having regular column size increased but for a non-allowed column data type.
            businessObjectFormatService.createBusinessObjectFormat(
                new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, updatedSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "Non-additive changes detected to the previously defined regular (non-partitioning) columns.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaNonAdditiveSchemaChangesColumnSizeIncreasedOriginalColumnSizeNotPositiveInteger()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        int i = 0;
        for (String originalColumnSize : Arrays.asList("NOT_AN_INTEGER", NEGATIVE_COLUMN_SIZE, ZERO_COLUMN_SIZE))
        {
            // Create an initial format schema with a regular column size having not a positive integer value.
            Schema initialSchema = new Schema(Arrays.asList(
                new SchemaColumn(COLUMN_NAME, COLUMN_DATA_TYPE_CHAR, originalColumnSize, NO_COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION)),
                NO_PARTITION_COLUMNS, SCHEMA_NULL_VALUE_BACKSLASH_N, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, PARTITION_KEY_GROUP);

            // We need to specify a unique format usage for each data type being tested to avoid an already exists exception.
            String formatUsage = String.format("%s_%d", FORMAT_USAGE_CODE, i++);

            // Create an initial version of the business object format.
            businessObjectFormatService.createBusinessObjectFormat(
                new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, formatUsage, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, initialSchema));

            // Create an updated schema having a regular column size set to a positive integer.
            Schema updatedSchema = (Schema) initialSchema.clone();
            updatedSchema.getColumns().get(0).setSize(COLUMN_SIZE);

            try
            {
                // Try to create a second version of the business object format with a new schema
                // having regular column size changed from a non-integer to a positive integer.
                businessObjectFormatService.createBusinessObjectFormat(
                    new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, formatUsage, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                        NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, updatedSchema));
                fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                    "Non-additive changes detected to the previously defined regular (non-partitioning) columns.", e.getMessage());
            }
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaNonAdditiveSchemaChangesColumnSizeIncreasedUpdatedColumnSizeNotPositiveInteger()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial format schema with a positive integer regular column size.
        Schema initialSchema = new Schema(
            Arrays.asList(new SchemaColumn(COLUMN_NAME, COLUMN_DATA_TYPE_CHAR, COLUMN_SIZE, NO_COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION)),
            NO_PARTITION_COLUMNS, SCHEMA_NULL_VALUE_BACKSLASH_N, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, PARTITION_KEY_GROUP);

        // Create an initial version of the business object format.
        businessObjectFormatService.createBusinessObjectFormat(
            new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, initialSchema));

        for (String updatedColumnSize : Arrays.asList("NOT_AN_INTEGER", NEGATIVE_COLUMN_SIZE, ZERO_COLUMN_SIZE))
        {
            // Create an updated schema having a regular column size set to value that is not a positive integer.
            Schema updatedSchema = (Schema) initialSchema.clone();
            updatedSchema.getColumns().get(0).setSize(updatedColumnSize);

            try
            {
                // Try to create a second version of the business object format with a new schema
                // having regular column size changed from a positive integer to a non-integer value.
                businessObjectFormatService.createBusinessObjectFormat(
                    new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                        NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, updatedSchema));
                fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                    "Non-additive changes detected to the previously defined regular (non-partitioning) columns.", e.getMessage());
            }
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaNonAdditiveSchemaChangesPartitionColumnsAdded()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of a business object format with a schema having no partition columns.
        Schema originalSchema = businessObjectFormatServiceTestHelper.getTestSchema();
        originalSchema.setPartitions(null);
        businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), originalSchema));

        // Try to create a second version of the business object format with a schema that now has partition columns.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema()));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "Non-additive changes detected to the previously defined partition columns.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithoutSchema()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        BusinessObjectFormatCreateRequest request;
        BusinessObjectFormat resultBusinessObjectFormat;

        // Create an initial version of the business object format without a schema.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), NO_SCHEMA);
        businessObjectFormatService.createBusinessObjectFormat(request);

        // Create a second version of the business object format without a schema.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), NO_SCHEMA);
        resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(resultBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                SECOND_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), NO_SCHEMA,
                resultBusinessObjectFormat);

        // Create a second version of the business object format with a schema.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(resultBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                THIRD_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectFormatServiceTestHelper.getTestSchema(), resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInvalidParameters()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Validate that we can create the first version of the format.
        BusinessObjectFormatCreateRequest request;

        // Try to perform a create using invalid namespace code.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an ObjectNotFoundException when non-existing namespace is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String
                .format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", request.getBusinessObjectDefinitionName(),
                    request.getNamespace()), e.getMessage());
        }

        // Try to create a business object format when namespace contains a forward slash character.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(addSlash(BDEF_NAMESPACE), BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when namespace contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }

        // Try to perform a create using invalid business object definition name.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an ObjectNotFoundException when non-existing business object definition name is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String
                .format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", request.getBusinessObjectDefinitionName(),
                    request.getNamespace()), e.getMessage());
        }

        // Try to create a business object format when business object definition name contains a forward slash character.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(BDEF_NAMESPACE, addSlash(BDEF_NAME), FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when business object definition name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition name can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object format when business object format usage contains a forward slash character.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(BDEF_NAMESPACE, BDEF_NAME, addSlash(FORMAT_USAGE_CODE), FORMAT_FILE_TYPE_CODE, PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when business object format usage contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object format usage can not contain a forward slash character.", e.getMessage());
        }

        // Try to perform a create using invalid format file type.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an ObjectNotFoundException when non-existing business object format file type code is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("File type with code \"%s\" doesn't exist.", request.getBusinessObjectFormatFileType()), e.getMessage());
        }

        // Try to create a business object format when business object format file type contains a forward slash character.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, addSlash(FORMAT_FILE_TYPE_CODE), PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when business object format file type contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object format file type can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object format when partition key contains a forward slash character.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, addSlash(PARTITION_KEY),
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when partition key contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Partition key can not contain a forward slash character.", e.getMessage());
        }

        // Try to perform a create using invalid partition key group.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        request.getSchema().setPartitionKeyGroup("I_DO_NOT_EXIST");
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an ObjectNotFoundException when non-existing partition key group is specified.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Partition key group \"%s\" doesn't exist.", request.getSchema().getPartitionKeyGroup()), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatLowerCaseParameters()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper
            .createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(), BDEF_NAME.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), PARTITION_KEY_GROUP.toUpperCase());

        // Create a first version of the format using lower case input parameters.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), PARTITION_KEY.toLowerCase(), FORMAT_DESCRIPTION.toLowerCase(), FORMAT_DOCUMENT_SCHEMA.toLowerCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        request.getSchema().setPartitionKeyGroup(PARTITION_KEY_GROUP.toLowerCase());
        // For the first schema partition column name, use an opposite case from the format partition key was specified in.
        request.getSchema().getPartitions().get(0).setName(PARTITION_KEY.toUpperCase());
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema();
        expectedSchema.setPartitionKeyGroup(PARTITION_KEY_GROUP.toUpperCase());
        expectedSchema.getPartitions().get(0).setName(PARTITION_KEY.toUpperCase());
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toLowerCase(), FORMAT_DESCRIPTION.toLowerCase(),
                FORMAT_DOCUMENT_SCHEMA.toLowerCase(), Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatMissingOptionalParameters()
    {
        // Create relative database entities including a business object definition.
        businessObjectFormatServiceTestHelper
            .createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_FILE_TYPE_CODE, PARTITION_KEY_GROUP);

        // Perform a create without specifying optional parameters.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, NO_FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
                PARTITION_KEY, NO_FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatMissingRequiredParameters()
    {
        // Try to perform a create without specifying business object definition name parameter.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema()));
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to perform a create without specifying format usage parameter.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema()));
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to perform a create without specifying format file type parameter.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema()));
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to perform a create without specifying format partition key parameter.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, BLANK_TEXT, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema()));
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition key must be specified.", e.getMessage());
        }

        // Try to create a business object format instance when attribute name is not specified.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, Arrays.asList(new Attribute(BLANK_TEXT, ATTRIBUTE_VALUE_1)),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema()));
            fail("Should throw an IllegalArgumentException when attribute name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An attribute name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatNoAttributeDefinitionPublishOptionSpecified()
    {
        // Create relative database entities including a business object definition.
        businessObjectFormatServiceTestHelper
            .createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_FILE_TYPE_CODE, PARTITION_KEY_GROUP);

        // Perform a create without specifying a publish option for an attribute definition.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, NO_FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                Arrays.asList(new AttributeDefinition(ATTRIBUTE_NAME_1_MIXED_CASE, null)), NO_SCHEMA);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object. The publish option is expected to default to "false".
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
                PARTITION_KEY, NO_FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                Arrays.asList(new AttributeDefinition(ATTRIBUTE_NAME_1_MIXED_CASE, NO_PUBLISH_ATTRIBUTE)), NO_SCHEMA, resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoPartitionKeyGroupSpecified()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a version of business object format without specifying a partition key group (using both blank text and a null value).
        Integer expectedBusinessObjectFormatVersion = INITIAL_FORMAT_VERSION;
        for (String partitionKeyGroupName : Arrays.asList(BLANK_TEXT, null))
        {
            BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                    businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
            request.getSchema().setPartitionKeyGroup(partitionKeyGroupName);
            BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

            // Validate the returned object.
            Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema();
            expectedSchema.setPartitionKeyGroup(null);
            businessObjectFormatServiceTestHelper
                .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, expectedBusinessObjectFormatVersion++,
                    LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA,
                    businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                    expectedSchema, businessObjectFormat);
        }
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaColumnName()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Try to create a business object format with an empty column name.
        SchemaColumn schemaColumn = new SchemaColumn();
        request.getSchema().getColumns().add(schemaColumn);
        schemaColumn.setName(BLANK_TEXT);
        schemaColumn.setType("TYPE");
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when schema has an empty column name.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A schema column name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaColumnType()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Try to create a business object format with an empty column data type.
        SchemaColumn schemaColumn = new SchemaColumn();
        request.getSchema().getColumns().add(schemaColumn);
        schemaColumn.setName("COLUMN_NAME");
        schemaColumn.setType(BLANK_TEXT);
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when schema has an empty column data type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A schema column data type must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaColumns()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Try to create a business object format with a schema without columns.
        request.getSchema().setColumns(null);
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when schema has no columns.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A schema must have at least one column.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaDelimiter()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Try to create a business object format with an empty schema escape character which is valid.
        request.getSchema().setDelimiter(null);
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema();

        // The escape characters gets trimmed which gets stored as the empty string which gets returned as null.
        expectedSchema.setDelimiter(null);
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaEscapeCharacter()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Try to create a business object format with an empty schema escape character which is valid.
        request.getSchema().setEscapeCharacter(" ");
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema();

        expectedSchema.setEscapeCharacter(" ");
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaNullValue()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Try to create a business object format with a non-specified schema null value.
        request.getSchema().setNullValue(null);
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when schema null value is null.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A schema null value can not be null.", e.getMessage());
        }

        // Try to create a business object format with an empty schema null value which is valid.
        request.getSchema().setNullValue("");
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema();

        // The escape characters gets trimmed which gets stored as the empty string which gets returned as null.
        expectedSchema.setNullValue(null);
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaPartitionColumnName()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Try to create a business object format with an empty partition column name.
        SchemaColumn partitionColumn = new SchemaColumn();
        request.getSchema().getPartitions().add(partitionColumn);
        partitionColumn.setName(BLANK_TEXT);
        partitionColumn.setType("TYPE");
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when schema has an empty partition column name.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A schema column name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaPartitionColumnType()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Try to create a business object format with an empty column data type.
        SchemaColumn partitionColumn = new SchemaColumn();
        request.getSchema().getPartitions().add(partitionColumn);
        partitionColumn.setName("COLUMN_NAME");
        partitionColumn.setType(BLANK_TEXT);
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when schema has an empty partition column data type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A schema column data type must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaPartitions()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request without schema partition columns.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        request.getSchema().setPartitions(null);

        // Create an initial version of business object format without schema partitions.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema();
        expectedSchema.setPartitions(null);
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNullSchemaEscapeCharacter()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Try to create a business object format with an empty schema escape character which is valid.
        request.getSchema().setEscapeCharacter(null);
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema();

        // The escape characters gets trimmed which gets stored as the empty string which gets returned as null.
        expectedSchema.setEscapeCharacter(null);
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatPartitionAndRegularColumnsHaveConflictingValues()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Try to create a business object format when partition column has a conflict value with the corresponding regular schema column.
        SchemaColumn schemaColumn = request.getSchema().getColumns().get(0);
        request.getSchema().getPartitions().get(0).setName(schemaColumn.getName());
        request.getSchema().getPartitions().get(0).setType("DIFFERENT_DATA_TYPE");
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when partition column has a conflict value with the corresponding regular schema column.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Schema data and partition column configurations with name \"%s\" have conflicting values. " +
                "All column values are case sensitive and must be identical.", schemaColumn.getName()), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatSchemaWhitespaceNullValueAndDelimiter()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of business object format with schema null value and delimiter as a whitespace character.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        request.getSchema().setNullValue(" ");
        request.getSchema().setDelimiter(" ");
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema();
        expectedSchema.setNullValue(" ");
        expectedSchema.setDelimiter(" ");
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatTopPartitionColumnNameNoMatchPartitionKey()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Try to create a business object format with the first schema partition column name does not matching the partition key.
        request.getSchema().getPartitions().get(0).setName("NOT_A_PARTITION_KEY");
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when the first schema partition column name does not match the partition key.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Partition key \"%s\" does not match the first schema partition column name \"%s\".", PARTITION_KEY,
                request.getSchema().getPartitions().get(0).getName()), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatTrimParameters()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a first version of the format using input parameters with leading and trailing empty spaces.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), addWhitespace(PARTITION_KEY), addWhitespace(FORMAT_DESCRIPTION), addWhitespace(FORMAT_DOCUMENT_SCHEMA),
                Arrays.asList(new Attribute(addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE), addWhitespace(ATTRIBUTE_VALUE_1))),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), addWhitespace(businessObjectFormatServiceTestHelper.getTestSchema()));
        request.getSchema().setPartitionKeyGroup(addWhitespace(PARTITION_KEY_GROUP));

        // Create an initial business object format version.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
                PARTITION_KEY, addWhitespace(FORMAT_DESCRIPTION), FORMAT_DOCUMENT_SCHEMA,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, addWhitespace(ATTRIBUTE_VALUE_1))),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema(),
                businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatAllBlankSpaceDocumentSchema()
    {
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a first version of the format with blank space document schema.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                StringUtils.repeat(" ", 10), Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1)),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        request.getSchema().setPartitionKeyGroup(PARTITION_KEY_GROUP);

        // Create an initial business object format version.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
                PARTITION_KEY, FORMAT_DESCRIPTION, "", Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1)),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema(),
                businessObjectFormat);
    }


    @Test
    public void testCreateBusinessObjectFormatNullDocumentSchema()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a first version of the format without document schema.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1)),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        request.getSchema().setPartitionKeyGroup(PARTITION_KEY_GROUP);

        // Create an initial business object format version.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
                PARTITION_KEY, FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1)),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema(),
                businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatUnprintableSchemaEscapeCharacter()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of business object format with a schema having an unprintable escape character.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        request.getSchema().setEscapeCharacter(String.valueOf((char) 1));
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema();
        expectedSchema.setEscapeCharacter(String.valueOf((char) 1));
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatUpperCaseParameters()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper
            .createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(), BDEF_NAME.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), PARTITION_KEY_GROUP.toLowerCase());

        // Create a first version of the format using upper case input parameters.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION.toUpperCase(), FORMAT_DOCUMENT_SCHEMA.toUpperCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());
        request.getSchema().setPartitionKeyGroup(PARTITION_KEY_GROUP.toUpperCase());
        // For the first schema partition column name, use an opposite case from the format partition key was specified in.
        request.getSchema().getPartitions().get(0).setName(PARTITION_KEY.toLowerCase());
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema();
        expectedSchema.setPartitionKeyGroup(PARTITION_KEY_GROUP.toLowerCase());
        expectedSchema.getPartitions().get(0).setName(PARTITION_KEY.toLowerCase());
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION.toUpperCase(),
                FORMAT_DOCUMENT_SCHEMA.toUpperCase(), Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testDeleteBusinessObjectFormat() throws Exception
    {
        // Create an initial version of a business object format.
        businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Retrieve the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService
            .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectFormatServiceTestHelper.getTestSchema(), deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatDataIsRegistered()
    {
        // Create an initial version of a business object format.
        businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat();

        // Retrieve the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Create a business object data entity associated with the business object format.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Try to delete the business object format.
        try
        {
            businessObjectFormatService.deleteBusinessObjectFormat(
                new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
            fail("Should throw an IllegalArgumentException when trying to delete a business object format that has business object data associated with it.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Can not delete a business object format that has business object data associated with it. " +
                    "Business object format: {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION), e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectFormatInvalidParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

        // Validate that we can perform a get on our business object format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES,
                NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, resultBusinessObjectFormat);

        // Try to perform a delete using invalid namespace code.
        try
        {
            businessObjectFormatService.deleteBusinessObjectFormat(
                new BusinessObjectFormatKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                    INITIAL_FORMAT_VERSION), e.getMessage());
        }

        // Try to perform a delete using invalid business object definition name.
        try
        {
            businessObjectFormatService.deleteBusinessObjectFormat(
                new BusinessObjectFormatKey(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                    INITIAL_FORMAT_VERSION), e.getMessage());
        }

        // Try to perform a delete using invalid format usage.
        try
        {
            businessObjectFormatService
                .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to perform a delete using invalid format file type.
        try
        {
            businessObjectFormatService
                .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to perform a delete using invalid format version.
        try
        {
            businessObjectFormatService
                .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999));
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999), e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectFormatLatestVersionWhenPreviousVersionExists()
    {
        // Create and persist two versions of the business object format.
        BusinessObjectFormatEntity initialVersionBusinessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, false, PARTITION_KEY);
        BusinessObjectFormatEntity latestVersionBusinessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

        // Validate that the initial version does not have the latest version flag set and that second version has it set.
        assertFalse(initialVersionBusinessObjectFormatEntity.getLatestVersion());
        assertTrue(latestVersionBusinessObjectFormatEntity.getLatestVersion());

        // Delete the latest (second) version of the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService
            .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(latestVersionBusinessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES,
                NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        latestVersionBusinessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION));
        assertNull(latestVersionBusinessObjectFormatEntity);

        // Validate that the initial version now has the latest version flag set.
        initialVersionBusinessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
        assertNotNull(initialVersionBusinessObjectFormatEntity);
        assertTrue(initialVersionBusinessObjectFormatEntity.getLatestVersion());
    }

    @Test
    public void testDeleteBusinessObjectFormatLowerCaseParameters()
    {
        // Create and persist a business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toUpperCase(), FORMAT_DOCUMENT_SCHEMA.toUpperCase(),
                LATEST_VERSION_FLAG_SET, PARTITION_KEY.toUpperCase());

        // Ensure that this business object format exists.
        assertNotNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));

        // Delete the business object format by passing alternate key value in lower case.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService.deleteBusinessObjectFormat(
            new BusinessObjectFormatKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toUpperCase(),
                FORMAT_DESCRIPTION.toUpperCase(), FORMAT_DOCUMENT_SCHEMA.toUpperCase(), NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA,
                deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatMissingOptionalParameters() throws Exception
    {
        // Create and persist a business object definition.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

        // Delete the business object format without specifying namespace code.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService
            .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES,
                NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, deletedBusinessObjectFormat);
        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatMissingRequiredParameters()
    {
        // Try to perform a delete without specifying business object definition name.
        try
        {
            businessObjectFormatService.deleteBusinessObjectFormat(
                new BusinessObjectFormatKey(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying business object format usage.
        try
        {
            businessObjectFormatService
                .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying business object format file type.
        try
        {
            businessObjectFormatService
                .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, INITIAL_FORMAT_VERSION));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying business object format version.
        try
        {
            businessObjectFormatService
                .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectFormatNotLatestVersion()
    {
        // Create and persist a business object format which is not marked as the latest version.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, false, PARTITION_KEY);

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService
            .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, false, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA,
                deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatTrimParameters()
    {
        // Create and persist a business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

        // Ensure that this business object format exists.
        assertNotNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService.deleteBusinessObjectFormat(
            new BusinessObjectFormatKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES,
                NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatUpperCaseParameters()
    {
        // Create and persist a business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toLowerCase(), FORMAT_DOCUMENT_SCHEMA.toLowerCase(),
                LATEST_VERSION_FLAG_SET, PARTITION_KEY.toLowerCase());

        // Ensure that this business object format exists.
        assertNotNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));

        // Delete the business object format by passing alternate key value in upper case.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService.deleteBusinessObjectFormat(
            new BusinessObjectFormatKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toLowerCase(),
                FORMAT_DESCRIPTION.toLowerCase(), FORMAT_DOCUMENT_SCHEMA.toLowerCase(), NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA,
                deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testGenerateBusinessObjectFormatDdl()
    {
        // Prepare test data.
        businessObjectFormatServiceTestHelper.createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        // Retrieve business object format ddl.
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService
            .generateBusinessObjectFormatDdl(businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME));

        // Validate the results.
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(AbstractServiceTest.PARTITION_COLUMNS.length, AbstractServiceTest.FIRST_COLUMN_NAME,
                AbstractServiceTest.FIRST_COLUMN_DATA_TYPE, AbstractServiceTest.ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, true, true), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlCollection()
    {
        // Prepare database entities required for testing.
        businessObjectFormatServiceTestHelper.createDatabaseEntitiesForBusinessObjectFormatDdlCollectionTesting();

        // Generate DDL for a collection of business object formats.
        BusinessObjectFormatDdlCollectionResponse resultBusinessObjectFormatDdlCollectionResponse = businessObjectFormatService
            .generateBusinessObjectFormatDdlCollection(businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlCollectionRequest());

        // Validate the response object.
        assertEquals(businessObjectFormatServiceTestHelper.getExpectedBusinessObjectFormatDdlCollectionResponse(),
            resultBusinessObjectFormatDdlCollectionResponse);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlInvalidParameters()
    {
        // Prepare test data.
        businessObjectFormatServiceTestHelper.createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        BusinessObjectFormatDdlRequest request;

        // Try to retrieve business object format ddl using non-existing format.
        request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setBusinessObjectDefinitionName("I_DO_NOT_EXIST");
        try
        {
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
            fail("Should throw an ObjectNotFoundException when non-existing business object format is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(request.getNamespace(), request.getBusinessObjectDefinitionName(),
                        request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()),
                e.getMessage());
        }

        // Try to retrieve business object format ddl using non-existing custom ddl.
        request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setCustomDdlName("I_DO_NOT_EXIST");
        try
        {
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
            fail("Should throw an ObjectNotFoundException when non-existing custom ddl is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Custom DDL with name \"%s\" does not exist for business object format with namespace \"%s\", " +
                    "business object definition name \"%s\", format usage \"%s\", format file type \"%s\", and format version \"%d\".", request.getCustomDdlName(),
                request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlLowerCaseParameters()
    {
        // Prepare test data.
        businessObjectFormatServiceTestHelper.createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        // Retrieve business object format ddl request with all string values in lower case.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setNamespace(request.getNamespace().toLowerCase());
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toLowerCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toLowerCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toLowerCase());
        request.setCustomDdlName(request.getCustomDdlName().toLowerCase());
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(AbstractServiceTest.PARTITION_COLUMNS.length, AbstractServiceTest.FIRST_COLUMN_NAME,
                AbstractServiceTest.FIRST_COLUMN_DATA_TYPE, AbstractServiceTest.ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, true, true), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlMissingOptionalParameters()
    {
        // Prepare test data without custom ddl.
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), CUSTOM_DDL_NAME);

        // Retrieve business object format ddl request without optional parameters.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(BLANK_TEXT);
        request.setBusinessObjectFormatVersion(null);
        request.setIncludeDropTableStatement(null);
        request.setIncludeIfNotExistsOption(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT,
                Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, false, false);
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl("", expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlMissingRequiredParameters()
    {
        BusinessObjectFormatDdlRequest request;

        // Try to retrieve business object format ddl when namespace parameter is not specified.
        request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setNamespace(BLANK_TEXT);
        try
        {
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when namespace parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to retrieve business object format ddl when business object definition name parameter is not specified.
        request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setBusinessObjectDefinitionName(BLANK_TEXT);
        try
        {
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when business object definition name parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to retrieve business object format ddl when business object format usage parameter is not specified.
        request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setBusinessObjectFormatUsage(BLANK_TEXT);
        try
        {
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when business object format usage parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to retrieve business object format ddl when business object format file type parameter is not specified.
        request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setBusinessObjectFormatFileType(BLANK_TEXT);
        try
        {
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when business object format file type parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to retrieve business object format ddl when output format parameter is not specified.
        request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setOutputFormat(null);
        try
        {
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when output format parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An output format must be specified.", e.getMessage());
        }

        // Try to retrieve business object format ddl when table name parameter is not specified.
        request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setTableName(BLANK_TEXT);
        try
        {
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when table name parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A table name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlAllKnownFileTypes()
    {
        // Get storage entity.
        StorageEntity storageEntity = storageDao.getStorageByName(StorageEntity.MANAGED_STORAGE);

        // Expected business object format file type to Hive file format mapping.
        HashMap<String, String> businessObjectFormatFileTypeMap = new HashMap<>();
        businessObjectFormatFileTypeMap.put(FileTypeEntity.BZ_FILE_TYPE, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT);
        businessObjectFormatFileTypeMap.put(FileTypeEntity.GZ_FILE_TYPE, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT);
        businessObjectFormatFileTypeMap.put(FileTypeEntity.ORC_FILE_TYPE, Hive13DdlGenerator.ORC_HIVE_FILE_FORMAT);
        businessObjectFormatFileTypeMap.put(FileTypeEntity.PARQUET_FILE_TYPE, Hive13DdlGenerator.PARQUET_HIVE_FILE_FORMAT);
        businessObjectFormatFileTypeMap.put(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT);

        for (String businessObjectFormatFileType : businessObjectFormatFileTypeMap.keySet())
        {
            // Prepare test data for the respective business object format file type.
            List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1);
            String partitionKey = partitionColumns.get(0).getName();
            BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, partitionKey, NO_PARTITION_KEY_GROUP,
                    businessObjectDefinitionServiceTestHelper.getNewAttributes(), SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                    SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns);

            for (String partitionValue : UNSORTED_PARTITION_VALUES)
            {
                BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, partitionValue,
                        NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
                String s3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(S3_KEY_PREFIX_VELOCITY_TEMPLATE, businessObjectFormatEntity,
                    businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity), storageEntity.getName());
                StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
                    .createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

                // Create two storage files.
                for (int i = 0; i < 2; i++)
                {
                    storageFileDaoTestHelper
                        .createStorageFileEntity(storageUnitEntity, String.format("%s/data%d.dat", s3KeyPrefix, i), FILE_SIZE_1_KB, ROW_COUNT_1000);
                }

                herdDao.saveAndRefresh(storageUnitEntity);
                herdDao.saveAndRefresh(businessObjectDataEntity);
            }

            // Retrieve business object format ddl.
            BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
            request.setBusinessObjectFormatFileType(businessObjectFormatFileType);
            BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

            // Validate the results.
            String expectedHiveFileFormat = businessObjectFormatFileTypeMap.get(businessObjectFormatFileType);
            String expectedDdl = businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatDdl(partitionColumns.size(), FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, expectedHiveFileFormat,
                    businessObjectFormatFileType, true, true);
            businessObjectFormatServiceTestHelper
                .validateBusinessObjectFormatDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION,
                    BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, null, expectedDdl, resultDdl);
        }
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlEscapeBackslashInRowFormat()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, BACKSLASH, BACKSLASH, BACKSLASH,
                schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object format ddl.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results - please note that we do not escape single backslash in null value.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\\\' ESCAPED BY '\\\\' NULL DEFINED AS '\\'";
        String expectedDdl = businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, expectedRowFormat,
                Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, true, true);
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlEscapeSingleQuoteInRowFormat()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, SINGLE_QUOTE, SINGLE_QUOTE, SINGLE_QUOTE,
                schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object format ddl.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\'' ESCAPED BY '\\'' NULL DEFINED AS '\\''";
        String expectedDdl = businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, expectedRowFormat,
                Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, true, true);
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlForwardSlashInPartitionColumnName()
    {
        // Prepare test data without custom ddl.
        String invalidPartitionColumnName = "INVALID_/_PRTN_CLMN";
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        partitionColumns.get(0).setName(invalidPartitionColumnName);
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, PARTITION_KEY, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, null);

        // Try to retrieve business object format ddl for the format that uses unsupported schema column data type.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        try
        {
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when partition column name contains a '/' character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Partition column name \"%s\" can not contain a '/' character. Business object format: " +
                    "{namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}", invalidPartitionColumnName, request.getNamespace(),
                request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(),
                request.getBusinessObjectFormatVersion()), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlMissingSchemaDelimiterCharacter()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object format ddl.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED NULL DEFINED AS '\\N'";
        String expectedDdl = businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, expectedRowFormat,
                Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, true, true);
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlMissingSchemaEscapeCharacter()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, SCHEMA_DELIMITER_PIPE, null,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object format ddl.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' NULL DEFINED AS '\\N'";
        String expectedDdl = businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, expectedRowFormat,
                Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, true, true);
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlMissingSchemaNullValue()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object format ddl.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' NULL DEFINED AS ''";
        String expectedDdl = businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, expectedRowFormat,
                Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, true, true);
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlNoPartitioning()
    {
        // Prepare test data without custom ddl.
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY,
                SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), null,
                null);

        // Retrieve business object format ddl for a non-partitioned table and without specifying custom ddl name.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(0, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, true, true);
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlNotSupportedFileType()
    {
        // Prepare test data without custom ddl.
        String businessObjectFileType = "UNKNOWN";
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(businessObjectFileType, PARTITION_KEY, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), null);

        // Try to retrieve business object format ddl for the format without custom ddl and that uses unsupported file type.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        request.setBusinessObjectFormatFileType(businessObjectFileType);
        try
        {
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when business object format has an unsupported file type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Unsupported format file type for business object format {namespace: \"%s\", businessObjectDefinitionName: \"%s\", " +
                    "businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}.", request.getNamespace(),
                request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(),
                request.getBusinessObjectFormatVersion()), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlNotSupportedSchemaColumnDataType()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> schemaColumns = schemaColumnDaoTestHelper.getTestSchemaColumns();
        SchemaColumn schemaColumn = new SchemaColumn();
        schemaColumns.add(schemaColumn);
        schemaColumn.setName("COLUMN");
        schemaColumn.setType("UNKNOWN");
        String partitionKey = schemaColumns.get(0).getName();
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, schemaColumnDaoTestHelper.getTestPartitionColumns(), null);

        // Try to retrieve business object format ddl for the format that uses unsupported schema column data type.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        try
        {
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when business object format has a column with an unsupported data type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Column \"%s\" has an unsupported data type \"%s\" in the schema for business object format " +
                    "{namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}.", schemaColumn.getName(), schemaColumn.getType(),
                request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlPartitionColumnIsAlsoRegularColumn()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> schemaColumns = schemaColumnDaoTestHelper.getTestSchemaColumns();
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        // Override the first schema column to be a partition column.
        schemaColumns.set(0, partitionColumns.get(0));
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns, null);

        // Retrieve business object format ddl without specifying custom ddl name.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(partitionColumns.size(), "ORGNL_PRTN_CLMN001", "DATE", ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, true, true);
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlSingleLevelPartitioning()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1);
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object format ddl without specifying custom ddl name.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(partitionColumns.size(), FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT,
                Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, true, true);
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlUnprintableCharactersInRowFormat()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        // Set schemaDelimiterCharacter to char(1), schemaEscapeCharacter to char(10), and schemaNullValue to char(128).
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, String.valueOf((char) 1),
                String.valueOf((char) 10), String.valueOf((char) 128), schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object format ddl request without business object format and data versions.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results - please note that 1 decimal = 1 octal, 10 decimal = 12 octal, and 128 decimal = 200 octal.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' ESCAPED BY '\\012' NULL DEFINED AS '\\200'";
        String expectedDdl = businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, expectedRowFormat,
                Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, true, true);
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoDropTable()
    {
        // Prepare test data without custom ddl.
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), CUSTOM_DDL_NAME);

        // Retrieve business object format ddl request without drop table statement.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setIncludeDropTableStatement(false);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT,
                Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, false, true);
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoPartitioning()
    {
        // Prepare non-partitioned test business object format with custom ddl.
        businessObjectFormatServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY,
                SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), null,
                CUSTOM_DDL_NAME);

        // Retrieve business object format ddl for a non-partitioned table.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(0, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, true, true);
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlTrimParameters()
    {
        // Prepare test data.
        businessObjectFormatServiceTestHelper.createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        // Retrieve business object format ddl request with all string values requiring trimming.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setBusinessObjectDefinitionName(addWhitespace(request.getBusinessObjectDefinitionName()));
        request.setBusinessObjectFormatUsage(addWhitespace(request.getBusinessObjectFormatUsage()));
        request.setBusinessObjectFormatFileType(addWhitespace(request.getBusinessObjectFormatFileType()));
        request.setTableName(addWhitespace(request.getTableName()));
        request.setCustomDdlName(addWhitespace(request.getCustomDdlName()));
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(AbstractServiceTest.PARTITION_COLUMNS.length, AbstractServiceTest.FIRST_COLUMN_NAME,
                AbstractServiceTest.FIRST_COLUMN_DATA_TYPE, AbstractServiceTest.ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, true, true), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlUpperCaseParameters()
    {
        // Prepare test data.
        businessObjectFormatServiceTestHelper.createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        // Retrieve business object format ddl request with all string values in upper case.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setNamespace(request.getNamespace().toUpperCase());
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toUpperCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toUpperCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toUpperCase());
        request.setCustomDdlName(request.getCustomDdlName().toUpperCase());
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(AbstractServiceTest.PARTITION_COLUMNS.length, AbstractServiceTest.FIRST_COLUMN_NAME,
                AbstractServiceTest.FIRST_COLUMN_DATA_TYPE, AbstractServiceTest.ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, true, true), resultDdl);
    }

    /**
     * Asserts that when replace columns is TRUE.
     */
    @Test
    public void testGenerateBusinessObjectFormatDdlWhenReplaceColumns()
    {
        businessObjectFormatServiceTestHelper.createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(BLANK_TEXT);
        businessObjectFormatDdlRequest.setReplaceColumns(true);
        businessObjectFormatDdlRequest.setIncludeDropTableStatement(false);
        businessObjectFormatDdlRequest.setIncludeIfNotExistsOption(false);
        BusinessObjectFormatDdl result = businessObjectFormatService.generateBusinessObjectFormatDdl(businessObjectFormatDdlRequest);

        Assert.assertEquals("result DDL", result.getDdl(),
            "ALTER TABLE `" + businessObjectFormatDdlRequest.getTableName() + "` REPLACE COLUMNS (\n" + "    `COLUMN001` TINYINT,\n" +
                "    `COLUMN002` SMALLINT COMMENT 'This is \\'COLUMN002\\' column. Here are \\'single\\' and \"double\" quotes along with a backslash \\.',\n" +
                "    `COLUMN003` INT,\n" + "    `COLUMN004` BIGINT,\n" + "    `COLUMN005` FLOAT,\n" + "    `COLUMN006` DOUBLE,\n" +
                "    `COLUMN007` DECIMAL,\n" + "    `COLUMN008` DECIMAL(p,s),\n" + "    `COLUMN009` DECIMAL,\n" + "    `COLUMN010` DECIMAL(p),\n" +
                "    `COLUMN011` DECIMAL(p,s),\n" + "    `COLUMN012` TIMESTAMP,\n" + "    `COLUMN013` DATE,\n" + "    `COLUMN014` STRING,\n" +
                "    `COLUMN015` VARCHAR(n),\n" + "    `COLUMN016` VARCHAR(n),\n" + "    `COLUMN017` CHAR(n),\n" + "    `COLUMN018` BOOLEAN,\n" +
                "    `COLUMN019` BINARY);");
    }

    /**
     * Asserts that when replace columns is TRUE, then setting customDdlName to non-null will cause an exception.
     */
    @Test
    public void testGenerateBusinessObjectFormatDdlWhenReplaceColumnsAndCustomDdlNameThrowsError()
    {
        final BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        businessObjectFormatDdlRequest.setReplaceColumns(true);
        businessObjectFormatDdlRequest.setIncludeDropTableStatement(null);
        businessObjectFormatDdlRequest.setIncludeIfNotExistsOption(null);
        businessObjectFormatDdlRequest.setCustomDdlName(CUSTOM_DDL_NAME);
        assertExceptionThrown(new Runnable()
        {
            public void run()
            {
                businessObjectFormatService.generateBusinessObjectFormatDdl(businessObjectFormatDdlRequest);
            }
        }, IllegalArgumentException.class, "'customDdlName' must not be specified when 'replaceColumns' is true");
    }

    /**
     * Asserts that when replace columns is TRUE, then setting includeDropTableStatement to TRUE will cause an exception.
     */
    @Test
    public void testGenerateBusinessObjectFormatDdlWhenReplaceColumnsAndIncludeDropTableStatementThrowsError()
    {
        final BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        businessObjectFormatDdlRequest.setReplaceColumns(true);
        businessObjectFormatDdlRequest.setIncludeDropTableStatement(true);
        businessObjectFormatDdlRequest.setIncludeIfNotExistsOption(null);
        businessObjectFormatDdlRequest.setCustomDdlName(null);
        assertExceptionThrown(new Runnable()
        {
            public void run()
            {
                businessObjectFormatService.generateBusinessObjectFormatDdl(businessObjectFormatDdlRequest);
            }
        }, IllegalArgumentException.class, "'includeDropTableStatement' must not be specified when 'replaceColumns' is true");
    }

    /**
     * Asserts that when replace columns is TRUE, then setting includeIfNotExistsOption to TRUE will cause an exception.
     */
    @Test
    public void testGenerateBusinessObjectFormatDdlWhenReplaceColumnsAndIncludeIfNotExistsOptionThrowsError()
    {
        final BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        businessObjectFormatDdlRequest.setReplaceColumns(true);
        businessObjectFormatDdlRequest.setIncludeDropTableStatement(null);
        businessObjectFormatDdlRequest.setIncludeIfNotExistsOption(true);
        businessObjectFormatDdlRequest.setCustomDdlName(null);
        assertExceptionThrown(new Runnable()
        {
            public void run()
            {
                businessObjectFormatService.generateBusinessObjectFormatDdl(businessObjectFormatDdlRequest);
            }
        }, IllegalArgumentException.class, "'includeIfNotExistsOption' must not be specified when 'replaceColumns' is true");
    }

    /**
     * Asserts that when replace columns is TRUE and other optional parameters are not specified.
     */
    @Test
    public void testGenerateBusinessObjectFormatDdlWhenReplaceColumnsMissingOptionalParameters()
    {
        businessObjectFormatServiceTestHelper.createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(null);
        businessObjectFormatDdlRequest.setReplaceColumns(true);
        businessObjectFormatDdlRequest.setIncludeDropTableStatement(null);
        businessObjectFormatDdlRequest.setIncludeIfNotExistsOption(null);
        BusinessObjectFormatDdl result = businessObjectFormatService.generateBusinessObjectFormatDdl(businessObjectFormatDdlRequest);

        Assert.assertEquals("result DDL", result.getDdl(),
            "ALTER TABLE `" + businessObjectFormatDdlRequest.getTableName() + "` REPLACE COLUMNS (\n" + "    `COLUMN001` TINYINT,\n" +
                "    `COLUMN002` SMALLINT COMMENT 'This is \\'COLUMN002\\' column. Here are \\'single\\' and \"double\" quotes along with a backslash \\.',\n" +
                "    `COLUMN003` INT,\n" + "    `COLUMN004` BIGINT,\n" + "    `COLUMN005` FLOAT,\n" + "    `COLUMN006` DOUBLE,\n" +
                "    `COLUMN007` DECIMAL,\n" + "    `COLUMN008` DECIMAL(p,s),\n" + "    `COLUMN009` DECIMAL,\n" + "    `COLUMN010` DECIMAL(p),\n" +
                "    `COLUMN011` DECIMAL(p,s),\n" + "    `COLUMN012` TIMESTAMP,\n" + "    `COLUMN013` DATE,\n" + "    `COLUMN014` STRING,\n" +
                "    `COLUMN015` VARCHAR(n),\n" + "    `COLUMN016` VARCHAR(n),\n" + "    `COLUMN017` CHAR(n),\n" + "    `COLUMN018` BOOLEAN,\n" +
                "    `COLUMN019` BINARY);");
    }

    @Test
    public void testGetBusinessObjectFormat()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat =
            businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectFormatServiceTestHelper.getTestSchema(), resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatInvalidParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Validate that we can perform a get on our business object format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES,
                NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, resultBusinessObjectFormat);

        // Try to perform a get using invalid namespace code.
        try
        {
            businessObjectFormatService.getBusinessObjectFormat(
                new BusinessObjectFormatKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                    INITIAL_FORMAT_VERSION), e.getMessage());
        }

        // Try to perform a get using invalid business object definition name.
        try
        {
            businessObjectFormatService.getBusinessObjectFormat(
                new BusinessObjectFormatKey(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                    INITIAL_FORMAT_VERSION), e.getMessage());
        }

        // Try to perform a get using invalid format usage.
        try
        {
            businessObjectFormatService
                .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to perform a get using invalid format file type.
        try
        {
            businessObjectFormatService
                .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to perform a get using invalid format version.
        try
        {
            businessObjectFormatService
                .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999));
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectFormatLowerCaseParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toUpperCase(), FORMAT_DOCUMENT_SCHEMA.toUpperCase(),
                LATEST_VERSION_FLAG_SET, PARTITION_KEY.toUpperCase());

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.getBusinessObjectFormat(
            new BusinessObjectFormatKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toUpperCase(),
                FORMAT_DESCRIPTION.toUpperCase(), FORMAT_DOCUMENT_SCHEMA.toUpperCase(), NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA,
                resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatMissingOptionalParameters()
    {
        // Create and persist a business object definition.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Perform a get without specifying business object format version (passed as a null value) and namespace code.
        // Please note that HerdDaoTest.testGetBusinessObjectFormatByAltKeyFormatVersionNotSpecified
        // already validates the SQl select logic, so we do not have to go over it here.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES,
                NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatMissingRequiredParameters()
    {
        // Try to perform a get without specifying business object definition name.
        try
        {
            businessObjectFormatService
                .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to perform a get without specifying business object format usage.
        try
        {
            businessObjectFormatService
                .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to perform a get without specifying business object format file type.
        try
        {
            businessObjectFormatService
                .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, INITIAL_FORMAT_VERSION));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectFormatTrimParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

        // Perform a get using input parameters with leading and trailing empty spaces.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.getBusinessObjectFormat(
            new BusinessObjectFormatKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES,
                NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatUpperCaseParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toLowerCase(), FORMAT_DOCUMENT_SCHEMA.toLowerCase(),
                LATEST_VERSION_FLAG_SET, PARTITION_KEY.toLowerCase());

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.getBusinessObjectFormat(
            new BusinessObjectFormatKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toLowerCase(),
                FORMAT_DESCRIPTION.toLowerCase(), FORMAT_DOCUMENT_SCHEMA.toLowerCase(), NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA,
                resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormats()
    {
        // Create and persist the relative business object definitions.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : businessObjectFormatDaoTestHelper.getTestBusinessObjectFormatKeys())
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                    key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, false,
                    PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition.
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), false);

        // Validate the returned object.
        assertEquals(businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());

        // Retrieve a list of the latest version business object format keys for the specified business object definition.
        resultKeys = businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), true);

        // Validate the returned object.
        assertEquals(businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatLatestVersionKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGetBusinessObjectFormatsWithFilters()
    {
        // Create and persist the relative business object definitions.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : businessObjectFormatDaoTestHelper.getTestBusinessObjectFormatKeys())
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                    key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, false,
                    PARTITION_KEY);
        }

        String filteredFormatUsage = FORMAT_USAGE_CODE;
        // Retrieve a list of business object format keys for the specified business object definition.
        BusinessObjectFormatKeys resultKeys = businessObjectFormatService
            .getBusinessObjectFormatsWithFilters(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), " " + filteredFormatUsage.toLowerCase() + " ", false);

        // Need to filter format usage
        List<BusinessObjectFormatKey> expectedKeyList = businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatKeys();
        expectedKeyList = expectedKeyList.stream().filter(formatKey -> (formatKey.getBusinessObjectFormatUsage().equalsIgnoreCase(filteredFormatUsage)))
            .collect(Collectors.toList());

        // Validate the returned object.
        assertEquals(expectedKeyList, resultKeys.getBusinessObjectFormatKeys());

        // Retrieve a list of the latest version business object format keys for the specified business object definition.
        resultKeys = businessObjectFormatService
            .getBusinessObjectFormatsWithFilters(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), " " + filteredFormatUsage.toLowerCase() + " ", true);

        expectedKeyList = businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatLatestVersionKeys();
        expectedKeyList = expectedKeyList.stream().filter(formatKey -> (formatKey.getBusinessObjectFormatUsage().equalsIgnoreCase(filteredFormatUsage)))
            .collect(Collectors.toList());

        // Validate the returned object.
        assertEquals(expectedKeyList, resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGetBusinessObjectFormatsInvalidParameters()
    {
        // Try to get business object formats using invalid namespace code.
        try
        {
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey("I_DO_NOT_EXIST", BDEF_NAME), false);
            fail("Should throw an ObjectNotFoundException when not able to find business object definition.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", BDEF_NAME, "I_DO_NOT_EXIST"),
                e.getMessage());
        }

        // Try to get business object formats using invalid business object definition name.
        try
        {
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE, "I_DO_NOT_EXIST"), false);
            fail("Should throw an ObjectNotFoundException when not able to find business object definition.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", "I_DO_NOT_EXIST", NAMESPACE),
                e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectFormatsLowerCaseParameters()
    {
        // Create and persist the relative business object definitions.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : businessObjectFormatDaoTestHelper.getTestBusinessObjectFormatKeys())
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                    key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, false,
                    PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition using lower case input parameters.
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()), false);

        // Validate the returned object.
        assertEquals(businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGetBusinessObjectFormatsMissingOptionalParameters()
    {
        // Create and persist the relative business object definitions.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : businessObjectFormatDaoTestHelper.getTestBusinessObjectFormatKeys())
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                    key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, false,
                    PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object
        // definition without passing the latestBusinessObjectFormatVersion flag.
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), false);

        // Validate the returned object.
        assertEquals(businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGetBusinessObjectFormatsMissingRequiredParameters()
    {
        // Try to get business object formats without specifying business object definition name.
        try
        {
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE, BLANK_TEXT), false);
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectFormatsTrimParameters()
    {
        // Create and persist the relative business object definitions.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : businessObjectFormatDaoTestHelper.getTestBusinessObjectFormatKeys())
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                    key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, false,
                    PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition using input parameters with leading and trailing empty spaces.
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME)), false);

        // Validate the returned object.
        assertEquals(businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGetBusinessObjectFormatsUpperCaseParameters()
    {
        // Create and persist the relative business object definitions.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : businessObjectFormatDaoTestHelper.getTestBusinessObjectFormatKeys())
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                    key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, false,
                    PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition using upper case input parameters
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()), false);

        // Validate the returned object.
        assertEquals(businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testUpdateBusinessObjectFormat()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat =
            businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Create a new partition key group for the update request.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and schema.
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2(), businessObjectFormatServiceTestHelper.getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2(), businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectFormatServiceTestHelper.getTestSchema2(), updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatAlreadyExistingDuplicateAttributes()
    {
        // Create and persist a business object format with duplicate attributes.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY, NO_PARTITION_KEY_GROUP, Arrays
                    .asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1),
                        new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_2)));

        // Try to update a business object format instance that already has duplicate attributes.
        try
        {
            businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                    businessObjectFormatServiceTestHelper.createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2,
                        Arrays.asList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_3)),
                        businessObjectFormatServiceTestHelper.getTestSchema2()));
            fail("Should throw an IllegalStateException when business object format has duplicate attributes.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Found duplicate attribute with name \"%s\" for business object format {%s}.", ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(),
                businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION)),
                e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatDuplicateAttributes()
    {
        // Try to update a business object format instance when duplicate attributes are specified.
        try
        {
            businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                    businessObjectFormatServiceTestHelper.createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA, Arrays
                            .asList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_3),
                                new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_3)),
                        businessObjectFormatServiceTestHelper.getTestSchema2()));
            fail("Should throw an IllegalArgumentException when duplicate attributes are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate attribute name found: %s", ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase()), e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatInitialSchemaPresentAndDataIsRegistered()
    {
        // Create an initial version of a business object format with format description and schema information.
        businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat();

        // Retrieve the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Create a new partition key group for the update request.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Create a business object data entity associated with the business object format.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Perform an update by changing the schema.
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes2(),
                businessObjectFormatServiceTestHelper.getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true,
                PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes2(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema2(),
                updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatInvalidParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

        // Create a business object format update request.
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2(), businessObjectFormatServiceTestHelper.getTestSchema2());

        // Create a new partition key group for the update request.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update on our business object format.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the results.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2(), NO_ATTRIBUTE_DEFINITIONS, businessObjectFormatServiceTestHelper.getTestSchema2(),
                businessObjectFormat);

        // Try to perform an update using invalid namespace code.
        try
        {
            businessObjectFormatService.updateBusinessObjectFormat(
                new BusinessObjectFormatKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                    INITIAL_FORMAT_VERSION), e.getMessage());
        }

        // Try to perform an update using invalid business object definition name.
        try
        {
            businessObjectFormatService.updateBusinessObjectFormat(
                new BusinessObjectFormatKey(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                    INITIAL_FORMAT_VERSION), e.getMessage());
        }

        // Try to perform a get using invalid business object definition name
        try
        {
            businessObjectFormatService.updateBusinessObjectFormat(
                new BusinessObjectFormatKey(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals("Business object format with namespace \"" + NAMESPACE + "\", business object definition name \"I_DO_NOT_EXIST\", format usage \"" +
                FORMAT_USAGE_CODE + "\", format file type \"" + FORMAT_FILE_TYPE_CODE + "\", and format version \"" + INITIAL_FORMAT_VERSION +
                "\" doesn't exist.", e.getMessage());
        }

        // Try to perform an update using invalid format usage.
        try
        {
            businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                    request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to perform an update using invalid format file type.
        try
        {
            businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION),
                    request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to perform an update using invalid format version.
        try
        {

            businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999), request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999), e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatLowerCaseParameters()
    {
        // Create an initial version of the format using upper case input parameters.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET,
                PARTITION_KEY.toUpperCase(), NO_PARTITION_KEY_GROUP,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())));

        // Create a new partition key group for the update request.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2.toUpperCase());

        // Perform an update by changing the format description and schema.
        Schema testSchema = businessObjectFormatServiceTestHelper.getTestSchema2();
        testSchema.setPartitionKeyGroup(testSchema.getPartitionKeyGroup().toLowerCase());
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2.toLowerCase(), FORMAT_DOCUMENT_SCHEMA_2.toLowerCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())),
                businessObjectFormatServiceTestHelper.getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService.updateBusinessObjectFormat(
            new BusinessObjectFormatKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema2();
        expectedSchema.setPartitionKeyGroup(expectedSchema.getPartitionKeyGroup().toUpperCase());
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toUpperCase(),
                FORMAT_DESCRIPTION_2.toLowerCase(), FORMAT_DOCUMENT_SCHEMA_2.toLowerCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toLowerCase())), null, expectedSchema,
                updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatMissingOptionalParameters()
    {
        // Create and persist a business object definition.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create and persist a valid business object format without description and with schema without any partitioning columns.
        BusinessObjectFormatEntity originalBusinessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY, PARTITION_KEY_GROUP,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), SCHEMA_DELIMITER_COMMA, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), null);

        // Create a new partition key group for the update request.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description to a blank text, replacing attributes with
        // an attribute with a blank text value, and replacing schema with a new schema not having partition columns.
        Schema testSchema2 = businessObjectFormatServiceTestHelper.getTestSchema2();
        testSchema2.setPartitions(null);
        for (String blankText : Arrays.asList(BLANK_TEXT, EMPTY_STRING, null))
        {
            BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatUpdateRequest(blankText, FORMAT_DOCUMENT_SCHEMA, Arrays.asList(new Attribute(ATTRIBUTE_NAME_4_MIXED_CASE, blankText)),
                    testSchema2);
            BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                    request);

            // Validate the returned object.
            businessObjectFormatServiceTestHelper
                .validateBusinessObjectFormat(originalBusinessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                    INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, blankText, FORMAT_DOCUMENT_SCHEMA,
                    Arrays.asList(new Attribute(ATTRIBUTE_NAME_4_MIXED_CASE, blankText)), NO_ATTRIBUTE_DEFINITIONS, testSchema2, resultBusinessObjectFormat);
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatMissingRequiredParameters()
    {
        // Create a business object format update request.
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2(), businessObjectFormatServiceTestHelper.getTestSchema2());

        // Try to perform an update without specifying business object definition name.
        try
        {
            businessObjectFormatService.updateBusinessObjectFormat(
                new BusinessObjectFormatKey(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);
            fail("Should throw an IllegalArgumentException when business object definition is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to perform an update without specifying business object format usage.
        try
        {
            businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                    request);
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to perform an update without specifying business object format file type.
        try
        {
            businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, INITIAL_FORMAT_VERSION), request);
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to perform an update without specifying business object format version.
        try
        {
            businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION),
                    request);
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to update a business object format instance when attribute name is not specified.
        try
        {
            businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                    businessObjectFormatServiceTestHelper.createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2,
                        Arrays.asList(new Attribute(BLANK_TEXT, ATTRIBUTE_VALUE_1)), businessObjectFormatServiceTestHelper.getTestSchema2()));
            fail("Should throw an IllegalArgumentException when attribute name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An attribute name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatNoChangesToDescriptionDocumentSchemaAndSchema()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat();

        // Perform an update without making any changes to format description and schema information.
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes2(),
                businessObjectFormatServiceTestHelper.getTestSchema());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2(), businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectFormatServiceTestHelper.getTestSchema(), updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatNoInitialDescriptionAndSchema()
    {
        // Create an initial version of a business object format without description and schema.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, null,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

        // Create a new partition key group for the update request.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and schema.
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2(), businessObjectFormatServiceTestHelper.getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2(), NO_ATTRIBUTE_DEFINITIONS, businessObjectFormatServiceTestHelper.getTestSchema2(),
                updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatNoInitialDocumentSchema()
    {
        // Create an initial version of a business object format without document schema.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, null,
                true, PARTITION_KEY);

        // Create a new partition key group for the update request.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the document schema.
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes2(),
                businessObjectFormatServiceTestHelper.getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2(), NO_ATTRIBUTE_DEFINITIONS, businessObjectFormatServiceTestHelper.getTestSchema2(),
                updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatDocumentSchemaToNull()
    {
        // Create an initial version of a business object format with some document schema
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, null,
                true, PARTITION_KEY);

        // Create a new partition key group for the update request.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the document schema to null
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2(), businessObjectFormatServiceTestHelper.getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2(), NO_ATTRIBUTE_DEFINITIONS, businessObjectFormatServiceTestHelper.getTestSchema2(),
                updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatRemoveAttributesAndSchema()
    {
        // Create an initial version of a business object format with attributes and schema.
        BusinessObjectFormat originalBusinessObjectFormat =
            businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(businessObjectDefinitionServiceTestHelper.getNewAttributes());

        BusinessObjectFormatUpdateRequest request;
        BusinessObjectFormat resultBusinessObjectFormat;

        // Perform an update to remove the business object format attributes and schema.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2, NO_ATTRIBUTES, NO_SCHEMA);
        resultBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2, NO_ATTRIBUTES,
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), NO_SCHEMA, resultBusinessObjectFormat);

        // Perform another update also without specifying both business object format attributes and schema.
        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_3, FORMAT_DOCUMENT_SCHEMA_3, NO_ATTRIBUTES, NO_SCHEMA);
        resultBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION_3, FORMAT_DOCUMENT_SCHEMA_3, NO_ATTRIBUTES,
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), NO_SCHEMA, resultBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatRemoveSchema()
    {
        // Create an initial version of a business object format with a description and a schema.
        BusinessObjectFormat originalBusinessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat();

        // Perform an update to remove the business object format schema.
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2(), NO_SCHEMA);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2(), businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), NO_SCHEMA,
                resultBusinessObjectFormat);
    }

    @Test
    public void testDeleteBusinessObjectFormatUsedAsDescriptiveFormat() throws Exception
    {
        // Create a version of a business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY, NO_PARTITION_KEY_GROUP);

        // Set this format version as descriptive format on the business object definition.
        businessObjectFormatEntity.getBusinessObjectDefinition().setDescriptiveBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDefinitionDao.saveAndRefresh(businessObjectFormatEntity.getBusinessObjectDefinition());

        // Validate the existence of the business object format entity.
        assertNotNull(businessObjectFormatDao
            .getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION)));

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService
            .deleteBusinessObjectFormat(new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION));

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS,
                NO_SCHEMA, deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao
            .getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION)));
    }

    @Test
    public void testUpdateBusinessObjectFormatTrimParameters()
    {
        // Create an initial version of a business object format.
        BusinessObjectFormat originalBusinessObjectFormat =
            businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Create a new partition key group for the update request.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and using input parameters with leading and trailing empty spaces.
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(addWhitespace(FORMAT_DESCRIPTION_2), addWhitespace(FORMAT_DOCUMENT_SCHEMA),
                Arrays.asList(new Attribute(addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE), addWhitespace(ATTRIBUTE_VALUE_1))),
                addWhitespace(businessObjectFormatServiceTestHelper.getTestSchema2()));
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService.updateBusinessObjectFormat(
            new BusinessObjectFormatKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, addWhitespace(FORMAT_DESCRIPTION_2), FORMAT_DOCUMENT_SCHEMA,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, addWhitespace(ATTRIBUTE_VALUE_1))),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema2(),
                updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatUpperCaseParameters()
    {
        // Create an initial version of the format using lower case input parameters.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toLowerCase(), FORMAT_DOCUMENT_SCHEMA.toLowerCase(),
                LATEST_VERSION_FLAG_SET, PARTITION_KEY.toLowerCase(), NO_PARTITION_KEY_GROUP,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())));

        // Create a new partition key group for the update request.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2.toLowerCase());

        // Perform an update by changing the format description and schema.
        Schema testSchema = businessObjectFormatServiceTestHelper.getTestSchema2();
        testSchema.setPartitionKeyGroup(testSchema.getPartitionKeyGroup().toUpperCase());
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2.toUpperCase(), FORMAT_DOCUMENT_SCHEMA_2.toUpperCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())), testSchema);
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService.updateBusinessObjectFormat(
            new BusinessObjectFormatKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        Schema expectedSchema = businessObjectFormatServiceTestHelper.getTestSchema2();
        expectedSchema.setPartitionKeyGroup(expectedSchema.getPartitionKeyGroup().toLowerCase());
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toLowerCase(),
                FORMAT_DESCRIPTION_2.toUpperCase(), FORMAT_DOCUMENT_SCHEMA_2.toUpperCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toUpperCase())), null, expectedSchema,
                updatedBusinessObjectFormat);
    }

    private void assertExceptionThrown(Runnable runnable, Class<? extends Exception> expectedExeceptionType, String expectedMessage)
    {
        try
        {
            runnable.run();
            Assert.fail("expected '" + expectedExeceptionType.getSimpleName() + "', but no exception was thrown.");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", expectedExeceptionType, e.getClass());
            Assert.assertEquals("thrown exception message", expectedMessage, e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatWithParentsNotExisting()
    {
        // Create relative database entities including a business object definition.
        businessObjectFormatServiceTestHelper
            .createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_FILE_TYPE_CODE, PARTITION_KEY_GROUP);

        List<BusinessObjectFormatKey> businessObjectFormatParents =
            Arrays.asList(new BusinessObjectFormatKey(NAMESPACE_2, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));

        // Perform a create without specifying optional parameters.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, NO_FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA);

        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
        }
        catch (IllegalArgumentException ex)
        {
            Assert.assertEquals(ex.getMessage(), "Parent business object format not found.");
        }
    }

    @Test
    public void testCreateBusinessObjectFormatWithParentsInValidParameters()
    {
        // Create relative database entities including a business object definition.
        businessObjectFormatServiceTestHelper
            .createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_FILE_TYPE_CODE, PARTITION_KEY_GROUP);

        List<BusinessObjectFormatKey> businessObjectFormatParents =
            Arrays.asList(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));

        // Perform a create without specifying optional parameters.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest("", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, NO_FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA);

        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
        }
        catch (IllegalArgumentException ex)
        {
            Assert.assertEquals(ex.getMessage(), "A namespace must be specified.");
        }

        businessObjectFormatParents = Arrays.asList(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1));

        request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, NO_FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA);

        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
        }
        catch (IllegalArgumentException ex)
        {
            Assert.assertEquals(ex.getMessage(), "Business object format version should be null.");
        }
    }

    @Test
    public void testCreateBusinessObjectFormatWithParents()
    {
        // Create relative database entities including a business object definition.
        businessObjectFormatServiceTestHelper
            .createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_FILE_TYPE_CODE, PARTITION_KEY_GROUP);

        businessObjectFormatServiceTestHelper
            .createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE_2, DATA_PROVIDER_NAME_2, BDEF_NAME_2, FORMAT_FILE_TYPE_CODE_2,
                PARTITION_KEY_GROUP_2);

        // parent business object format
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, NO_FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA);


        List<BusinessObjectFormatKey> businessObjectFormatParents =
            Arrays.asList(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));
        BusinessObjectFormatCreateRequest request2 = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, PARTITION_KEY,
                NO_FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_ATTRIBUTE_DEFINITIONS,
                NO_SCHEMA);


        BusinessObjectFormat resultBusinessObjectFormatV0 = businessObjectFormatService.createBusinessObjectFormat(request);
        BusinessObjectFormat resultBusinessObjectFormatChildV0 = businessObjectFormatService.createBusinessObjectFormat(request2);

        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);
        BusinessObjectFormatKey childBusinessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, null);

        //create parent, child relationship
        BusinessObjectFormatEntity formatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey);
        BusinessObjectFormatEntity childFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(childBusinessObjectFormatKey);
        childFormatEntity.getBusinessObjectFormatParents().add(formatEntity);
        formatEntity.getBusinessObjectFormatChildren().add(childFormatEntity);

        //create a new version of the child format
        BusinessObjectFormat resultBusinessObjectFormatChildV1 = businessObjectFormatService.createBusinessObjectFormat(request2);
        BusinessObjectFormat expectedBusinessObjectFormatChild = resultBusinessObjectFormatChildV0;
        expectedBusinessObjectFormatChild.setId(resultBusinessObjectFormatChildV1.getId());
        expectedBusinessObjectFormatChild.setBusinessObjectFormatVersion(resultBusinessObjectFormatChildV1.getBusinessObjectFormatVersion());
        expectedBusinessObjectFormatChild.setBusinessObjectFormatParents(resultBusinessObjectFormatChildV1.getBusinessObjectFormatParents());

        assertEquals(resultBusinessObjectFormatChildV1.getBusinessObjectFormatParents().size(), 1);
        assertEquals(expectedBusinessObjectFormatChild, resultBusinessObjectFormatChildV1);

        //create a new version of the parent
        BusinessObjectFormat resultBusinessObjectFormatParentV2 = businessObjectFormatService.createBusinessObjectFormat(request);
        assertEquals(resultBusinessObjectFormatParentV2.getBusinessObjectFormatChildren().size(), 1);
        BusinessObjectFormat expectedBusinessObjectParent = resultBusinessObjectFormatV0;
        expectedBusinessObjectParent.setId(resultBusinessObjectFormatParentV2.getId());
        expectedBusinessObjectParent.setBusinessObjectFormatVersion(resultBusinessObjectFormatParentV2.getBusinessObjectFormatVersion());
        expectedBusinessObjectParent.setBusinessObjectFormatChildren(resultBusinessObjectFormatParentV2.getBusinessObjectFormatChildren());
        assertEquals(expectedBusinessObjectParent, resultBusinessObjectFormatParentV2);
    }

    @Test
    public void testGetBusinessObjectFormatWithParents()
    {
        setupBusinessObjectFormatParentChild();
        BusinessObjectFormatKey businessObjectFormat =
            new BusinessObjectFormatKey(NAMESPACE + " ", BDEF_NAME.toLowerCase(), " " + FORMAT_USAGE_CODE, "  " + FORMAT_FILE_TYPE_CODE + " ", null);
        BusinessObjectFormatKey childBusinessObjectFormat = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, null);

        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.getBusinessObjectFormat(businessObjectFormat);
        BusinessObjectFormat resultChildBusinessObjectFormat = businessObjectFormatService.getBusinessObjectFormat(childBusinessObjectFormat);

        assertEquals(0, resultBusinessObjectFormat.getBusinessObjectFormatParents().size());
        assertEquals(1, resultChildBusinessObjectFormat.getBusinessObjectFormatParents().size());
        assertEquals(1, resultBusinessObjectFormat.getBusinessObjectFormatChildren().size());
    }

    @Test
    public void testUpdateBusinessObjectFormatParentsValidation()
    {
        List<BusinessObjectFormatKey> businessObjectFormatParents = null;

        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);
        BusinessObjectFormatParentsUpdateRequest request = null;

        try
        {
            businessObjectFormatService.updateBusinessObjectFormatParents(businessObjectFormatKey, request);
        }
        catch (IllegalArgumentException ex)
        {
            Assert.assertEquals("A Business Object Format Parents Update Request is required.", ex.getMessage());
        }

        businessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1);
        request = new BusinessObjectFormatParentsUpdateRequest();
        try
        {
            businessObjectFormatService.updateBusinessObjectFormatParents(businessObjectFormatKey, request);
        }
        catch (IllegalArgumentException ex)
        {
            Assert.assertEquals("Business object format version must not be specified.", ex.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatParents()
    {
        // Create relative database entities including a business object definition.
        setupBusinessObjectFormatParentChild();

        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);
        BusinessObjectFormatKey parentBusinessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, null);
        BusinessObjectFormatParentsUpdateRequest updateRequest = new BusinessObjectFormatParentsUpdateRequest();
        updateRequest.setBusinessObjectFormatParents(Arrays.asList(parentBusinessObjectFormatKey));

        BusinessObjectFormat format = businessObjectFormatService.getBusinessObjectFormat(businessObjectFormatKey);
        format.setBusinessObjectFormatParents(Arrays.asList(parentBusinessObjectFormatKey));
        BusinessObjectFormat resultFormat = businessObjectFormatService.updateBusinessObjectFormatParents(businessObjectFormatKey, updateRequest);

        Assert.assertEquals(format, resultFormat);
        //wipe out the parents
        businessObjectFormatService.getBusinessObjectFormat(businessObjectFormatKey);
        format.setBusinessObjectFormatParents(new ArrayList<>());
        updateRequest.setBusinessObjectFormatParents(new ArrayList<>());
        resultFormat = businessObjectFormatService.updateBusinessObjectFormatParents(businessObjectFormatKey, updateRequest);
        format.setBusinessObjectFormatParents(new ArrayList<>());
        Assert.assertEquals(format, resultFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatParentsDuplicateParents()
    {
        // Create relative database entities including a business object definition.
        setupBusinessObjectFormatParentChild();

        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create keys for business object format parents that are duplicates except for the case.
        List<BusinessObjectFormatKey> parentBusinessObjectFormatKeys = Arrays.asList(
            new BusinessObjectFormatKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE_2.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), NO_FORMAT_VERSION),
            new BusinessObjectFormatKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE_2.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), NO_FORMAT_VERSION));

        // Update parents for the business object format.
        BusinessObjectFormatParentsUpdateRequest businessObjectFormatParentsUpdateRequest = new BusinessObjectFormatParentsUpdateRequest();
        businessObjectFormatParentsUpdateRequest.setBusinessObjectFormatParents(parentBusinessObjectFormatKeys);
        BusinessObjectFormat resultBusinessObjectFormat =
            businessObjectFormatService.updateBusinessObjectFormatParents(businessObjectFormatKey, businessObjectFormatParentsUpdateRequest);

        // Validate the result. Only one business object format parent is expected to be listed.
        assertNotNull(resultBusinessObjectFormat);
        assertEquals(Arrays.asList(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION)),
            resultBusinessObjectFormat.getBusinessObjectFormatParents());
    }

    private void setupBusinessObjectFormatParentChild()
    {
        // Create relative database entities including a business object definition.
        businessObjectFormatServiceTestHelper
            .createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_FILE_TYPE_CODE, PARTITION_KEY_GROUP);

        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, NO_FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA);

        //create parent format
        BusinessObjectFormatKey businessObjectFormat = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);
        businessObjectFormatService.createBusinessObjectFormat(request);
        //List<BusinessObjectFormatKey> businessObjectFormatParents = Arrays.asList(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));
        BusinessObjectFormatCreateRequest childRequest = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, NO_FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA);

        //create child format
        businessObjectFormatService.createBusinessObjectFormat(childRequest);
        BusinessObjectFormatKey childAltBusinessObjectFormat =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, null);

        BusinessObjectFormatEntity formatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormat);
        BusinessObjectFormatEntity childFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(childAltBusinessObjectFormat);
        childFormatEntity.getBusinessObjectFormatParents().add(formatEntity);
        formatEntity.getBusinessObjectFormatChildren().add(childFormatEntity);
    }

    @Test
    public void testDeleteBusinessObjectFormatWithChildren()
    {
        setupBusinessObjectFormatParentChild();
        BusinessObjectFormatKey altBusinessObjectFormat = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0);
        BusinessObjectFormatEntity formatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(altBusinessObjectFormat);

        String errorMessage = String.format("Can not delete a business object format that has children associated with it. Business object format: {%s}",
            businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(formatEntity));

        try
        {
            businessObjectFormatService.deleteBusinessObjectFormat(altBusinessObjectFormat);
            fail("should not get here");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals(errorMessage, ex.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatWithGlobalAttributeMissing()
    {
        globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        try
        {
            // Create an initial version of a business object format.
            BusinessObjectFormat businessObjectFormat =
                businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(businessObjectDefinitionServiceTestHelper.getNewAttributes());
            fail("should throw exception before");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals(String.format("The business object format has a required attribute \"%s\" which was not specified or has a value which is blank.",
                GLOBAL_ATTRIBUTE_DEFINITON_NAME), ex.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatWithGlobalAttributes()
    {
        globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);
        //create one non-format level global attribute
        globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL + "_1", GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        attributes.add(new Attribute(GLOBAL_ATTRIBUTE_DEFINITON_NAME, "test attribute 1"));

        // Create an initial version of a business object format.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);
        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, attributes, businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectFormatServiceTestHelper.getTestSchema(), businessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatWithMissingGlobalAttributes()
    {
        globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);
        //create one non-format level global attribute
        globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL + "_1", GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        attributes.add(new Attribute(GLOBAL_ATTRIBUTE_DEFINITON_NAME, "test attribute 1"));

        // Create an initial version of a business object format.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);

        // Create a new partition key group for the update request.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        try
        {
            // Perform an update by changing the description and schema.
            BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
                .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2,
                    businessObjectDefinitionServiceTestHelper.getNewAttributes2(), businessObjectFormatServiceTestHelper.getTestSchema2());
            BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                    request);
            fail("should throw exception before");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals(String.format("The business object format has a required attribute \"%s\" which was not specified or has a value which is blank.",
                GLOBAL_ATTRIBUTE_DEFINITON_NAME), ex.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatWithMissingGlobalAttributesEmptyValue()
    {
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity = createGlobalAttributeDefinitionEntityWithAllowedAttributeValues();
        String invalidAttributeValue = "";
        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        attributes.add(new Attribute(GLOBAL_ATTRIBUTE_DEFINITON_NAME, invalidAttributeValue));

        try
        {
            // Create an initial version of a business object format.
            BusinessObjectFormat businessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);
            fail("Should throw exception before");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals(String.format("The business object format has a required attribute \"%s\" which was not specified or has a value which is blank.",
                GLOBAL_ATTRIBUTE_DEFINITON_NAME), ex.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatWithGlobalAttributes()
    {
        globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);
        //create one non-format level global attribute
        globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL + "_1", GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        attributes.add(new Attribute(GLOBAL_ATTRIBUTE_DEFINITON_NAME, "test attribute 1"));

        // Create an initial version of a business object format.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);

        // Create a new partition key group for the update request.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);


        List<Attribute> newAttributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        newAttributes.add(new Attribute(GLOBAL_ATTRIBUTE_DEFINITON_NAME, "test attribute 2"));

        // Perform an update by changing the description and schema.
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2, newAttributes,
                businessObjectFormatServiceTestHelper.getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2, newAttributes,
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema2(),
                updatedBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatWithGlobalAttributesAndAllowedAttributeValuesNegative()
    {
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity = createGlobalAttributeDefinitionEntityWithAllowedAttributeValues();
        String invalidAttributeValue = "test attribute 1";
        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        attributes.add(new Attribute(GLOBAL_ATTRIBUTE_DEFINITON_NAME, invalidAttributeValue));

        try
        {
            // Create an initial version of a business object format.
            BusinessObjectFormat businessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);
            fail("Should throw exception before");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals(String
                .format("The business object format attribute \"%s\" value \"%s\" is not from allowed attribute values.", GLOBAL_ATTRIBUTE_DEFINITON_NAME,
                    invalidAttributeValue), ex.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatWithGlobalAttributesAndAllowedAttributeValuesNegative2()
    {
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity = createGlobalAttributeDefinitionEntityWithAllowedAttributeValues();
        String invalidAttributeValue = ALLOWED_ATTRIBUTE_VALUE.toLowerCase();
        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        attributes.add(new Attribute(GLOBAL_ATTRIBUTE_DEFINITON_NAME, invalidAttributeValue));

        attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        attributes.add(new Attribute(GLOBAL_ATTRIBUTE_DEFINITON_NAME, invalidAttributeValue));
        // attribute value should be case sensitive
        try
        {
            // Create an initial version of a business object format.
            BusinessObjectFormat businessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);
            fail("Should throw exception before");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals(String
                .format("The business object format attribute \"%s\" value \"%s\" is not from allowed attribute values.", GLOBAL_ATTRIBUTE_DEFINITON_NAME,
                    ALLOWED_ATTRIBUTE_VALUE.toLowerCase()), ex.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatWithGlobalAttributesAndAllowedAttributeValues()
    {
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity = createGlobalAttributeDefinitionEntityWithAllowedAttributeValues();
        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        attributes.add(new Attribute(GLOBAL_ATTRIBUTE_DEFINITON_NAME.toLowerCase(), ALLOWED_ATTRIBUTE_VALUE_2));

        // Create an initial version of a business object format.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);
        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
                FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, attributes, businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectFormatServiceTestHelper.getTestSchema(), businessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatAttributes()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat =
            businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(businessObjectDefinitionServiceTestHelper.getNewAttributes());

        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes2();
        BusinessObjectFormatAttributesUpdateRequest request = new BusinessObjectFormatAttributesUpdateRequest(attributes);

        // Perform an update by changing the description and schema.
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService.updateBusinessObjectFormatAttributes(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, attributes,
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema(),
                updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatAttributeDefinitions()
    {
        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        // Create an initial version of a business object format with format description and schema information.
        // Attributes are passed rather attribute definations as this method also set attribute definition by default and no need to create another method
        BusinessObjectFormat originalBusinessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);


        List<AttributeDefinition> attributeDefinitions = businessObjectFormatServiceTestHelper.getTestAttributeDefinitions2();
        BusinessObjectFormatAttributeDefinitionsUpdateRequest request = new BusinessObjectFormatAttributeDefinitionsUpdateRequest(attributeDefinitions);

        // Perform an update by changing the description and schema.
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService.updateBusinessObjectFormatAttributeDefinitions(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, attributes, attributeDefinitions,
                businessObjectFormatServiceTestHelper.getTestSchema(), updatedBusinessObjectFormat);

    }

    @Test
    public void testUpdateBusinessObjectFormatAttributeDefinitionsMissingRequiredParameters()
    {
        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        BusinessObjectFormat originalBusinessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);

        // Null as required parameter
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(null, AbstractServiceTest.NO_PUBLISH_ATTRIBUTE));
        // Perform an update by changing the attribute definition to null.
        BusinessObjectFormatAttributeDefinitionsUpdateRequest request = new BusinessObjectFormatAttributeDefinitionsUpdateRequest(attributeDefinitions);

        try
        {
            businessObjectFormatService.updateBusinessObjectFormatAttributeDefinitions(
                new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals(String.format("An attribute definition name must be specified."), ex.getMessage());
        }

        // Blank string as required parameter
        attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition("", AbstractServiceTest.NO_PUBLISH_ATTRIBUTE));
        // Perform an update by changing the attribute definition to null.
        request = new BusinessObjectFormatAttributeDefinitionsUpdateRequest(attributeDefinitions);

        try
        {
            businessObjectFormatService.updateBusinessObjectFormatAttributeDefinitions(
                new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals(String.format("An attribute definition name must be specified."), ex.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatAttributeDefinitionsNullAsParameters()
    {
        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        BusinessObjectFormat originalBusinessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);

        // null check for attribute definitions value
        // Perform an update by changing the attribute definition to null.
        BusinessObjectFormatAttributeDefinitionsUpdateRequest request = new BusinessObjectFormatAttributeDefinitionsUpdateRequest(null);

        try
        {
            businessObjectFormatService.updateBusinessObjectFormatAttributeDefinitions(
                new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals(String.format("A business object format attribute definitions list is required."), ex.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatAttributeDefinitionsMissingOptionalParameters()
    {
        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        BusinessObjectFormat originalBusinessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);

        // Passing null as optional parameter
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, null));
        // Perform an update by changing the attribute definition with an empty list.
        BusinessObjectFormatAttributeDefinitionsUpdateRequest request = new BusinessObjectFormatAttributeDefinitionsUpdateRequest(attributeDefinitions);

        try
        {
            businessObjectFormatService.updateBusinessObjectFormatAttributeDefinitions(
                new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (NullPointerException ex)
        {
            assertEquals(null, ex.getMessage());
        }

    }

    @Test
    public void testUpdateBusinessObjectFormatAttributeDefinitionsTrim()
    {
        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        BusinessObjectFormat originalBusinessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);

        // Trim should work on required parameter
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        List<AttributeDefinition> validateAttributeDefinitions = new ArrayList<>();
        validateAttributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE));
        attributeDefinitions.add(new AttributeDefinition(" " + AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE + " ", AbstractServiceTest.PUBLISH_ATTRIBUTE));

        // Perform an update by changing the attribute definition to null.
        BusinessObjectFormatAttributeDefinitionsUpdateRequest request = new BusinessObjectFormatAttributeDefinitionsUpdateRequest(attributeDefinitions);

        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService.updateBusinessObjectFormatAttributeDefinitions(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, attributes,
                validateAttributeDefinitions, businessObjectFormatServiceTestHelper.getTestSchema(), updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatAttributeDefinitionsUpperAndLowerCase()
    {
        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        BusinessObjectFormat originalBusinessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);

        // Upper case and lower case
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        List<AttributeDefinition> validateAttributeDefinitions = new ArrayList<>();
        validateAttributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE));
        attributeDefinitions.add(new AttributeDefinition(" " + AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE + " ", AbstractServiceTest.PUBLISH_ATTRIBUTE));
        // Perform an update by changing the attribute definition to null.
        BusinessObjectFormatAttributeDefinitionsUpdateRequest request = new BusinessObjectFormatAttributeDefinitionsUpdateRequest(attributeDefinitions);

        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService.updateBusinessObjectFormatAttributeDefinitions(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, attributes,
                validateAttributeDefinitions, businessObjectFormatServiceTestHelper.getTestSchema(), updatedBusinessObjectFormat);

        // upper case and lower case
        attributeDefinitions = new ArrayList<>();
        // List<AttributeDefinition> attributeDefinitions = businessObjectFormatServiceTestHelper.getTestAttributeDefinitions2();
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), AbstractServiceTest.PUBLISH_ATTRIBUTE));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), AbstractServiceTest.PUBLISH_ATTRIBUTE));
        // Perform an update by changing the attribute definition with an empty list.
        request = new BusinessObjectFormatAttributeDefinitionsUpdateRequest(attributeDefinitions);

        try
        {
            businessObjectFormatService.updateBusinessObjectFormatAttributeDefinitions(
                new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals(String.format("Duplicate attribute definition name \"%s\" found.", ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase()), ex.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatAttributeDefinitionsDuplicateAttributeDefinitions()
    {
        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes();
        BusinessObjectFormat originalBusinessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat(attributes);

        List<AttributeDefinition> attributeDefinitions = businessObjectFormatServiceTestHelper.getTestAttributeDefinitions2();
        // Check for the duplicate attribute definition.
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.NO_PUBLISH_ATTRIBUTE));
        BusinessObjectFormatAttributeDefinitionsUpdateRequest request = new BusinessObjectFormatAttributeDefinitionsUpdateRequest(attributeDefinitions);
        try
        {
            businessObjectFormatService.updateBusinessObjectFormatAttributeDefinitions(
                new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals(String.format("Duplicate attribute definition name \"%s\" found.", ATTRIBUTE_NAME_1_MIXED_CASE), ex.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatAttributeDefinitionsBusinessObjectFormatNoExists()
    {
        BusinessObjectFormatAttributeDefinitionsUpdateRequest request =
            new BusinessObjectFormatAttributeDefinitionsUpdateRequest(businessObjectFormatServiceTestHelper.getTestAttributeDefinitions2());
        try
        {
            businessObjectFormatService.updateBusinessObjectFormatAttributeDefinitions(
                new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);
            fail("Business object format does not exist: No update con be performed if the format does not exists.");
        }
        catch (ObjectNotFoundException ex)
        {
            assertEquals(String.format(
                "Business object format with namespace \"%s\", business object definition name \"%s\", format usage \"%s\", format file type \"%s\", and format version \"%s\" doesn't exist.",
                NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), ex.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatWithAllowNonBackwardsCompatibleChangesSet()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of a business object format with a schema.
        BusinessObjectFormat businessObjectFormatV0 = businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema()));

        // Validate allowNonBackwardsCompatibleChanges is not set on businessObjectFormatV0
        assertNull(businessObjectFormatV0.isAllowNonBackwardsCompatibleChanges());

        // Create business object format key for format V0.
        BusinessObjectFormatKey businessObjectFormatKeyV0 =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Create a business object format create request with non-backwards compatible schema changes, schema that has a different null value.
        Schema nonBackwardsCompatibleSchema = businessObjectFormatServiceTestHelper.getTestSchema();
        nonBackwardsCompatibleSchema.setNullValue(SCHEMA_NULL_VALUE_NULL_WORD);
        BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), nonBackwardsCompatibleSchema);

        // Try to create a second version of the business object format with non-backwards compatible schema changes.
        // We expected to fail as allowNonBackwardsCompatibleChanges is not set.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatCreateRequest);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "New format version null value does not match to the previous format version null value.", e.getMessage());
        }

        // Get the initial format version of the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntityV0 = businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKeyV0);
        assertNotNull(businessObjectFormatEntityV0);

        // Update businessObjectFormatEntityV0 to allowNonBackwardsCompatibleChanges to false.
        businessObjectFormatEntityV0.setAllowNonBackwardsCompatibleChanges(false);
        assertFalse(businessObjectFormatEntityV0.isAllowNonBackwardsCompatibleChanges());

        // Try to create a second version of the business object format with non-backwards compatible schema changes..
        // We expected to fail as allowNonBackwardsCompatibleChanges is set to false.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatCreateRequest);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "New format version null value does not match to the previous format version null value.", e.getMessage());
        }

        // Get the initial format version and validate that the allowNonBackwardsCompatibleChanges is set to false.
        businessObjectFormatV0 = businessObjectFormatService.getBusinessObjectFormat(businessObjectFormatKeyV0);
        assertFalse(businessObjectFormatV0.isAllowNonBackwardsCompatibleChanges());

        // Update businessObjectFormatEntityV0 to allowNonBackwardsCompatibleChanges to true.
        businessObjectFormatEntityV0.setAllowNonBackwardsCompatibleChanges(true);
        assertTrue(businessObjectFormatEntityV0.isAllowNonBackwardsCompatibleChanges());

        // Call create business object format when allowNonBackwardsCompatibleChanges is set to true.
        BusinessObjectFormat businessObjectFormatV1 = businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatCreateRequest);

        // Validate businessObjectFormatV1 and businessObjectFormatEntityV0.
        assertNotNull(businessObjectFormatV1);
        assertEquals(true, businessObjectFormatV1.isAllowNonBackwardsCompatibleChanges());
        assertEquals(SECOND_FORMAT_VERSION, Integer.valueOf(businessObjectFormatV1.getBusinessObjectFormatVersion()));
        assertNull(businessObjectFormatEntityV0.isAllowNonBackwardsCompatibleChanges());

        // Get the initial format version and validate that the allowNonBackwardsCompatibleChanges is set to true.
        businessObjectFormatV0 = businessObjectFormatService.getBusinessObjectFormat(businessObjectFormatKeyV0);
        assertTrue(businessObjectFormatV0.isAllowNonBackwardsCompatibleChanges());

        // Delete the second version of business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService
            .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION));
        assertNotNull(deletedBusinessObjectFormat);
        assertTrue(deletedBusinessObjectFormat.isAllowNonBackwardsCompatibleChanges());

        // Get the initial format version of the business object format entity.
        businessObjectFormatEntityV0 = businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKeyV0);

        // Validate the schema compatibility changes got propagated to the latest format version.
        assertNotNull(businessObjectFormatEntityV0);
        assertEquals(true, businessObjectFormatEntityV0.isAllowNonBackwardsCompatibleChanges());
        assertEquals(INITIAL_FORMAT_VERSION, Integer.valueOf(businessObjectFormatEntityV0.getBusinessObjectFormatVersion()));

        // Validate initial version business object format entity.
        assertTrue(businessObjectFormatEntityV0.isAllowNonBackwardsCompatibleChanges());
    }

    private GlobalAttributeDefinitionEntity createGlobalAttributeDefinitionEntityWithAllowedAttributeValues()
    {
        // Create attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);
        List<String> allowedAttributeValueList = Arrays.asList(ALLOWED_ATTRIBUTE_VALUE, ALLOWED_ATTRIBUTE_VALUE_2);

        // Create and persist a attribute value list key entity.
        AttributeValueListEntity attributeValueListEntity =
            attributeValueListDaoTestHelper.createAttributeValueListEntity(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create and persist a list of allowed attribute values.
        List<AllowedAttributeValueEntity> allowedAttributeValueEntities =
            allowedAttributeValueDaoTestHelper.createAllowedAttributeValueEntities(attributeValueListKey, allowedAttributeValueList);
        attributeValueListEntity.getAllowedAttributeValues().addAll(allowedAttributeValueEntities);

        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);
        globalAttributeDefinitionEntity.setAttributeValueList(attributeValueListEntity);

        return globalAttributeDefinitionEntity;
    }
}

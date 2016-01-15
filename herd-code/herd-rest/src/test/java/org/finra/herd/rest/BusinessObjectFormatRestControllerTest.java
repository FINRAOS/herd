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
package org.finra.herd.rest;

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

import javax.persistence.PersistenceException;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.junit.Test;

import org.finra.herd.core.Command;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.AttributeDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdl;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKeys;
import org.finra.herd.model.api.xml.BusinessObjectFormatUpdateRequest;
import org.finra.herd.model.api.xml.Schema;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.helper.Hive13DdlGenerator;

/**
 * This class tests various functionality within the business object format REST controller.
 */
public class BusinessObjectFormatRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateBusinessObjectFormat()
    {
        // Create an initial version of a business object format.
        BusinessObjectFormat businessObjectFormat = createTestBusinessObjectFormat();

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), getTestSchema(), businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatMissingRequiredParameters()
    {
        // Try to perform a create without specifying business object definition name parameter.
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getTestAttributeDefinitions(), getTestSchema()));
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to perform a create without specifying format usage parameter.
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getTestAttributeDefinitions(), getTestSchema()));
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to perform a create without specifying format file type parameter.
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getTestAttributeDefinitions(), getTestSchema()));
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to perform a create without specifying format partition key parameter.
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, BLANK_TEXT, FORMAT_DESCRIPTION,
                    getTestAttributeDefinitions(), getTestSchema()));
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format partition key must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatMissingOptionalParameters()
    {
        // Create relative database entities including a business object definition.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE_CD, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_FILE_TYPE_CODE, PARTITION_KEY_GROUP);

        // Perform a create without specifying optional parameters.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, "",
                getTestAttributeDefinitions(), getTestSchema());
        request.setAttributeDefinitions(null);
        request.setSchema(null);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, true, PARTITION_KEY, null,
            null, null, resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoPartitionKeyGroupSpecified()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a version of business object format without specifying a partition key group (using both blank text and a null value).
        Integer expectedBusinessObjectFormatVersion = INITIAL_FORMAT_VERSION;
        for (String partitionKeyGroupName : Arrays.asList(BLANK_TEXT, null))
        {
            BusinessObjectFormatCreateRequest request =
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getTestAttributeDefinitions(), getTestSchema());
            request.getSchema().setPartitionKeyGroup(partitionKeyGroupName);
            BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

            // Validate the returned object.
            Schema expectedSchema = getTestSchema();
            expectedSchema.setPartitionKeyGroup(null);
            validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, expectedBusinessObjectFormatVersion++, true,
                PARTITION_KEY, FORMAT_DESCRIPTION, getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
        }
    }

    @Test
    public void testCreateBusinessObjectFormatTrimParameters()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a first version of the format using input parameters with leading and trailing empty spaces.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), addWhitespace(PARTITION_KEY), addWhitespace(FORMAT_DESCRIPTION), getTestAttributeDefinitions(),
                addWhitespace(getTestSchema()));
        request.getSchema().setPartitionKeyGroup(addWhitespace(PARTITION_KEY_GROUP));

        // Create an initial business object format version.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, true, PARTITION_KEY,
            addWhitespace(FORMAT_DESCRIPTION), getTestAttributeDefinitions(), getTestSchema(), businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatUpperCaseParameters()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE_CD.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(), BOD_NAME.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), PARTITION_KEY_GROUP.toLowerCase());

        // Create a first version of the format using upper case input parameters.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION.toUpperCase(), getTestAttributeDefinitions(),
                getTestSchema());
        request.getSchema().setPartitionKeyGroup(PARTITION_KEY_GROUP.toUpperCase());
        // For he first schema partition column name, use an opposite case from the format partition key was specified in.
        request.getSchema().getPartitions().get(0).setName(PARTITION_KEY.toLowerCase());
        BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setPartitionKeyGroup(PARTITION_KEY_GROUP.toLowerCase());
        expectedSchema.getPartitions().get(0).setName(PARTITION_KEY.toLowerCase());
        validateBusinessObjectFormat(null, NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), 0, true, PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION.toUpperCase(), getTestAttributeDefinitions(),
            expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatLowerCaseParameters()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE_CD.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(), BOD_NAME.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), PARTITION_KEY_GROUP.toUpperCase());

        // Create a first version of the format using lower case input parameters.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), PARTITION_KEY.toLowerCase(), FORMAT_DESCRIPTION.toLowerCase(), getTestAttributeDefinitions(),
                getTestSchema());
        request.getSchema().setPartitionKeyGroup(PARTITION_KEY_GROUP.toLowerCase());
        // For the first schema partition column name, use an opposite case from the format partition key was specified in.
        request.getSchema().getPartitions().get(0).setName(PARTITION_KEY.toUpperCase());
        BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setPartitionKeyGroup(PARTITION_KEY_GROUP.toUpperCase());
        expectedSchema.getPartitions().get(0).setName(PARTITION_KEY.toUpperCase());
        validateBusinessObjectFormat(null, NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), 0, true, PARTITION_KEY.toLowerCase(), FORMAT_DESCRIPTION.toLowerCase(), getTestAttributeDefinitions(),
            expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInvalidParameters()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Validate that we can create the first version of the format.
        BusinessObjectFormatCreateRequest request;

        // Try to perform a create using invalid namespace code.
        request =
            createBusinessObjectFormatCreateRequest("I_DO_NOT_EXIST", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(request);
            fail("Should throw an ObjectNotFoundException when non-existing namespace code is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String
                .format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", request.getBusinessObjectDefinitionName(),
                    request.getNamespace()), e.getMessage());
        }

        // Try to perform a create using invalid business object definition name.
        request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(request);
            fail("Should throw an ObjectNotFoundException when non-existing business object definition name is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String
                .format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", request.getBusinessObjectDefinitionName(),
                    request.getNamespace()), e.getMessage());
        }

        // Try to perform a create using invalid format file type.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), getTestSchema());
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(request);
            fail("Should throw an ObjectNotFoundException when non-existing business object format file type code is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("File type with code \"%s\" doesn't exist.", request.getBusinessObjectFormatFileType()), e.getMessage());
        }

        // Try to perform a create using invalid partition key group.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), getTestSchema());
        request.getSchema().setPartitionKeyGroup("I_DO_NOT_EXIST");
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(request);
            fail("Should throw an ObjectNotFoundException when non-existing partition key group is specified.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Partition key group \"%s\" doesn't exist.", request.getSchema().getPartitionKeyGroup()), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatCreateSecondVersion()
    {
        // Create an initial version of a business object format.
        createTestBusinessObjectFormat();

        // Create a second version of the business object format.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, true, PARTITION_KEY,
            FORMAT_DESCRIPTION, getTestAttributeDefinitions(), getTestSchema(), resultBusinessObjectFormat);

        // Check if we now have only one latest format version - our first version is not marked as latest anymore.
        // Please note that we do not have to validate if the first format version is not marked as "latest" now, since having more that one
        // format versions with the latestVersion flag set to Yes produces exception in herdDao.getBusinessObjectFormatByAltKey() method.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            herdDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));
        assertEquals(SECOND_FORMAT_VERSION, businessObjectFormatEntity.getBusinessObjectFormatVersion());
        assertEquals(true, businessObjectFormatEntity.getLatestVersion());
    }

    @Test
    public void testCreateBusinessObjectFormatCreateSecondVersionNoPartitioningColumns()
    {
        // Create and persist an initial version of a business object format with schema without any partitioning columns.
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
            SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(RANDOM_SUFFIX), null);

        // Create a second version of the business object format without partitioning columns.
        Schema testSchema = getTestSchema();
        testSchema.setPartitions(null);
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), testSchema);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setPartitions(null);
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, true, PARTITION_KEY,
            FORMAT_DESCRIPTION, getTestAttributeDefinitions(), expectedSchema, resultBusinessObjectFormat);

        // Check if we now have only one latest format version - our first version is not marked as latest anymore.
        // Please note that we do not have to validate if the first format version is not marked as "latest" now, since having more that one
        // format versions with the latestVersion flag set to Yes produces exception in herdDao.getBusinessObjectFormatByAltKey() method.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            herdDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));
        assertEquals(SECOND_FORMAT_VERSION, businessObjectFormatEntity.getBusinessObjectFormatVersion());
        assertEquals(true, businessObjectFormatEntity.getLatestVersion());
    }

    @Test
    public void testCreateBusinessObjectFormatIncorrectLatestVersion() throws Exception
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a first version of the format with the latest flag set to false.
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, false,
            PARTITION_KEY);

        try
        {
            // Try to create a new format version for this format.
            final BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest =
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getTestAttributeDefinitions(), getTestSchema());
            executeWithoutLogging(SqlExceptionHelper.class, new Command()
            {
                @Override
                public void execute()
                {
                    businessObjectFormatRestController.createBusinessObjectFormat(businessObjectFormatCreateRequest);
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
    public void testCreateBusinessObjectFormatDuplicateAttributeDefinitions()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

        // Add in duplicate attribute definition.
        AttributeDefinition attributeDefinition = new AttributeDefinition();
        attributeDefinition.setName(ATTRIBUTE_NAME_1_MIXED_CASE);
        request.getAttributeDefinitions().add(attributeDefinition);

        // Create the business object format which is invalid since duplicate attribute definitions are present.
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(request);
            fail("Expecting an Illegal Argument Exception to be thrown which wasn't since duplicate attribute definitions are present.");
        }
        catch (IllegalArgumentException ex)
        {
            // We are expecting this exception so do nothing.
        }
    }

    @Test
    public void testCreateBusinessObjectFormatSchemaWhitespaceNullValueAndDelimiter()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of business object format with schema null value and delimiter as a whitespace character.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());
        request.getSchema().setNullValue(" ");
        request.getSchema().setDelimiter(" ");
        BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setNullValue(" ");
        expectedSchema.setDelimiter(" ");
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaNullValue()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format with a non-specified schema null value.
        request.getSchema().setNullValue(null);
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when schema null value is null.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A schema null value can not be null.", e.getMessage());
        }

        // Try to create a business object format with an empty schema null value which is valid.
        request.getSchema().setNullValue("");
        BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();

        // The escape characters gets trimmed which gets stored as the empty string which gets returned as null.
        expectedSchema.setNullValue(null);
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaDelimiter()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format with an empty schema escape character which is valid.
        request.getSchema().setDelimiter(null);
        BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();

        // The escape characters gets trimmed which gets stored as the empty string which gets returned as null.
        expectedSchema.setDelimiter(null);
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaEscapeCharacter()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format with an empty schema escape character which is valid.
        request.getSchema().setEscapeCharacter(" ");
        BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();

        expectedSchema.setEscapeCharacter(" ");
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNullSchemaEscapeCharacter()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format with an empty schema escape character which is valid.
        request.getSchema().setEscapeCharacter(null);
        BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();

        // The escape characters gets trimmed which gets stored as the empty string which gets returned as null.
        expectedSchema.setEscapeCharacter(null);
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatUnprintableSchemaEscapeCharacter()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of business object format with a schema having an unprintable escape character.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());
        request.getSchema().setEscapeCharacter(String.valueOf((char) 1));
        BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setEscapeCharacter(String.valueOf((char) 1));
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaColumns()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format with a schema without columns.
        request.getSchema().setColumns(null);
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when schema has no columns.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A schema must have at least one column.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaColumnName()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format with an empty column name.
        SchemaColumn schemaColumn = new SchemaColumn();
        request.getSchema().getColumns().add(schemaColumn);
        schemaColumn.setName(BLANK_TEXT);
        schemaColumn.setType("TYPE");
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(request);
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
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format with an empty column data type.
        SchemaColumn schemaColumn = new SchemaColumn();
        request.getSchema().getColumns().add(schemaColumn);
        schemaColumn.setName("COLUMN_NAME");
        schemaColumn.setType(BLANK_TEXT);
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when schema has an empty column data type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A schema column data type must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatDuplicateColumns()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

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
            businessObjectFormatRestController.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when duplicate column names are present.");
        }
        catch (IllegalArgumentException e)
        {
            String expectedErrorMessage = String.format("Duplicate schema column name \"DUPLICATE_COLUMN_NAME\" found.");
            assertEquals(expectedErrorMessage, e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaPartitions()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request without schema partition columns.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());
        request.getSchema().setPartitions(null);

        // Create an initial version of business object format without schema partitions.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setPartitions(null);
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaPartitionColumnName()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format with an empty partition column name.
        SchemaColumn partitionColumn = new SchemaColumn();
        request.getSchema().getPartitions().add(partitionColumn);
        partitionColumn.setName(BLANK_TEXT);
        partitionColumn.setType("TYPE");
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(request);
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
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format with an empty column data type.
        SchemaColumn partitionColumn = new SchemaColumn();
        request.getSchema().getPartitions().add(partitionColumn);
        partitionColumn.setName("COLUMN_NAME");
        partitionColumn.setType(BLANK_TEXT);
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when schema has an empty partition column data type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A schema column data type must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatDuplicatePartitionColumns()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

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
            businessObjectFormatRestController.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when duplicate partition column names are present.");
        }
        catch (IllegalArgumentException e)
        {
            String expectedErrorMessage = String.format("Duplicate schema column name \"DUPLICATE_COLUMN_NAME\" found.");
            assertEquals(expectedErrorMessage, e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatPartitionAndRegularColumnsHaveConflictingValues()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format when partition column has a conflict value with the corresponding regular schema column.
        SchemaColumn schemaColumn = request.getSchema().getColumns().get(0);
        request.getSchema().getPartitions().get(0).setName(schemaColumn.getName());
        request.getSchema().getPartitions().get(0).setType("DIFFERENT_DATA_TYPE");
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when partition column has a conflict value with the corresponding regular schema column.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Schema data and partition column configurations with name \"%s\" have conflicting values. " +
                "All column values are case sensitive and must be identical.", schemaColumn.getName()), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatTopPartitionColumnNameNoMatchPartitionKey()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format with the first schema partition column name does not matching the partition key.
        request.getSchema().getPartitions().get(0).setName("NOT_A_PARTITION_KEY");
        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when the first schema partition column name does not match the partition key.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Partition key \"%s\" does not match the first schema partition column name \"%s\".", PARTITION_KEY,
                request.getSchema().getPartitions().get(0).getName()), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithoutSchema()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format without a schema.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), null);
        businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Create a second version of the business object format with a schema.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), getTestSchema());
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, true, PARTITION_KEY,
            FORMAT_DESCRIPTION, getTestAttributeDefinitions(), getTestSchema(), resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaNoSchemaChanges()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format with a schema.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());
        businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Create a second version of the business object format with a schema that is identical to the initial version schema.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), getTestSchema());
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, true, PARTITION_KEY,
            FORMAT_DESCRIPTION, getTestAttributeDefinitions(), getTestSchema(), resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaHavingNullRowFormatValuesNoSchemaChanges()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format with a schema having null row format values.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());
        request.getSchema().setNullValue("");   // This is a required parameter, so it cannot be set to null.
        request.getSchema().setDelimiter(null);
        request.getSchema().setEscapeCharacter(null);
        businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Create a second version of the business object format with a schema that is identical to the initial version schema.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), getTestSchema());
        request.getSchema().setNullValue("");   // This is a required parameter, so it cannot be set to null.
        request.getSchema().setDelimiter(null);
        request.getSchema().setEscapeCharacter(null);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        // Please note that the escape character which was passed as an empty string gets returned as null.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setNullValue(null);
        expectedSchema.setDelimiter(null);
        expectedSchema.setEscapeCharacter(null);
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, true, PARTITION_KEY,
            FORMAT_DESCRIPTION, getTestAttributeDefinitions(), expectedSchema, resultBusinessObjectFormat);

        // Create a third version of the business object format with a schema that is identical to the initial version schema,
        // except that we now pass empty string values for all three row format parameters.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), getTestSchema());
        request.getSchema().setNullValue("");
        request.getSchema().setDelimiter("");
        request.getSchema().setEscapeCharacter("");
        resultBusinessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION, true, PARTITION_KEY,
            FORMAT_DESCRIPTION, getTestAttributeDefinitions(), expectedSchema, resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaAdditiveSchemaChanges()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format with a schema.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());
        businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Create a second version of the business object format with a new schema that is additive to the previous format version schema.
        Schema newFormatVersionSchema = getTestSchema();
        SchemaColumn newSchemaColumn = new SchemaColumn();
        newFormatVersionSchema.getColumns().add(newSchemaColumn);
        newSchemaColumn.setName("NEW_COLUMN");
        newSchemaColumn.setType("TYPE");
        request = createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), newFormatVersionSchema);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, true, PARTITION_KEY,
            FORMAT_DESCRIPTION, getTestAttributeDefinitions(), newFormatVersionSchema, resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaNonAdditiveSchemaChanges()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format with a schema.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getTestAttributeDefinitions(), getTestSchema());
        businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Try to create a second version of the business object format with a new schema that is not additive to the previous format version schema.
        Schema newFormatVersionSchema;

        try
        {
            businessObjectFormatRestController.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getTestAttributeDefinitions(), null));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. New format version schema is not specified.",
                e.getMessage());
        }

        try
        {
            newFormatVersionSchema = getTestSchema();
            newFormatVersionSchema.setNullValue(SCHEMA_NULL_VALUE_NULL_WORD);
            businessObjectFormatRestController.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getTestAttributeDefinitions(), newFormatVersionSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "New format version null value does not match to the previous format version null value.", e.getMessage());
        }

        try
        {
            newFormatVersionSchema = getTestSchema();
            newFormatVersionSchema.setDelimiter(SCHEMA_DELIMITER_COMMA);
            businessObjectFormatRestController.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getTestAttributeDefinitions(), newFormatVersionSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "New format version delimiter character does not match to the previous format version delimiter character.", e.getMessage());
        }

        try
        {
            newFormatVersionSchema = getTestSchema();
            newFormatVersionSchema.setEscapeCharacter(SCHEMA_ESCAPE_CHARACTER_TILDE);
            businessObjectFormatRestController.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getTestAttributeDefinitions(), newFormatVersionSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "New format version escape character does not match to the previous format version escape character.", e.getMessage());
        }

        try
        {
            newFormatVersionSchema = getTestSchema();
            newFormatVersionSchema.setPartitions(getTestSchema2().getPartitions());
            businessObjectFormatRestController.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getTestAttributeDefinitions(), newFormatVersionSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "New format version partition columns do not match to the previous format version partition columns.", e.getMessage());
        }

        try
        {
            newFormatVersionSchema = getTestSchema();
            newFormatVersionSchema.setColumns(getTestSchema2().getColumns());
            businessObjectFormatRestController.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getTestAttributeDefinitions(), newFormatVersionSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "Changes detected to the previously defined regular (non-partitioning) columns.", e.getMessage());
        }
    }

    // Tests for Update Business Object Format

    @Test
    public void testUpdateBusinessObjectFormat()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat();

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and schema.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION_2, getTestAttributeDefinitions(), getTestSchema2(), updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatMissingRequiredParameter()
    {
        // Create a business object format update request.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, getTestSchema2());

        // Try to perform an update without specifying business object definition name.
        try
        {
            businessObjectFormatRestController
                .updateBusinessObjectFormat(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);
            fail("Should throw an IllegalArgumentException when business object definition is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to perform an update without specifying business object format usage.
        try
        {
            businessObjectFormatRestController
                .updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to perform an update without specifying business object format file type.
        try
        {
            businessObjectFormatRestController
                .updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, INITIAL_FORMAT_VERSION, request);
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to perform an update without specifying business object format version.
        try
        {
            businessObjectFormatRestController.updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, request);
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatMissingOptionalParameters()
    {
        // Create and persist a business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION);

        // Create and persist a valid business object format without description and with schema without any partitioning columns.
        BusinessObjectFormatEntity originalBusinessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY, PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_COMMA, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), null);

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and schema and without specifying a namespace code.
        Schema testSchema2 = getTestSchema2();
        testSchema2.setPartitions(null);
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, testSchema2);
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION_2, new ArrayList<AttributeDefinition>(), testSchema2, updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatRemoveFormatDescription()
    {
        // Create an initial version of a business object format with a description and a schema.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat();

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update to set the format description to null.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(null, getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, null, getTestAttributeDefinitions(), getTestSchema2(), updatedBusinessObjectFormat);

        // Perform a second update this time using an empty string description parameter.
        request.setDescription("");
        updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, null, getTestAttributeDefinitions(), getTestSchema2(), updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatRemoveFormatSchema()
    {
        // Create an initial version of a business object format with a description and a schema.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat();

        // Perform an update to remove the business object format schema.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, null);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION_2, getTestAttributeDefinitions(), null, resultBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatTrimParameters()
    {
        // Create an initial version of a business object format.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat();

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and using input parameters with leading and trailing empty spaces.
        BusinessObjectFormatUpdateRequest request =
            createBusinessObjectFormatUpdateRequest(addWhitespace(FORMAT_DESCRIPTION_2), addWhitespace(getTestSchema2()));
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, addWhitespace(FORMAT_DESCRIPTION_2), getTestAttributeDefinitions(), getTestSchema2(),
            updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatUpperCaseParameters()
    {
        // Create an initial version of the format using lower case input parameters.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toLowerCase(), true, PARTITION_KEY.toLowerCase());

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2.toLowerCase());

        // Perform an update by changing the format description and schema.
        Schema testSchema = getTestSchema2();
        testSchema.setPartitionKeyGroup(testSchema.getPartitionKeyGroup().toUpperCase());
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2.toUpperCase(), testSchema);
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema2();
        expectedSchema.setPartitionKeyGroup(expectedSchema.getPartitionKeyGroup().toLowerCase());
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, true, PARTITION_KEY.toLowerCase(), FORMAT_DESCRIPTION_2.toUpperCase(), null,
            expectedSchema, updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatLowerCaseParameters()
    {
        // Create an initial version of the format using upper case input parameters.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true, PARTITION_KEY.toUpperCase());

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2.toUpperCase());

        // Perform an update by changing the format description and schema.
        Schema testSchema = getTestSchema2();
        testSchema.setPartitionKeyGroup(testSchema.getPartitionKeyGroup().toLowerCase());
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2.toLowerCase(), getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema2();
        expectedSchema.setPartitionKeyGroup(expectedSchema.getPartitionKeyGroup().toUpperCase());
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, true, PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION_2.toLowerCase(), null,
            expectedSchema, updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatInvalidParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Create a business object format update request.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, getTestSchema2());

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update on our business object format.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Validate the results.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, Boolean.TRUE, PARTITION_KEY, FORMAT_DESCRIPTION_2, null, getTestSchema2(), businessObjectFormat);

        // Try to perform an update using invalid namespace code.
        try
        {
            businessObjectFormatRestController
                .updateBusinessObjectFormat("I_DO_NOT_EXIST", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION), e.getMessage());
        }

        // Try to perform an update using invalid business object definition name.
        try
        {
            businessObjectFormatRestController
                .updateBusinessObjectFormat(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION), e.getMessage());
        }

        // Try to perform a get using invalid business object definition name
        try
        {
            businessObjectFormatRestController
                .updateBusinessObjectFormat(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals("Business object format with namespace \"" + NAMESPACE_CD + "\", business object definition name \"I_DO_NOT_EXIST\", format usage \""
                + FORMAT_USAGE_CODE + "\", format file type \"" + FORMAT_FILE_TYPE_CODE + "\", and format version \"" + INITIAL_FORMAT_VERSION
                + "\" doesn't exist.", e.getMessage());
        }

        // Try to perform an update using invalid format usage.
        try
        {
            businessObjectFormatRestController
                .updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to perform an update using invalid format file type.
        try
        {

            businessObjectFormatRestController
                .updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION, request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to perform an update using invalid format version.
        try
        {

            businessObjectFormatRestController.updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999, request);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999),
                e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatNoInitialDescriptionAndSchema()
    {
        // Create an initial version of a business object format without description and schema.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, null, true,
                PARTITION_KEY);

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and schema.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION_2, null, getTestSchema2(), updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatNoChangesToDescriptionAndSchema()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat();

        // Perform an update without making any changes to format description and schema information.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION, getTestSchema());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, getTestAttributeDefinitions(), getTestSchema(), updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatInitialSchemaPresentAndDataIsRegistered()
    {
        // Create an initial version of a business object format with format description and schema information.
        createTestBusinessObjectFormat();

        // Retrieve the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Create a business object data entity associated with the business object format.
        createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Perform an update by changing the schema.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION, getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true,
            PARTITION_KEY, FORMAT_DESCRIPTION, getTestAttributeDefinitions(), getTestSchema2(), updatedBusinessObjectFormat);

        // TODO: This will become a negative test case once we restore the update schema check in updateBusinessObjectFormat() method.
        // Try to update business object format schema information.
        //BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION, getTestSchema2());
        //try
        //{
        //    businessObjectFormatRestController.updateBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);
        //    fail("Should throw an IllegalArgumentException when updating schema information for a business object format " +
        //        "that has an existing schema defined and business object data associated with it.");
        //}
        //catch (IllegalArgumentException e)
        //{
        //    assertEquals(String.format("Can not update schema information for a business object format that has an existing schema defined and business " +
        //        "object data associated with it. Business object format: {businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
        //        "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
        //        INITIAL_FORMAT_VERSION), e.getMessage());
        //}
    }

    // Tests for Get Business Object Format

    @Test
    public void testGetBusinessObjectFormat()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat();

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController
            .getBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, getTestAttributeDefinitions(), getTestSchema(), resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatMissingRequiredParameters()
    {
        // Try to perform a get without specifying business object definition name.
        try
        {
            businessObjectFormatRestController.getBusinessObjectFormat(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to perform a get without specifying business object format usage.
        try
        {
            businessObjectFormatRestController.getBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to perform a get without specifying business object format file type.
        try
        {
            businessObjectFormatRestController.getBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, INITIAL_FORMAT_VERSION);
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectFormatMissingOptionalParameters()
    {
        // Create and persist a business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION);

        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Perform a get without specifying business object format version (passed as a null value) and namespace code.
        // Please note that HerdDaoTest.testGetBusinessObjectFormatByAltKeyFormatVersionNotSpecified
        // already validates the SQl select logic, so we do not have to go over it here.
        BusinessObjectFormat resultBusinessObjectFormat =
            businessObjectFormatRestController.getBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, null, null, resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatTrimParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Perform a get using input parameters with leading and trailing empty spaces.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController
            .getBusinessObjectFormat(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, null, null, resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatUpperCaseParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toLowerCase(), true, PARTITION_KEY.toLowerCase());

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController
            .getBusinessObjectFormat(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, true, PARTITION_KEY.toLowerCase(), FORMAT_DESCRIPTION.toLowerCase(), null, null,
            resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatLowerCaseParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toUpperCase(), true, PARTITION_KEY.toUpperCase());

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController
            .getBusinessObjectFormat(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, true, PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION.toUpperCase(), null, null,
            resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatInvalidParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Validate that we can perform a get on our business object format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController
            .getBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, null, null, resultBusinessObjectFormat);

        // Try to perform a get using invalid namespace code.
        try
        {
            businessObjectFormatRestController
                .getBusinessObjectFormat("I_DO_NOT_EXIST", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION), e.getMessage());
        }

        // Try to perform a get using invalid business object definition name.
        try
        {
            businessObjectFormatRestController
                .getBusinessObjectFormat(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION), e.getMessage());
        }

        // Try to perform a get using invalid format usage.
        try
        {
            businessObjectFormatRestController.getBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to perform a get using invalid format file type.
        try
        {
            businessObjectFormatRestController.getBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to perform a get using invalid format version.
        try
        {
            businessObjectFormatRestController.getBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999),
                e.getMessage());
        }
    }

    // Tests for Get Business Object Formats

    @Test
    public void testGetBusinessObjectFormats()
    {
        // Create and persist the relative business object definitions.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME_2, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME_2, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : getTestBusinessObjectFormatKeys())
        {
            createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, false, PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition.
        BusinessObjectFormatKeys resultKeys = businessObjectFormatRestController.getBusinessObjectFormats(NAMESPACE_CD, BOD_NAME, false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());

        // Retrieve a list of the latest version business object format keys for the specified business object definition.
        resultKeys = businessObjectFormatRestController.getBusinessObjectFormats(NAMESPACE_CD, BOD_NAME, true);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatLatestVersionKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGetBusinessObjectFormatsMissingRequiredParameters()
    {
        // Try to get business object formats without specifying business object definition name.
        try
        {
            businessObjectFormatRestController.getBusinessObjectFormat(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION);
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectFormatsMissingOptionalParameters()
    {
        // Create and persist the relative business object definitions.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME_2, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME_2, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : getTestBusinessObjectFormatKeys())
        {
            createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, false, PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object
        // definition without passing the latestBusinessObjectFormatVersion flag.
        BusinessObjectFormatKeys resultKeys = businessObjectFormatRestController.getBusinessObjectFormats(NAMESPACE_CD, BOD_NAME, null);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGetBusinessObjectFormatsTrimParameters()
    {
        // Create and persist the relative business object definitions.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME_2, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME_2, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : getTestBusinessObjectFormatKeys())
        {
            createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, false, PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition using input parameters with leading and trailing empty spaces.
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatRestController.getBusinessObjectFormats(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGetBusinessObjectFormatsUpperCaseParameters()
    {
        // Create and persist the relative business object definitions.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME_2, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME_2, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : getTestBusinessObjectFormatKeys())
        {
            createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, false, PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition using upper case input parameters
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatRestController.getBusinessObjectFormats(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGetBusinessObjectFormatsLowerCaseParameters()
    {
        // Create and persist the relative business object definitions.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME_2, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME_2, DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : getTestBusinessObjectFormatKeys())
        {
            createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, false, PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition using lower case input parameters.
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatRestController.getBusinessObjectFormats(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGetBusinessObjectFormatsInvalidParameters()
    {
        // Try to get business object formats using invalid namespace code.
        try
        {
            businessObjectFormatRestController.getBusinessObjectFormats("I_DO_NOT_EXIST", BOD_NAME, false);
            fail("Should throw an ObjectNotFoundException when not able to find business object definition.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", BOD_NAME, "I_DO_NOT_EXIST"),
                e.getMessage());
        }

        // Try to get business object formats using invalid business object definition name.
        try
        {
            businessObjectFormatRestController.getBusinessObjectFormats(NAMESPACE_CD, "I_DO_NOT_EXIST", false);
            fail("Should throw an ObjectNotFoundException when not able to find business object definition.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", "I_DO_NOT_EXIST", NAMESPACE_CD),
                e.getMessage());
        }
    }

    // Tests for Delete Business Object Format

    @Test
    public void testDeleteBusinessObjectFormat() throws Exception
    {
        // Create an initial version of a business object format.
        createTestBusinessObjectFormat();

        // Retrieve the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatRestController
            .deleteBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, getTestAttributeDefinitions(), getTestSchema(), deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatMissingRequiredParameters()
    {
        // Try to perform a delete without specifying business object definition name.
        try
        {
            businessObjectFormatRestController
                .deleteBusinessObjectFormat(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying business object format usage.
        try
        {
            businessObjectFormatRestController.deleteBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying business object format file type.
        try
        {
            businessObjectFormatRestController.deleteBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, INITIAL_FORMAT_VERSION);
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying business object format version.
        try
        {
            businessObjectFormatRestController.deleteBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectFormatMissingOptionalParameters() throws Exception
    {
        // Create and persist a business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION);

        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Delete the business object format without specifying namespace code.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatRestController
            .deleteBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, null, null, deletedBusinessObjectFormat);
        // Ensure that this business object format is no longer there.
        assertNull(herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatTrimParameters()
    {
        // Create and persist a business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Ensure that this business object format exists.
        assertNotNull(herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatRestController
            .deleteBusinessObjectFormat(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, null, null, deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatUpperCaseParameters()
    {
        // Create and persist a business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toLowerCase(), true, PARTITION_KEY.toLowerCase());

        // Ensure that this business object format exists.
        assertNotNull(herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));

        // Delete the business object format by passing alternate key value in upper case.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatRestController
            .deleteBusinessObjectFormat(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, true, PARTITION_KEY.toLowerCase(), FORMAT_DESCRIPTION.toLowerCase(), null, null,
            deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatLowerCaseParameters()
    {
        // Create and persist a business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toUpperCase(), true, PARTITION_KEY.toUpperCase());

        // Ensure that this business object format exists.
        assertNotNull(herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));

        // Delete the business object format by passing alternate key value in lower case.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatRestController
            .deleteBusinessObjectFormat(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, true, PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION.toUpperCase(), null, null,
            deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatInvalidParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Validate that we can perform a get on our business object format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController
            .getBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, null, null, resultBusinessObjectFormat);

        // Try to perform a delete using invalid namespace code.
        try
        {
            businessObjectFormatRestController
                .deleteBusinessObjectFormat("I_DO_NOT_EXIST", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION), e.getMessage());
        }

        // Try to perform a delete using invalid business object definition name.
        try
        {
            businessObjectFormatRestController
                .deleteBusinessObjectFormat(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION), e.getMessage());
        }

        // Try to perform a delete using invalid format usage.
        try
        {
            businessObjectFormatRestController
                .deleteBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to perform a delete using invalid format file type.
        try
        {
            businessObjectFormatRestController.deleteBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to perform a delete using invalid format version.
        try
        {
            businessObjectFormatRestController.deleteBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999),
                e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectFormatNotLatestVersion()
    {
        // Create and persist a business object format which is not marked as the latest version.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                false, PARTITION_KEY);

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatRestController
            .deleteBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, false, PARTITION_KEY, FORMAT_DESCRIPTION, null, null, deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatLatestVersionWhenPreviousVersionExists()
    {
        // Create and persist two versions of the business object format.
        BusinessObjectFormatEntity initialVersionBusinessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                false, PARTITION_KEY);
        BusinessObjectFormatEntity latestVersionBusinessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Validate that the initial version does not have the latest version flag set and that second version has it set.
        assertFalse(initialVersionBusinessObjectFormatEntity.getLatestVersion());
        assertTrue(latestVersionBusinessObjectFormatEntity.getLatestVersion());

        // Delete the latest (second) version of the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatRestController
            .deleteBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(latestVersionBusinessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            SECOND_DATA_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, null, null, deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        latestVersionBusinessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION));
        assertNull(latestVersionBusinessObjectFormatEntity);

        // Validate that the initial version now has the latest version flag set.
        initialVersionBusinessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
        assertNotNull(initialVersionBusinessObjectFormatEntity);
        assertTrue(initialVersionBusinessObjectFormatEntity.getLatestVersion());
    }

    @Test
    public void testDeleteBusinessObjectFormatDataIsRegistered()
    {
        // Create an initial version of a business object format.
        createTestBusinessObjectFormat();

        // Retrieve the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Create a business object data entity associated with the business object format.
        createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Try to delete the business object format.
        try
        {
            businessObjectFormatRestController
                .deleteBusinessObjectFormat(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
            fail("Should throw an IllegalArgumentException when trying to delete a business object format that has business object data associated with it.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Can not delete a business object format that has business object data associated with it. " +
                "Business object format: {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}", NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectFormatDdl()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        BusinessObjectFormatDdlRequest request;
        BusinessObjectFormatDdl resultDdl;

        // Retrieve business object data ddl.
        request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, getBusinessObjectFormatExpectedDdl(), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlMissingRequiredParameters()
    {
        BusinessObjectFormatDdlRequest request;

        // Try to retrieve business object data ddl when namespace parameter is not specified.
        request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setNamespace(BLANK_TEXT);
        try
        {
            businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when namespace parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when business object definition name parameter is not specified.
        request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setBusinessObjectDefinitionName(BLANK_TEXT);
        try
        {
            businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when business object definition name parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when business object format usage parameter is not specified.
        request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setBusinessObjectFormatUsage(BLANK_TEXT);
        try
        {
            businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when business object format usage parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage name must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when business object format file type parameter is not specified.
        request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setBusinessObjectFormatFileType(BLANK_TEXT);
        try
        {
            businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when business object format file type parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when output format parameter is not specified.
        request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setOutputFormat(null);
        try
        {
            businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when output format parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An output format must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when table name parameter is not specified.
        request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setTableName(BLANK_TEXT);
        try
        {
            businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);
            fail("Should throw an IllegalArgumentException when table name parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A table name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlMissingOptionalParameters()
    {
        // Prepare test data without custom ddl.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, SCHEMA_DELIMITER_PIPE,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), getTestPartitionColumns(), CUSTOM_DDL_NAME);

        // Retrieve business object data ddl request without optional parameters.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(BLANK_TEXT);
        request.setBusinessObjectFormatVersion(null);
        request.setIncludeDropTableStatement(null);
        request.setIncludeIfNotExistsOption(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedDdl =
            getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, false, false);
        validateBusinessObjectFormatDdl("", expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlTrimParameters()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        // Retrieve business object data ddl request with all string values requiring trimming.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setBusinessObjectDefinitionName(addWhitespace(request.getBusinessObjectDefinitionName()));
        request.setBusinessObjectFormatUsage(addWhitespace(request.getBusinessObjectFormatUsage()));
        request.setBusinessObjectFormatFileType(addWhitespace(request.getBusinessObjectFormatFileType()));
        request.setTableName(addWhitespace(request.getTableName()));
        request.setCustomDdlName(addWhitespace(request.getCustomDdlName()));
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, getBusinessObjectFormatExpectedDdl(), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlUpperCaseParameters()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        // Retrieve business object data ddl request with all string values in upper case.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setNamespace(request.getNamespace().toUpperCase());
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toUpperCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toUpperCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toUpperCase());
        request.setCustomDdlName(request.getCustomDdlName().toUpperCase());
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, getBusinessObjectFormatExpectedDdl(), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlLowerCaseParameters()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        // Retrieve business object data ddl request with all string values in lower case.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setNamespace(request.getNamespace().toLowerCase());
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toLowerCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toLowerCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toLowerCase());
        request.setCustomDdlName(request.getCustomDdlName().toLowerCase());
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, getBusinessObjectFormatExpectedDdl(), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlInvalidParameters()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        BusinessObjectFormatDdlRequest request;

        // Try to retrieve business object data ddl using non-existing format.
        request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setBusinessObjectDefinitionName("I_DO_NOT_EXIST");
        try
        {
            businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);
            fail("Should throw an ObjectNotFoundException when non-existing business object format is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(request.getNamespace(), request.getBusinessObjectDefinitionName(),
                request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()), e.getMessage());
        }

        // Try to retrieve business object data ddl using non-existing custom ddl.
        request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setCustomDdlName("I_DO_NOT_EXIST");
        try
        {
            businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);
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
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlPartitionColumnIsAlsoRegularColumn()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> schemaColumns = getTestSchemaColumns();
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();
        // Override the first schema column to be a partition column.
        schemaColumns.set(0, partitionColumns.get(0));
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, SCHEMA_DELIMITER_PIPE,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns, null);

        // Retrieve business object data ddl without specifying custom ddl name.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedDdl = getExpectedDdl(partitionColumns.size(), "ORGNL_PRTN_CLMN001", "DATE", ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
        validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlSingleLevelPartitioning()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = getTestPartitionColumns().subList(0, 1);
        String partitionKey = partitionColumns.get(0).getName();
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, SCHEMA_DELIMITER_PIPE,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object data ddl without specifying custom ddl name.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedDdl =
            getExpectedDdl(partitionColumns.size(), FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
        validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoPartitioning()
    {
        // Prepare non-partitioned test business object data with custom ddl.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY,
            SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), null, CUSTOM_DDL_NAME);

        // Retrieve business object data ddl for a non-partitioned table.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedDdl =
            getExpectedDdl(0, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
        validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlNoPartitioning()
    {
        // Prepare test data without custom ddl.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY,
            SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), null, null);

        // Retrieve business object data ddl for a non-partitioned table and without specifying custom ddl name.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedDdl =
            getExpectedDdl(0, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
        validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlSubpartitionKeysHaveHyphens()
    {
        // Prepare test data with subpartition using key values with hyphens instead of underscores.
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, SCHEMA_DELIMITER_PIPE,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), partitionColumns, CUSTOM_DDL_NAME);

        // Retrieve business object data ddl.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.  Please note that we expect hyphens in subpartition key values.
        String expectedDdl =
            getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, true, true, true);
        validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlMissingSchemaDelimiterCharacter()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
            SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object data ddl.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED NULL DEFINED AS '\\N'";
        String expectedDdl =
            getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, expectedRowFormat, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
        validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlMissingSchemaEscapeCharacter()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, SCHEMA_DELIMITER_PIPE, null,
            SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object data ddl.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' NULL DEFINED AS '\\N'";
        String expectedDdl =
            getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, expectedRowFormat, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
        validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlMissingSchemaNullValue()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, SCHEMA_DELIMITER_PIPE,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object data ddl.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' NULL DEFINED AS ''";
        String expectedDdl =
            getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, expectedRowFormat, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
        validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlEscapeSingleQuoteInRowFormat()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, SINGLE_QUOTE, SINGLE_QUOTE, SINGLE_QUOTE,
            getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object data ddl.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\'' ESCAPED BY '\\'' NULL DEFINED AS '\\''";
        String expectedDdl =
            getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, expectedRowFormat, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
        validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlEscapeBackslashInRowFormat()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, BACKSLASH, BACKSLASH, BACKSLASH,
            getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object data ddl.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results - please note that we do not escape single backslash in null value.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\\\' ESCAPED BY '\\\\' NULL DEFINED AS '\\'";
        String expectedDdl =
            getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, expectedRowFormat, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
        validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlUnprintableCharactersInRowFormat()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        // Set schemaDelimiterCharacter to char(1), schemaEscapeCharacter to char(10), and schemaNullValue to char(128).
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, String.valueOf((char) 1), String.valueOf((char) 10),
            String.valueOf((char) 128), getTestSchemaColumns(), partitionColumns, null);

        // Retrieve business object data ddl request without business object format and data versions.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(null);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results - please note that 1 decimal = 1 octal, 10 decimal = 12 octal, and 128 decimal = 200 octal.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' ESCAPED BY '\\012' NULL DEFINED AS '\\200'";
        String expectedDdl =
            getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, expectedRowFormat, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
        validateBusinessObjectFormatDdl(null, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlForwardSlashInPartitionColumnName()
    {
        // Prepare test data without custom ddl.
        String invalidPartitionColumnName = "INVALID_/_PRTN_CLMN";
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();
        partitionColumns.get(0).setName(invalidPartitionColumnName);
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, PARTITION_KEY, SCHEMA_DELIMITER_PIPE,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), partitionColumns, null);

        // Try to retrieve business object data ddl for the format that uses unsupported schema column data type.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(null);
        try
        {
            businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);
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
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlNotSupportedSchemaColumnDataType()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> schemaColumns = getTestSchemaColumns();
        SchemaColumn schemaColumn = new SchemaColumn();
        schemaColumns.add(schemaColumn);
        schemaColumn.setName("COLUMN");
        schemaColumn.setType("UNKNOWN");
        String partitionKey = schemaColumns.get(0).getName();
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, SCHEMA_DELIMITER_PIPE,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, getTestPartitionColumns(), null);

        // Try to retrieve business object data ddl for the format that uses unsupported schema column data type.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(null);
        try
        {
            businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);
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
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlAllKnownFileTypes()
    {
        // Get storage entity.
        StorageEntity storageEntity = herdDao.getStorageByName(StorageEntity.MANAGED_STORAGE);

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
            List<SchemaColumn> partitionColumns = getTestPartitionColumns().subList(0, 1);
            String partitionKey = partitionColumns.get(0).getName();
            BusinessObjectFormatEntity businessObjectFormatEntity =
                createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, FORMAT_DESCRIPTION,
                    LATEST_VERSION_FLAG_SET, partitionKey, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                    SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), partitionColumns);

            for (String partitionValue : UNSORTED_PARTITION_VALUES)
            {
                BusinessObjectDataEntity businessObjectDataEntity =
                    createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, partitionValue,
                        NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
                String s3KeyPrefix =
                    businessObjectDataHelper.buildS3KeyPrefix(businessObjectFormatEntity, herdDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity));
                StorageUnitEntity storageUnitEntity =
                    createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

                // Create two storage files.
                for (int i = 0; i < 2; i++)
                {
                    createStorageFileEntity(storageUnitEntity, String.format("%s/data%d.dat", s3KeyPrefix, i), FILE_SIZE_1_KB, ROW_COUNT_1000);
                }

                herdDao.saveAndRefresh(storageUnitEntity);
                herdDao.saveAndRefresh(businessObjectDataEntity);
            }

            // Retrieve business object data ddl.
            BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(null);
            request.setBusinessObjectFormatFileType(businessObjectFormatFileType);
            BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

            // Validate the results.
            String expectedHiveFileFormat = businessObjectFormatFileTypeMap.get(businessObjectFormatFileType);
            String expectedDdl = getExpectedDdl(partitionColumns.size(), FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, expectedHiveFileFormat,
                businessObjectFormatFileType, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
            validateBusinessObjectFormatDdl(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION,
                BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, null, expectedDdl, resultDdl);
        }
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlNotSupportedFileType()
    {
        // Prepare test data without custom ddl.
        String businessObjectFileType = "UNKNOWN";
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(businessObjectFileType, PARTITION_KEY, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
            SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), getTestPartitionColumns(), null);

        // Try to retrieve business object data ddl for the format without custom ddl and that uses unsupported file type.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(null);
        request.setBusinessObjectFormatFileType(businessObjectFileType);
        try
        {
            businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);
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
    public void testGenerateBusinessObjectFormatDdlNoDropTable()
    {
        // Prepare test data without custom ddl.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, SCHEMA_DELIMITER_PIPE,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), getTestPartitionColumns(), CUSTOM_DDL_NAME);

        // Retrieve business object data ddl request without drop table statement.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setIncludeDropTableStatement(false);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedDdl =
            getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, false, true);
        validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlCollection()
    {
        // Prepare database entities required for testing.
        createDatabaseEntitiesForBusinessObjectFormatDdlCollectionTesting();

        // Generate DDL for a collection of business object data.
        BusinessObjectFormatDdlCollectionResponse resultBusinessObjectFormatDdlCollectionResponse =
            businessObjectFormatRestController.generateBusinessObjectFormatDdlCollection(getTestBusinessObjectFormatDdlCollectionRequest());

        // Validate the response object.
        assertEquals(getExpectedBusinessObjectFormatDdlCollectionResponse(), resultBusinessObjectFormatDdlCollectionResponse);
    }
}

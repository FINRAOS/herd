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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import javax.persistence.PersistenceException;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.core.Command;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdl;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionRequest;
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

public class BusinessObjectFormatServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "businessObjectFormatServiceImpl")
    private BusinessObjectFormatService businessObjectFormatServiceImpl;

    @Test
    public void testCreateBusinessObjectFormat()
    {
        // Create an initial version of a business object format.
        BusinessObjectFormat businessObjectFormat = createTestBusinessObjectFormat(getNewAttributes());

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
            FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), getTestSchema(), businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatMissingRequiredParameters()
    {
        // Try to perform a create without specifying business object definition name parameter.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), getTestSchema()));
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to perform a create without specifying format usage parameter.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), getTestSchema()));
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to perform a create without specifying format file type parameter.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), getTestSchema()));
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to perform a create without specifying format partition key parameter.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, BLANK_TEXT, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), getTestSchema()));
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format partition key must be specified.", e.getMessage());
        }

        // Try to create a business object format instance when attribute name is not specified.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    Arrays.asList(new Attribute(BLANK_TEXT, ATTRIBUTE_VALUE_1)), getTestAttributeDefinitions(), getTestSchema()));
            fail("Should throw an IllegalArgumentException when attribute name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An attribute name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatMissingOptionalParameters()
    {
        // Create relative database entities including a business object definition.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_FILE_TYPE_CODE, PARTITION_KEY_GROUP);

        // Perform a create without specifying optional parameters.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, NO_FORMAT_DESCRIPTION,
                getNewAttributes(), NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
            PARTITION_KEY, NO_FORMAT_DESCRIPTION, getNewAttributes(), NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, resultBusinessObjectFormat);
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
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
            request.getSchema().setPartitionKeyGroup(partitionKeyGroupName);
            BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

            // Validate the returned object.
            Schema expectedSchema = getTestSchema();
            expectedSchema.setPartitionKeyGroup(null);
            validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, expectedBusinessObjectFormatVersion++,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), expectedSchema,
                businessObjectFormat);
        }
    }

    @Test
    public void testCreateBusinessObjectFormatNoAttributeDefinitionPublishOptionSpecified()
    {
        // Create relative database entities including a business object definition.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_FILE_TYPE_CODE, PARTITION_KEY_GROUP);

        // Perform a create without specifying a publish option for an attribute definition.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, NO_FORMAT_DESCRIPTION,
                getNewAttributes(), Arrays.asList(new AttributeDefinition(ATTRIBUTE_NAME_1_MIXED_CASE, null)), NO_SCHEMA);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object. The publish option is expected to default to "false".
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
            PARTITION_KEY, NO_FORMAT_DESCRIPTION, getNewAttributes(), Arrays.asList(new AttributeDefinition(ATTRIBUTE_NAME_1_MIXED_CASE, NO_PUBLISH_ATTRIBUTE)),
            NO_SCHEMA, resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatTrimParameters()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a first version of the format using input parameters with leading and trailing empty spaces.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), addWhitespace(PARTITION_KEY), addWhitespace(FORMAT_DESCRIPTION),
                Arrays.asList(new Attribute(addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE), addWhitespace(ATTRIBUTE_VALUE_1))), getTestAttributeDefinitions(),
                addWhitespace(getTestSchema()));
        request.getSchema().setPartitionKeyGroup(addWhitespace(PARTITION_KEY_GROUP));

        // Create an initial business object format version.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
            PARTITION_KEY, addWhitespace(FORMAT_DESCRIPTION), Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, addWhitespace(ATTRIBUTE_VALUE_1))),
            getTestAttributeDefinitions(), getTestSchema(), businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatUpperCaseParameters()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(), BDEF_NAME.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), PARTITION_KEY_GROUP.toLowerCase());

        // Create a first version of the format using upper case input parameters.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION.toUpperCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())), getTestAttributeDefinitions(),
                getTestSchema());
        request.getSchema().setPartitionKeyGroup(PARTITION_KEY_GROUP.toUpperCase());
        // For the first schema partition column name, use an opposite case from the format partition key was specified in.
        request.getSchema().getPartitions().get(0).setName(PARTITION_KEY.toLowerCase());
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setPartitionKeyGroup(PARTITION_KEY_GROUP.toLowerCase());
        expectedSchema.getPartitions().get(0).setName(PARTITION_KEY.toLowerCase());
        validateBusinessObjectFormat(null, NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION.toUpperCase(),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())), getTestAttributeDefinitions(),
            expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatLowerCaseParameters()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(), BDEF_NAME.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), PARTITION_KEY_GROUP.toUpperCase());

        // Create a first version of the format using lower case input parameters.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), PARTITION_KEY.toLowerCase(), FORMAT_DESCRIPTION.toLowerCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())), getTestAttributeDefinitions(),
                getTestSchema());
        request.getSchema().setPartitionKeyGroup(PARTITION_KEY_GROUP.toLowerCase());
        // For the first schema partition column name, use an opposite case from the format partition key was specified in.
        request.getSchema().getPartitions().get(0).setName(PARTITION_KEY.toUpperCase());
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setPartitionKeyGroup(PARTITION_KEY_GROUP.toUpperCase());
        expectedSchema.getPartitions().get(0).setName(PARTITION_KEY.toUpperCase());
        validateBusinessObjectFormat(null, NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toLowerCase(), FORMAT_DESCRIPTION.toLowerCase(),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())), getTestAttributeDefinitions(),
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
            createBusinessObjectFormatCreateRequest("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an ObjectNotFoundException when non-existing namespace code is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String
                .format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", request.getBusinessObjectDefinitionName(),
                    request.getNamespace()), e.getMessage());
        }

        // Try to create a business object format when namespace contains a slash character.
        request = createBusinessObjectFormatCreateRequest(addSlash(BDEF_NAMESPACE), BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY,
            FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when namespace contains a slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a slash character.", e.getMessage());
        }

        // Try to perform a create using invalid business object definition name.
        request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
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

        // Try to create a business object format when business object definition name contains a slash character.
        request = createBusinessObjectFormatCreateRequest(BDEF_NAMESPACE, addSlash(BDEF_NAME), FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY,
            FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when business object definition name contains a slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition name can not contain a slash character.", e.getMessage());
        }

        // Try to create a business object format when business object format usage contains a slash character.
        request = createBusinessObjectFormatCreateRequest(BDEF_NAMESPACE, BDEF_NAME, addSlash(FORMAT_USAGE_CODE), FORMAT_FILE_TYPE_CODE, PARTITION_KEY,
            FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when business object format usage contains a slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object format usage can not contain a slash character.", e.getMessage());
        }

        // Try to perform a create using invalid format file type.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", PARTITION_KEY, FORMAT_DESCRIPTION,
            getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an ObjectNotFoundException when non-existing business object format file type code is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("File type with code \"%s\" doesn't exist.", request.getBusinessObjectFormatFileType()), e.getMessage());
        }

        // Try to create a business object format when business object format file type contains a slash character.
        request = createBusinessObjectFormatCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, addSlash(FORMAT_FILE_TYPE_CODE), PARTITION_KEY,
            FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when business object format file type contains a slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object format file type can not contain a slash character.", e.getMessage());
        }

        // Try to create a business object format when partition key contains a slash character.
        request = createBusinessObjectFormatCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, addSlash(PARTITION_KEY),
            FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(request);
            fail("Should throw an IllegalArgumentException when partition key contains a slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object format partition key can not contain a slash character.", e.getMessage());
        }

        // Try to perform a create using invalid partition key group.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
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
    public void testCreateBusinessObjectFormatCreateSecondVersion()
    {
        // Create an initial version of a business object format.
        createTestBusinessObjectFormat();

        // Create a second version of the business object format.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
            PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), getTestSchema(), resultBusinessObjectFormat);

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
        createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, PARTITION_KEY_GROUP, getNewAttributes(), SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
            SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(RANDOM_SUFFIX), null);

        // Create a second version of the business object format without partitioning columns.
        Schema testSchema = getTestSchema();
        testSchema.setPartitions(null);
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), testSchema);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setPartitions(null);
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
            PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), expectedSchema, resultBusinessObjectFormat);

        // Check if we now have only one latest format version - our first version is not marked as latest anymore.
        // Please note that we do not have to validate if the first format version is not marked as "latest" now, since having more that one
        // format versions with the latestVersion flag set to Yes produces exception in herdDao.getBusinessObjectFormatByAltKey() method.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao
            .getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));
        assertEquals(SECOND_FORMAT_VERSION, businessObjectFormatEntity.getBusinessObjectFormatVersion());
        assertEquals(true, businessObjectFormatEntity.getLatestVersion());
    }

    @Test
    public void testCreateBusinessObjectFormatIncorrectLatestVersion() throws Exception
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a first version of the format with the latest flag set to false.
        createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, false,
            PARTITION_KEY);

        try
        {
            // Try to create a new format version for this format.
            final BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest =
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
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
    public void testCreateBusinessObjectFormatDuplicateAttributeDefinitions()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

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
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    Arrays.asList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_3),
                        new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_3)), getTestAttributeDefinitions(), getTestSchema()));
            fail("Should throw an IllegalArgumentException when duplicate attributes are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate attribute name found: %s", ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase()), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatSchemaWhitespaceNullValueAndDelimiter()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of business object format with schema null value and delimiter as a whitespace character.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        request.getSchema().setNullValue(" ");
        request.getSchema().setDelimiter(" ");
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setNullValue(" ");
        expectedSchema.setDelimiter(" ");
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
            FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaNullValue()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

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
        Schema expectedSchema = getTestSchema();

        // The escape characters gets trimmed which gets stored as the empty string which gets returned as null.
        expectedSchema.setNullValue(null);
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
            FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaDelimiter()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format with an empty schema escape character which is valid.
        request.getSchema().setDelimiter(null);
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();

        // The escape characters gets trimmed which gets stored as the empty string which gets returned as null.
        expectedSchema.setDelimiter(null);
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
            FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaEscapeCharacter()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format with an empty schema escape character which is valid.
        request.getSchema().setEscapeCharacter(" ");
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();

        expectedSchema.setEscapeCharacter(" ");
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
            FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNullSchemaEscapeCharacter()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

        // Try to create a business object format with an empty schema escape character which is valid.
        request.getSchema().setEscapeCharacter(null);
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();

        // The escape characters gets trimmed which gets stored as the empty string which gets returned as null.
        expectedSchema.setEscapeCharacter(null);
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
            FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatUnprintableSchemaEscapeCharacter()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of business object format with a schema having an unprintable escape character.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        request.getSchema().setEscapeCharacter(String.valueOf((char) 1));
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setEscapeCharacter(String.valueOf((char) 1));
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
            FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaColumns()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

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
    public void testCreateBusinessObjectFormatNoSchemaColumnName()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

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
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

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
    public void testCreateBusinessObjectFormatDuplicateColumns()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

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
    public void testCreateBusinessObjectFormatNoSchemaPartitions()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a business object format create request without schema partition columns.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        request.getSchema().setPartitions(null);

        // Create an initial version of business object format without schema partitions.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setPartitions(null);
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
            FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatNoSchemaPartitionColumnName()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

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
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

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
    public void testCreateBusinessObjectFormatDuplicatePartitionColumns()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

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
    public void testCreateBusinessObjectFormatPartitionAndRegularColumnsHaveConflictingValues()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

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
    public void testCreateBusinessObjectFormatTopPartitionColumnNameNoMatchPartitionKey()
    {
        // Create a business object format create request.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

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
    public void testCreateBusinessObjectFormatInitialVersionExistsWithoutSchema()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        BusinessObjectFormatCreateRequest request;
        BusinessObjectFormat resultBusinessObjectFormat;

        // Create an initial version of the business object format without a schema.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getNewAttributes(), getTestAttributeDefinitions(), NO_SCHEMA);
        businessObjectFormatService.createBusinessObjectFormat(request);

        // Create a second version of the business object format without a schema.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getNewAttributes(), getTestAttributeDefinitions(), NO_SCHEMA);
        resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(resultBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), NO_SCHEMA,
            resultBusinessObjectFormat);

        // Create a second version of the business object format with a schema.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(resultBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), getTestSchema(),
            resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaNoSchemaChanges()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format with a schema.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        businessObjectFormatService.createBusinessObjectFormat(request);

        // Create a second version of the business object format with a schema that is identical to the initial version schema.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
            PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), getTestSchema(), resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaHavingNullRowFormatValuesNoSchemaChanges()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        BusinessObjectFormatCreateRequest request;
        BusinessObjectFormat resultBusinessObjectFormat;

        // Create an initial version of the business object format with a schema having null row format values.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
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
        request = createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        request.getSchema().setNullValue(EMPTY_STRING);   // This is a required parameter, so it cannot be set to null.
        request.getSchema().setDelimiter(null);
        request.getSchema().setEscapeCharacter(null);
        resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        // Please note that the escape character which was passed as an empty string gets returned as null.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setNullValue(null);
        expectedSchema.setDelimiter(null);
        expectedSchema.setEscapeCharacter(null);
        validateBusinessObjectFormat(resultBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), expectedSchema,
            resultBusinessObjectFormat);

        // Create a third version of the business object format with a schema that is identical to the initial version schema,
        // except that we now pass empty string values for all three row format parameters.
        request = createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        request.getSchema().setNullValue(EMPTY_STRING);
        request.getSchema().setDelimiter(EMPTY_STRING);
        request.getSchema().setEscapeCharacter(EMPTY_STRING);
        resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(resultBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), expectedSchema,
            resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaAdditiveSchemaChangesNewColumnAdded()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format with a schema.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
        businessObjectFormatService.createBusinessObjectFormat(request);

        // Create a second version of the business object format with a new schema that is additive to the previous format version schema.
        Schema newFormatVersionSchema = getTestSchema();
        SchemaColumn newSchemaColumn = new SchemaColumn();
        newFormatVersionSchema.getColumns().add(newSchemaColumn);
        newSchemaColumn.setName("NEW_COLUMN");
        newSchemaColumn.setType("TYPE");
        request = createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
            getNewAttributes(), getTestAttributeDefinitions(), newFormatVersionSchema);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
            PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), newFormatVersionSchema, resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaAdditiveSchemaChangesColumnDescriptionUpdated()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

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
                NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, initialSchema));

        // Create a second version of the business object format with the schema columns having updated descriptions.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(
            new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, COLUMN_NAME_2, FORMAT_DESCRIPTION,
                NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, updatedSchema));

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
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

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
                    NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, initialSchema));

            // Create a second version of the business object format having increased column sizes for both regular and partition columns.
            BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(
                new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, formatUsage, FORMAT_FILE_TYPE_CODE, COLUMN_NAME_2, FORMAT_DESCRIPTION,
                    NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, updatedSchema));

            // Validate the returned object.
            BusinessObjectFormat expectedBusinessObjectFormat = (BusinessObjectFormat) initialBusinessObjectFormat.clone();
            expectedBusinessObjectFormat.setId(resultBusinessObjectFormat.getId());
            expectedBusinessObjectFormat.setBusinessObjectFormatVersion(SECOND_FORMAT_VERSION);
            expectedBusinessObjectFormat.setSchema(updatedSchema);
            assertEquals(expectedBusinessObjectFormat, resultBusinessObjectFormat);
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaNonAdditiveSchemaChanges()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of a business object format with a schema.
        businessObjectFormatService.createBusinessObjectFormat(
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema()));

        Schema newSchema;

        // Try to create a second version of the business object format without a schema.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), NO_SCHEMA));
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
            newSchema = getTestSchema();
            newSchema.setNullValue(SCHEMA_NULL_VALUE_NULL_WORD);
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), newSchema));
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
            newSchema = getTestSchema();
            newSchema.setDelimiter(SCHEMA_DELIMITER_COMMA);
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), newSchema));
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
            newSchema = getTestSchema();
            newSchema.setEscapeCharacter(SCHEMA_ESCAPE_CHARACTER_TILDE);
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), newSchema));
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
            newSchema = getTestSchema();
            newSchema.setPartitions(null);
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), newSchema));
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
            newSchema = getTestSchema();
            newSchema.setPartitions(newSchema.getPartitions().subList(0, newSchema.getPartitions().size() - 1));
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), newSchema));
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
            newSchema = getTestSchema();
            newSchema.setPartitions(getTestSchema2().getPartitions());
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), newSchema));
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
            newSchema = getTestSchema();
            newSchema.setColumns(newSchema.getColumns().subList(0, newSchema.getColumns().size() - 1));
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), newSchema));
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
            newSchema = getTestSchema();
            newSchema.setColumns(getTestSchema2().getColumns());
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), newSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "Non-additive changes detected to the previously defined regular (non-partitioning) columns.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaHavingNullRowFormatValuesNonAdditiveSchemaChanges()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format with a schema having null row format values.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());
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
            newSchema = getTestSchema();
            newSchema.setNullValue(SCHEMA_NULL_VALUE_NULL_WORD);
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), newSchema));
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
            newSchema = getTestSchema();
            newSchema.setNullValue(EMPTY_STRING);   // This is a required parameter, so it cannot be set to null.
            newSchema.setDelimiter(SCHEMA_DELIMITER_COMMA);
            newSchema.setEscapeCharacter(null);
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), newSchema));
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
            newSchema = getTestSchema();
            newSchema.setNullValue(EMPTY_STRING);   // This is a required parameter, so it cannot be set to null.
            newSchema.setDelimiter(null);
            newSchema.setEscapeCharacter(SCHEMA_ESCAPE_CHARACTER_TILDE);
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), newSchema));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "New format version escape character does not match to the previous format version escape character.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaNonAdditiveSchemaChangesPartitionColumnsAdded()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of a business object format with a schema having no partition columns.
        Schema originalSchema = getTestSchema();
        originalSchema.setPartitions(null);
        businessObjectFormatService.createBusinessObjectFormat(
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), originalSchema));

        // Try to create a second version of the business object format with a schema that now has partition columns.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    getNewAttributes(), getTestAttributeDefinitions(), getTestSchema()));
            fail("Should throw an IllegalArgumentException when the new format version is not \"additive\" to the previous format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("New format version schema is not \"additive\" to the previous format version schema. " +
                "Non-additive changes detected to the previously defined partition columns.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectFormatInitialVersionExistsWithSchemaNonAdditiveSchemaChangesColumnSizeIncreasedForNotAllowedDataType()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial format schema.
        Schema initialSchema = new Schema(
            Arrays.asList(new SchemaColumn(COLUMN_NAME, COLUMN_DATA_TYPE, COLUMN_SIZE, NO_COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION)),
            NO_PARTITION_COLUMNS, SCHEMA_NULL_VALUE_BACKSLASH_N, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, PARTITION_KEY_GROUP);

        // Create an initial version of the business object format.
        businessObjectFormatService.createBusinessObjectFormat(
            new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, initialSchema));

        // Create an updated schema having a regular column size increased.
        Schema updatedSchema = (Schema) initialSchema.clone();
        updatedSchema.getColumns().get(0).setSize(COLUMN_SIZE_2);

        try
        {
            // Try to create a second version of the business object format with a new schema
            // having regular column size increased but for a non-allowed column data type.
            businessObjectFormatService.createBusinessObjectFormat(
                new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, updatedSchema));
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
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial format schema.
        Schema initialSchema = new Schema(
            Arrays.asList(new SchemaColumn(COLUMN_NAME, COLUMN_DATA_TYPE_CHAR, COLUMN_SIZE_2, NO_COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION)),
            NO_PARTITION_COLUMNS, SCHEMA_NULL_VALUE_BACKSLASH_N, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, PARTITION_KEY_GROUP);

        // Create an initial version of the business object format.
        businessObjectFormatService.createBusinessObjectFormat(
            new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, initialSchema));

        // Create an updated schema having a regular column size decreased.
        Schema updatedSchema = (Schema) initialSchema.clone();
        updatedSchema.getColumns().get(0).setSize(COLUMN_SIZE);

        try
        {
            // Try to create a second version of the business object format with a new schema having regular column size decreased.
            businessObjectFormatService.createBusinessObjectFormat(
                new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, updatedSchema));
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
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

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
                    NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, initialSchema));

            // Create an updated schema having a regular column size set to a positive integer.
            Schema updatedSchema = (Schema) initialSchema.clone();
            updatedSchema.getColumns().get(0).setSize(COLUMN_SIZE);

            try
            {
                // Try to create a second version of the business object format with a new schema
                // having regular column size changed from a non-integer to a positive integer.
                businessObjectFormatService.createBusinessObjectFormat(
                    new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, formatUsage, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                        NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, updatedSchema));
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
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial format schema with a positive integer regular column size.
        Schema initialSchema = new Schema(
            Arrays.asList(new SchemaColumn(COLUMN_NAME, COLUMN_DATA_TYPE_CHAR, COLUMN_SIZE, NO_COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION)),
            NO_PARTITION_COLUMNS, SCHEMA_NULL_VALUE_BACKSLASH_N, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, PARTITION_KEY_GROUP);

        // Create an initial version of the business object format.
        businessObjectFormatService.createBusinessObjectFormat(
            new BusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, initialSchema));

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
                        NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, updatedSchema));
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
    public void testUpdateBusinessObjectFormat()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat(getNewAttributes());

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and schema.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, getNewAttributes2(), getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION_2, getNewAttributes2(), getTestAttributeDefinitions(),
            getTestSchema2(), updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatMissingRequiredParameters()
    {
        // Create a business object format update request.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, getNewAttributes2(), getTestSchema2());

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
                    createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, Arrays.asList(new Attribute(BLANK_TEXT, ATTRIBUTE_VALUE_1)),
                        getTestSchema2()));
            fail("Should throw an IllegalArgumentException when attribute name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An attribute name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatMissingOptionalParameters()
    {
        // Create and persist a business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create and persist a valid business object format without description and with schema without any partitioning columns.
        BusinessObjectFormatEntity originalBusinessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY, PARTITION_KEY_GROUP, getNewAttributes(), SCHEMA_DELIMITER_COMMA, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), null);

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description to a blank text, replacing attributes with
        // an attribute with a blank text value, and replacing schema with a new schema not having partition columns.
        Schema testSchema2 = getTestSchema2();
        testSchema2.setPartitions(null);
        for (String blankText : Arrays.asList(BLANK_TEXT, EMPTY_STRING, null))
        {
            BusinessObjectFormatUpdateRequest request =
                createBusinessObjectFormatUpdateRequest(blankText, Arrays.asList(new Attribute(ATTRIBUTE_NAME_4_MIXED_CASE, blankText)), testSchema2);
            BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                    request);

            // Validate the returned object.
            validateBusinessObjectFormat(originalBusinessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, blankText, Arrays.asList(new Attribute(ATTRIBUTE_NAME_4_MIXED_CASE, blankText)),
                NO_ATTRIBUTE_DEFINITIONS, testSchema2, resultBusinessObjectFormat);
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatRemoveAttributesAndSchema()
    {
        // Create an initial version of a business object format with attributes and schema.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat(getNewAttributes());

        BusinessObjectFormatUpdateRequest request;
        BusinessObjectFormat resultBusinessObjectFormat;

        // Perform an update to remove the business object format attributes and schema.
        request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, NO_ATTRIBUTES, NO_SCHEMA);
        resultBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION_2, NO_ATTRIBUTES, getTestAttributeDefinitions(), NO_SCHEMA,
            resultBusinessObjectFormat);

        // Perform another update also without specifying both business object format attributes and schema.
        request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_3, NO_ATTRIBUTES, NO_SCHEMA);
        resultBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION_3, NO_ATTRIBUTES, getTestAttributeDefinitions(), NO_SCHEMA,
            resultBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatRemoveSchema()
    {
        // Create an initial version of a business object format with a description and a schema.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat();

        // Perform an update to remove the business object format schema.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, getNewAttributes2(), NO_SCHEMA);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION_2, getNewAttributes2(), getTestAttributeDefinitions(), NO_SCHEMA,
            resultBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatTrimParameters()
    {
        // Create an initial version of a business object format.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat(getNewAttributes());

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and using input parameters with leading and trailing empty spaces.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(addWhitespace(FORMAT_DESCRIPTION_2),
            Arrays.asList(new Attribute(addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE), addWhitespace(ATTRIBUTE_VALUE_1))), addWhitespace(getTestSchema2()));
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService.updateBusinessObjectFormat(
            new BusinessObjectFormatKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, addWhitespace(FORMAT_DESCRIPTION_2),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, addWhitespace(ATTRIBUTE_VALUE_1))), getTestAttributeDefinitions(), getTestSchema2(),
            updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatUpperCaseParameters()
    {
        // Create an initial version of the format using lower case input parameters.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toLowerCase(), LATEST_VERSION_FLAG_SET,
                PARTITION_KEY.toLowerCase(), NO_PARTITION_KEY_GROUP,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())));

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2.toLowerCase());

        // Perform an update by changing the format description and schema.
        Schema testSchema = getTestSchema2();
        testSchema.setPartitionKeyGroup(testSchema.getPartitionKeyGroup().toUpperCase());
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2.toUpperCase(),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())), testSchema);
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService.updateBusinessObjectFormat(
            new BusinessObjectFormatKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema2();
        expectedSchema.setPartitionKeyGroup(expectedSchema.getPartitionKeyGroup().toLowerCase());
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toLowerCase(),
            FORMAT_DESCRIPTION_2.toUpperCase(), Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toUpperCase())), null,
            expectedSchema, updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatLowerCaseParameters()
    {
        // Create an initial version of the format using upper case input parameters.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toUpperCase(),
                NO_PARTITION_KEY_GROUP, Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())));

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2.toUpperCase());

        // Perform an update by changing the format description and schema.
        Schema testSchema = getTestSchema2();
        testSchema.setPartitionKeyGroup(testSchema.getPartitionKeyGroup().toLowerCase());
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2.toLowerCase(),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())), getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService.updateBusinessObjectFormat(
            new BusinessObjectFormatKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema2();
        expectedSchema.setPartitionKeyGroup(expectedSchema.getPartitionKeyGroup().toUpperCase());
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toUpperCase(),
            FORMAT_DESCRIPTION_2.toLowerCase(), Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toLowerCase())), null,
            expectedSchema, updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatInvalidParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Create a business object format update request.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, getNewAttributes2(), getTestSchema2());

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update on our business object format.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the results.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION_2, getNewAttributes2(), NO_ATTRIBUTE_DEFINITIONS, getTestSchema2(),
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
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
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
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
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
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
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
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION),
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
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999),
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
                    createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, Arrays
                        .asList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_3),
                            new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_3)), getTestSchema2()));
            fail("Should throw an IllegalArgumentException when duplicate attributes are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate attribute name found: %s", ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase()), e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatAlreadyExistingDuplicateAttributes()
    {
        // Create and persist a business object format with duplicate attributes.
        createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, NO_PARTITION_KEY_GROUP, Arrays
            .asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1),
                new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_2)));

        // Try to update a business object format instance that already has duplicate attributes.
        try
        {
            businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                    createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2,
                        Arrays.asList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_3)), getTestSchema2()));
            fail("Should throw an IllegalStateException when business object format has duplicate attributes.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Found duplicate attribute with name \"%s\" for business object format {%s}.", ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(),
                getExpectedBusinessObjectFormatKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectFormatNoInitialDescriptionAndSchema()
    {
        // Create an initial version of a business object format without description and schema.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, null, true, PARTITION_KEY);

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and schema.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, getNewAttributes2(), getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION_2, getNewAttributes2(), NO_ATTRIBUTE_DEFINITIONS, getTestSchema2(),
            updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatNoChangesToDescriptionAndSchema()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat();

        // Perform an update without making any changes to format description and schema information.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION, getNewAttributes2(), getTestSchema());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes2(), getTestAttributeDefinitions(),
            getTestSchema(), updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatInitialSchemaPresentAndDataIsRegistered()
    {
        // Create an initial version of a business object format with format description and schema information.
        createTestBusinessObjectFormat();

        // Retrieve the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Create a business object data entity associated with the business object format.
        createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Perform an update by changing the schema.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION, getNewAttributes2(), getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true, PARTITION_KEY,
            FORMAT_DESCRIPTION, getNewAttributes2(), getTestAttributeDefinitions(), getTestSchema2(), updatedBusinessObjectFormat);

        // TODO: This will become a negative test case once we restore the update schema check in updateBusinessObjectFormat() method.
        // Try to update business object format schema information.
        //BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION, getTestSchema2());
        //try
        //{
        //    businessObjectFormatService.updateBusinessObjectFormat(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);
        //    fail("Should throw an IllegalArgumentException when updating schema information for a business object format " +
        //        "that has an existing schema defined and business object data associated with it.");
        //}
        //catch (IllegalArgumentException e)
        //{
        //    assertEquals(String.format("Can not update schema information for a business object format that has an existing schema defined and business " +
        //        "object data associated with it. Business object format: {businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
        //        "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
        //        INITIAL_FORMAT_VERSION), e.getMessage());
        //}
    }

    @Test
    public void testGetBusinessObjectFormat()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat(getNewAttributes());

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(),
            getTestSchema(), resultBusinessObjectFormat);
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
    public void testGetBusinessObjectFormatMissingOptionalParameters()
    {
        // Create and persist a business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Perform a get without specifying business object format version (passed as a null value) and namespace code.
        // Please note that HerdDaoTest.testGetBusinessObjectFormatByAltKeyFormatVersionNotSpecified
        // already validates the SQl select logic, so we do not have to go over it here.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatTrimParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Perform a get using input parameters with leading and trailing empty spaces.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.getBusinessObjectFormat(
            new BusinessObjectFormatKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatUpperCaseParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toLowerCase(), LATEST_VERSION_FLAG_SET,
                PARTITION_KEY.toLowerCase());

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.getBusinessObjectFormat(
            new BusinessObjectFormatKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toLowerCase(), FORMAT_DESCRIPTION.toLowerCase(),
            NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatLowerCaseParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toUpperCase(), LATEST_VERSION_FLAG_SET,
                PARTITION_KEY.toUpperCase());

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.getBusinessObjectFormat(
            new BusinessObjectFormatKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION.toUpperCase(),
            NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatInvalidParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Validate that we can perform a get on our business object format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, resultBusinessObjectFormat);

        // Try to perform a get using invalid namespace code.
        try
        {
            businessObjectFormatService.getBusinessObjectFormat(
                new BusinessObjectFormatKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
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
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
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
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
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
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION),
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
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999),
                e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectFormats()
    {
        // Create and persist the relative business object definitions.
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : getTestBusinessObjectFormatKeys())
        {
            createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, false, PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition.
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());

        // Retrieve a list of the latest version business object format keys for the specified business object definition.
        resultKeys = businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), true);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatLatestVersionKeys(), resultKeys.getBusinessObjectFormatKeys());
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
    public void testGetBusinessObjectFormatsMissingOptionalParameters()
    {
        // Create and persist the relative business object definitions.
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : getTestBusinessObjectFormatKeys())
        {
            createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, false, PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object
        // definition without passing the latestBusinessObjectFormatVersion flag.
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGetBusinessObjectFormatsTrimParameters()
    {
        // Create and persist the relative business object definitions.
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : getTestBusinessObjectFormatKeys())
        {
            createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, false, PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition using input parameters with leading and trailing empty spaces.
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME)), false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGetBusinessObjectFormatsUpperCaseParameters()
    {
        // Create and persist the relative business object definitions.
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : getTestBusinessObjectFormatKeys())
        {
            createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, false, PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition using upper case input parameters
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()), false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGetBusinessObjectFormatsLowerCaseParameters()
    {
        // Create and persist the relative business object definitions.
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : getTestBusinessObjectFormatKeys())
        {
            createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, false, PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition using lower case input parameters.
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()), false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());
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
    public void testDeleteBusinessObjectFormat() throws Exception
    {
        // Create an initial version of a business object format.
        createTestBusinessObjectFormat(getNewAttributes());

        // Retrieve the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService
            .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), getTestSchema(),
            deletedBusinessObjectFormat);

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
    public void testDeleteBusinessObjectFormatMissingOptionalParameters() throws Exception
    {
        // Create and persist a business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Delete the business object format without specifying namespace code.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService
            .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, deletedBusinessObjectFormat);
        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatTrimParameters()
    {
        // Create and persist a business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Ensure that this business object format exists.
        assertNotNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService.deleteBusinessObjectFormat(
            new BusinessObjectFormatKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatUpperCaseParameters()
    {
        // Create and persist a business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toLowerCase(), LATEST_VERSION_FLAG_SET,
                PARTITION_KEY.toLowerCase());

        // Ensure that this business object format exists.
        assertNotNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));

        // Delete the business object format by passing alternate key value in upper case.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService.deleteBusinessObjectFormat(
            new BusinessObjectFormatKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toLowerCase(), FORMAT_DESCRIPTION.toLowerCase(),
            NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatLowerCaseParameters()
    {
        // Create and persist a business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toUpperCase(), LATEST_VERSION_FLAG_SET,
                PARTITION_KEY.toUpperCase());

        // Ensure that this business object format exists.
        assertNotNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));

        // Delete the business object format by passing alternate key value in lower case.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService.deleteBusinessObjectFormat(
            new BusinessObjectFormatKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION.toUpperCase(),
            NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatInvalidParameters()
    {
        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Validate that we can perform a get on our business object format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, resultBusinessObjectFormat);

        // Try to perform a delete using invalid namespace code.
        try
        {
            businessObjectFormatService.deleteBusinessObjectFormat(
                new BusinessObjectFormatKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
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
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
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
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
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
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION),
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
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 999),
                e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectFormatNotLatestVersion()
    {
        // Create and persist a business object format which is not marked as the latest version.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, false,
                PARTITION_KEY);

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService
            .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
            false, PARTITION_KEY, FORMAT_DESCRIPTION, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatLatestVersionWhenPreviousVersionExists()
    {
        // Create and persist two versions of the business object format.
        BusinessObjectFormatEntity initialVersionBusinessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, false,
                PARTITION_KEY);
        BusinessObjectFormatEntity latestVersionBusinessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Validate that the initial version does not have the latest version flag set and that second version has it set.
        assertFalse(initialVersionBusinessObjectFormatEntity.getLatestVersion());
        assertTrue(latestVersionBusinessObjectFormatEntity.getLatestVersion());

        // Delete the latest (second) version of the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService
            .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(latestVersionBusinessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, PARTITION_KEY, FORMAT_DESCRIPTION, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA,
            deletedBusinessObjectFormat);

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
    public void testDeleteBusinessObjectFormatDataIsRegistered()
    {
        // Create an initial version of a business object format.
        createTestBusinessObjectFormat();

        // Retrieve the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Create a business object data entity associated with the business object format.
        createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, INITIAL_DATA_VERSION, true, BDATA_STATUS);

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
    public void testGenerateBusinessObjectFormatDdl()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        // Retrieve business object data ddl.
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME));

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
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
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
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
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
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
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
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
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
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
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
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
            businessObjectFormatService.generateBusinessObjectFormatDdl(request);
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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

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
            List<SchemaColumn> partitionColumns = getTestPartitionColumns().subList(0, 1);
            String partitionKey = partitionColumns.get(0).getName();
            BusinessObjectFormatEntity businessObjectFormatEntity =
                createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, FORMAT_DESCRIPTION,
                    LATEST_VERSION_FLAG_SET, partitionKey, NO_PARTITION_KEY_GROUP, getNewAttributes(), SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                    SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), partitionColumns);

            for (String partitionValue : UNSORTED_PARTITION_VALUES)
            {
                BusinessObjectDataEntity businessObjectDataEntity =
                    createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, partitionValue,
                        NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
                String s3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(S3_KEY_PREFIX_VELOCITY_TEMPLATE, businessObjectFormatEntity,
                    businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity), storageEntity.getName());
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
            BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

            // Validate the results.
            String expectedHiveFileFormat = businessObjectFormatFileTypeMap.get(businessObjectFormatFileType);
            String expectedDdl = getExpectedDdl(partitionColumns.size(), FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, expectedHiveFileFormat,
                businessObjectFormatFileType, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
            validateBusinessObjectFormatDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION,
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
    public void testGenerateBusinessObjectFormatDdlNoDropTable()
    {
        // Prepare test data without custom ddl.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, SCHEMA_DELIMITER_PIPE,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), getTestPartitionColumns(), CUSTOM_DDL_NAME);

        // Retrieve business object data ddl request without drop table statement.
        BusinessObjectFormatDdlRequest request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        request.setIncludeDropTableStatement(false);
        BusinessObjectFormatDdl resultDdl = businessObjectFormatService.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        String expectedDdl =
            getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, false, true);
        validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, expectedDdl, resultDdl);
    }

    /**
     * Asserts that when replace columns is TRUE.
     */
    @Test
    public void testGenerateBusinessObjectFormatDdlWhenReplaceColumns()
    {
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = getTestBusinessObjectFormatDdlRequest(BLANK_TEXT);
        businessObjectFormatDdlRequest.setReplaceColumns(true);
        businessObjectFormatDdlRequest.setIncludeDropTableStatement(false);
        businessObjectFormatDdlRequest.setIncludeIfNotExistsOption(false);
        BusinessObjectFormatDdl result = businessObjectFormatService.generateBusinessObjectFormatDdl(businessObjectFormatDdlRequest);

        Assert.assertEquals("result DDL", result.getDdl(), "ALTER TABLE `" + businessObjectFormatDdlRequest.getTableName() + "` REPLACE COLUMNS (\n" +
            "    `COLUMN001` TINYINT,\n" +
            "    `COLUMN002` SMALLINT COMMENT 'This is \\'COLUMN002\\' column. Here are \\'single\\' and \"double\" quotes along with a backslash \\.',\n" +
            "    `COLUMN003` INT,\n" +
            "    `COLUMN004` BIGINT,\n" +
            "    `COLUMN005` FLOAT,\n" +
            "    `COLUMN006` DOUBLE,\n" +
            "    `COLUMN007` DECIMAL,\n" +
            "    `COLUMN008` DECIMAL(p,s),\n" +
            "    `COLUMN009` DECIMAL,\n" +
            "    `COLUMN010` DECIMAL(p),\n" +
            "    `COLUMN011` DECIMAL(p,s),\n" +
            "    `COLUMN012` TIMESTAMP,\n" +
            "    `COLUMN013` DATE,\n" +
            "    `COLUMN014` STRING,\n" +
            "    `COLUMN015` VARCHAR(n),\n" +
            "    `COLUMN016` VARCHAR(n),\n" +
            "    `COLUMN017` CHAR(n),\n" +
            "    `COLUMN018` BOOLEAN,\n" +
            "    `COLUMN019` BINARY);");
    }

    /**
     * Asserts that when replace columns is TRUE and other optional parameters are not specified.
     */
    @Test
    public void testGenerateBusinessObjectFormatDdlWhenReplaceColumnsMissingOptionalParameters()
    {
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = getTestBusinessObjectFormatDdlRequest(null);
        businessObjectFormatDdlRequest.setReplaceColumns(true);
        businessObjectFormatDdlRequest.setIncludeDropTableStatement(null);
        businessObjectFormatDdlRequest.setIncludeIfNotExistsOption(null);
        BusinessObjectFormatDdl result = businessObjectFormatService.generateBusinessObjectFormatDdl(businessObjectFormatDdlRequest);

        Assert.assertEquals("result DDL", result.getDdl(), "ALTER TABLE `" + businessObjectFormatDdlRequest.getTableName() + "` REPLACE COLUMNS (\n" +
            "    `COLUMN001` TINYINT,\n" +
            "    `COLUMN002` SMALLINT COMMENT 'This is \\'COLUMN002\\' column. Here are \\'single\\' and \"double\" quotes along with a backslash \\.',\n" +
            "    `COLUMN003` INT,\n" +
            "    `COLUMN004` BIGINT,\n" +
            "    `COLUMN005` FLOAT,\n" +
            "    `COLUMN006` DOUBLE,\n" +
            "    `COLUMN007` DECIMAL,\n" +
            "    `COLUMN008` DECIMAL(p,s),\n" +
            "    `COLUMN009` DECIMAL,\n" +
            "    `COLUMN010` DECIMAL(p),\n" +
            "    `COLUMN011` DECIMAL(p,s),\n" +
            "    `COLUMN012` TIMESTAMP,\n" +
            "    `COLUMN013` DATE,\n" +
            "    `COLUMN014` STRING,\n" +
            "    `COLUMN015` VARCHAR(n),\n" +
            "    `COLUMN016` VARCHAR(n),\n" +
            "    `COLUMN017` CHAR(n),\n" +
            "    `COLUMN018` BOOLEAN,\n" +
            "    `COLUMN019` BINARY);");
    }

    /**
     * Asserts that when replace columns is TRUE, then setting includeDropTableStatement to TRUE will cause an exception.
     */
    @Test
    public void testGenerateBusinessObjectFormatDdlWhenReplaceColumnsAndIncludeDropTableStatementThrowsError()
    {
        final BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = getTestBusinessObjectFormatDdlRequest(null);
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
        final BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = getTestBusinessObjectFormatDdlRequest(null);
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
     * Asserts that when replace columns is TRUE, then setting customDdlName to non-null will cause an exception.
     */
    @Test
    public void testGenerateBusinessObjectFormatDdlWhenReplaceColumnsAndCustomDdlNameThrowsError()
    {
        final BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = getTestBusinessObjectFormatDdlRequest(null);
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

    @Test
    public void testGenerateBusinessObjectFormatDdlCollection()
    {
        // Prepare database entities required for testing.
        createDatabaseEntitiesForBusinessObjectFormatDdlCollectionTesting();

        // Generate DDL for a collection of business object data.
        BusinessObjectFormatDdlCollectionResponse resultBusinessObjectFormatDdlCollectionResponse =
            businessObjectFormatService.generateBusinessObjectFormatDdlCollection(getTestBusinessObjectFormatDdlCollectionRequest());

        // Validate the response object.
        assertEquals(getExpectedBusinessObjectFormatDdlCollectionResponse(), resultBusinessObjectFormatDdlCollectionResponse);
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
}

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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdl;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKeys;
import org.finra.herd.model.api.xml.BusinessObjectFormatUpdateRequest;
import org.finra.herd.model.api.xml.Schema;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
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
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true, PARTITION_KEY, FORMAT_DESCRIPTION,
            getNewAttributes(), getTestAttributeDefinitions(), getTestSchema(), businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatMissingRequiredParameters()
    {
        // Try to create a business object format instance when attribute name is not specified.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
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
        createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE_CD, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_FILE_TYPE_CODE, PARTITION_KEY_GROUP);

        // Perform a create without specifying optional parameters.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, NO_FORMAT_DESCRIPTION,
                NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, true, PARTITION_KEY,
            NO_FORMAT_DESCRIPTION, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA, resultBusinessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatTrimParameters()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create a first version of the format using input parameters with leading and trailing empty spaces.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), addWhitespace(PARTITION_KEY), addWhitespace(FORMAT_DESCRIPTION),
                Arrays.asList(new Attribute(addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE), addWhitespace(ATTRIBUTE_VALUE_1))), getTestAttributeDefinitions(),
                addWhitespace(getTestSchema()));
        request.getSchema().setPartitionKeyGroup(addWhitespace(PARTITION_KEY_GROUP));

        // Create an initial business object format version.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, true, PARTITION_KEY,
            addWhitespace(FORMAT_DESCRIPTION), Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, addWhitespace(ATTRIBUTE_VALUE_1))),
            getTestAttributeDefinitions(), getTestSchema(), businessObjectFormat);
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
                FORMAT_FILE_TYPE_CODE.toUpperCase(), PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION.toUpperCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())), getTestAttributeDefinitions(),
                getTestSchema());
        request.getSchema().setPartitionKeyGroup(PARTITION_KEY_GROUP.toUpperCase());
        // For he first schema partition column name, use an opposite case from the format partition key was specified in.
        request.getSchema().getPartitions().get(0).setName(PARTITION_KEY.toLowerCase());
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema();
        expectedSchema.setPartitionKeyGroup(PARTITION_KEY_GROUP.toLowerCase());
        expectedSchema.getPartitions().get(0).setName(PARTITION_KEY.toLowerCase());
        validateBusinessObjectFormat(null, NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), 0, true, PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION.toUpperCase(),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())), getTestAttributeDefinitions(),
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
        validateBusinessObjectFormat(null, NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), 0, true, PARTITION_KEY.toLowerCase(), FORMAT_DESCRIPTION.toLowerCase(),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())), getTestAttributeDefinitions(),
            expectedSchema, businessObjectFormat);
    }

    @Test
    public void testCreateBusinessObjectFormatDuplicateAttributes()
    {
        // Try to create a business object format instance when duplicate attributes are specified.
        try
        {
            businessObjectFormatService.createBusinessObjectFormat(
                createBusinessObjectFormatCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                    Arrays.asList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_3),
                        new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_3)), getTestAttributeDefinitions(), getTestSchema()));
            fail("Should throw an IllegalArgumentException when duplicate attributes are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate attribute name found: %s", ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase()), e.getMessage());
        }
    }

    // Tests for Update Business Object Format

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
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION_2, getNewAttributes2(), getTestAttributeDefinitions(), getTestSchema2(),
            updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatMissingRequiredParameters()
    {
        // Try to update a business object format instance when attribute name is not specified.
        try
        {
            businessObjectFormatService.updateBusinessObjectFormat(
                new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, Arrays.asList(new Attribute(BLANK_TEXT, ATTRIBUTE_VALUE_1)), getTestSchema2()));
            fail("Should throw an IllegalArgumentException when attribute name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An attribute name must be specified.", e.getMessage());
        }
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
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, null, getTestAttributeDefinitions(), getTestSchema2(), updatedBusinessObjectFormat);

        // Perform a second update this time using an empty string description parameter.
        request.setDescription("");
        updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, null, getTestAttributeDefinitions(), getTestSchema2(), updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatRemoveAttributes()
    {
        // Create an initial version of a business object format with attributes.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat(getNewAttributes());

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update to remove the business object format attributes.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, NO_ATTRIBUTES, getTestSchema2());
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION_2, NO_ATTRIBUTES, getTestAttributeDefinitions(), getTestSchema2(),
            resultBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatRemoveFormatSchema()
    {
        // Create an initial version of a business object format with a description and a schema.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat();

        // Perform an update to remove the business object format schema.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, null);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION_2, getTestAttributeDefinitions(), null, resultBusinessObjectFormat);
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
            new BusinessObjectFormatKey(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, addWhitespace(FORMAT_DESCRIPTION_2),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, addWhitespace(ATTRIBUTE_VALUE_1))), getTestAttributeDefinitions(), getTestSchema2(),
            updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatUpperCaseParameters()
    {
        // Create an initial version of the format using lower case input parameters.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION.toLowerCase(), true, PARTITION_KEY.toLowerCase(),
                NO_PARTITION_KEY_GROUP, Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())));

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2.toLowerCase());

        // Perform an update by changing the format description and schema.
        Schema testSchema = getTestSchema2();
        testSchema.setPartitionKeyGroup(testSchema.getPartitionKeyGroup().toUpperCase());
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2.toUpperCase(),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())), testSchema);
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService.updateBusinessObjectFormat(
            new BusinessObjectFormatKey(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema2();
        expectedSchema.setPartitionKeyGroup(expectedSchema.getPartitionKeyGroup().toLowerCase());
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION, true, PARTITION_KEY.toLowerCase(), FORMAT_DESCRIPTION_2.toUpperCase(),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toUpperCase())), null, expectedSchema,
            updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatLowerCaseParameters()
    {
        // Create an initial version of the format using upper case input parameters.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
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
            new BusinessObjectFormatKey(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        Schema expectedSchema = getTestSchema2();
        expectedSchema.setPartitionKeyGroup(expectedSchema.getPartitionKeyGroup().toUpperCase());
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), INITIAL_FORMAT_VERSION, true, PARTITION_KEY.toUpperCase(), FORMAT_DESCRIPTION_2.toLowerCase(),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toLowerCase())), null, expectedSchema,
            updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatDuplicateAttributes()
    {
        // Try to update a business object format instance when duplicate attributes are specified.
        try
        {
            businessObjectFormatService
                .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
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

    // Tests for Get Business Object Format

    @Test
    public void testGetBusinessObjectFormat()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat(getNewAttributes());

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), getTestSchema(),
            resultBusinessObjectFormat);
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
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME), false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());

        // Retrieve a list of the latest version business object format keys for the specified business object definition.
        resultKeys = businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME), true);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatLatestVersionKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    // Tests for Delete Business Object Format

    @Test
    public void testDeleteBusinessObjectFormat() throws Exception
    {
        // Create an initial version of a business object format.
        createTestBusinessObjectFormat(getNewAttributes());

        // Retrieve the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService
            .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), getTestSchema(),
            deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
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
    public void testGenerateBusinessObjectFormatDdlNoCustomDdlNoPartitioning()
    {
        // Prepare test data without custom ddl.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY,
            SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, getTestSchemaColumns(), null, NO_CUSTOM_DDL_NAME);

        // Retrieve business object data ddl.
        BusinessObjectFormatDdl resultDdl =
            businessObjectFormatService.generateBusinessObjectFormatDdl(getTestBusinessObjectFormatDdlRequest(NO_CUSTOM_DDL_NAME));

        // Validate the results.
        String expectedDdl =
            getExpectedDdl(0, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, NO_PARTITION_VALUES, NO_SUBPARTITION_VALUES, false, true, true);
        validateBusinessObjectFormatDdl(NO_CUSTOM_DDL_NAME, expectedDdl, resultDdl);
    }

    /**
     * This method is to get the coverage for the business object format service method that starts the new transaction.
     */
    @Test
    public void testBusinessObjectDataServiceMethodsNewTx() throws Exception
    {
        BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = new BusinessObjectFormatDdlRequest();
        try
        {
            businessObjectFormatServiceImpl.generateBusinessObjectFormatDdl(businessObjectFormatDdlRequest);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
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

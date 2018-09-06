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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.velocity.exception.MethodInvocationException;
import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;

/**
 * This class tests the getS3KeyPrefix functionality within the storage unit service.
 */
public class StorageUnitServiceGetS3KeyPrefixTest extends AbstractServiceTest
{
    @Test
    public void testGetS3KeyPrefix()
    {
        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get an S3 key prefix by passing all parameters including partition key, business object data version,
        // and "create new version" flag (has no effect when data version is specified).
        S3KeyPrefixInformation resultS3KeyPrefixInformation = storageUnitService.getS3KeyPrefix(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), testPartitionKey, STORAGE_NAME, false);

        // Get the expected S3 key prefix value using the business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), DATA_VERSION);

        // Validate the results.
        assertEquals(new S3KeyPrefixInformation(expectedS3KeyPrefix), resultS3KeyPrefixInformation);
    }

    @Test
    public void testGetS3KeyPrefixMissingRequiredParameters()
    {
        // Try to get an S3 key prefix when business object definition name is not specified.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, STORAGE_NAME, false);
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to get an S3 key prefix when business object format usage is not specified.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, STORAGE_NAME, false);
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to get an S3 key prefix when business object format file type is not specified.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, STORAGE_NAME, false);
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to get an S3 key prefix when partition value is not specified.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, STORAGE_NAME, false);
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to get an S3 key prefix when subpartition value is not specified.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT), DATA_VERSION), PARTITION_KEY, STORAGE_NAME, false);
            fail("Should throw an IllegalArgumentException when subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetS3KeyPrefixMissingOptionalParameters()
    {
        // Create a business object definition.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Test if we can get an S3 key prefix without specifying optional parameters
        // including passing all allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Get the test partition columns.
            List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
            String testPartitionKey = testPartitionColumns.get(0).getName();
            List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, subPartitionValues.size() + 1);

            // Get an S3 key prefix without passing any of the optional parameters.
            S3KeyPrefixInformation resultS3KeyPrefixInformation = storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    null), BLANK_TEXT, BLANK_TEXT, null);

            // Get the expected S3 key prefix value using the initial business object data version.
            String expectedS3KeyPrefix =
                getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                    PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                    SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), INITIAL_DATA_VERSION);

            // Validate the results.
            assertEquals(new S3KeyPrefixInformation(expectedS3KeyPrefix), resultS3KeyPrefixInformation);
        }
    }

    @Test
    public void testGetS3KeyPrefixMissingOptionalParametersPassedAsNulls()
    {
        // Create a business object definition.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();

        // Get an S3 key prefix by passing null values for the namespace and partition key.
        S3KeyPrefixInformation resultS3KeyPrefixInformation = storageUnitService.getS3KeyPrefix(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES,
                DATA_VERSION), null, null, false);

        // Get the expected S3 key prefix value using the business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, null, null, DATA_VERSION);

        // Validate the results.
        assertEquals(new S3KeyPrefixInformation(expectedS3KeyPrefix), resultS3KeyPrefixInformation);
    }

    @Test
    public void testGetS3KeyPrefixTrimParameters()
    {
        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get an S3 key prefix by using input parameters with leading and trailing empty spaces.
        S3KeyPrefixInformation resultS3KeyPrefixInformation = storageUnitService.getS3KeyPrefix(
            new BusinessObjectDataKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION),
            addWhitespace(testPartitionKey), addWhitespace(STORAGE_NAME), false);

        // Get the expected S3 key prefix value using the business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), DATA_VERSION);

        // Validate the results.
        assertEquals(new S3KeyPrefixInformation(expectedS3KeyPrefix), resultS3KeyPrefixInformation);
    }

    @Test
    public void testGetS3KeyPrefixUpperCaseParameters()
    {
        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get an S3 key prefix using upper case input parameters (except for case-sensitive partition values).
        S3KeyPrefixInformation resultS3KeyPrefixInformation = storageUnitService.getS3KeyPrefix(
            new BusinessObjectDataKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION), testPartitionKey.toUpperCase(), STORAGE_NAME.toUpperCase(), false);

        // Get the expected S3 key prefix value using the business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), DATA_VERSION);

        // Validate the results.
        assertEquals(new S3KeyPrefixInformation(expectedS3KeyPrefix), resultS3KeyPrefixInformation);
    }

    @Test
    public void testGetS3KeyPrefixLowerCaseParameters()
    {
        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get an S3 key prefix using lower case input parameters (except for case-sensitive partition values).
        S3KeyPrefixInformation resultS3KeyPrefixInformation = storageUnitService.getS3KeyPrefix(
            new BusinessObjectDataKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION), testPartitionKey.toLowerCase(), STORAGE_NAME.toLowerCase(), false);

        // Get the expected S3 key prefix value using the business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), DATA_VERSION);

        // Validate the results.
        assertEquals(new S3KeyPrefixInformation(expectedS3KeyPrefix), resultS3KeyPrefixInformation);
    }

    @Test
    public void testGetS3KeyPrefixInvalidParameters()
    {
        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();

        // Try to get an S3 key prefix using invalid namespace.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), testPartitionKey, STORAGE_NAME, false);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }

        // Try to get an S3 key prefix using invalid business object definition name.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), testPartitionKey, STORAGE_NAME, false);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }

        // Try to get an S3 key prefix using invalid format usage.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), testPartitionKey, STORAGE_NAME, false);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }

        // Try to get an S3 key prefix using invalid format file type.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), testPartitionKey, STORAGE_NAME, false);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION),
                e.getMessage());
        }

        // Try to get an S3 key prefix using invalid business object format version.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), testPartitionKey, STORAGE_NAME, false);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to get an S3 key prefix using invalid partition key.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), "I_DO_NOT_EXIST", STORAGE_NAME, false);
            fail("Should throw an IllegalArgumentException when using an invalid partition key.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Partition key \"%s\" doesn't match configured business object format partition key \"%s\".", "I_DO_NOT_EXIST", testPartitionKey),
                e.getMessage());
        }

        // Try to get an S3 key prefix using invalid storage name.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), testPartitionKey, "I_DO_NOT_EXIST", false);
            fail("Should throw an ObjectNotFoundException when using an invalid storage name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", "I_DO_NOT_EXIST"), e.getMessage());
        }

        // Try to get an S3 key prefix using too many subpartition values.
        try
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.add("EXTRA_SUBPARTITION_VALUE");
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    testSubPartitionValues, DATA_VERSION), testPartitionKey, STORAGE_NAME, false);
            fail("Should throw an IllegalArgumentException when passing too many subpartition values.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Exceeded maximum number of allowed subpartitions: %d.", BusinessObjectDataEntity.MAX_SUBPARTITIONS), e.getMessage());
        }
    }

    @Test
    public void testGetS3KeyPrefixNonS3Storage()
    {
        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(true);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();

        // Create a non-S3 storage.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_2, STORAGE_PLATFORM_CODE);

        // Try to get an S3 key prefix specifying non-S3 storage.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), testPartitionKey, STORAGE_NAME_2, false);
            fail("Should throw an IllegalArgumentException when specifying an non-S3 storage.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The specified storage \"%s\" is not an S3 storage platform.", STORAGE_NAME_2), e.getMessage());
        }
    }

    @Test
    public void testGetS3KeyPrefixMissingKeyPrefixVelocityTemplate()
    {
        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(true);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();

        // Create an S3 storage entity without the S3 key prefix template configured.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_2, StoragePlatformEntity.S3);

        // Try to get an S3 key prefix specifying an S3 storage without the S3 key prefix velocity template configured.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), testPartitionKey, STORAGE_NAME_2, false);
            fail("Should throw an IllegalArgumentException when specifying an S3 storage without the S3 key prefix velocity template configured.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage \"%s\" has no S3 key prefix velocity template configured.", STORAGE_NAME_2), e.getMessage());
        }

        // Create an S3 storage entity with the S3 key prefix template configured to a blank string.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_3, StoragePlatformEntity.S3,
            Arrays.asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), BLANK_TEXT)));

        // Try to get an S3 key prefix specifying an S3 storage with the S3 key prefix velocity template being a blank string.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), testPartitionKey, STORAGE_NAME_3, false);
            fail("Should throw an IllegalArgumentException when specifying an S3 storage with the S3 key prefix velocity template being a blank string.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage \"%s\" has no S3 key prefix velocity template configured.", STORAGE_NAME_3), e.getMessage());
        }
    }

    @Test
    public void testGetS3KeyPrefixKeyPrefixVelocityTemplateProducesBlankS3KeyPrefix()
    {
        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(true);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();

        final String testS3KeyPrefixVelocityTemplate = "#if($CollectionUtils.isNotEmpty($businessObjectDataSubPartitions.keySet()))#end";

        // Create an S3 storage entity with the S3 key prefix template that would result in a blank S3 key prefix value.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_2, StoragePlatformEntity.S3, Arrays.asList(
            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                testS3KeyPrefixVelocityTemplate)));

        // Try to get an S3 key prefix specifying an S3 storage with the S3 key prefix velocity template that results in a blank S3 key prefix value.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), testPartitionKey, STORAGE_NAME_2, false);
            fail("Should throw an IllegalArgumentException when specifying an S3 storage with the S3 key prefix velocity template " +
                "that results in a blank S3 key prefix value.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("S3 key prefix velocity template \"%s\" configured for \"%s\" storage results in an empty S3 key prefix.",
                testS3KeyPrefixVelocityTemplate, STORAGE_NAME_2), e.getMessage());
        }
    }

    @Test
    public void testGetS3KeyPrefixUndefinedVelocityVariable()
    {
        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(true);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();

        // Create an undefined velocity variable.
        String undefinedVelocityVariable = "$UNDEFINED_VARIABLE";

        // Create an S3 storage entity with the S3 key prefix template containing an undefined variable.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_2, StoragePlatformEntity.S3, Arrays.asList(
            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), undefinedVelocityVariable)));

        // Try to get an S3 key prefix when the S3 key prefix velocity template contains an undefined variable.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), testPartitionKey, STORAGE_NAME_2, false);
            fail("Should throw an MethodInvocationException when the S3 key prefix velocity template contains an undefined variable.");
        }
        catch (MethodInvocationException e)
        {
            assertEquals(String.format("Variable %s has not been set at %s[line 1, column 1]", undefinedVelocityVariable,
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE)), e.getMessage());
        }
    }

    /**
     * <p> Tests cases where get S3 key prefix is called with create new version flag set to true and false, and the data does not exist. </p> <h1>Case 1</h1>
     * <ul> <li>Given: <ul> <li>A business object format</li> <li>No business object data</li> </ul> </li> <li>When: <ul> <li>Get S3 key prefix is called with
     * create new version flag set to false</li> </ul> </li> <li>Then: <ul> <li>The S3 key prefix should have data version set to 0</li> </ul> </li> </ul>
     * <h1>Case 2</h1> <ul> <li>Given: <ul> <li>Same as case 1</li> </ul> </li> <li>When: <ul> <li>Get S3 key prefix is called with create new version flag set
     * to true</li> </ul> </li> <li>Then: <ul> <li>The S3 key prefix should have data version set to 0</li> </ul> </li> </ul>
     */
    @Test
    public void testGetS3KeyPrefixNoDataVersionSpecifiedInitialDataVersionNoExists() throws Exception
    {
        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get the expected S3 key prefix value using the initial business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), INITIAL_DATA_VERSION);

        // Get an S3 key prefix for the initial version of the business object data with and without createNewVersion flag set.
        for (Boolean createNewVersionFlag : new Boolean[] {false, true})
        {
            S3KeyPrefixInformation resultS3KeyPrefixInformation = storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    null), testPartitionKey, STORAGE_NAME, createNewVersionFlag);

            // Validate the results.
            assertEquals(new S3KeyPrefixInformation(expectedS3KeyPrefix), resultS3KeyPrefixInformation);
        }
    }

    @Test
    public void testGetS3KeyPrefixNoDataVersionSpecifiedLatestDataVersionExistsCreateNewVersionIsTrue()
    {
        // Create database entities required for testing. Please note that we are passing the flag to create a business object data entity.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(true);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get an S3 key prefix for the next business object data by not passing the business object data version and passing the create new version flag.
        S3KeyPrefixInformation resultS3KeyPrefixInformation = storageUnitService.getS3KeyPrefix(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                null), testPartitionKey, STORAGE_NAME, true);

        // Get the expected S3 key prefix value using the next business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), DATA_VERSION + 1);

        // Validate the results.
        assertEquals(new S3KeyPrefixInformation(expectedS3KeyPrefix), resultS3KeyPrefixInformation);
    }

    @Test
    public void testGetS3KeyPrefixNoDataVersionSpecifiedLatestDataVersionExistsCreateNewVersionIsFalse()
    {
        // Create database entities required for testing. Please note that we are passing the flag to create a business object data entity.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForGetS3KeyPrefixTesting(true);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();

        // Try to get an S3 key prefix for the next business object data with the create new version flag not set to "true".
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    null), testPartitionKey, STORAGE_NAME, false);
        }
        catch (AlreadyExistsException e)
        {
            assertEquals("Initial version of the business object data already exists.", e.getMessage());
        }
    }

    @Test
    public void testGetS3PrefixWhenSchemaWithoutPartitionColumns()
    {
        // Get a list of test schema partition columns and use the first column name as the partition key.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();

        // Create and persist a business object format entity without partitions
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, partitionKey, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), null);

        // Create and persist an S3 storage with the S3 key prefix velocity template attribute.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3,
            configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), S3_KEY_PREFIX_VELOCITY_TEMPLATE);

        // getS3Prefix request should fail with an appropriate message.
        try
        {
            storageUnitService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    null), partitionKey, STORAGE_NAME, false);
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Schema partition(s) must be defined when using subpartition values for business object format {%s}.",
                businessObjectFormatHelper.businessObjectFormatKeyToString(businessObjectFormatHelper
                    .getBusinessObjectFormatKey(businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity)))), e.getMessage());
        }
    }
}

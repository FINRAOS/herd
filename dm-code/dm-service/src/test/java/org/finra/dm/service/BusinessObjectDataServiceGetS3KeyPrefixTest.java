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
package org.finra.dm.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.dm.model.AlreadyExistsException;
import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.S3KeyPrefixInformation;
import org.finra.dm.model.api.xml.SchemaColumn;

/**
 * This class tests the getS3KeyPrefix functionality within the business object data REST controller.
 */
public class BusinessObjectDataServiceGetS3KeyPrefixTest extends AbstractServiceTest
{
    @Test
    public void testGetS3KeyPrefix()
    {
        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get an S3 key prefix by passing all parameters including partition key, business object data version,
        // and "create new version" flag (has no effect when data version is specified).
        S3KeyPrefixInformation resultS3KeyPrefixInformation = businessObjectDataService.getS3KeyPrefix(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), testPartitionKey, false);

        // Get the expected S3 key prefix value using the business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE_CD, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), DATA_VERSION);

        // Validate the results.
        assertNotNull(resultS3KeyPrefixInformation);
        assertEquals(expectedS3KeyPrefix, resultS3KeyPrefixInformation.getS3KeyPrefix());
    }

    @Test
    public void testGetS3KeyPrefixLegacy()
    {
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get an S3 key prefix by calling the legacy endpoint (without a namespace).
        S3KeyPrefixInformation resultS3KeyPrefixInformation = businessObjectDataService.getS3KeyPrefix(
            new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), testPartitionKey, false);

        // Get the expected S3 key prefix value using the business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE_CD, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), DATA_VERSION);

        // Validate the results.
        assertNotNull(resultS3KeyPrefixInformation);
        assertEquals(expectedS3KeyPrefix, resultS3KeyPrefixInformation.getS3KeyPrefix());
    }

    @Test
    public void testGetS3KeyPrefixMissingRequiredParameters()
    {
        // Try to get an S3 key prefix when business object definition name is not specified.
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY, false);
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to get an S3 key prefix when business object format usage is not specified.
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, false);
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to get an S3 key prefix when business object format file type is not specified.
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, false);
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to get an S3 key prefix when partition value is not specified.
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, false);
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to get an S3 key prefix when subpartition value is not specified.
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT), DATA_VERSION), PARTITION_KEY, false);
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
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Test if we can get an S3 key prefix without specifying optional parameters
        // including passing all allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Get the test partition columns.
            List<SchemaColumn> testPartitionColumns = getTestPartitionColumns();
            String testPartitionKey = testPartitionColumns.get(0).getName();
            List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, subPartitionValues.size() + 1);

            // Get an S3 key prefix without passing any of the optional parameters.
            S3KeyPrefixInformation resultS3KeyPrefixInformation = businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(BLANK_TEXT, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    null), BLANK_TEXT, null);

            // Get the expected S3 key prefix value using the initial business object data version.
            String expectedS3KeyPrefix =
                getExpectedS3KeyPrefix(NAMESPACE_CD, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                    PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                    SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), INITIAL_DATA_VERSION);

            // Validate the results.
            assertNotNull(resultS3KeyPrefixInformation);
            assertEquals(expectedS3KeyPrefix, resultS3KeyPrefixInformation.getS3KeyPrefix());
        }
    }

    @Test
    public void testGetS3KeyPrefixMissingOptionalParametersPassedAsNulls()
    {
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();

        // Get an S3 key prefix by passing null values for the namespace and partition key.
        S3KeyPrefixInformation resultS3KeyPrefixInformation = businessObjectDataService.getS3KeyPrefix(
            new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES,
                DATA_VERSION), null, false);

        // Get the expected S3 key prefix value using the business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE_CD, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, null, null, DATA_VERSION);

        // Validate the results.
        assertNotNull(resultS3KeyPrefixInformation);
        assertEquals(expectedS3KeyPrefix, resultS3KeyPrefixInformation.getS3KeyPrefix());
    }

    @Test
    public void testGetS3KeyPrefixTrimParameters()
    {
        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get an S3 key prefix by using input parameters with leading and trailing empty spaces.
        S3KeyPrefixInformation resultS3KeyPrefixInformation = businessObjectDataService.getS3KeyPrefix(
            new BusinessObjectDataKey(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION),
            addWhitespace(testPartitionKey), false);

        // Get the expected S3 key prefix value using the business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE_CD, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), DATA_VERSION);

        // Validate the results.
        assertNotNull(resultS3KeyPrefixInformation);
        assertEquals(expectedS3KeyPrefix, resultS3KeyPrefixInformation.getS3KeyPrefix());
    }

    @Test
    public void testGetS3KeyPrefixUpperCaseParameters()
    {
        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get an S3 key prefix using upper case input parameters (except for case-sensitive partition values).
        S3KeyPrefixInformation resultS3KeyPrefixInformation = businessObjectDataService.getS3KeyPrefix(
            new BusinessObjectDataKey(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION), testPartitionKey.toUpperCase(), false);

        // Get the expected S3 key prefix value using the business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE_CD, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), DATA_VERSION);

        // Validate the results.
        assertNotNull(resultS3KeyPrefixInformation);
        assertEquals(expectedS3KeyPrefix, resultS3KeyPrefixInformation.getS3KeyPrefix());
    }

    @Test
    public void testGetS3KeyPrefixLowerCaseParameters()
    {
        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get an S3 key prefix using lower case input parameters (except for case-sensitive partition values).
        S3KeyPrefixInformation resultS3KeyPrefixInformation = businessObjectDataService.getS3KeyPrefix(
            new BusinessObjectDataKey(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION), testPartitionKey.toLowerCase(), false);

        // Get the expected S3 key prefix value using the business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE_CD, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), DATA_VERSION);

        // Validate the results.
        assertNotNull(resultS3KeyPrefixInformation);
        assertEquals(expectedS3KeyPrefix, resultS3KeyPrefixInformation.getS3KeyPrefix());
    }

    @Test
    public void testGetS3KeyPrefixInvalidParameters()
    {
        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();

        // Try to get an S3 key prefix using invalid namespace.
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey("I_DO_NOT_EXIST", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), testPartitionKey, false);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage("I_DO_NOT_EXIST", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }

        // Try to get an S3 key prefix using invalid business object definition name.
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), testPartitionKey, false);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }

        // Try to get an S3 key prefix using invalid format usage.        
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), testPartitionKey, false);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }

        // Try to get an S3 key prefix using invalid format file type.
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), testPartitionKey, false);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION),
                e.getMessage());
        }

        // Try to get an S3 key prefix using invalid business object format version.
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), testPartitionKey, false);
            fail("Should throw an ObjectNotFoundException when not able to find business object format.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION),
                e.getMessage());
        }

        // Try to get an S3 key prefix using invalid partition key.
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), "I_DO_NOT_EXIST", false);
            fail("Should throw an IllegalArgumentException when using an invalid partition key.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Partition key \"%s\" doesn't match configured business object format partition key \"%s\".", "I_DO_NOT_EXIST", testPartitionKey),
                e.getMessage());
        }

        // Try to get an S3 key prefix using too many subpartition values.
        try
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.add("EXTRA_SUBPARTITION_VALUE");
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    testSubPartitionValues, DATA_VERSION), testPartitionKey, false);
            fail("Should throw an IllegalArgumentException when passing too many subpartition values.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Exceeded maximum number of allowed subpartitions: %d.", BusinessObjectDataEntity.MAX_SUBPARTITIONS), e.getMessage());
        }
    }

    /**
     * Test getS3KeyPrefix() without a namespace when no legacy definition exists.
     */
    @Test
    public void testGetS3KeyPrefixNoLegacyBusinessObjectDefinitionExists()
    {
        // Try to get an S3 key prefix by not specifying a namespace when legacy business object definition does not exist.
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, false);
            fail("Suppose to throw an ObjectNotFoundException when legacy business object definition does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Legacy business object definition with name \"%s\" doesn't exist.", BOD_NAME), e.getMessage());
        }
    }

    /**
     * Test getS3KeyPrefix() without a namespace when multiple legacy business definitions exist. This is an edge case and should not happen for normal
     * operations. The legacy flag is only set for existing definitions and the existing definitions' names are always unique across namespaces. This case would
     * only occur if the database was modified directly.
     */
    @Test
    public void testGetS3KeyPrefixMultipleLegacyBusinessObjectDefinitionsExist()
    {
        // Create multiple legacy business object definitions with the same business object definition name.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Try to get an S3 key prefix by not specifying a namespace when multiple legacy business object definitions exist with the same name.
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, false);
            fail("Suppose to throw an IllegalArgumentException when multiple legacy business object definitions exist with the same name.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Found more than one business object definition with parameters {businessObjectDefinitionName=\"%s\", legacy=\"Y\"}.", BOD_NAME),
                e.getMessage());
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
        createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get the expected S3 key prefix value using the initial business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE_CD, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), INITIAL_DATA_VERSION);

        // Get an S3 key prefix for the initial version of the business object data with and without createNewVersion flag set.
        for (Boolean createNewVersionFlag : new Boolean[] {false, true})
        {
            S3KeyPrefixInformation resultS3KeyPrefixInformation = businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, null), testPartitionKey, createNewVersionFlag);

            // Validate the results.
            assertNotNull(resultS3KeyPrefixInformation);
            assertEquals(expectedS3KeyPrefix, resultS3KeyPrefixInformation.getS3KeyPrefix());
        }
    }

    @Test
    public void testGetS3KeyPrefixNoDataVersionSpecifiedLatestDataVersionExistsCreateNewVersionIsTrue()
    {
        // Create database entities required for testing. Please note that we are passing the flag to create a business object data entity.
        createDatabaseEntitiesForGetS3KeyPrefixTesting(true);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get an S3 key prefix for the next business object data by not passing the business object data version and passing the create new version flag.
        S3KeyPrefixInformation resultS3KeyPrefixInformation = businessObjectDataService.getS3KeyPrefix(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                null), testPartitionKey, true);

        // Get the expected S3 key prefix value using the next business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE_CD, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), DATA_VERSION + 1);

        // Validate the results.
        assertNotNull(resultS3KeyPrefixInformation);
        assertEquals(expectedS3KeyPrefix, resultS3KeyPrefixInformation.getS3KeyPrefix());
    }

    @Test
    public void testGetS3KeyPrefixNoDataVersionSpecifiedLatestDataVersionExistsCreateNewVersionIsFalse()
    {
        // Create database entities required for testing. Please note that we are passing the flag to create a business object data entity.
        createDatabaseEntitiesForGetS3KeyPrefixTesting(true);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();

        // Try to get an S3 key prefix for the next business object data with the create new version flag not set to "true".
        try
        {
            businessObjectDataService.getS3KeyPrefix(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, null), testPartitionKey, false);
        }
        catch (AlreadyExistsException e)
        {
            assertEquals("Initial version of the business object data already exists.", e.getMessage());
        }
    }
}

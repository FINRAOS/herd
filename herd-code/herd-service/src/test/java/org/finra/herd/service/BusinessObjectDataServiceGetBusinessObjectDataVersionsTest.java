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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataVersions;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;

/**
 * This class tests get business object data versions functionality within the business object data REST controller.
 */
public class BusinessObjectDataServiceGetBusinessObjectDataVersionsTest extends AbstractServiceTest
{
    private static final int NUMBER_OF_FORMAT_VERSIONS = 2;

    private static final int NUMBER_OF_DATA_VERSIONS_PER_FORMAT_VERSION = 3;

    @Test
    public void testGetBusinessObjectDataVersions()
    {
        // Create and persist test database entities.
        createTestDatabaseEntities(SUBPARTITION_VALUES);

        // Retrieve the relative business object data version by specifying values for all input parameters.
        for (int businessObjectFormatVersion = INITIAL_FORMAT_VERSION; businessObjectFormatVersion < NUMBER_OF_FORMAT_VERSIONS; businessObjectFormatVersion++)
        {
            for (int businessObjectDataVersion = INITIAL_DATA_VERSION; businessObjectDataVersion < NUMBER_OF_DATA_VERSIONS_PER_FORMAT_VERSION;
                businessObjectDataVersion++)
            {
                BusinessObjectDataVersions businessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
                    new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion, PARTITION_VALUE,
                        SUBPARTITION_VALUES, businessObjectDataVersion));

                // Validate the returned object.
                assertNotNull(businessObjectDataVersions);
                assertEquals(1, businessObjectDataVersions.getBusinessObjectDataVersions().size());
                businessObjectDataServiceTestHelper
                    .validateBusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion, PARTITION_VALUE,
                        SUBPARTITION_VALUES, businessObjectDataVersion,
                        businessObjectDataVersions.getBusinessObjectDataVersions().get(0).getBusinessObjectDataKey());
                assertEquals(BDATA_STATUS, businessObjectDataVersions.getBusinessObjectDataVersions().get(0).getStatus());
            }
        }
    }

    @Test
    public void testGetBusinessObjectDataVersionsMissingRequiredParameters()
    {
        // Try to get business object data versions when namespace is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectDataVersions(
                new BusinessObjectDataKey(BLANK_TEXT, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to get business object data versions when business object definition name is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectDataVersions(
                new BusinessObjectDataKey(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to get business object data versions when business object format usage is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectDataVersions(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to get business object data versions when business object format file type is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectDataVersions(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to get business object data versions when partition value is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectDataVersions(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT, SUBPARTITION_VALUES,
                    DATA_VERSION));
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to get business object data versions when subpartition value is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectDataVersions(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT), DATA_VERSION));
            fail("Should throw an IllegalArgumentException when subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataMissingOptionalParameters()
    {
        // Create and persist a business object data entities without subpartition values.
        createTestDatabaseEntities(NO_SUBPARTITION_VALUES);

        // Retrieve business object data versions without specifying any of the optional parameters including the subpartition values.
        BusinessObjectDataVersions resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, null, null));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(NUMBER_OF_FORMAT_VERSIONS * NUMBER_OF_DATA_VERSIONS_PER_FORMAT_VERSION,
            resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());
    }

    @Test
    public void testGetBusinessObjectDataMissingFormatVersionParameter()
    {
        // Create and persist a business object data entities.
        createTestDatabaseEntities(SUBPARTITION_VALUES);

        // Retrieve business object data versions without specifying business object format version.
        BusinessObjectDataVersions resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, SUBPARTITION_VALUES,
                INITIAL_DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(NUMBER_OF_FORMAT_VERSIONS, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());
    }

    @Test
    public void testGetBusinessObjectDataMissingDataVersionParameter()
    {
        // Create and persist a business object data entities.
        createTestDatabaseEntities(SUBPARTITION_VALUES);

        // Retrieve business object data versions without specifying business object data version.
        BusinessObjectDataVersions resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, null));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(NUMBER_OF_DATA_VERSIONS_PER_FORMAT_VERSION, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());
    }

    @Test
    public void testGetBusinessObjectDataAttributeTrimParameters()
    {
        // Create and persist test database entities.
        createTestDatabaseEntities(SUBPARTITION_VALUES);

        // Retrieve the business object data versions using input parameters with leading and trailing empty spaces.
        BusinessObjectDataVersions resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), INITIAL_FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES),
                INITIAL_DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(1, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());
    }

    @Test
    public void testGetBusinessObjectDataUpperCaseParameters()
    {
        // Create and persist test database entities.
        createTestDatabaseEntities(SUBPARTITION_VALUES);

        // Retrieve business object data versions using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectDataVersions resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                INITIAL_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(1, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());
    }

    @Test
    public void testGetBusinessObjectDataLowerCaseParameters()
    {
        // Create and persist test database entities.
        createTestDatabaseEntities(SUBPARTITION_VALUES);

        // Retrieve business object data versions using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectDataVersions resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                INITIAL_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(1, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());
    }

    @Test
    public void testGetBusinessObjectDataInvalidParameters()
    {
        // Create and persist test database entities.
        createTestDatabaseEntities(SUBPARTITION_VALUES);

        // Validate that we can retrieve all business object data versions.
        BusinessObjectDataVersions resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(1, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());

        // Try to get business object data versions using invalid namespace.
        resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(0, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());

        // Try to get business object data versions using invalid business object definition name.
        resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(0, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());

        // Try to get business object data versions using invalid format usage.
        resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(0, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());

        // Try to get business object data versions using invalid format file type.
        resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", INITIAL_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                INITIAL_DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(0, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());

        // Try to get business object data versions using invalid partition value.
        resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, "I_DO_NOT_EXIST",
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(0, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());

        // Try to get business object data versions using invalid subpartition value.
        for (int i = 0; i < SUBPARTITION_VALUES.size(); i++)
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.set(i, "I_DO_NOT_EXIST");
            resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                    testSubPartitionValues, INITIAL_DATA_VERSION));

            // Validate the returned object.
            assertNotNull(resultBusinessObjectDataVersions);
            assertEquals(0, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());

        }

        // Try to get business object data versions using too many subpartition values.
        try
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.add("EXTRA_SUBPARTITION_VALUE");
            businessObjectDataService.getBusinessObjectDataVersions(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                    testSubPartitionValues, INITIAL_DATA_VERSION));
            fail("Should throw an IllegalArgumentException when passing too many subpartition values.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Exceeded maximum number of allowed subpartitions: %d.", BusinessObjectDataEntity.MAX_SUBPARTITIONS), e.getMessage());
        }

        // Try to get business object data versions using invalid business object format version.
        resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(0, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());

        // Try to get business object data versions using invalid business object data version.
        resultBusinessObjectDataVersions = businessObjectDataService.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INVALID_DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(0, resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());
    }

    /**
     * Create and persist database entities required for testing.
     */
    private void createTestDatabaseEntities(List<String> subPartitionValues)
    {
        for (int businessObjectFormatVersion = INITIAL_FORMAT_VERSION; businessObjectFormatVersion < NUMBER_OF_FORMAT_VERSIONS; businessObjectFormatVersion++)
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion,
                    FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, businessObjectFormatVersion == SECOND_FORMAT_VERSION, PARTITION_KEY);

            for (int businessObjectDataVersion = INITIAL_DATA_VERSION; businessObjectDataVersion < NUMBER_OF_DATA_VERSIONS_PER_FORMAT_VERSION;
                businessObjectDataVersion++)
            {
                businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion,
                        PARTITION_VALUE, subPartitionValues, businessObjectDataVersion, businessObjectDataVersion == SECOND_DATA_VERSION, BDATA_STATUS);
            }
        }
    }
}

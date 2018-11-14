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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;

public class BusinessObjectFormatDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetBusinessObjectFormatByAltKeyAllParamsSpecified()
    {
        // Create relative database entities.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);
        fileTypeDaoTestHelper.createFileTypeEntity(FORMAT_FILE_TYPE_CODE, "Description of " + FORMAT_FILE_TYPE_CODE);

        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, "Test format 0", "Document schema 0", false,
                PARTITION_KEY);
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, "Test format 1", "Document schema 1", true,
                PARTITION_KEY);
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 2, "Test format 2", "Document schema 2", false,
                PARTITION_KEY);

        for (int businessObjectFormatVersion = 0; businessObjectFormatVersion < 3; businessObjectFormatVersion++)
        {
            // Retrieve business object format entity by specifying values for all alternate key fields.
            BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
                new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion));

            // Validate the results.
            assertNotNull(businessObjectFormatEntity);
            assertTrue(businessObjectFormatEntity.getBusinessObjectDefinition().getName().equals(BDEF_NAME));
            assertTrue(businessObjectFormatEntity.getUsage().equals(FORMAT_USAGE_CODE));
            assertTrue(businessObjectFormatEntity.getFileType().getCode().equals(FORMAT_FILE_TYPE_CODE));
            assertTrue(businessObjectFormatEntity.getBusinessObjectFormatVersion() == businessObjectFormatVersion);
            assertTrue(businessObjectFormatEntity.getLatestVersion() == (businessObjectFormatVersion == 1));
            assertTrue(businessObjectFormatEntity.getPartitionKey().equals(PARTITION_KEY));
            assertTrue(businessObjectFormatEntity.getDescription().equals(String.format("Test format %d", businessObjectFormatVersion)));
            assertTrue(businessObjectFormatEntity.getDocumentSchema().equals(String.format("Document schema %d", businessObjectFormatVersion)));
        }

        // Try invalid values for all input parameters.
        assertNull(businessObjectFormatDao
            .getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0)));
        assertNull(businessObjectFormatDao
            .getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0)));
        assertNull(businessObjectFormatDao
            .getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, 0)));
        assertNull(
            businessObjectFormatDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", 0)));
        assertNull(businessObjectFormatDao
            .getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 4)));
    }

    @Test
    public void testGetBusinessObjectFormatByAltKeyAllParamsUpperCase()
    {
        // Create relative database entities.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(), BDEF_DESCRIPTION);
        fileTypeDaoTestHelper.createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toLowerCase(), "Description of " + FORMAT_FILE_TYPE_CODE);
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                INITIAL_FORMAT_VERSION, "Test format 0", "Document Schema 0", Boolean.TRUE, PARTITION_KEY);

        // Retrieve business object format entity by specifying values for all text alternate key fields in upper case.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                INITIAL_FORMAT_VERSION));

        // Validate the results.
        assertNotNull(businessObjectFormatEntity);
        assertTrue(businessObjectFormatEntity.getBusinessObjectDefinition().getName().equals(BDEF_NAME.toLowerCase()));
        assertTrue(businessObjectFormatEntity.getUsage().equals(FORMAT_USAGE_CODE.toLowerCase()));
        assertTrue(businessObjectFormatEntity.getFileType().getCode().equals(FORMAT_FILE_TYPE_CODE.toLowerCase()));
        assertTrue(businessObjectFormatEntity.getBusinessObjectFormatVersion().equals(INITIAL_FORMAT_VERSION));
        assertTrue(businessObjectFormatEntity.getLatestVersion());
        assertTrue(businessObjectFormatEntity.getPartitionKey().equals(PARTITION_KEY));
        assertTrue(businessObjectFormatEntity.getDescription().equals("Test format 0"));
        assertTrue(businessObjectFormatEntity.getDocumentSchema().equals("Document Schema 0"));
    }

    @Test
    public void testGetBusinessObjectFormatByAltKeyAllParamsLowerCase()
    {
        // Create relative database entities.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(), BDEF_DESCRIPTION);
        fileTypeDaoTestHelper.createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toUpperCase(), "Description of " + FORMAT_FILE_TYPE_CODE);
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                INITIAL_FORMAT_VERSION, "Test format 0", "Document Schema 0", Boolean.TRUE, PARTITION_KEY);

        // Retrieve business object format entity by specifying values for all text alternate key fields in lower case.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                INITIAL_FORMAT_VERSION));

        // Validate the results.
        assertNotNull(businessObjectFormatEntity);
        assertTrue(businessObjectFormatEntity.getBusinessObjectDefinition().getName().equals(BDEF_NAME.toUpperCase()));
        assertTrue(businessObjectFormatEntity.getUsage().equals(FORMAT_USAGE_CODE.toUpperCase()));
        assertTrue(businessObjectFormatEntity.getFileType().getCode().equals(FORMAT_FILE_TYPE_CODE.toUpperCase()));
        assertTrue(businessObjectFormatEntity.getBusinessObjectFormatVersion().equals(INITIAL_FORMAT_VERSION));
        assertTrue(businessObjectFormatEntity.getLatestVersion());
        assertTrue(businessObjectFormatEntity.getPartitionKey().equals(PARTITION_KEY));
        assertTrue(businessObjectFormatEntity.getDescription().equals("Test format 0"));
        assertTrue(businessObjectFormatEntity.getDocumentSchema().equals("Document Schema 0"));
    }

    @Test
    public void testGetBusinessObjectFormatByAltKeyFormatVersionNotSpecified()
    {
        // Create relative database entities.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);
        fileTypeDaoTestHelper.createFileTypeEntity(FORMAT_FILE_TYPE_CODE, "Description of " + FORMAT_FILE_TYPE_CODE);

        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, "Test format 0", "Document Schema 0", false,
                PARTITION_KEY);
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, "Test format 1", "Document Schema 1", true,
                PARTITION_KEY);
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 2, "Test format 2", "Document Schema 2", false,
                PARTITION_KEY);

        // Retrieve business object format entity by specifying all values for the alternate key fields except for the format version.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao
            .getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));

        // Validate the results.
        assertNotNull(businessObjectFormatEntity);
        assertTrue(businessObjectFormatEntity.getBusinessObjectDefinition().getName().equals(BDEF_NAME));
        assertTrue(businessObjectFormatEntity.getUsage().equals(FORMAT_USAGE_CODE));
        assertTrue(businessObjectFormatEntity.getFileType().getCode().equals(FORMAT_FILE_TYPE_CODE));
        assertTrue(businessObjectFormatEntity.getBusinessObjectFormatVersion() == 1);
        assertTrue(businessObjectFormatEntity.getLatestVersion());
        assertTrue(businessObjectFormatEntity.getPartitionKey().equals(PARTITION_KEY));
        assertTrue(businessObjectFormatEntity.getDescription().equals("Test format 1"));
        assertTrue(businessObjectFormatEntity.getDocumentSchema().equals("Document Schema 1"));

        // Let add a second LATEST format version.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 3, "Test format 3", "Test format 3", true,
                PARTITION_KEY);

        try
        {
            // Now we should get an exception, since there are more than one format with the Latest Version flag set to TRUE.
            businessObjectFormatDao
                .getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));
            fail("Should throw an IllegalArgumentException if finds more than one business object format marked as latest.");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage().startsWith("Found more than one business object format"));
        }
    }

    @Test
    public void testGetBusinessObjectFormatMaxVersion()
    {
        // Create and persist two versions of the business object format.
        for (Integer version : Arrays.asList(INITIAL_FORMAT_VERSION, SECOND_FORMAT_VERSION))
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, version, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, false, PARTITION_KEY);
        }

        // Retrieve the latest (maximum available) business object format version.
        Integer resultLatestVersion = businessObjectFormatDao
            .getBusinessObjectFormatMaxVersion(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));

        // Validate the results.
        assertEquals(SECOND_DATA_VERSION, resultLatestVersion);
    }

    @Test
    public void testGetBusinessObjectFormatCountByPartitionKeyGroup()
    {
        // Create a partition key group.
        PartitionKeyGroupEntity testPartitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create a business object format that uses this partition key group.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY, PARTITION_KEY_GROUP);

        // Get the number of business object formats that use this partition key group.
        Long result = businessObjectFormatDao.getBusinessObjectFormatCountByPartitionKeyGroup(testPartitionKeyGroupEntity);

        // Validate the results.
        assertEquals(Long.valueOf(1L), result);
    }

    @Test
    public void testGetBusinessObjectFormatCountByPartitionKeys()
    {
        // Get a list of partition columns that is larger than number of partitions supported by business object data registration.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        assertTrue(CollectionUtils.size(partitionColumns) > BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1);

        // Get a list of regular columns.
        List<SchemaColumn> regularColumns = schemaColumnDaoTestHelper.getTestSchemaColumns();

        // Create a business object format with schema that has one more partition columns than supported by business object data registration.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, partitionColumns.get(0).getName(), NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, regularColumns, partitionColumns);

        // Create one more namespace.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2);

        // Create one more file type.
        fileTypeDaoTestHelper.createFileTypeEntity(FORMAT_FILE_TYPE_CODE_2);

        // Create a list of partition keys from the list of partition columns up to the maximum
        // number of partition levels supported by business object data registration.
        List<String> partitionKeys = new ArrayList<>();
        for (int i = 0; i < BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1; i++)
        {
            partitionKeys.add(partitionColumns.get(0).getName());
        }

        // Get business object format record count by passing all parameters including
        // the maximum number of partition keys supported by business object data registration.
        assertEquals(Long.valueOf(1L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, partitionKeys));

        // Get business object format record count when passing only required parameters.
        assertEquals(Long.valueOf(1L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, null));

        // Get business object format record count when passing all string parameters in upper case.
        assertEquals(Long.valueOf(1L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, Arrays
                    .asList(partitionColumns.get(0).getName().toUpperCase(), partitionColumns.get(1).getName().toUpperCase(),
                        partitionColumns.get(2).getName().toUpperCase(), partitionColumns.get(3).getName().toUpperCase(),
                        partitionColumns.get(4).getName().toUpperCase())));

        // Get business object format record count when passing all string parameters in lower case.
        assertEquals(Long.valueOf(1L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, Arrays
                    .asList(partitionColumns.get(0).getName().toLowerCase(), partitionColumns.get(1).getName().toLowerCase(),
                        partitionColumns.get(2).getName().toLowerCase(), partitionColumns.get(3).getName().toLowerCase(),
                        partitionColumns.get(4).getName().toLowerCase())));

        // Get business object format record count when passing partition keys in reverse order.
        assertEquals(Long.valueOf(1L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays
                .asList(partitionColumns.get(4).getName(), partitionColumns.get(3).getName(), partitionColumns.get(2).getName(),
                    partitionColumns.get(1).getName(), partitionColumns.get(0).getName())));

        // Get business object format record count when passing a non-existing namespace.
        assertEquals(Long.valueOf(0L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(I_DO_NOT_EXIST, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, partitionKeys));

        // Get business object format record count when passing a non-existing file type.
        assertEquals(Long.valueOf(0L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, I_DO_NOT_EXIST, FORMAT_VERSION, partitionKeys));

        // Get business object format record count when passing an invalid value for each of the parameters individually.
        assertEquals(Long.valueOf(0L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE_2, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, partitionKeys));
        assertEquals(Long.valueOf(0L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, INVALID_VALUE, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, partitionKeys));
        assertEquals(Long.valueOf(0L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, BDEF_NAME, INVALID_VALUE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, partitionKeys));
        assertEquals(Long.valueOf(0L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION, partitionKeys));
        assertEquals(Long.valueOf(0L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, partitionKeys));
        assertEquals(Long.valueOf(0L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays
                .asList(INVALID_VALUE, partitionColumns.get(1).getName(), partitionColumns.get(2).getName(), partitionColumns.get(3).getName(),
                    partitionColumns.get(4).getName())));
        assertEquals(Long.valueOf(0L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays
                .asList(partitionColumns.get(0).getName(), INVALID_VALUE, partitionColumns.get(2).getName(), partitionColumns.get(3).getName(),
                    partitionColumns.get(4).getName())));
        assertEquals(Long.valueOf(0L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays
                .asList(partitionColumns.get(0).getName(), partitionColumns.get(1).getName(), INVALID_VALUE, partitionColumns.get(3).getName(),
                    partitionColumns.get(4).getName())));
        assertEquals(Long.valueOf(0L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays
                .asList(partitionColumns.get(0).getName(), partitionColumns.get(1).getName(), partitionColumns.get(2).getName(), INVALID_VALUE,
                    partitionColumns.get(4).getName())));
        assertEquals(Long.valueOf(0L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays
                .asList(partitionColumns.get(0).getName(), partitionColumns.get(1).getName(), partitionColumns.get(2).getName(),
                    partitionColumns.get(3).getName(), INVALID_VALUE)));

        // Get business object format record count when passing partition key matching partition column
        // at partition level which is greater than supported by business object data registration.
        assertEquals(Long.valueOf(0L), businessObjectFormatDao
            .getBusinessObjectFormatCountByPartitionKeys(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                Collections.singletonList(partitionColumns.get(BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1).getName())));
    }

    @Test
    public void testGetBusinessObjectFormatIdsByBusinessObjectDefinition()
    {
        // Create two business object definition entities.
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities = Arrays
            .asList(businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION),
                businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION));

        // Create two business object formats under the first business object definition.
        List<BusinessObjectFormatEntity> businessObjectFormatEntities = Arrays.asList(businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY), businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, FORMAT_DESCRIPTION_2,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY));

        // Test "less than", "equal", and "greater than" scenario for all three business object definitions.
        assertEquals(Arrays.asList(businessObjectFormatEntities.get(0).getId(), businessObjectFormatEntities.get(1).getId()),
            businessObjectFormatDao.getBusinessObjectFormatIdsByBusinessObjectDefinition(businessObjectDefinitionEntities.get(0)));
        assertEquals(Collections.emptyList(),
            businessObjectFormatDao.getBusinessObjectFormatIdsByBusinessObjectDefinition(businessObjectDefinitionEntities.get(1)));
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
        List<BusinessObjectFormatKey> resultKeys =
            businessObjectFormatDao.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), false);

        // Validate the returned object.
        assertEquals(businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatKeys(), resultKeys);

        // Retrieve a list of the latest version business object format keys for the specified business object definition.
        resultKeys = businessObjectFormatDao.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), true);

        // Validate the returned object.
        assertEquals(businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatLatestVersionKeys(), resultKeys);
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

        // Retrieve a list of business object format keys for the specified business object definition.
        List<BusinessObjectFormatKey> resultKeys =
            businessObjectFormatDao.getBusinessObjectFormatsWithFilters(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), FORMAT_USAGE_CODE, false);

        // Need to filter format usage
        List<BusinessObjectFormatKey> expectedKeyList = businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatKeys();
        expectedKeyList = expectedKeyList.stream().filter(formatKey -> (formatKey.getBusinessObjectFormatUsage().equalsIgnoreCase(FORMAT_USAGE_CODE)))
            .collect(Collectors.toList());

        // Validate the returned object.
        assertEquals(expectedKeyList, resultKeys);

        // Retrieve a list of the latest version business object format keys for the specified business object definition.
        resultKeys =
            businessObjectFormatDao.getBusinessObjectFormatsWithFilters(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), FORMAT_USAGE_CODE, true);

        // Need to filter format usage
        expectedKeyList = businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatLatestVersionKeys();
        expectedKeyList = expectedKeyList.stream().filter(formatKey -> (formatKey.getBusinessObjectFormatUsage().equalsIgnoreCase(FORMAT_USAGE_CODE)))
            .collect(Collectors.toList());

        // Validate the returned object.
        assertEquals(expectedKeyList, resultKeys);
    }

    @Test
    public void testGetBusinessObjectFormatEntitiesByBusinessObjectDefinition()
    {
        // Create relative database entities.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(), BDEF_DESCRIPTION);
        fileTypeDaoTestHelper.createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toLowerCase(), "Description of " + FORMAT_FILE_TYPE_CODE);

        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE_2.toLowerCase(), BDEF_NAME_2.toLowerCase(), DATA_PROVIDER_NAME_2.toLowerCase(), BDEF_DESCRIPTION_2);

        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE_2, BDEF_NAME_2.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                INITIAL_FORMAT_VERSION, "Test format 0", "Document Schema 0", Boolean.FALSE, PARTITION_KEY);

        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                INITIAL_FORMAT_VERSION, "Test format 0", "Document Schema 0", Boolean.FALSE, PARTITION_KEY);

        BusinessObjectFormatEntity businessObjectFormatEntityV1 = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                SECOND_FORMAT_VERSION, "Test format 0", "Document Schema 0", Boolean.TRUE, PARTITION_KEY);

        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        // Retrieve business object format entity by specifying values for all text alternate key fields in upper case.
        List<BusinessObjectFormatEntity> businessObjectFormatList =
            businessObjectFormatDao.getLatestVersionBusinessObjectFormatsByBusinessObjectDefinition(businessObjectDefinitionKey);
        assertEquals(businessObjectFormatList.size(), 1);
        assertEquals(businessObjectFormatList.get(0), businessObjectFormatEntityV1);
    }
}

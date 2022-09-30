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

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
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
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, "Test format 0", "Document schema 0",
                "Document schema url 0", false, PARTITION_KEY);
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, "Test format 1", "Document schema 1",
                "Document schema url 1", true, PARTITION_KEY);
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 2, "Test format 2", "Document schema 2",
                "Document schema url 2", false, PARTITION_KEY);

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
            assertTrue(businessObjectFormatEntity.getDocumentSchemaUrl().equals(String.format("Document schema url %d", businessObjectFormatVersion)));
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
                INITIAL_FORMAT_VERSION, "Test format 0", "Document Schema 0", "Document Schema Url 0", Boolean.TRUE, PARTITION_KEY);

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
        assertTrue(businessObjectFormatEntity.getDocumentSchemaUrl().equals("Document Schema Url 0"));
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
                INITIAL_FORMAT_VERSION, "Test format 0", "Document Schema 0", "Document Schema Url 0", Boolean.TRUE, PARTITION_KEY);

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
        assertTrue(businessObjectFormatEntity.getDocumentSchemaUrl().equals("Document Schema Url 0"));
    }

    @Test
    public void testGetBusinessObjectFormatByAltKeyFormatVersionNotSpecified()
    {
        // Create relative database entities.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);
        fileTypeDaoTestHelper.createFileTypeEntity(FORMAT_FILE_TYPE_CODE, "Description of " + FORMAT_FILE_TYPE_CODE);

        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, "Test format 0", "Document Schema 0",
                "Document Schema Url 0", false, PARTITION_KEY);
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, "Test format 1", "Document Schema 1",
                "Document Schema Url 1", true, PARTITION_KEY);
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 2, "Test format 2", "Document Schema 2",
                "Document Schema Url 2", false, PARTITION_KEY);

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
        assertTrue(businessObjectFormatEntity.getDocumentSchemaUrl().equals("Document Schema Url 1"));

        // Let add a second LATEST format version.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 3, "Test format 3", "Document Schema 3",
                "Document Schema Url 3", true, PARTITION_KEY);

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
                    FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, false, PARTITION_KEY);
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
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, true, PARTITION_KEY, PARTITION_KEY_GROUP);

        // Get the number of business object formats that use this partition key group.
        Long result = businessObjectFormatDao.getBusinessObjectFormatCountByPartitionKeyGroup(testPartitionKeyGroupEntity);

        // Validate the results.
        assertEquals(Long.valueOf(1L), result);
    }

    @Test
    public void testGetBusinessObjectFormatCountByPartitionKeys()
    {
        // Create a business object formats registered under the same business object definition but with different usage, file type, and versions.
        List<BusinessObjectFormatEntity> businessObjectFormatEntities = new ArrayList<>();

        businessObjectFormatEntities.add(
            businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2,
                FORMAT_VERSION, FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY,
                NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, NO_SCHEMA_CUSTOM_TBL_PROPERTIES,
                SCHEMA_NULL_VALUE_BACKSLASH_N, NO_COLUMNS, NO_PARTITION_COLUMNS));

        businessObjectFormatEntities.add(
            businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION,
                FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY, NO_PARTITION_KEY_GROUP,
                NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, NO_SCHEMA_CUSTOM_TBL_PROPERTIES,
                SCHEMA_NULL_VALUE_BACKSLASH_N, NO_COLUMNS, NO_PARTITION_COLUMNS));

        businessObjectFormatEntities.add(
            businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2,
                FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY, NO_PARTITION_KEY_GROUP,
                NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, NO_SCHEMA_CUSTOM_TBL_PROPERTIES,
                SCHEMA_NULL_VALUE_BACKSLASH_N, NO_COLUMNS, NO_PARTITION_COLUMNS));

        businessObjectFormatEntities.add(
            businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY, NO_PARTITION_KEY_GROUP,
                NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, NO_SCHEMA_CUSTOM_TBL_PROPERTIES,
                SCHEMA_NULL_VALUE_BACKSLASH_N, NO_COLUMNS, NO_PARTITION_COLUMNS));

        // Get business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectFormatEntities.get(0).getBusinessObjectDefinition();

        // Get both file type entities used in the business object formats created above.
        FileTypeEntity fileTypeEntity = businessObjectFormatEntities.get(2).getFileType();
        FileTypeEntity fileTypeEntity2 = businessObjectFormatEntities.get(0).getFileType();

        // Create one more business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity2 =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(NAMESPACE_2, BDEF_NAME_2),
                DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create one more file type entity.
        FileTypeEntity fileTypeEntity3 = fileTypeDaoTestHelper.createFileTypeEntity(FORMAT_FILE_TYPE_CODE_3);

        // Get business object format ids by passing all parameters.
        assertEquals(Long.valueOf(1L),
            businessObjectFormatDao.getBusinessObjectFormatCountBySearchKeyElements(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity,
                FORMAT_VERSION));
        assertEquals(Long.valueOf(1L),
            businessObjectFormatDao.getBusinessObjectFormatCountBySearchKeyElements(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity,
                FORMAT_VERSION_2));
        assertEquals(Long.valueOf(1L),
            businessObjectFormatDao.getBusinessObjectFormatCountBySearchKeyElements(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity2,
                FORMAT_VERSION));
        assertEquals(Long.valueOf(1L),
            businessObjectFormatDao.getBusinessObjectFormatCountBySearchKeyElements(businessObjectDefinitionEntity, FORMAT_USAGE_CODE_2, fileTypeEntity2,
                FORMAT_VERSION));

        // Get business object format ids when passing only required parameters.
        assertEquals(Long.valueOf(4L),
            businessObjectFormatDao.getBusinessObjectFormatCountBySearchKeyElements(businessObjectDefinitionEntity, NO_FORMAT_USAGE_CODE, null,
                NO_FORMAT_VERSION));

        // Get business object format ids when passing all string parameters in upper case.
        assertEquals(Long.valueOf(2L),
            businessObjectFormatDao.getBusinessObjectFormatCountBySearchKeyElements(businessObjectDefinitionEntity, FORMAT_USAGE_CODE.toUpperCase(),
                fileTypeEntity, NO_FORMAT_VERSION));

        // Get business object format ids when passing all string parameters in lower case.
        assertEquals(Long.valueOf(2L),
            businessObjectFormatDao.getBusinessObjectFormatCountBySearchKeyElements(businessObjectDefinitionEntity, FORMAT_USAGE_CODE.toLowerCase(),
                fileTypeEntity, NO_FORMAT_VERSION));

        // Get business object format ids  when passing an invalid value for each of the parameters individually.
        assertEquals(Long.valueOf(0L),
            businessObjectFormatDao.getBusinessObjectFormatCountBySearchKeyElements(businessObjectDefinitionEntity2, FORMAT_USAGE_CODE, fileTypeEntity,
                FORMAT_VERSION));
        assertEquals(Long.valueOf(0L),
            businessObjectFormatDao.getBusinessObjectFormatCountBySearchKeyElements(businessObjectDefinitionEntity, FORMAT_USAGE_CODE_3, fileTypeEntity,
                FORMAT_VERSION));
        assertEquals(Long.valueOf(0L),
            businessObjectFormatDao.getBusinessObjectFormatCountBySearchKeyElements(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity3,
                FORMAT_VERSION));
        assertEquals(Long.valueOf(0L),
            businessObjectFormatDao.getBusinessObjectFormatCountBySearchKeyElements(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity,
                FORMAT_VERSION_2 + 1));
    }

    @Test
    public void testGetBusinessObjectFormatIdsByBusinessObjectDefinition()
    {
        // Create two business object definition entities.
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities = Arrays
            .asList(businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION),
                businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION));

        // Create two business object formats under the first business object definition with the second format having its latest version flag set to true.
        List<BusinessObjectFormatEntity> businessObjectFormatEntities = Arrays.asList(businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY), businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, FORMAT_DESCRIPTION_2,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY));

        // Select all business object formats registered under the first business object definition.
        assertEquals(Arrays.asList(businessObjectFormatEntities.get(0).getId(), businessObjectFormatEntities.get(1).getId()),
            businessObjectFormatDao.getBusinessObjectFormatIdsByBusinessObjectDefinition(businessObjectDefinitionEntities.get(0), false));

        // Select only latest business object format versions registered under the first business object definition.
        assertEquals(Arrays.asList(businessObjectFormatEntities.get(1).getId()),
            businessObjectFormatDao.getBusinessObjectFormatIdsByBusinessObjectDefinition(businessObjectDefinitionEntities.get(0), true));

        // Test edge case when business object definition has no formats registered.
        assertEquals(Collections.emptyList(),
            businessObjectFormatDao.getBusinessObjectFormatIdsByBusinessObjectDefinition(businessObjectDefinitionEntities.get(1), false));
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
                    key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA,
                    FORMAT_DOCUMENT_SCHEMA_URL, false, PARTITION_KEY);
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
                    key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA,
                    FORMAT_DOCUMENT_SCHEMA_URL, false, PARTITION_KEY);
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
    public void testGetLatestVersionBusinessObjectFormatsByBusinessObjectDefinition()
    {
        // Create two business object definitions.
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities = Arrays
            .asList(businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION),
                businessObjectDefinitionDaoTestHelper
                    .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION));

        // Create multiple formats in reverse order of business object format usage and file types for the first business object definition including several
        // latest version business object formats that we expect to be selected by the method under test.
        List<BusinessObjectFormatEntity> businessObjectFormatEntities = Arrays.asList(businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION,
                    FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY),
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, SECOND_FORMAT_VERSION,
                    FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY),
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, THIRD_FORMAT_VERSION,
                    FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY),
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                    FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY),
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION,
                    FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY),
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION,
                    FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY),
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE_2, SECOND_FORMAT_VERSION,
                    FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY),
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                    FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY));

        // Create a list of expected latest business object format versions to be returned by the method under test.
        List<BusinessObjectFormatEntity> expectedBusinessObjectFormatEntities =
            Arrays.asList(businessObjectFormatEntities.get(5), businessObjectFormatEntities.get(4), businessObjectFormatEntities.get(2));

        // Retrieve latest version business object format entities.
        assertEquals(expectedBusinessObjectFormatEntities,
            businessObjectFormatDao.getLatestVersionBusinessObjectFormatsByBusinessObjectDefinition(businessObjectDefinitionEntities.get(0)));

        // Try to retrieve a list of latest version business object format entities by specifying the second business object definition.
        assertTrue(businessObjectFormatDao.getLatestVersionBusinessObjectFormatsByBusinessObjectDefinition(businessObjectDefinitionEntities.get(1)).isEmpty());
    }

    @Test
    public void testGetPartitionLevelsBySearchKeyElementsAndPartitionKeys()
    {
        // Declare lists of partition and regular columns to be used in this test.
        List<SchemaColumn> partitionColumns;
        List<SchemaColumn> regularColumns;

        // Create two schema columns to be used as our target columns in this test.
        SchemaColumn testSchemaColumn = new SchemaColumn(COLUMN_NAME, COLUMN_DATA_TYPE, COLUMN_SIZE, COLUMN_REQUIRED, COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION);

        // Save the name of the original partition columns.
        String primaryPartitionColumnName = schemaColumnDaoTestHelper.getTestPartitionColumns().get(0).getName();
        String firstSubPartitionColumnName = schemaColumnDaoTestHelper.getTestPartitionColumns().get(1).getName();
        String secondSubPartitionColumnName = schemaColumnDaoTestHelper.getTestPartitionColumns().get(2).getName();
        String thirdSubPartitionColumnName = schemaColumnDaoTestHelper.getTestPartitionColumns().get(3).getName();

        // Create multiple business object format versions that:
        // * contain our test column as a primary partition column
        // * contain our test column as a sub-partition column that is at partition level that is supported by business object data registration
        // * contain our test column as a sub-partition column that is at partition level that is not supported by business object data registration
        // * does not contain our test column
        // * contain our test column as a regular column (not as partition column)
        // * contain our test column as both regular column and partition column (this is allowed)

        // Create a variable to track business object format version.
        int businessObjectFormatVersion = 0;

        // Create business object format versions that contain our test column as partition column starting from primary column and finishing outside
        // of partition level supported by business object data registration (for partition level being 0-based it needs to stop at MAX_SUBPARTITIONS + 1).
        for (int partitionLevel = 0; partitionLevel < BusinessObjectDataEntity.MAX_SUBPARTITIONS + 2; partitionLevel++)
        {
            partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
            partitionColumns.set(partitionLevel, testSchemaColumn);
            regularColumns = schemaColumnDaoTestHelper.getTestSchemaColumns();
            businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                businessObjectFormatVersion, FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET,
                partitionColumns.get(0).getName(), NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA,
                SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE,
                NO_SCHEMA_CUSTOM_TBL_PROPERTIES, SCHEMA_NULL_VALUE_BACKSLASH_N, regularColumns, partitionColumns);
            businessObjectFormatVersion++;
        }

        // Create business object format version that does not contain our test column.
        partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        regularColumns = schemaColumnDaoTestHelper.getTestSchemaColumns();
        businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            businessObjectFormatVersion, FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET,
            partitionColumns.get(0).getName(), NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA,
            SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE,
            NO_SCHEMA_CUSTOM_TBL_PROPERTIES, SCHEMA_NULL_VALUE_BACKSLASH_N, regularColumns, partitionColumns);
        businessObjectFormatVersion++;

        // Create business object format version that contains our test column as a regular column (not as partition column).
        partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        regularColumns = schemaColumnDaoTestHelper.getTestSchemaColumns();
        regularColumns.set(0, testSchemaColumn);
        businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            businessObjectFormatVersion, FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET,
            partitionColumns.get(0).getName(), NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA,
            SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE,
            NO_SCHEMA_CUSTOM_TBL_PROPERTIES, SCHEMA_NULL_VALUE_BACKSLASH_N, regularColumns, partitionColumns);
        businessObjectFormatVersion++;

        // Create business object format version that contains our test column as both regular column and partition column (this is allowed)
        partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        partitionColumns.set(1, testSchemaColumn);
        regularColumns = schemaColumnDaoTestHelper.getTestSchemaColumns();
        regularColumns.set(2, testSchemaColumn);
        businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            businessObjectFormatVersion, FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET,
            partitionColumns.get(0).getName(), NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA,
            SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE,
            NO_SCHEMA_CUSTOM_TBL_PROPERTIES, SCHEMA_NULL_VALUE_BACKSLASH_N, regularColumns, partitionColumns);

        // Get business object definition entity for the test business object format versions.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));
        assertNotNull(businessObjectDefinitionEntity);

        // Get file type entity from for the test business object format versions.
        FileTypeEntity fileTypeEntity = fileTypeDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE);
        assertNotNull(fileTypeEntity);

        // Create one more business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity2 =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(NAMESPACE_2, BDEF_NAME_2),
                DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create one more file type entity.
        FileTypeEntity fileTypeEntity2 = fileTypeDaoTestHelper.createFileTypeEntity(FORMAT_FILE_TYPE_CODE_2);

        // Declare objects to be used to validate the results.
        List<List<Integer>> results;
        List<List<Integer>> expectedResults;

        // Call the method under test with all parameters specified.
        results =
            businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity, 0,
                Collections.singletonList(COLUMN_NAME));

        // Validate the results.
        expectedResults = Collections.singletonList(Collections.singletonList(1));
        assertEquals(expectedResults, results);

        // Call the method under test with all string parameters passed in upper case.
        results = businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity, FORMAT_USAGE_CODE.toUpperCase(),
            fileTypeEntity, 0, Collections.singletonList(COLUMN_NAME.toUpperCase()));

        // Validate the results.
        assertEquals(expectedResults, results);

        // Call the method under test with all string parameters passed in lower case.
        results = businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity, FORMAT_USAGE_CODE.toLowerCase(),
            fileTypeEntity, 0, Collections.singletonList(COLUMN_NAME.toLowerCase()));

        // Validate the results.
        assertEquals(expectedResults, results);

        // Call the method under test with all optional parameters not specified.
        results = businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity, NO_FORMAT_USAGE_CODE, null,
            NO_FORMAT_VERSION, Collections.emptyList());

        // Validate the results.
        expectedResults = Collections.emptyList();
        assertEquals(expectedResults, results);

        // Call the method under test with all optional parameters not specified except for a single partition key.
        results = businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity, NO_FORMAT_USAGE_CODE, null,
            NO_FORMAT_VERSION, Collections.singletonList(COLUMN_NAME));

        // Validate the results.
        expectedResults = Collections.singletonList(Arrays.asList(1, 2, 3, 4, 5, 2));
        assertEquals(expectedResults, results);

        // Call the method under test with wrong or invalid parameters.
        expectedResults = Collections.singletonList(Collections.emptyList());
        assertEquals(expectedResults,
            businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity2, FORMAT_USAGE_CODE, fileTypeEntity, 0,
                Collections.singletonList(COLUMN_NAME)));
        assertEquals(expectedResults,
            businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity, FORMAT_USAGE_CODE_2, fileTypeEntity,
                0, Collections.singletonList(COLUMN_NAME)));
        assertEquals(expectedResults,
            businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity2, 0,
                Collections.singletonList(COLUMN_NAME)));
        assertEquals(expectedResults,
            businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity, 99,
                Collections.singletonList(COLUMN_NAME)));
        assertEquals(expectedResults,
            businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity, 0,
                Collections.singletonList(I_DO_NOT_EXIST)));

        // Call the method under test with all optional parameters not specified except and specifying 2 partition keys.
        results = businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity, NO_FORMAT_USAGE_CODE, null,
            NO_FORMAT_VERSION, Arrays.asList(COLUMN_NAME, primaryPartitionColumnName));

        // Validate the results.
        expectedResults = Arrays.asList(Arrays.asList(2, 3, 4, 5, 2), Arrays.asList(1, 1, 1, 1, 1));
        assertEquals(expectedResults, results);

        // Call the method under test with all optional parameters not specified except and specifying 3 partition keys.
        results = businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity, NO_FORMAT_USAGE_CODE, null,
            NO_FORMAT_VERSION, Arrays.asList(COLUMN_NAME, primaryPartitionColumnName, firstSubPartitionColumnName));

        // Validate the results.
        expectedResults = Arrays.asList(Arrays.asList(3, 4, 5), Arrays.asList(1, 1, 1), Arrays.asList(2, 2, 2));
        assertEquals(expectedResults, results);

        // Call the method under test with all optional parameters not specified except and specifying 4 partition keys.
        results = businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity, NO_FORMAT_USAGE_CODE, null,
            NO_FORMAT_VERSION, Arrays.asList(COLUMN_NAME, firstSubPartitionColumnName, primaryPartitionColumnName, secondSubPartitionColumnName));

        // Validate the results.
        expectedResults = Arrays.asList(Arrays.asList(4, 5), Arrays.asList(2, 2), Arrays.asList(1, 1), Arrays.asList(3, 3));
        assertEquals(expectedResults, results);

        // Call the method under test with all optional parameters not specified except and specifying 5 partition keys.
        results = businessObjectFormatDao.getPartitionLevelsBySearchKeyElementsAndPartitionKeys(businessObjectDefinitionEntity, NO_FORMAT_USAGE_CODE, null,
            NO_FORMAT_VERSION,
            Arrays.asList(firstSubPartitionColumnName, primaryPartitionColumnName, thirdSubPartitionColumnName, COLUMN_NAME, secondSubPartitionColumnName));

        // Validate the results.
        expectedResults = Arrays.asList(Collections.singletonList(2), Collections.singletonList(1), Collections.singletonList(4), Collections.singletonList(5),
            Collections.singletonList(3));
        assertEquals(expectedResults, results);
    }
}

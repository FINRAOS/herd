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
package org.finra.dm.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;

import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.dao.impl.DmDaoImpl;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationKey;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.dm.model.api.xml.BusinessObjectFormatKey;
import org.finra.dm.model.api.xml.CustomDdlKey;
import org.finra.dm.model.api.xml.EmrClusterDefinitionKey;
import org.finra.dm.model.api.xml.ExpectedPartitionValueKey;
import org.finra.dm.model.api.xml.FileTypeKey;
import org.finra.dm.model.api.xml.NamespaceKey;
import org.finra.dm.model.api.xml.PartitionKeyGroupKey;
import org.finra.dm.model.api.xml.PartitionValueRange;
import org.finra.dm.model.api.xml.SchemaColumn;
import org.finra.dm.model.api.xml.StorageKey;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.ConfigurationEntity;
import org.finra.dm.model.jpa.CustomDdlEntity;
import org.finra.dm.model.jpa.EmrClusterDefinitionEntity;
import org.finra.dm.model.jpa.ExpectedPartitionValueEntity;
import org.finra.dm.model.jpa.FileTypeEntity;
import org.finra.dm.model.jpa.JmsMessageEntity;
import org.finra.dm.model.jpa.JobDefinitionEntity;
import org.finra.dm.model.jpa.NamespaceEntity;
import org.finra.dm.model.jpa.OnDemandPriceEntity;
import org.finra.dm.model.jpa.PartitionKeyGroupEntity;
import org.finra.dm.model.jpa.SecurityFunctionEntity;
import org.finra.dm.model.jpa.SecurityRoleEntity;
import org.finra.dm.model.jpa.SecurityRoleFunctionEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.jpa.StorageFileEntity;
import org.finra.dm.model.jpa.StorageUnitEntity;

/**
 * This class tests various functionality within the DAO class.
 */
public class DmDaoTest extends AbstractDaoTest
{
    public static final String TEST_KEY = "test_key" + RANDOM_SUFFIX;
    public static final String TEST_VALUE = "test_value" + RANDOM_SUFFIX;

    @Autowired
    private CacheManager cacheManager;

    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();

        // Create a test configuration entry.
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(TEST_KEY);
        configurationEntity.setValue(TEST_VALUE);
        dmDao.saveAndRefresh(configurationEntity);
    }

    // Configuration

    @Test
    public void testReadAllConfigurations() throws Exception
    {
        // Test that we are able to read configurations from the database using our DAO tier.
        List<ConfigurationEntity> configurations = dmDao.findAll(ConfigurationEntity.class);
        assertTrue(configurations.size() > 0);
        for (ConfigurationEntity configuration : configurations)
        {
            if (TEST_KEY.equals(configuration.getKey()) && TEST_VALUE.equals(configuration.getValue()))
            {
                // We found our inserted test key/test value.
                return;
            }
        }
        fail("A configuration with key \"" + TEST_KEY + "\" was expected, but not found.");
    }

    @Test
    public void testReadConfigurationByKey() throws Exception
    {
        // Read a configuration entry by a key and verify that it exists and contains our test value.
        ConfigurationEntity configurationEntity = dmDao.getConfigurationByKey(TEST_KEY);
        assertNotNull(configurationEntity);
        assertTrue(TEST_KEY.equals(configurationEntity.getKey()));
        assertTrue(TEST_VALUE.equals(configurationEntity.getValue()));
    }

    // Namespace

    @Test
    public void testGetNamespaceByKey()
    {
        // Create a namespace entity.
        createNamespaceEntity(NAMESPACE_CD);

        // Retrieve the namespace entity.
        NamespaceEntity resultNamespaceEntity = dmDao.getNamespaceByKey(new NamespaceKey(NAMESPACE_CD));

        // Validate the results.
        assertEquals(NAMESPACE_CD, resultNamespaceEntity.getCode());
    }

    @Test
    public void testGetNamespaces()
    {
        // Create and persist namespace entities.
        for (NamespaceKey key : getTestNamespaceKeys())
        {
            createNamespaceEntity(key.getNamespaceCode());
        }

        // Retrieve a list of namespace keys.
        List<NamespaceKey> resultNamespaceKeys = dmDao.getNamespaces();

        // Validate the returned object.
        assertNotNull(resultNamespaceKeys);
        assertTrue(resultNamespaceKeys.containsAll(getTestNamespaceKeys()));
    }

    // BusinessObjectDefinition

    @Test
    public void testGetBusinessObjectDefinitionByKey()
    {
        // Create two non-legacy business object definitions having the same business object definition name.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, false);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, false);

        // Get the business object definition by key.
        BusinessObjectDefinitionEntity resultBusinessObjectDefinitionEntity =
            dmDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME));

        // Validate the returned object.
        validateBusinessObjectDefinitionEntity(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION,
            resultBusinessObjectDefinitionEntity);
    }

    @Test
    public void testGetLegacyBusinessObjectDefinitionByName()
    {
        // Create a legacy and a non-legacy business object definitions having the same business object definition name.
        BusinessObjectDefinitionEntity legacyBusinessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, false);

        // Get the business object definition by name.
        BusinessObjectDefinitionEntity resultBusinessObjectDefinitionEntity = dmDao.getLegacyBusinessObjectDefinitionByName(BOD_NAME);

        // Validate the returned object.
        validateBusinessObjectDefinitionEntity(legacyBusinessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION,
            resultBusinessObjectDefinitionEntity);
    }

    @Test
    public void testGetBusinessObjectDefinitions() throws Exception
    {
        // Create and persist business object definition entities.
        for (BusinessObjectDefinitionKey key : getTestBusinessObjectDefinitionKeys())
        {
            createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        }

        // Retrieve a list of business object definition keys for the specified namespace.
        List<BusinessObjectDefinitionKey> resultKeys = dmDao.getBusinessObjectDefinitions(NAMESPACE_CD);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectDefinitionKeys(), resultKeys);
    }

    @Test
    public void testGetBusinessObjectDefinitionsLegacy() throws Exception
    {
        // Create and persist business object definition entities.
        for (BusinessObjectDefinitionKey key : getTestBusinessObjectDefinitionKeys())
        {
            createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        }

        // Retrieve a list of business object definition keys stored in the system by calling a legacy endpoint.
        List<BusinessObjectDefinitionKey> resultKeys = dmDao.getBusinessObjectDefinitions();

        // Validate the returned object.
        assertNotNull(resultKeys);
        assertTrue(resultKeys.containsAll(getTestBusinessObjectDefinitionKeys()));
    }

    // FileType

    @Test
    public void testGetFileTypeByCode()
    {
        // Create relative database entities.
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE, "Description of " + FORMAT_FILE_TYPE_CODE);

        // Retrieve file type entity.
        FileTypeEntity fileTypeEntity = dmDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE);

        // Validate the results.
        assertNotNull(fileTypeEntity);
        assertTrue(fileTypeEntity.getCode().equals(FORMAT_FILE_TYPE_CODE));
        assertTrue(fileTypeEntity.getDescription().equals("Description of " + FORMAT_FILE_TYPE_CODE));
    }

    @Test
    public void testGetFileTypeByCodeInUpperCase()
    {
        // Create relative database entities.
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toLowerCase(), "Description of " + FORMAT_FILE_TYPE_CODE.toLowerCase());

        // Retrieve file type entity.
        FileTypeEntity fileTypeEntity = dmDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE.toUpperCase());

        // Validate the results.
        assertNotNull(fileTypeEntity);
        assertTrue(fileTypeEntity.getCode().equals(FORMAT_FILE_TYPE_CODE.toLowerCase()));
        assertTrue(fileTypeEntity.getDescription().equals("Description of " + FORMAT_FILE_TYPE_CODE.toLowerCase()));
    }

    @Test
    public void testGetFileTypeByCodeInLowerCase()
    {
        // Create relative database entities.
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toUpperCase(), "Description of " + FORMAT_FILE_TYPE_CODE.toUpperCase());

        // Retrieve file type entity.
        FileTypeEntity fileTypeEntity = dmDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE.toLowerCase());

        // Validate the results.
        assertNotNull(fileTypeEntity);
        assertTrue(fileTypeEntity.getCode().equals(FORMAT_FILE_TYPE_CODE.toUpperCase()));
        assertTrue(fileTypeEntity.getDescription().equals("Description of " + FORMAT_FILE_TYPE_CODE.toUpperCase()));
    }

    @Test
    public void testGetFileTypeByCodeInvalidCode()
    {
        // Create relative database entities.
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE, "Description of " + FORMAT_FILE_TYPE_CODE);

        // Try to retrieve file type entity using an invalid code value.
        assertNull(dmDao.getFileTypeByCode("I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetFileTypeByCodeMultipleRecordsFound()
    {
        // Create relative database entities.
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toUpperCase(), "Description of " + FORMAT_FILE_TYPE_CODE.toUpperCase());
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toLowerCase(), "Description of " + FORMAT_FILE_TYPE_CODE.toLowerCase());

        try
        {
            // Try retrieve file type entity.
            dmDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE);
            fail("Should throw an IllegalArgumentException if finds more than one file type entities with the same code.");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage().startsWith("Found more than one file type"));
        }
    }

    @Test
    public void testGetFileTypes() throws Exception
    {
        // Create and persist file type entities.
        for (FileTypeKey key : getTestFileTypeKeys())
        {
            createFileTypeEntity(key.getFileTypeCode());
        }

        // Retrieve a list of file type keys.
        List<FileTypeKey> resultFileTypeKeys = dmDao.getFileTypes();

        // Validate the returned object.
        assertNotNull(resultFileTypeKeys);
        assertTrue(resultFileTypeKeys.containsAll(getTestFileTypeKeys()));
    }

    // BusinessObjectFormat

    @Test
    public void testGetBusinessObjectFormatByAltKeyAllParamsSpecified()
    {
        // Create relative database entities.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, false);
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE, "Description of " + FORMAT_FILE_TYPE_CODE);

        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, "Test format 0", false, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, "Test format 1", true, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 2, "Test format 2", false, PARTITION_KEY);

        for (int businessObjectFormatVersion = 0; businessObjectFormatVersion < 3; businessObjectFormatVersion++)
        {
            // Retrieve business object format entity by specifying values for all alternate key fields.
            BusinessObjectFormatEntity businessObjectFormatEntity = dmDao.getBusinessObjectFormatByAltKey(
                new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion));

            // Validate the results.
            assertNotNull(businessObjectFormatEntity);
            assertTrue(businessObjectFormatEntity.getBusinessObjectDefinition().getName().equals(BOD_NAME));
            assertTrue(businessObjectFormatEntity.getUsage().equals(FORMAT_USAGE_CODE));
            assertTrue(businessObjectFormatEntity.getFileType().getCode().equals(FORMAT_FILE_TYPE_CODE));
            assertTrue(businessObjectFormatEntity.getBusinessObjectFormatVersion() == businessObjectFormatVersion);
            assertTrue(businessObjectFormatEntity.getLatestVersion() == (businessObjectFormatVersion == 1));
            assertTrue(businessObjectFormatEntity.getPartitionKey().equals(PARTITION_KEY));
            assertTrue(businessObjectFormatEntity.getDescription().equals(String.format("Test format %d", businessObjectFormatVersion)));
        }

        // Try invalid values for all input parameters.
        assertNull(dmDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey("I_DO_NOT_EXIST", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0)));
        assertNull(
            dmDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0)));
        assertNull(dmDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, 0)));
        assertNull(dmDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", 0)));
        assertNull(dmDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 4)));
    }

    @Test
    public void testGetBusinessObjectFormatByAltKeyAllParamsUpperCase()
    {
        // Create relative database entities.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(), BOD_DESCRIPTION, false);
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toLowerCase(), "Description of " + FORMAT_FILE_TYPE_CODE);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
            INITIAL_FORMAT_VERSION, "Test format 0", Boolean.TRUE, PARTITION_KEY);

        // Retrieve business object format entity by specifying values for all text alternate key fields in upper case.
        BusinessObjectFormatEntity businessObjectFormatEntity = dmDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                INITIAL_FORMAT_VERSION));

        // Validate the results.
        assertNotNull(businessObjectFormatEntity);
        assertTrue(businessObjectFormatEntity.getBusinessObjectDefinition().getName().equals(BOD_NAME.toLowerCase()));
        assertTrue(businessObjectFormatEntity.getUsage().equals(FORMAT_USAGE_CODE.toLowerCase()));
        assertTrue(businessObjectFormatEntity.getFileType().getCode().equals(FORMAT_FILE_TYPE_CODE.toLowerCase()));
        assertTrue(businessObjectFormatEntity.getBusinessObjectFormatVersion().equals(INITIAL_FORMAT_VERSION));
        assertTrue(businessObjectFormatEntity.getLatestVersion());
        assertTrue(businessObjectFormatEntity.getPartitionKey().equals(PARTITION_KEY));
        assertTrue(businessObjectFormatEntity.getDescription().equals("Test format 0"));
    }

    @Test
    public void testGetBusinessObjectFormatByAltKeyAllParamsLowerCase()
    {
        // Create relative database entities.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(), BOD_DESCRIPTION, false);
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toUpperCase(), "Description of " + FORMAT_FILE_TYPE_CODE);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
            INITIAL_FORMAT_VERSION, "Test format 0", Boolean.TRUE, PARTITION_KEY);

        // Retrieve business object format entity by specifying values for all text alternate key fields in lower case.
        BusinessObjectFormatEntity businessObjectFormatEntity = dmDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                INITIAL_FORMAT_VERSION));

        // Validate the results.
        assertNotNull(businessObjectFormatEntity);
        assertTrue(businessObjectFormatEntity.getBusinessObjectDefinition().getName().equals(BOD_NAME.toUpperCase()));
        assertTrue(businessObjectFormatEntity.getUsage().equals(FORMAT_USAGE_CODE.toUpperCase()));
        assertTrue(businessObjectFormatEntity.getFileType().getCode().equals(FORMAT_FILE_TYPE_CODE.toUpperCase()));
        assertTrue(businessObjectFormatEntity.getBusinessObjectFormatVersion().equals(INITIAL_FORMAT_VERSION));
        assertTrue(businessObjectFormatEntity.getLatestVersion());
        assertTrue(businessObjectFormatEntity.getPartitionKey().equals(PARTITION_KEY));
        assertTrue(businessObjectFormatEntity.getDescription().equals("Test format 0"));
    }

    @Test
    public void testGetBusinessObjectFormatByAltKeyFormatVersionNotSpecified()
    {
        // Create relative database entities.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, false);
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE, "Description of " + FORMAT_FILE_TYPE_CODE);

        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, "Test format 0", false, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, "Test format 1", true, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 2, "Test format 2", false, PARTITION_KEY);

        // Retrieve business object format entity by specifying all values for the alternate key fields except for the format version.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            dmDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));

        // Validate the results.
        assertNotNull(businessObjectFormatEntity);
        assertTrue(businessObjectFormatEntity.getBusinessObjectDefinition().getName().equals(BOD_NAME));
        assertTrue(businessObjectFormatEntity.getUsage().equals(FORMAT_USAGE_CODE));
        assertTrue(businessObjectFormatEntity.getFileType().getCode().equals(FORMAT_FILE_TYPE_CODE));
        assertTrue(businessObjectFormatEntity.getBusinessObjectFormatVersion() == 1);
        assertTrue(businessObjectFormatEntity.getLatestVersion());
        assertTrue(businessObjectFormatEntity.getPartitionKey().equals(PARTITION_KEY));
        assertTrue(businessObjectFormatEntity.getDescription().equals(String.format("Test format 1")));

        // Let add a second LATEST format version.
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 3, "Test format 3", true, PARTITION_KEY);

        try
        {
            // Now we should get an exception, since there are more than one format with the Latest Version flag set to TRUE.
            dmDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));
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
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, version, FORMAT_DESCRIPTION, false,
                PARTITION_KEY);
        }

        // Retrieve the latest (maximum available) business object format version.
        Integer resultLatestVersion =
            dmDao.getBusinessObjectFormatMaxVersion(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));

        // Validate the results.
        assertEquals(SECOND_DATA_VERSION, resultLatestVersion);
    }

    @Test
    public void testGetBusinessObjectFormatCount()
    {
        // Create a partition key group.
        PartitionKeyGroupEntity testPartitionKeyGroupEntity = createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create a business object format that uses this partition key group.
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
            PARTITION_KEY, PARTITION_KEY_GROUP);

        // Get the number of business object formats that use this partition key group.
        Long result = dmDao.getBusinessObjectFormatCount(testPartitionKeyGroupEntity);

        // Validate the results.
        assertEquals(Long.valueOf(1L), result);
    }

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
        List<BusinessObjectFormatKey> resultKeys = dmDao.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME), false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys);

        // Retrieve a list of the latest version business object format keys for the specified business object definition.
        resultKeys = dmDao.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME), true);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatLatestVersionKeys(), resultKeys);
    }

    // PartitionKeyGroup

    @Test
    public void testGetPartitionKeyGroupByKey()
    {
        // Create relative database entities.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Retrieve partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = dmDao.getPartitionKeyGroupByKey(new PartitionKeyGroupKey(PARTITION_KEY_GROUP));

        // Validate the results.
        assertNotNull(partitionKeyGroupEntity);
        assertTrue(partitionKeyGroupEntity.getPartitionKeyGroupName().equals(PARTITION_KEY_GROUP));
    }

    @Test
    public void testGetPartitionKeyGroupByKeyInUpperCase()
    {
        // Create relative database entities.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toLowerCase());

        // Retrieve partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = dmDao.getPartitionKeyGroupByName(PARTITION_KEY_GROUP.toUpperCase());

        // Validate the results.
        assertNotNull(partitionKeyGroupEntity);
        assertTrue(partitionKeyGroupEntity.getPartitionKeyGroupName().equals(PARTITION_KEY_GROUP.toLowerCase()));
    }

    @Test
    public void testGetPartitionKeyGroupByKeyInLowerCase()
    {
        // Create relative database entities.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toUpperCase());

        // Retrieve partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = dmDao.getPartitionKeyGroupByName(PARTITION_KEY_GROUP.toLowerCase());

        // Validate the results.
        assertNotNull(partitionKeyGroupEntity);
        assertTrue(partitionKeyGroupEntity.getPartitionKeyGroupName().equals(PARTITION_KEY_GROUP.toUpperCase()));
    }

    @Test
    public void testGetPartitionKeyGroupByKeyInvalidKey()
    {
        // Create relative database entities.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Try to retrieve partition key group entity using an invalid name.
        assertNull(dmDao.getPartitionKeyGroupByName("I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetPartitionKeyGroupByKeyMultipleRecordsFound()
    {
        // Create relative database entities.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toUpperCase());
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toLowerCase());

        // Try to retrieve a partition key group when multiple entities exist with the same name (using case insensitive string comparison).
        try
        {
            dmDao.getPartitionKeyGroupByName(PARTITION_KEY_GROUP);
            fail("Should throw an IllegalArgumentException if finds more than one partition key group entities with the same name.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one \"%s\" partition key group.", PARTITION_KEY_GROUP), e.getMessage());
        }
    }

    @Test
    public void testGetPartitionKeyGroups()
    {
        // Create and persist two partition key group entities.
        for (PartitionKeyGroupKey partitionKeyGroupKey : getTestPartitionKeyGroupKeys())
        {
            createPartitionKeyGroupEntity(partitionKeyGroupKey.getPartitionKeyGroupName());
        }

        // Get the list of partition key groups.
        List<PartitionKeyGroupKey> resultPartitionKeyGroupKeys = dmDao.getPartitionKeyGroups();

        // Validate the results.
        assertNotNull(resultPartitionKeyGroupKeys);
        assertTrue(resultPartitionKeyGroupKeys.containsAll(getTestPartitionKeyGroupKeys()));
    }

    // ExpectedPartitionValue

    @Test
    public void testGetExpectedPartitionValue()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of test expected partition values.
        createExpectedPartitionValueEntities(partitionKeyGroupEntity, getTestUnsortedExpectedPartitionValues());

        // Get expected partition value for different offset values.
        List<String> testSortedExpectedPartitionValues = getTestSortedExpectedPartitionValues();
        int testExpectedPartitionValueIndex = 3;
        for (Integer offset : Arrays.asList(-2, 0, 2))
        {
            ExpectedPartitionValueEntity resultExpectedPartitionValueEntity = dmDao.getExpectedPartitionValue(
                new ExpectedPartitionValueKey(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(testExpectedPartitionValueIndex)), offset);

            // Validate the returned object.
            resultExpectedPartitionValueEntity.getPartitionValue().equals(testSortedExpectedPartitionValues.get(testExpectedPartitionValueIndex + offset));
        }
    }

    @Test
    public void testGetExpectedPartitionValueWithOffsetExpectedPartitionValueNoExists()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a single test expected partition value.
        createExpectedPartitionValueEntities(partitionKeyGroupEntity, Arrays.asList(PARTITION_VALUE));

        // Validate that we get null back when passing an existing expected partition value but giving an invalid offset.
        for (Integer offset : Arrays.asList(-1, 1))
        {
            assertNull(dmDao.getExpectedPartitionValue(new ExpectedPartitionValueKey(PARTITION_KEY_GROUP, PARTITION_VALUE), offset));
        }
    }

    /**
     * Test DAO method to retrieve expected partition values by range.
     */
    @Test
    public void testGetExpectedPartitionValuesByGroupAndRange()
    {
        createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        PartitionValueRange partitionValueRange = new PartitionValueRange();
        partitionValueRange.setStartPartitionValue(getDateAsString(2014, 3, 11));
        partitionValueRange.setEndPartitionValue(getDateAsString(2014, 3, 17));
        List<ExpectedPartitionValueEntity> expectedPartitionValueEntities =
            dmDao.getExpectedPartitionValuesByGroupAndRange(PARTITION_KEY_GROUP, partitionValueRange);

        assertEquals(expectedPartitionValueEntities.size(), 5, expectedPartitionValueEntities.size());
        assertEquals(expectedPartitionValueEntities.get(0).getPartitionValue(), getDateAsString(2014, 3, 11));
        assertEquals(expectedPartitionValueEntities.get(1).getPartitionValue(), getDateAsString(2014, 3, 14));
        assertEquals(expectedPartitionValueEntities.get(2).getPartitionValue(), getDateAsString(2014, 3, 15));
        assertEquals(expectedPartitionValueEntities.get(3).getPartitionValue(), getDateAsString(2014, 3, 16));
        assertEquals(expectedPartitionValueEntities.get(4).getPartitionValue(), getDateAsString(2014, 3, 17));
    }

    /**
     * Test DAO method to retrieve expected partition values with no range (specified 2 ways). In the month of April, 2014, the number of values (i.e.
     * non-weekend days) is 22.
     */
    @Test
    public void testGetExpectedPartitionValuesByGroupAndNoRange()
    {
        createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        // Null range.
        List<ExpectedPartitionValueEntity> expectedPartitionValueEntities = dmDao.getExpectedPartitionValuesByGroupAndRange(PARTITION_KEY_GROUP, null);

        assertEquals(expectedPartitionValueEntities.size(), 22, expectedPartitionValueEntities.size());

        // Range with no start or end.
        PartitionValueRange partitionValueRange = new PartitionValueRange();
        expectedPartitionValueEntities = dmDao.getExpectedPartitionValuesByGroupAndRange(PARTITION_KEY_GROUP, partitionValueRange);

        assertEquals(expectedPartitionValueEntities.size(), 22, expectedPartitionValueEntities.size());
    }

    // CustomDdl

    @Test
    public void testGetCustomDdlByKey()
    {
        // Create and persist a custom DDL entity.
        CustomDdlEntity customDdlEntity =
            createCustomDdlEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        // Retrieve the custom DDL.
        CustomDdlEntity resultCustomDdlEntity =
            dmDao.getCustomDdlByKey(new CustomDdlKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME));

        // Validate the returned object.
        assertEquals(customDdlEntity.getId(), resultCustomDdlEntity.getId());
    }

    @Test
    public void testGetCustomDdls()
    {
        // List of test custom DDL names.
        List<String> testCustomDdlNames = Arrays.asList(CUSTOM_DDL_NAME, CUSTOM_DDL_NAME_2);

        // Create and persist a custom DDL entities.
        for (String customDdlName : testCustomDdlNames)
        {
            createCustomDdlEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, customDdlName, TEST_DDL);
        }

        // Retrieve a list of custom DDL keys.
        List<CustomDdlKey> resultCustomDdlKeys =
            dmDao.getCustomDdls(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION));

        // Validate the returned object.
        assertNotNull(resultCustomDdlKeys);
        assertEquals(testCustomDdlNames.size(), resultCustomDdlKeys.size());
        for (int i = 0; i < testCustomDdlNames.size(); i++)
        {
            validateCustomDdlKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testCustomDdlNames.get(i),
                resultCustomDdlKeys.get(i));
        }
    }

    // BusinessObjectData

    @Test
    public void testGetBusinessObjectDataByAltKey()
    {
        // Execute the same set of tests on business object data entities created with and without subpartition values.
        for (List<String> subPartitionValues : Arrays.asList(SUBPARTITION_VALUES, NO_SUBPARTITION_VALUES))
        {
            // Create a business object data entity.
            BusinessObjectDataEntity businessObjectDataEntity =
                createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, true, BDATA_STATUS);

            // Get the business object data by key.
            BusinessObjectDataEntity resultBusinessObjectDataEntity = dmDao.getBusinessObjectDataByAltKey(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION));

            // Validate the returned object.
            assertNotNull(resultBusinessObjectDataEntity);
            assertEquals(businessObjectDataEntity.getId(), resultBusinessObjectDataEntity.getId());

            // Get the business object data by key without specifying business object format version, which is an optional parameter.
            resultBusinessObjectDataEntity = dmDao.getBusinessObjectDataByAltKey(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION));

            // Validate the returned object.
            assertNotNull(resultBusinessObjectDataEntity);
            assertEquals(businessObjectDataEntity.getId(), resultBusinessObjectDataEntity.getId());

            // Get the business object data by key without specifying both business object format version and business object data version optional parameters.
            resultBusinessObjectDataEntity = dmDao.getBusinessObjectDataByAltKey(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues, null));

            // Validate the returned object.
            assertNotNull(resultBusinessObjectDataEntity);
            assertEquals(businessObjectDataEntity.getId(), resultBusinessObjectDataEntity.getId());
        }
    }

    @Test
    public void testGetBusinessObjectDataByAltKeyNoDataVersionSpecifiedMultipleRecordsFound()
    {
        // Create and persist multiple latest business object data entities for the same format version.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            INITIAL_DATA_VERSION, true, BDATA_STATUS);
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            SECOND_DATA_VERSION, true, BDATA_STATUS);

        try
        {
            dmDao.getBusinessObjectDataByAltKey(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, null));
            fail("Should throw an IllegalArgumentException when multiple latest business object data instances exist.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one business object data instance with parameters {namespace=\"%s\", " +
                "businessObjectDefinitionName=\"%s\", businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", " +
                "businessObjectFormatVersion=\"%d\", businessObjectDataPartitionValue=\"%s\", businessObjectDataSubPartitionValues=\"%s,%s,%s,%s\", " +
                "businessObjectDataVersion=\"null\", businessObjectDataStatus=\"null\"}.", NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2),
                SUBPARTITION_VALUES.get(3)), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataByAltKeyAndStatus()
    {
        // Execute the same set of tests on business object data entities created with and without subpartition values.
        for (List<String> subPartitionValues : Arrays.asList(SUBPARTITION_VALUES, NO_SUBPARTITION_VALUES))
        {
            // Create a business object data entity.
            BusinessObjectDataEntity businessObjectDataEntity =
                createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, true, BDATA_STATUS);

            // Get the business object data by key and business object data status, but without
            // specifying both business object format version and business object data version.
            BusinessObjectDataEntity resultBusinessObjectDataEntity = dmDao.getBusinessObjectDataByAltKeyAndStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues, null),
                BDATA_STATUS);

            // Validate the results.
            assertNotNull(resultBusinessObjectDataEntity);
            assertEquals(businessObjectDataEntity.getId(), resultBusinessObjectDataEntity.getId());

            // Get the business object data by key using a wrong business object data status and without
            // specifying both business object format version and business object data version.
            resultBusinessObjectDataEntity = dmDao.getBusinessObjectDataByAltKeyAndStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues, null),
                BDATA_STATUS_2);

            // Validate the results.
            assertNull(resultBusinessObjectDataEntity);
        }
    }

    @Test
    public void testGetBusinessObjectDataByAltKeyAndStatusOlderFormatVersionHasNewerDataVersion()
    {
        // Create two business object data instances that have newer data version in the older format version.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, SECOND_DATA_VERSION, true, BDATA_STATUS);
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Get the business object data by key using a wrong business object data status and without
        // specifying both business object format version and business object data version.
        BusinessObjectDataEntity resultBusinessObjectDataEntity = dmDao.getBusinessObjectDataByAltKeyAndStatus(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, SUBPARTITION_VALUES, null),
            BDATA_STATUS);

        // Validate the results.
        assertNotNull(resultBusinessObjectDataEntity);
        assertEquals(SECOND_FORMAT_VERSION, resultBusinessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion());
        assertEquals(INITIAL_DATA_VERSION, resultBusinessObjectDataEntity.getVersion());
    }

    @Test
    public void testGetBusinessObjectDataMaxVersion()
    {
        // Execute the same set of tests on the sets of business object data entities with and without subpartition values.
        for (List<String> subPartitionValues : Arrays.asList(SUBPARTITION_VALUES, NO_SUBPARTITION_VALUES))
        {
            // Create multiple version of the same business object data with the latest
            // version not having the latest flag set.  The latest flag is incorrectly set for the second version. This is done
            // to validate that the latest flag is not used by this method to find the latest (maximum) business object data version.
            for (Integer version : Arrays.asList(INITIAL_DATA_VERSION, SECOND_DATA_VERSION, THIRD_DATA_VERSION))
            {
                createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, version, version.equals(SECOND_DATA_VERSION), BDATA_STATUS);
            }

            // Get the maximum business object data version for this business object data.
            Integer maxVersion = dmDao.getBusinessObjectDataMaxVersion(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    null));

            // Validate the results.
            assertEquals(THIRD_DATA_VERSION, maxVersion);
        }
    }

    @Test
    public void testGetBusinessObjectDataPartitionValue()
    {
        // Create database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null, new ArrayList<SchemaColumn>(), new ArrayList<SchemaColumn>(),
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, SUBPARTITION_VALUES);

        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Get the maximum available partition value.
        assertEquals(STORAGE_1_GREATEST_PARTITION_VALUE, dmDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, DATA_VERSION,
                Arrays.asList(STORAGE_NAME), null, null));

        // Get the minimum available partition value.
        assertEquals(STORAGE_1_LEAST_PARTITION_VALUE, dmDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, DATA_VERSION,
                Arrays.asList(STORAGE_NAME)));

        // Get the maximum available partition value by not passing any of the optional parameters.
        assertEquals(STORAGE_1_GREATEST_PARTITION_VALUE, dmDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, Arrays.asList(STORAGE_NAME), null,
            null));
    }

    @Test
    public void testGetBusinessObjectDataMaxPartitionValueWithUpperAndLowerBounds()
    {
        // Create database entities required for testing.
        createStorageUnitEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2, SUBPARTITION_VALUES,
            DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME);

        // Test retrieving the maximum available partition value using an upper bound partition value.
        assertNull(dmDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, Arrays.asList(STORAGE_NAME),
            PARTITION_VALUE, null));
        assertEquals(PARTITION_VALUE_2, dmDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, Arrays.asList(STORAGE_NAME),
            PARTITION_VALUE_2, null));
        assertEquals(PARTITION_VALUE_2, dmDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, Arrays.asList(STORAGE_NAME),
            PARTITION_VALUE_3, null));

        // Test retrieving the maximum available partition value using a lower bound partition value.
        assertEquals(PARTITION_VALUE_2, dmDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, Arrays.asList(STORAGE_NAME), null,
            PARTITION_VALUE));
        assertEquals(PARTITION_VALUE_2, dmDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, Arrays.asList(STORAGE_NAME), null,
            PARTITION_VALUE_2));
        assertNull(dmDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, Arrays.asList(STORAGE_NAME), null,
            PARTITION_VALUE_3));
    }

    /**
     * This unit test validates that we do not rely on the business object data latest version flag when selecting an aggregate on the business object data
     * partition value.
     */
    @Test
    public void testGetBusinessObjectDataPartitionValueLatestDataVersionNotInStorage()
    {
        // Create a business object format entity.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(businessObjectFormatKey, FORMAT_DESCRIPTION, true, PARTITION_KEY);

        // Create two versions of business object data instances with the latest version not located in the test storage.
        List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays
            .asList(createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BDATA_STATUS),
                createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUBPARTITION_VALUES, SECOND_DATA_VERSION, true, BDATA_STATUS));

        // Create a storage instance and a storage unit for the initial business object data version only.
        StorageEntity storageEntity = createStorageEntity(STORAGE_NAME);
        createStorageUnitEntity(storageEntity, businessObjectDataEntities.get(0));

        // Get the maximum available partition value in the test storage without specifying business object data version.
        assertEquals(PARTITION_VALUE, dmDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, null,
                Arrays.asList(STORAGE_NAME), null, null));

        // Get the minimum available partition value in the test storage without specifying business object data version.
        assertEquals(PARTITION_VALUE, dmDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, null,
                Arrays.asList(STORAGE_NAME)));
    }

    @Test
    public void testGetBusinessObjectDataCount()
    {
        // Create a business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY, PARTITION_KEY_GROUP);

        // Create a business object data entity associated with the business object format.
        createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Get the number of business object formats that use this partition key group.
        Long result =
            dmDao.getBusinessObjectDataCount(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION));

        // Validate the results.
        assertEquals(Long.valueOf(1L), result);
    }

    @Test
    public void testGetBusinessObjectDataEntitiesByKey()
    {
        // Execute the same set of tests on the sets of business object data entities with and without subpartition values.
        for (List<String> subPartitionValues : Arrays.asList(SUBPARTITION_VALUES, NO_SUBPARTITION_VALUES))
        {
            // Create two business object data entities that differ on both format version and data version.
            List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(
                createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, SECOND_DATA_VERSION, false, BDATA_STATUS),
                createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, THIRD_DATA_VERSION, false, BDATA_STATUS));

            // Retrieve the first business object data entity by specifying the entire business object data key.
            List<BusinessObjectDataEntity> resultBusinessObjectDataEntities = dmDao.getBusinessObjectDataEntities(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, SECOND_DATA_VERSION));

            // Validate the results.
            assertNotNull(resultBusinessObjectDataEntities);
            assertEquals(1, resultBusinessObjectDataEntities.size());
            assertEquals(businessObjectDataEntities.get(0).getId(), resultBusinessObjectDataEntities.get(0).getId());

            // Retrieve both business object data entities by not specifying both format and data versions.
            resultBusinessObjectDataEntities = dmDao.getBusinessObjectDataEntities(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues, null));

            // Validate the results.
            assertNotNull(resultBusinessObjectDataEntities);
            assertTrue(resultBusinessObjectDataEntities.containsAll(businessObjectDataEntities));
            assertTrue(businessObjectDataEntities.containsAll(resultBusinessObjectDataEntities));
        }
    }

    @Test
    public void testGetBusinessObjectDataEntitiesByPartitionFiltersAndStorage()
    {
        // Create database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null, new ArrayList<SchemaColumn>(), new ArrayList<SchemaColumn>(),
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, SUBPARTITION_VALUES);

        // Build a list of partition values, large enough to cause executing the select queries in chunks.
        List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < DmDaoImpl.MAX_PARTITION_FILTERS_PER_REQUEST; i++)
        {
            partitionValues.add(String.format("%s-%s", PARTITION_VALUE, i));
        }
        partitionValues.addAll(UNSORTED_PARTITION_VALUES);

        // Build a list of partition filters to select the "available" business object data.
        // We add a second level partition value to partition filters here just for conditional coverage.
        List<List<String>> partitionFilters = new ArrayList<>();
        for (String partitionValue : partitionValues)
        {
            partitionFilters.add(Arrays.asList(partitionValue, SUBPARTITION_VALUES.get(0), null, null, null));
        }

        // Retrieve the available business object data per specified parameters.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities1 = dmDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                partitionFilters, DATA_VERSION, null, STORAGE_NAME);

        // Validate the results.
        assertNotNull(resultBusinessObjectDataEntities1);
        assertEquals(STORAGE_1_AVAILABLE_PARTITION_VALUES.size(), resultBusinessObjectDataEntities1.size());
        for (int i = 0; i < STORAGE_1_AVAILABLE_PARTITION_VALUES.size(); i++)
        {
            assertEquals(STORAGE_1_AVAILABLE_PARTITION_VALUES.get(i), resultBusinessObjectDataEntities1.get(i).getPartitionValue());
        }

        // Retrieve the available business object data without specifying a business object format version, which is an optional parameter.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities2 = dmDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, DATA_VERSION, null, STORAGE_NAME);

        // Validate the results.
        assertEquals(resultBusinessObjectDataEntities1, resultBusinessObjectDataEntities2);

        // Retrieve the available business object data without specifying both business object format version and business object data version.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities3 = dmDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, null, STORAGE_NAME);

        // Validate the results.
        assertEquals(resultBusinessObjectDataEntities1, resultBusinessObjectDataEntities3);

        // Retrieve the business object data with VALID business object data status without
        // specifying both business object format version and business object data version.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities4 = dmDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME);

        // Validate the results.
        assertEquals(resultBusinessObjectDataEntities1, resultBusinessObjectDataEntities4);

        // Retrieve the available business object data with wrong business object data status and without
        // specifying both business object format version and business object data version.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities5 = dmDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, BDATA_STATUS, STORAGE_NAME);

        // Validate the results.
        assertTrue(resultBusinessObjectDataEntities5.isEmpty());
    }

    @Test
    public void testGetBusinessObjectDataEntitiesByPartitionFiltersAndStorageOlderFormatVersionHasNewerDataVersion()
    {
        // Create two business object data instances that have newer data version in the older format version.
        List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, true, BDATA_STATUS),
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS));

        // Create a storage instance and relative storage units.
        StorageEntity storageEntity = createStorageEntity(STORAGE_NAME);
        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities)
        {
            createStorageUnitEntity(storageEntity, businessObjectDataEntity);
        }

        // Build a list of partition filters to select the "available" business object data.
        List<List<String>> partitionFilters = new ArrayList<>();
        partitionFilters.add(Arrays.asList(PARTITION_VALUE, null, null, null, null));

        // Retrieve the available business object data without specifying both business object format version and business object data version.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities = dmDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, BDATA_STATUS, STORAGE_NAME);

        // Validate the results.
        assertNotNull(resultBusinessObjectDataEntities);
        assertEquals(1, resultBusinessObjectDataEntities.size());
        assertEquals(SECOND_FORMAT_VERSION, resultBusinessObjectDataEntities.get(0).getBusinessObjectFormat().getBusinessObjectFormatVersion());
        assertEquals(INITIAL_DATA_VERSION, resultBusinessObjectDataEntities.get(0).getVersion());
    }

    /**
     * This unit test validates that we do not rely on the business object data latest version flag when selecting business object entities by partition filters
     * and storage.
     */
    @Test
    public void testGetBusinessObjectDataEntitiesByPartitionFiltersAndStorageLatestDataVersionNotInStorage()
    {
        // Create several versions of business object data instances with the latest version not located in the test storage and
        // the second version having different business object data status.
        List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BDATA_STATUS),
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, false, BDATA_STATUS_2),
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, THIRD_DATA_VERSION, true, BDATA_STATUS));

        // Create a storage instance and relative storage units for the first two business object data versions.
        StorageEntity storageEntity = createStorageEntity(STORAGE_NAME);
        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities.subList(0, 2))
        {
            createStorageUnitEntity(storageEntity, businessObjectDataEntity);
        }

        // Build a list of partition filters to select the "available" business object data.
        List<List<String>> partitionFilters = new ArrayList<>();
        partitionFilters.add(Arrays.asList(PARTITION_VALUE, null, null, null, null));

        // Retrieve a business object data in the test storage without specifying business object data version
        // - the latest available business object data with the specified business object data status.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities = dmDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                partitionFilters, null, BDATA_STATUS, STORAGE_NAME);

        // Validate the results - we expect to get back the initial business object data version.
        assertNotNull(resultBusinessObjectDataEntities);
        assertEquals(1, resultBusinessObjectDataEntities.size());
        assertEquals(INITIAL_DATA_VERSION, resultBusinessObjectDataEntities.get(0).getVersion());

        // Retrieve a business object data in the test storage without specifying both business object data status
        // and business object data version - the latest available business object data regardless of the status.
        resultBusinessObjectDataEntities = dmDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                partitionFilters, null, null, STORAGE_NAME);

        // Validate the results - we expect to get back the second business object data version.
        assertNotNull(resultBusinessObjectDataEntities);
        assertEquals(1, resultBusinessObjectDataEntities.size());
        assertEquals(SECOND_DATA_VERSION, resultBusinessObjectDataEntities.get(0).getVersion());
    }

    /**
     * Validates that we correctly select business object data entities per specified storage name, threshold minutes and excluded business object status
     * values.
     */
    @Test
    public void testGetBusinessObjectDataFromStorageWithThreshold()
    {
        // Create the database entities required for testing.
        List<String> storageNames = Arrays.asList(STORAGE_NAME, STORAGE_NAME_2);
        List<String> businessObjectDataStatuses = Arrays.asList(BDATA_STATUS, BDATA_STATUS_2);
        List<Integer> createdOnTimestampMinutesOffsets = Arrays.asList(5, 15, 20);
        int counter = 0;
        for (String storageName : storageNames)
        {
            StorageEntity storageEntity = createStorageEntity(storageName);
            for (String businessObjectDataStatus : businessObjectDataStatuses)
            {
                for (Integer offset : createdOnTimestampMinutesOffsets)
                {
                    BusinessObjectDataEntity businessObjectDataEntity =
                        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                            String.format("%s-%d", PARTITION_VALUE, counter++), SUBPARTITION_VALUES, DATA_VERSION, true, businessObjectDataStatus);
                    // Apply the offset in minutes to createdOn value.
                    businessObjectDataEntity.setCreatedOn(new Timestamp(businessObjectDataEntity.getCreatedOn().getTime() - offset * 60 * 1000));
                    createStorageUnitEntity(storageEntity, businessObjectDataEntity);
                    dmDao.saveAndRefresh(businessObjectDataEntity);
                }
            }
        }

        // Select a subset of test business object entities.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities =
            dmDao.getBusinessObjectDataFromStorageOlderThan(STORAGE_NAME, 10, Arrays.asList(BDATA_STATUS_2));

        // Validate the results.
        assertNotNull(resultBusinessObjectDataEntities);
        assertEquals(2, resultBusinessObjectDataEntities.size());
        for (BusinessObjectDataEntity businessObjectDataEntity : resultBusinessObjectDataEntities)
        {
            assertEquals(1, businessObjectDataEntity.getStorageUnits().size());
            assertEquals(STORAGE_NAME, businessObjectDataEntity.getStorageUnits().iterator().next().getStorage().getName());
            assertEquals(BDATA_STATUS, businessObjectDataEntity.getStatus().getCode());
        }
    }

    // Storage

    @Test
    public void testGetStorages()
    {
        // Create and persist storage entities.
        for (StorageKey key : getTestStorageKeys())
        {
            createStorageEntity(key.getStorageName());
        }

        // Retrieve a list of storage keys.
        List<StorageKey> resultStorageKeys = dmDao.getStorages();

        // Validate the returned object.
        assertNotNull(resultStorageKeys);
        assertTrue(resultStorageKeys.containsAll(getTestStorageKeys()));
    }

    // StorageUnit

    @Test
    public void testGetStorageUnitByStorageNameAndDirectoryPath()
    {
        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS);
        StorageEntity storageEntity = createStorageEntity(STORAGE_NAME);
        StorageUnitEntity storageUnitEntity = createStorageUnitEntity(storageEntity, businessObjectDataEntity, STORAGE_DIRECTORY_PATH);

        // Retrieve the relative storage file entities and validate the results.
        assertEquals(storageUnitEntity, dmDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME, STORAGE_DIRECTORY_PATH));

        // Test case insensitivity for the storage name.
        assertEquals(storageUnitEntity, dmDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME.toUpperCase(), STORAGE_DIRECTORY_PATH));
        assertEquals(storageUnitEntity, dmDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME.toLowerCase(), STORAGE_DIRECTORY_PATH));

        // Test case sensitivity of the storage directory path.
        assertNull(dmDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME, STORAGE_DIRECTORY_PATH.toUpperCase()));
        assertNull(dmDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME, STORAGE_DIRECTORY_PATH.toLowerCase()));

        // Confirm negative results when using wrong input parameters.
        assertNull(dmDao.getStorageUnitByStorageNameAndDirectoryPath("I_DO_NOT_EXIST", TEST_S3_KEY_PREFIX));
        assertNull(dmDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME, "I_DO_NOT_EXIST"));
    }

    /**
     * Tests {@link DmDao#getStorageUnitByBusinessObjectDataAndStorageName(BusinessObjectDataEntity, String)}.
     */
    @Test
    public void testGetStorageUnitByBusinessObjectDataAndStorageName()
    {
        BusinessObjectDataEntity businessObjectDataEntity = createBusinessObjectDataEntity();
        StorageUnitEntity storageUnitEntity = createStorageUnitEntity(createStorageEntity(STORAGE_NAME), businessObjectDataEntity);

        // test retrieval by name
        assertEquals(storageUnitEntity, dmDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME));

        // test retrieval by name, case insensitive
        assertEquals(storageUnitEntity, dmDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME.toUpperCase()));
        assertEquals(storageUnitEntity, dmDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME.toLowerCase()));

        // test retrieval failure
        assertNull(dmDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, "I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetStorageUnitsByStorageAndBusinessObjectData()
    {
        // Create database entities required for testing.

        // Create a storage entity.
        StorageEntity storageEntity = createStorageEntity(STORAGE_NAME);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Create a storage unit.
        createStorageUnitEntity(storageEntity, businessObjectDataEntity, TEST_S3_KEY_PREFIX);

        // Retrieve storage unit entities by storage and business object data.
        List<StorageUnitEntity> resultStorageUnitEntities =
            dmDao.getStorageUnitsByStorageAndBusinessObjectData(storageEntity, Arrays.asList(businessObjectDataEntity));

        // Validate the results.
        assertNotNull(resultStorageUnitEntities);
        assertEquals(1, resultStorageUnitEntities.size());
        assertEquals(TEST_S3_KEY_PREFIX, resultStorageUnitEntities.get(0).getDirectoryPath());
    }

    @Test
    public void testGetStorageUnitsByPartitionFiltersAndStorages()
    {
        // Create database entities required for testing.
        List<StorageUnitEntity> expectedMultiStorageAvailableStorageUnits =
            createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null, new ArrayList<SchemaColumn>(), new ArrayList<SchemaColumn>(),
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, SUBPARTITION_VALUES, STORAGE_NAMES);

        // Build a list of partition values, large enough to cause executing the select queries in chunks.
        List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < DmDaoImpl.MAX_PARTITION_FILTERS_PER_REQUEST; i++)
        {
            partitionValues.add(String.format("%s-%s", PARTITION_VALUE, i));
        }
        partitionValues.addAll(UNSORTED_PARTITION_VALUES);

        // Build a list of partition filters to select the "available" business object data.
        // We add a second level partition value to partition filters here just for conditional coverage.
        List<List<String>> partitionFilters = new ArrayList<>();
        for (String partitionValue : partitionValues)
        {
            partitionFilters.add(Arrays.asList(partitionValue, SUBPARTITION_VALUES.get(0), null, null, null));
        }

        // Retrieve the available business object data per specified parameters.
        List<StorageUnitEntity> resultStorageUnitEntities1 = dmDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION, null,
            STORAGE_NAMES);

        // Validate the results.
        assertNotNull(resultStorageUnitEntities1);
        assertEquals(expectedMultiStorageAvailableStorageUnits, resultStorageUnitEntities1);

        // Retrieve the available business object data without specifying a business object format version, which is an optional parameter.
        List<StorageUnitEntity> resultStorageUnitEntities2 = dmDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, DATA_VERSION, null, STORAGE_NAMES);

        // Validate the results.
        assertEquals(resultStorageUnitEntities1, resultStorageUnitEntities2);

        // Retrieve the available business object data without specifying both business object format version and business object data version.
        List<StorageUnitEntity> resultStorageUnitEntities3 = dmDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, null, STORAGE_NAMES);

        // Validate the results.
        assertEquals(resultStorageUnitEntities1, resultStorageUnitEntities3);

        // Retrieve the business object data with VALID business object data status without
        // specifying both business object format version and business object data version.
        List<StorageUnitEntity> resultStorageUnitEntities4 = dmDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, BusinessObjectDataStatusEntity.VALID, STORAGE_NAMES);

        // Validate the results.
        assertEquals(resultStorageUnitEntities1, resultStorageUnitEntities4);

        // Retrieve the available business object data with wrong business object data status and without
        // specifying both business object format version and business object data version.
        List<StorageUnitEntity> resultStorageUnitEntities5 = dmDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, BDATA_STATUS, STORAGE_NAMES);

        // Validate the results.
        assertTrue(resultStorageUnitEntities5.isEmpty());
    }

    // StorageFile

    @Test
    public void testGetStorageFileByStorageNameAndFilePath()
    {
        // Create relative database entities.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS);
        StorageEntity storageEntity = dmDao.getStorageByName(StorageEntity.MANAGED_STORAGE);
        StorageUnitEntity storageUnitEntity = createStorageUnitEntity(storageEntity, businessObjectDataEntity);

        for (String file : LOCAL_FILES)
        {
            createStorageFileEntity(storageUnitEntity, file, FILE_SIZE_1_KB, ROW_COUNT_1000);
        }

        // Retrieve the relative storage file entities and validate the results.
        for (String file : LOCAL_FILES)
        {
            StorageFileEntity storageFileEntity = dmDao.getStorageFileByStorageNameAndFilePath(StorageEntity.MANAGED_STORAGE, file);
            assertTrue(storageFileEntity.getPath().compareTo(file) == 0);
            assertTrue(storageFileEntity.getFileSizeBytes().compareTo(FILE_SIZE_1_KB) == 0);
            assertTrue(storageFileEntity.getRowCount().compareTo(ROW_COUNT_1000) == 0);
        }

        // Confirm negative results when using wrong input parameters.
        assertNull(dmDao.getStorageFileByStorageNameAndFilePath("I_DO_NOT_EXIST", LOCAL_FILES.get(0)));
        assertNull(dmDao.getStorageFileByStorageNameAndFilePath(StorageEntity.MANAGED_STORAGE, "I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetStorageFileByStorageNameAndFilePathDuplicateFiles() throws Exception
    {
        // Create relative database entities.
        BusinessObjectDataEntity businessObjectDataEntity1 =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, Boolean.TRUE, BDATA_STATUS);
        BusinessObjectDataEntity businessObjectDataEntity2 =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION + 1, Boolean.TRUE, BDATA_STATUS);

        StorageEntity storageEntity = dmDao.getStorageByName(StorageEntity.MANAGED_STORAGE);

        StorageUnitEntity storageUnitEntity1 = createStorageUnitEntity(storageEntity, businessObjectDataEntity1);
        StorageUnitEntity storageUnitEntity2 = createStorageUnitEntity(storageEntity, businessObjectDataEntity2);

        createStorageFileEntity(storageUnitEntity1, LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);
        createStorageFileEntity(storageUnitEntity2, LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);

        try
        {
            // Try to retrieve storage file.
            dmDao.getStorageFileByStorageNameAndFilePath(StorageEntity.MANAGED_STORAGE, LOCAL_FILE);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage().startsWith("Found more than one storage file with parameters"));
        }
    }

    @Test
    public void testGetStorageFileCount()
    {
        // Create relative database entities.
        createDatabaseEntitiesForStorageFilesTesting();

        // Validate that we can get correct count for each file.
        for (String file : LOCAL_FILES)
        {
            assertEquals(Long.valueOf(1L), dmDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE, file));
        }

        // Validate that we can get correct file count using upper and lower storage name.
        assertEquals(Long.valueOf(1L), dmDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE.toUpperCase(), LOCAL_FILES.get(0)));
        assertEquals(Long.valueOf(1L), dmDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE.toLowerCase(), LOCAL_FILES.get(0)));

        // Get 0 file count by specifying non-existing storage.
        assertEquals(Long.valueOf(0L), dmDao.getStorageFileCount("I_DO_NOT_EXIST", LOCAL_FILES.get(0)));

        // Get 0 file count by specifying non-existing file path prefix.
        assertEquals(Long.valueOf(0L), dmDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE, "I_DO_NOT_EXIST"));

        // Validate that we can get correct count of files from the LOCAL_FILES list that match "folder" file path prefix.
        assertEquals(Long.valueOf(3L), dmDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE, "folder"));
    }

    @Test
    public void testGetStorageFilesByStorageAndFilePathPrefix()
    {
        // Create relative database entities.
        createDatabaseEntitiesForStorageFilesTesting();

        List<StorageFileEntity> storageFileEntities;

        // Validate that we can retrieve each file.
        for (String file : LOCAL_FILES)
        {
            storageFileEntities = dmDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE, file);
            assertEquals(1, storageFileEntities.size());
            assertEquals(file, storageFileEntities.get(0).getPath());
        }

        // Validate that we can retrieve a file using upper and lower storage name.
        assertEquals(LOCAL_FILES.get(0),
            dmDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE.toUpperCase(), LOCAL_FILES.get(0)).get(0).getPath());
        assertEquals(LOCAL_FILES.get(0),
            dmDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE.toLowerCase(), LOCAL_FILES.get(0)).get(0).getPath());

        // Try to get file entities by specifying non-existing storage.
        assertEquals(0, dmDao.getStorageFilesByStorageAndFilePathPrefix("I_DO_NOT_EXIST", LOCAL_FILES.get(0)).size());

        // Try to get file entities by specifying non-existing file path prefix.
        assertEquals(0, dmDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE, "I_DO_NOT_EXIST").size());

        // Validate that we can retrieve the last 3 files in the expected order from the LOCAL_FILES list that match "folder" file path prefix.
        storageFileEntities = dmDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE, "folder");
        List<String> expectedFiles = Arrays.asList(LOCAL_FILES.get(5), LOCAL_FILES.get(4), LOCAL_FILES.get(3));
        assertEquals(expectedFiles.size(), storageFileEntities.size());
        for (int i = 0; i < expectedFiles.size(); i++)
        {
            assertEquals(expectedFiles.get(i), storageFileEntities.get(i).getPath());
        }
    }

    @Test
    public void testGetStorageFilesByStorageUnits()
    {
        // Create database entities required for testing.

        // Create a storage entity.
        StorageEntity storageEntity = createStorageEntity(STORAGE_NAME);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Create a storage unit along with the storage files entities.
        StorageUnitEntity storageUnitEntity = createStorageUnitEntity(storageEntity, businessObjectDataEntity);
        for (String file : LOCAL_FILES)
        {
            createStorageFileEntity(storageUnitEntity, file, FILE_SIZE_1_KB, ROW_COUNT_1000);
        }

        // Retrieve storage file entities by storage and business object data.
        List<StorageFileEntity> resultStorageFileEntities = dmDao.getStorageFilesByStorageUnits(Arrays.asList(storageUnitEntity));

        // Validate the results.
        assertNotNull(resultStorageFileEntities);
        assertEquals(LOCAL_FILES.size(), resultStorageFileEntities.size());
        for (int i = 0; i < resultStorageFileEntities.size(); i++)
        {
            assertEquals(SORTED_LOCAL_FILES.get(i), resultStorageFileEntities.get(i).getPath());
        }
    }

    private void createDatabaseEntitiesForStorageFilesTesting()
    {
        // Create relative database entities.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS);
        StorageEntity storageEntity = dmDao.getStorageByName(StorageEntity.MANAGED_STORAGE);
        StorageUnitEntity storageUnitEntity = createStorageUnitEntity(storageEntity, businessObjectDataEntity);

        for (String file : LOCAL_FILES)
        {
            createStorageFileEntity(storageUnitEntity, file, FILE_SIZE_1_KB, ROW_COUNT_1000);
        }
    }

    // JobDefinition

    /**
     * Tests the happy path scenario by providing all the parameters.
     */
    @Test
    public void testGetJobDefinitionByAltKey()
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = createNamespaceEntity(NAMESPACE_CD);

        // Create and persist a job definition entity.
        createJobDefinitionEntity(namespaceEntity, JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID);

        // Call the API to query the newly added entity by providing the app and job details
        JobDefinitionEntity jobDefinitionEntityResult = dmDao.getJobDefinitionByAltKey(NAMESPACE_CD, JOB_NAME);

        // Fail if there is any problem in the result
        assertNotNull(jobDefinitionEntityResult);
        assertEquals(NAMESPACE_CD, jobDefinitionEntityResult.getNamespace().getCode());
        assertEquals(JOB_NAME, jobDefinitionEntityResult.getName());
        assertEquals(JOB_DESCRIPTION, jobDefinitionEntityResult.getDescription());
        assertEquals(ACTIVITI_ID, jobDefinitionEntityResult.getActivitiId());
    }

    /**
     * Tests the scenario by providing a job name that doesn't exist.
     */
    @Test
    public void testGetJobDefinitionByAltKeyJobNameNoExists()
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = createNamespaceEntity(NAMESPACE_CD);

        // Create a job definition entity
        createJobDefinitionEntity(namespaceEntity, JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID);

        // Call the API to query the newly added entity by providing the app and a job name that doesn't exist.
        JobDefinitionEntity jobDefinitionEntityResult = dmDao.getJobDefinitionByAltKey(NAMESPACE_CD, JOB_NAME_2);

        // Validate the results.
        assertNull(jobDefinitionEntityResult);
    }

    /**
     * Tests the scenario by providing the wrong app name.
     */
    @Test
    public void testGetJobDefinitionByAltKeyNamespaceNoExists()
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = createNamespaceEntity(NAMESPACE_CD);
        createNamespaceEntity(NAMESPACE_CD_2);

        // Create and persist a new job definition entity.
        createJobDefinitionEntity(namespaceEntity, JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID);

        // Call the API to query the newly added entity by providing an namespace code that doesn't exist and a job name that does exist.
        JobDefinitionEntity jobDefinitionEntityResult = dmDao.getJobDefinitionByAltKey(NAMESPACE_CD_2, JOB_NAME);

        // Validate the results.
        assertNull(jobDefinitionEntityResult);
    }

    /**
     * Tests the scenario by finding multiple job definition records.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetJobDefinitionByAltKeyMultipleRecordsFound()
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = createNamespaceEntity(NAMESPACE_CD);

        // Create two job definitions different.
        for (String jobName : Arrays.asList(JOB_NAME.toUpperCase(), JOB_NAME.toLowerCase()))
        {
            // Create a job definition entity.  Please note that we need to pass unique activity ID value.
            createJobDefinitionEntity(namespaceEntity, jobName, JOB_DESCRIPTION, jobName + ACTIVITI_ID);
        }

        // Try to retrieve the the job definition.
        dmDao.getJobDefinitionByAltKey(NAMESPACE_CD, JOB_NAME);
    }

    // EmrClusterDefinition

    /**
     * Tests the happy path scenario by providing all the parameters.
     */
    @Test
    public void testGetEmrClusterDefinitionByAltKey() throws IOException
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = createNamespaceEntity(NAMESPACE_CD);

        // Create and persist a EMR cluster definition entity.
        createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Call the API to query the newly added entity by providing the app and EMT cluster definition details
        EmrClusterDefinitionEntity emrClusterDefinitionEntityResult =
            dmDao.getEmrClusterDefinitionByAltKey(new EmrClusterDefinitionKey(NAMESPACE_CD, EMR_CLUSTER_DEFINITION_NAME));

        // Fail if there is any problem in the result
        assertNotNull(emrClusterDefinitionEntityResult);
        assertEquals(NAMESPACE_CD, emrClusterDefinitionEntityResult.getNamespace().getCode());
        assertEquals(EMR_CLUSTER_DEFINITION_NAME, emrClusterDefinitionEntityResult.getName());
    }

    /**
     * Tests the scenario by providing a EMR Cluster name that doesn't exist.
     */
    @Test
    public void testGetEmrClusterDefinitionByAltKeyDefinitionNameNoExists() throws IOException
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = createNamespaceEntity(NAMESPACE_CD);

        // Create a EMR Cluster definition entity
        createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Call the API to query the newly added entity by providing the app and a definition name that doesn't exist.
        EmrClusterDefinitionEntity emrClusterDefinitionEntityResult = dmDao.getEmrClusterDefinitionByAltKey(NAMESPACE_CD, EMR_CLUSTER_DEFINITION_NAME_2);

        // Validate the results.
        assertNull(emrClusterDefinitionEntityResult);
    }

    /**
     * Tests the scenario by providing the wrong app name.
     */
    @Test
    public void testGetEmrClusterDefinitionByAltKeyNamespaceNoExists() throws IOException
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = createNamespaceEntity(NAMESPACE_CD);
        createNamespaceEntity(NAMESPACE_CD_2);

        // Create a EMR Cluster definition entity
        createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Call the API to query the newly added entity by providing the app and a definition name that doesn't exist.
        EmrClusterDefinitionEntity emrClusterDefinitionEntityResult = dmDao.getEmrClusterDefinitionByAltKey(NAMESPACE_CD_2, EMR_CLUSTER_DEFINITION_NAME_2);

        // Validate the results.
        assertNull(emrClusterDefinitionEntityResult);
    }

    /**
     * Tests the scenario by finding multiple EMR Cluster definition records.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetEmrClusterDefinitionByAltKeyMultipleRecordsFound() throws IOException
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = createNamespaceEntity(NAMESPACE_CD);

        // Create two EMR cluster definitions different.
        for (String definitionName : Arrays.asList(EMR_CLUSTER_DEFINITION_NAME.toUpperCase(), EMR_CLUSTER_DEFINITION_NAME.toLowerCase()))
        {
            // Create a EMR cluster definition entity.
            createEmrClusterDefinitionEntity(namespaceEntity, definitionName,
                IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));
        }

        // Try to retrieve the the job definition.
        dmDao.getEmrClusterDefinitionByAltKey(NAMESPACE_CD, EMR_CLUSTER_DEFINITION_NAME);
    }

    // BusinessObjectDataNotification

    @Test
    public void testGetBusinessObjectDataNotificationByAltKey()
    {
        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());

        // Retrieve this business object data notification.
        BusinessObjectDataNotificationRegistrationEntity resultBusinessObjectDataNotificationEntity =
            dmDao.getBusinessObjectDataNotificationByAltKey(new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataNotificationEntity);
        assertEquals(businessObjectDataNotificationRegistrationEntity.getId(), resultBusinessObjectDataNotificationEntity.getId());
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationKeys()
    {
        // Create and persist a set of business object data notification registration entities.
        for (BusinessObjectDataNotificationRegistrationKey key : getTestBusinessObjectDataNotificationRegistrationKeys())
        {
            createBusinessObjectDataNotificationRegistrationEntity(key.getNamespace(), key.getNotificationName(), NOTIFICATION_EVENT_TYPE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());
        }

        // Retrieve a list of business object  data notification registration keys for the specified namespace.
        List<BusinessObjectDataNotificationRegistrationKey> resultKeys = dmDao.getBusinessObjectDataNotificationRegistrationKeys(NAMESPACE_CD);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectDataNotificationRegistrationKeys(), resultKeys);
    }

    /**
     * Tests the getBusinessObjectDataNotificationRegistrations() method. Tests the happy path scenario by providing all the parameters.
     */
    @Test
    public void testGetBusinessObjectDataNotifications()
    {
        // Create and persist a business object data notification registration entity with all optional parameters specified.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Retrieve the business object notification matching the filter criteria.
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntities =
            dmDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey);

        // Validate the returned object.
        assertNotNull(businessObjectDataNotificationEntities);
        assertEquals(1, businessObjectDataNotificationEntities.size());
        assertEquals(businessObjectDataNotificationRegistrationEntity.getId(), businessObjectDataNotificationEntities.get(0).getId());
    }

    /**
     * Tests the getBusinessObjectDataNotificationRegistrations() method.
     */
    @Test
    public void testGetBusinessObjectDataNotificationsMissingOptionalFilterParameters()
    {
        // Create and persist a business object data notification registration entity with all optional filter parameters missing.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, BOD_NAME, null, null, null, null,
                getTestJobActions());

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Retrieve the business object notification matching the filter criteria.
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntities =
            dmDao.getBusinessObjectDataNotificationRegistrations(NOTIFICATION_EVENT_TYPE, businessObjectDataKey);

        // Validate the returned object.
        assertNotNull(businessObjectDataNotificationEntities);
        assertEquals(1, businessObjectDataNotificationEntities.size());
        assertEquals(businessObjectDataNotificationRegistrationEntity.getId(), businessObjectDataNotificationEntities.get(0).getId());
    }

    // SecurityFunction

    @Test
    public void testGetSecurityFunctionsByRole() throws Exception
    {
        // Create role and function.
        SecurityRoleEntity securityRoleEntity = new SecurityRoleEntity();
        securityRoleEntity.setCode("TEST_ROLE");
        dmDao.saveAndRefresh(securityRoleEntity);

        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode("FN_UT_SECURITY_FUNCTION");
        dmDao.saveAndRefresh(securityFunctionEntity);

        List<String> functions = dmDao.getSecurityFunctionsForRole("TEST_ROLE");

        // Add new role to functions mapping.
        SecurityRoleFunctionEntity securityRoleFunctionEntity = new SecurityRoleFunctionEntity();
        securityRoleFunctionEntity.setSecurityRole(securityRoleEntity);
        securityRoleFunctionEntity.setSecurityFunction(securityFunctionEntity);
        dmDao.saveAndRefresh(securityRoleFunctionEntity);

        List<String> functions2 = dmDao.getSecurityFunctionsForRole("TEST_ROLE");

        // Since the functions method is cached, the test function will not be retrieved.
        assertEquals(functions, functions2);

        // Clear the cache and retrieve the functions again.
        cacheManager.getCache(DaoSpringModuleConfig.DM_CACHE_NAME).clear();

        functions2 = dmDao.getSecurityFunctionsForRole("TEST_ROLE");

        assertNotEquals(functions, functions2);
    }

    @Test
    public void testGetSecurityFunctions() throws Exception
    {
        List<String> functions = dmDao.getSecurityFunctions();

        // Add a function in functions.
        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode("FN_UT_SECURITY_FUNCTION");

        dmDao.saveAndRefresh(securityFunctionEntity);

        List<String> functions2 = dmDao.getSecurityFunctions();

        // Since the functions method is cached, the test function will not be retrieved.
        assertEquals(functions, functions2);

        // Clear the cache and retrieve the functions again.
        cacheManager.getCache(DaoSpringModuleConfig.DM_CACHE_NAME).clear();

        functions2 = dmDao.getSecurityFunctions();

        assertNotEquals(functions, functions2);
    }

    // JmsMessage

    @Test
    public void testGetOldestJmsMessage() throws Exception
    {
        // Prepare database entries required for testing.
        List<JmsMessageEntity> jmsMessageEntities =
            Arrays.asList(createJmsMessageEntity(JMS_QUEUE_NAME, MESSAGE_TEXT), createJmsMessageEntity(JMS_QUEUE_NAME_2, MESSAGE_TEXT_2));

        // Retrieve the oldest JMS message.
        JmsMessageEntity oldestJmsMessageEntity = dmDao.getOldestJmsMessage();

        // Validate the results.
        assertNotNull(oldestJmsMessageEntity);
        assertEquals(jmsMessageEntities.get(0).getId(), oldestJmsMessageEntity.getId());
    }

    @Test
    public void testGetOldestJmsMessageQueueIsEmpty() throws Exception
    {
        // Try to retrieve the oldest JMS message from an empty queue table.
        JmsMessageEntity oldestJmsMessageEntity = dmDao.getOldestJmsMessage();

        // Validate the results.
        assertNull(oldestJmsMessageEntity);
    }

    // OnDemandPricing

    @Test
    public void testGetOnDemandPrice()
    {
        // Create database entities required for testing.
        OnDemandPriceEntity onDemandPriceEntity = createOnDemandPriceEntity(AWS_REGION, EC2_INSTANCE_TYPE);

        // Retrieve and validate this entity.
        assertEquals(onDemandPriceEntity, dmDao.getOnDemandPrice(AWS_REGION, EC2_INSTANCE_TYPE));

        // Test case sensitivity for the region and EC2 instance type.
        assertNull(dmDao.getOnDemandPrice(AWS_REGION.toUpperCase(), EC2_INSTANCE_TYPE));
        assertNull(dmDao.getOnDemandPrice(AWS_REGION.toLowerCase(), EC2_INSTANCE_TYPE));
        assertNull(dmDao.getOnDemandPrice(AWS_REGION, EC2_INSTANCE_TYPE.toUpperCase()));
        assertNull(dmDao.getOnDemandPrice(AWS_REGION, EC2_INSTANCE_TYPE.toLowerCase()));

        // Confirm negative results when using wrong input parameters.
        assertNull(dmDao.getOnDemandPrice("I_DO_NOT_EXIST", EC2_INSTANCE_TYPE));
        assertNull(dmDao.getOnDemandPrice(AWS_REGION, "I_DO_NOT_EXIST"));
    }

    // Helper methods.

    /**
     * Gets a date as a string.
     *
     * @param year the year of the date.
     * @param month the month of the date. Note that month is 0-based as per GregorianCalendar.
     * @param day the day of the date.
     *
     * @return the date as a string in the format using DmDao.DEFAULT_SINGLE_DAY_DATE_MASK.
     */
    private String getDateAsString(int year, int month, int day)
    {
        return new SimpleDateFormat(DmDao.DEFAULT_SINGLE_DAY_DATE_MASK).format(new GregorianCalendar(year, month, day).getTime());
    }
}

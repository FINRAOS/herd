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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.impl.HerdDaoImpl;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.api.xml.DataProviderKey;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.api.xml.ExpectedPartitionValueKey;
import org.finra.herd.model.api.xml.FileTypeKey;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.StoragePolicyPriorityLevel;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.ConfigurationEntity;
import org.finra.herd.model.jpa.CustomDdlEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.ExpectedPartitionValueEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.JmsMessageEntity;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.OnDemandPriceEntity;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;
import org.finra.herd.model.jpa.SecurityFunctionEntity;
import org.finra.herd.model.jpa.SecurityRoleEntity;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * This class tests various functionality within the DAO class.
 */
public class HerdDaoTest extends AbstractDaoTest
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
        herdDao.saveAndRefresh(configurationEntity);
    }

    // Configuration

    @Test
    public void testReadAllConfigurations() throws Exception
    {
        // Test that we are able to read configurations from the database using our DAO tier.
        List<ConfigurationEntity> configurations = herdDao.findAll(ConfigurationEntity.class);
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
        ConfigurationEntity configurationEntity = herdDao.getConfigurationByKey(TEST_KEY);
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
        NamespaceEntity resultNamespaceEntity = herdDao.getNamespaceByKey(new NamespaceKey(NAMESPACE_CD));

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
        List<NamespaceKey> resultNamespaceKeys = herdDao.getNamespaces();

        // Validate the returned object.
        assertNotNull(resultNamespaceKeys);
        assertTrue(resultNamespaceKeys.containsAll(getTestNamespaceKeys()));
    }

    // DataProvider

    @Test
    public void testGetDataProviderByKey()
    {
        // Create a data provider entity.
        DataProviderEntity dataProviderEntity = createDataProviderEntity(DATA_PROVIDER_NAME);

        // Retrieve the data provider entity.
        assertEquals(dataProviderEntity, herdDao.getDataProviderByKey(new DataProviderKey(DATA_PROVIDER_NAME)));

        // Test case insensitivity of data provider key.
        assertEquals(dataProviderEntity, herdDao.getDataProviderByKey(new DataProviderKey(DATA_PROVIDER_NAME.toUpperCase())));
        assertEquals(dataProviderEntity, herdDao.getDataProviderByKey(new DataProviderKey(DATA_PROVIDER_NAME.toLowerCase())));
    }

    @Test
    public void testGetDataProviderByName()
    {
        // Create a data provider entity.
        DataProviderEntity dataProviderEntity = createDataProviderEntity(DATA_PROVIDER_NAME);

        // Retrieve the data provider entity.
        assertEquals(dataProviderEntity, herdDao.getDataProviderByName(DATA_PROVIDER_NAME));

        // Test case insensitivity of data provider name.
        assertEquals(dataProviderEntity, herdDao.getDataProviderByName(DATA_PROVIDER_NAME.toUpperCase()));
        assertEquals(dataProviderEntity, herdDao.getDataProviderByName(DATA_PROVIDER_NAME.toLowerCase()));
    }

    @Test
    public void testGetDataProviders()
    {
        // Create and persist data provider entities.
        for (DataProviderKey key : DATA_PROVIDER_KEYS)
        {
            createDataProviderEntity(key.getDataProviderName());
        }

        // Retrieve a list of data provider keys and validate the returned object.
        assertEquals(DATA_PROVIDER_KEYS, herdDao.getDataProviders());
    }

    // BusinessObjectDefinition

    @Test
    public void testGetBusinessObjectDefinitionByKey()
    {
        // Create two business object definitions having the same business object definition name.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION);

        // Get the business object definition by key.
        BusinessObjectDefinitionEntity resultBusinessObjectDefinitionEntity =
            herdDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME));

        // Validate the returned object.
        validateBusinessObjectDefinitionEntity(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION,
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
        List<BusinessObjectDefinitionKey> resultKeys = herdDao.getBusinessObjectDefinitions(NAMESPACE_CD);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectDefinitionKeys(), resultKeys);
    }

    // FileType

    @Test
    public void testGetFileTypeByCode()
    {
        // Create relative database entities.
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE, "Description of " + FORMAT_FILE_TYPE_CODE);

        // Retrieve file type entity.
        FileTypeEntity fileTypeEntity = herdDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE);

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
        FileTypeEntity fileTypeEntity = herdDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE.toUpperCase());

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
        FileTypeEntity fileTypeEntity = herdDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE.toLowerCase());

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
        assertNull(herdDao.getFileTypeByCode("I_DO_NOT_EXIST"));
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
            herdDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE);
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
        List<FileTypeKey> resultFileTypeKeys = herdDao.getFileTypes();

        // Validate the returned object.
        assertNotNull(resultFileTypeKeys);
        assertTrue(resultFileTypeKeys.containsAll(getTestFileTypeKeys()));
    }

    // BusinessObjectFormat

    @Test
    public void testGetBusinessObjectFormatByAltKeyAllParamsSpecified()
    {
        // Create relative database entities.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION);
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE, "Description of " + FORMAT_FILE_TYPE_CODE);

        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, "Test format 0", false, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, "Test format 1", true, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 2, "Test format 2", false, PARTITION_KEY);

        for (int businessObjectFormatVersion = 0; businessObjectFormatVersion < 3; businessObjectFormatVersion++)
        {
            // Retrieve business object format entity by specifying values for all alternate key fields.
            BusinessObjectFormatEntity businessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(
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
        assertNull(
            herdDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey("I_DO_NOT_EXIST", BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0)));
        assertNull(
            herdDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0)));
        assertNull(herdDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, 0)));
        assertNull(herdDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", 0)));
        assertNull(herdDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 4)));
    }

    @Test
    public void testGetBusinessObjectFormatByAltKeyAllParamsUpperCase()
    {
        // Create relative database entities.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(), BOD_DESCRIPTION);
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toLowerCase(), "Description of " + FORMAT_FILE_TYPE_CODE);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
            INITIAL_FORMAT_VERSION, "Test format 0", Boolean.TRUE, PARTITION_KEY);

        // Retrieve business object format entity by specifying values for all text alternate key fields in upper case.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(
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
        createBusinessObjectDefinitionEntity(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(), BOD_DESCRIPTION);
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toUpperCase(), "Description of " + FORMAT_FILE_TYPE_CODE);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
            INITIAL_FORMAT_VERSION, "Test format 0", Boolean.TRUE, PARTITION_KEY);

        // Retrieve business object format entity by specifying values for all text alternate key fields in lower case.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(
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
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION);
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE, "Description of " + FORMAT_FILE_TYPE_CODE);

        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, "Test format 0", false, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, "Test format 1", true, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 2, "Test format 2", false, PARTITION_KEY);

        // Retrieve business object format entity by specifying all values for the alternate key fields except for the format version.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            herdDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));

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
            herdDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));
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
            herdDao.getBusinessObjectFormatMaxVersion(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));

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
        Long result = herdDao.getBusinessObjectFormatCount(testPartitionKeyGroupEntity);

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
        List<BusinessObjectFormatKey> resultKeys = herdDao.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME), false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys);

        // Retrieve a list of the latest version business object format keys for the specified business object definition.
        resultKeys = herdDao.getBusinessObjectFormats(new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME), true);

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
        PartitionKeyGroupEntity partitionKeyGroupEntity = herdDao.getPartitionKeyGroupByKey(new PartitionKeyGroupKey(PARTITION_KEY_GROUP));

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
        PartitionKeyGroupEntity partitionKeyGroupEntity = herdDao.getPartitionKeyGroupByName(PARTITION_KEY_GROUP.toUpperCase());

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
        PartitionKeyGroupEntity partitionKeyGroupEntity = herdDao.getPartitionKeyGroupByName(PARTITION_KEY_GROUP.toLowerCase());

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
        assertNull(herdDao.getPartitionKeyGroupByName("I_DO_NOT_EXIST"));
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
            herdDao.getPartitionKeyGroupByName(PARTITION_KEY_GROUP);
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
        List<PartitionKeyGroupKey> resultPartitionKeyGroupKeys = herdDao.getPartitionKeyGroups();

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
            ExpectedPartitionValueEntity resultExpectedPartitionValueEntity = herdDao.getExpectedPartitionValue(
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
            assertNull(herdDao.getExpectedPartitionValue(new ExpectedPartitionValueKey(PARTITION_KEY_GROUP, PARTITION_VALUE), offset));
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
            herdDao.getExpectedPartitionValuesByGroupAndRange(PARTITION_KEY_GROUP, partitionValueRange);

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
        List<ExpectedPartitionValueEntity> expectedPartitionValueEntities = herdDao.getExpectedPartitionValuesByGroupAndRange(PARTITION_KEY_GROUP, null);

        assertEquals(expectedPartitionValueEntities.size(), 22, expectedPartitionValueEntities.size());

        // Range with no start or end.
        PartitionValueRange partitionValueRange = new PartitionValueRange();
        expectedPartitionValueEntities = herdDao.getExpectedPartitionValuesByGroupAndRange(PARTITION_KEY_GROUP, partitionValueRange);

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
            herdDao.getCustomDdlByKey(new CustomDdlKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME));

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
            herdDao.getCustomDdls(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION));

        // Validate the returned object.
        assertNotNull(resultCustomDdlKeys);
        assertEquals(testCustomDdlNames.size(), resultCustomDdlKeys.size());
        for (int i = 0; i < testCustomDdlNames.size(); i++)
        {
            validateCustomDdlKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testCustomDdlNames.get(i),
                resultCustomDdlKeys.get(i));
        }
    }

    // BusinessObjectDataStatus

    @Test
    public void testGetBusinessObjectDataStatusByCode()
    {
        // Create database entities required for testing.
        createBusinessObjectDataStatusEntity(BDATA_STATUS, DESCRIPTION);
        createBusinessObjectDataStatusEntity(BDATA_STATUS_2, DESCRIPTION_2);

        // Retrieve the relative business object data status entities and validate the results.
        assertEquals(BDATA_STATUS, herdDao.getBusinessObjectDataStatusByCode(BDATA_STATUS).getCode());
        assertEquals(BDATA_STATUS_2, herdDao.getBusinessObjectDataStatusByCode(BDATA_STATUS_2).getCode());

        // Test case insensitivity for the business object data status code.
        assertEquals(BDATA_STATUS, herdDao.getBusinessObjectDataStatusByCode(BDATA_STATUS.toUpperCase()).getCode());
        assertEquals(BDATA_STATUS, herdDao.getBusinessObjectDataStatusByCode(BDATA_STATUS.toLowerCase()).getCode());

        // Confirm negative results when using non-existing business object data status code.
        assertNull(herdDao.getBusinessObjectDataStatusByCode("I_DO_NOT_EXIST"));
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
            BusinessObjectDataEntity resultBusinessObjectDataEntity = herdDao.getBusinessObjectDataByAltKey(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION));

            // Validate the returned object.
            assertNotNull(resultBusinessObjectDataEntity);
            assertEquals(businessObjectDataEntity.getId(), resultBusinessObjectDataEntity.getId());

            // Get the business object data by key without specifying business object format version, which is an optional parameter.
            resultBusinessObjectDataEntity = herdDao.getBusinessObjectDataByAltKey(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION));

            // Validate the returned object.
            assertNotNull(resultBusinessObjectDataEntity);
            assertEquals(businessObjectDataEntity.getId(), resultBusinessObjectDataEntity.getId());

            // Get the business object data by key without specifying both business object format version and business object data version optional parameters.
            resultBusinessObjectDataEntity = herdDao.getBusinessObjectDataByAltKey(
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
            herdDao.getBusinessObjectDataByAltKey(
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
            BusinessObjectDataEntity resultBusinessObjectDataEntity = herdDao.getBusinessObjectDataByAltKeyAndStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues, null),
                BDATA_STATUS);

            // Validate the results.
            assertNotNull(resultBusinessObjectDataEntity);
            assertEquals(businessObjectDataEntity.getId(), resultBusinessObjectDataEntity.getId());

            // Get the business object data by key using a wrong business object data status and without
            // specifying both business object format version and business object data version.
            resultBusinessObjectDataEntity = herdDao.getBusinessObjectDataByAltKeyAndStatus(
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
        BusinessObjectDataEntity resultBusinessObjectDataEntity = herdDao.getBusinessObjectDataByAltKeyAndStatus(
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
            Integer maxVersion = herdDao.getBusinessObjectDataMaxVersion(
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
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null, new ArrayList<>(), new ArrayList<>(),
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, SUBPARTITION_VALUES, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Get the maximum available partition value.
        assertEquals(STORAGE_1_GREATEST_PARTITION_VALUE, herdDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, DATA_VERSION,
                BusinessObjectDataStatusEntity.VALID, Arrays.asList(STORAGE_NAME), null, null));

        // Get the minimum available partition value.
        assertEquals(STORAGE_1_LEAST_PARTITION_VALUE, herdDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, DATA_VERSION,
                BusinessObjectDataStatusEntity.VALID, Arrays.asList(STORAGE_NAME)));

        // Get the maximum available partition value by not passing any of the optional parameters.
        assertEquals(STORAGE_1_GREATEST_PARTITION_VALUE, herdDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, null, Arrays.asList(STORAGE_NAME),
                null, null));
    }

    @Test
    public void testGetBusinessObjectDataMaxPartitionValueWithUpperAndLowerBounds()
    {
        // Create database entities required for testing.
        createStorageUnitEntity(STORAGE_NAME, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
            SUBPARTITION_VALUES, DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Test retrieving the maximum available partition value using an upper bound partition value.
        assertNull(herdDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Arrays.asList(STORAGE_NAME), PARTITION_VALUE, null));
        assertEquals(PARTITION_VALUE_2, herdDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Arrays.asList(STORAGE_NAME), PARTITION_VALUE_2, null));
        assertEquals(PARTITION_VALUE_2, herdDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Arrays.asList(STORAGE_NAME), PARTITION_VALUE_3, null));

        // Test retrieving the maximum available partition value using a lower bound partition value.
        assertEquals(PARTITION_VALUE_2, herdDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Arrays.asList(STORAGE_NAME), null, PARTITION_VALUE));
        assertEquals(PARTITION_VALUE_2, herdDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Arrays.asList(STORAGE_NAME), null, PARTITION_VALUE_2));
        assertNull(herdDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Arrays.asList(STORAGE_NAME), null, PARTITION_VALUE_3));
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
        StorageUnitStatusEntity storageUnitStatusEntity = createStorageUnitStatusEntity(STORAGE_UNIT_STATUS);
        createStorageUnitEntity(storageEntity, businessObjectDataEntities.get(0), storageUnitStatusEntity, NO_STORAGE_DIRECTORY_PATH);

        // Get the maximum available partition value in the test storage without specifying business object data version.
        assertEquals(PARTITION_VALUE, herdDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, null, BDATA_STATUS,
                Arrays.asList(STORAGE_NAME), null, null));

        // Get the minimum available partition value in the test storage without specifying business object data version.
        assertEquals(PARTITION_VALUE, herdDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, null, BDATA_STATUS,
                Arrays.asList(STORAGE_NAME)));
    }

    /**
     * This unit test validates that we do not rely on the business object data latest version flag when selecting an aggregate on the business object data
     * partition value.
     */
    @Test
    public void testGetBusinessObjectDataPartitionValueBusinessObjectDataStatusSpecified()
    {
        // Create database entities required for testing.
        createStorageUnitEntity(STORAGE_NAME, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);
        createStorageUnitEntity(STORAGE_NAME, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);
        createStorageUnitEntity(STORAGE_NAME, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
            SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS_2, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);
        createStorageUnitEntity(STORAGE_NAME, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_3,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS_2, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Get the maximum available partition value for the relative business object data status without specifying business object data version.
        assertEquals(PARTITION_VALUE_2, herdDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, null, BDATA_STATUS,
                Arrays.asList(STORAGE_NAME), null, null));
        assertEquals(PARTITION_VALUE_3, herdDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, null, BDATA_STATUS_2,
                Arrays.asList(STORAGE_NAME), null, null));

        // Get the minimum available partition value in the test storage without specifying business object data version.
        assertEquals(PARTITION_VALUE, herdDao.getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), null, BDATA_STATUS,
            Arrays.asList(STORAGE_NAME)));
        assertEquals(PARTITION_VALUE_2, herdDao.getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), null, BDATA_STATUS_2,
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
            herdDao.getBusinessObjectDataCount(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION));

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
            List<BusinessObjectDataEntity> resultBusinessObjectDataEntities = herdDao.getBusinessObjectDataEntities(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, SECOND_DATA_VERSION));

            // Validate the results.
            assertNotNull(resultBusinessObjectDataEntities);
            assertEquals(1, resultBusinessObjectDataEntities.size());
            assertEquals(businessObjectDataEntities.get(0).getId(), resultBusinessObjectDataEntities.get(0).getId());

            // Retrieve both business object data entities by not specifying both format and data versions.
            resultBusinessObjectDataEntities = herdDao.getBusinessObjectDataEntities(
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
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null, new ArrayList<>(), new ArrayList<>(),
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, SUBPARTITION_VALUES, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Build a list of partition values, large enough to cause executing the select queries in chunks.
        List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < HerdDaoImpl.MAX_PARTITION_FILTERS_PER_REQUEST; i++)
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
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities1 = herdDao
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
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities2 = herdDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, DATA_VERSION, null, STORAGE_NAME);

        // Validate the results.
        assertEquals(resultBusinessObjectDataEntities1, resultBusinessObjectDataEntities2);

        // Retrieve the available business object data without specifying both business object format version and business object data version.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities3 = herdDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, null, STORAGE_NAME);

        // Validate the results.
        assertEquals(resultBusinessObjectDataEntities1, resultBusinessObjectDataEntities3);

        // Retrieve the business object data with VALID business object data status without
        // specifying both business object format version and business object data version.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities4 = herdDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME);

        // Validate the results.
        assertEquals(resultBusinessObjectDataEntities1, resultBusinessObjectDataEntities4);

        // Retrieve the available business object data with wrong business object data status and without
        // specifying both business object format version and business object data version.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities5 = herdDao
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
        StorageUnitStatusEntity storageUnitStatusEntity = createStorageUnitStatusEntity(STORAGE_UNIT_STATUS);
        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities)
        {
            createStorageUnitEntity(storageEntity, businessObjectDataEntity, storageUnitStatusEntity, NO_STORAGE_DIRECTORY_PATH);
        }

        // Build a list of partition filters to select the "available" business object data.
        List<List<String>> partitionFilters = new ArrayList<>();
        partitionFilters.add(Arrays.asList(PARTITION_VALUE, null, null, null, null));

        // Retrieve the available business object data without specifying both business object format version and business object data version.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities = herdDao
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
        StorageUnitStatusEntity storageUnitStatusEntity = createStorageUnitStatusEntity(STORAGE_UNIT_STATUS);
        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities.subList(0, 2))
        {
            createStorageUnitEntity(storageEntity, businessObjectDataEntity, storageUnitStatusEntity, NO_STORAGE_DIRECTORY_PATH);
        }

        // Build a list of partition filters to select the "available" business object data.
        List<List<String>> partitionFilters = new ArrayList<>();
        partitionFilters.add(Arrays.asList(PARTITION_VALUE, null, null, null, null));

        // Retrieve a business object data in the test storage without specifying business object data version
        // - the latest available business object data with the specified business object data status.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities = herdDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                partitionFilters, null, BDATA_STATUS, STORAGE_NAME);

        // Validate the results - we expect to get back the initial business object data version.
        assertNotNull(resultBusinessObjectDataEntities);
        assertEquals(1, resultBusinessObjectDataEntities.size());
        assertEquals(INITIAL_DATA_VERSION, resultBusinessObjectDataEntities.get(0).getVersion());

        // Retrieve a business object data in the test storage without specifying both business object data status
        // and business object data version - the latest available business object data regardless of the status.
        resultBusinessObjectDataEntities = herdDao
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
        StorageUnitStatusEntity storageUnitStatusEntity = createStorageUnitStatusEntity(STORAGE_UNIT_STATUS);
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
                    createStorageUnitEntity(storageEntity, businessObjectDataEntity, storageUnitStatusEntity, NO_STORAGE_DIRECTORY_PATH);
                    herdDao.saveAndRefresh(businessObjectDataEntity);
                }
            }
        }

        // Select a subset of test business object entities.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities =
            herdDao.getBusinessObjectDataFromStorageOlderThan(STORAGE_NAME, 10, Arrays.asList(BDATA_STATUS_2));

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

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePolicies()
    {
        // Create and persist a storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // For all possible storage policy priority levels, create and persist a storage policy entity matching to the business object data.
        Map<StoragePolicyPriorityLevel, StoragePolicyEntity> input = new LinkedHashMap<>();

        // Storage policy filter has business object definition, usage, and file type specified.
        input.put(new StoragePolicyPriorityLevel(false, false, false),
            createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
                StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                STORAGE_NAME, STORAGE_NAME_2));

        // Storage policy filter has only business object definition specified.
        input.put(new StoragePolicyPriorityLevel(false, true, true),
            createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME_2),
                StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BOD_NAMESPACE, BOD_NAME, NO_FORMAT_USAGE_CODE,
                NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2));

        // Storage policy filter has only usage and file type specified.
        input.put(new StoragePolicyPriorityLevel(true, false, false),
            createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD_2, STORAGE_POLICY_NAME),
                StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, NO_BOD_NAMESPACE, NO_BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2));

        // Storage policy filter has no fields specified.
        input.put(new StoragePolicyPriorityLevel(true, true, true),
            createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD_2, STORAGE_POLICY_NAME_2),
                StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, NO_BOD_NAMESPACE, NO_BOD_NAME, NO_FORMAT_USAGE_CODE,
                NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2));

        // For each storage policy priority level, retrieve business object data matching to the relative storage policy.
        for (Map.Entry<StoragePolicyPriorityLevel, StoragePolicyEntity> entry : input.entrySet())
        {
            StoragePolicyPriorityLevel storagePolicyPriorityLevel = entry.getKey();
            StoragePolicyEntity storagePolicyEntity = entry.getValue();

            // Retrieve the match.
            Map<BusinessObjectDataEntity, StoragePolicyEntity> result =
                herdDao.getBusinessObjectDataEntitiesMatchingStoragePolicies(storagePolicyPriorityLevel, Arrays.asList(BDATA_STATUS), 0, MAX_RESULT);

            // Validate the results.
            assertEquals(1, result.size());
            assertTrue(result.containsKey(storageUnitEntity.getBusinessObjectData()));
            assertEquals(storagePolicyEntity, result.get(storageUnitEntity.getBusinessObjectData()));
        }
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesTestingStartPositionAndMaxResult()
    {
        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, STORAGE_NAME_2);

        // Create and persist a storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity1 =
            createStorageUnitEntity(STORAGE_NAME, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Apply the offset in days to business object data "created on" value.
        ageBusinessObjectData(storageUnitEntity1.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 1);

        // Create and persist a second storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity2 =
            createStorageUnitEntity(STORAGE_NAME, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Also apply an offset to business object data "created on" value, but make this business object data older than the first.
        ageBusinessObjectData(storageUnitEntity2.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 2);

        // Try to retrieve both business object data instances as matching to the storage policy, but with max result limit set to 1.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = herdDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 0, 1);

        // Validate the results. Only the oldest business object data should get selected.
        assertEquals(1, result.size());
        assertTrue(result.containsKey(storageUnitEntity2.getBusinessObjectData()));
        assertEquals(storagePolicyEntity, result.get(storageUnitEntity2.getBusinessObjectData()));

        // Try to retrieve the second business object data instance matching to the storage policy
        // by specifying the relative start position and max result limit set.
        result = herdDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 1, 1);

        // Validate the results. Now, the second oldest business object data should get selected.
        assertEquals(1, result.size());
        assertTrue(result.containsKey(storageUnitEntity1.getBusinessObjectData()));
        assertEquals(storagePolicyEntity, result.get(storageUnitEntity1.getBusinessObjectData()));
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesMultipleStoragePoliciesMatchBusinessObjectData()
    {
        // Create and persist a storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist two storage policy entities with identical storage policy filters.
        createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, STORAGE_NAME_2);
        createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME_2),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, STORAGE_NAME_2);

        // Retrieve business object data matching storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = herdDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 0,
                MAX_RESULT);

        // Validate the results. Only a single match should get returned.
        assertEquals(1, result.size());
        assertTrue(result.containsKey(storageUnitEntity.getBusinessObjectData()));
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesInvalidSourceStorage()
    {
        // Create and persist a storage policy entity.
        createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, STORAGE_NAME_2);

        // Create and persist a storage unit which is not in the storage policy filter storage.
        createStorageUnitEntity(STORAGE_NAME_3, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Try to retrieve the business object data matching to the storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = herdDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 0,
                MAX_RESULT);

        // Validate the results.
        assertEquals(0, result.size());
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesInvalidBusinessObjectDataStatus()
    {
        // Create and persist a storage policy entity.
        createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, STORAGE_NAME_2);

        // Create and persist a storage unit in the storage policy filter storage, but having "not supported" business object data status.
        createStorageUnitEntity(STORAGE_NAME, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS_2, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Try to retrieve the business object data matching to the storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = herdDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 0,
                MAX_RESULT);

        // Validate the results.
        assertEquals(0, result.size());
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesDestinationStorageUnitAlreadyExists()
    {
        // Create and persist a storage policy entity.
        createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, STORAGE_NAME_2);

        // Create and persist a storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Add a storage unit for this business object data in the storage policy destination storage.
        createStorageUnitEntity(STORAGE_NAME_2, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Try to retrieve the business object data matching to the storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = herdDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 0,
                MAX_RESULT);

        // Validate the results.
        assertEquals(0, result.size());
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
        List<StorageKey> resultStorageKeys = herdDao.getStorages();

        // Validate the returned object.
        assertNotNull(resultStorageKeys);
        assertTrue(resultStorageKeys.containsAll(getTestStorageKeys()));
    }

    // StorageUnitStatus

    @Test
    public void testGetStorageUnitStatusByCode()
    {
        // Create database entities required for testing.
        createStorageUnitStatusEntity(STORAGE_UNIT_STATUS, DESCRIPTION, STORAGE_UNIT_STATUS_AVAILABLE_FLAG_SET);
        createStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2, DESCRIPTION_2, NO_STORAGE_UNIT_STATUS_AVAILABLE_FLAG_SET);

        // Retrieve the relative storage unit status entities and validate the results.
        assertEquals(STORAGE_UNIT_STATUS, herdDao.getStorageUnitStatusByCode(STORAGE_UNIT_STATUS).getCode());
        assertEquals(STORAGE_UNIT_STATUS_2, herdDao.getStorageUnitStatusByCode(STORAGE_UNIT_STATUS_2).getCode());

        // Test case insensitivity for the storage unit status code.
        assertEquals(STORAGE_UNIT_STATUS, herdDao.getStorageUnitStatusByCode(STORAGE_UNIT_STATUS.toUpperCase()).getCode());
        assertEquals(STORAGE_UNIT_STATUS, herdDao.getStorageUnitStatusByCode(STORAGE_UNIT_STATUS.toLowerCase()).getCode());

        // Confirm negative results when using non-existing storage unit status code.
        assertNull(herdDao.getStorageUnitStatusByCode("I_DO_NOT_EXIST"));
    }

    // StorageUnit

    @Test
    public void testGetStorageUnitByStorageNameAndDirectoryPath()
    {
        // Create database entities required for testing.
        StorageUnitEntity storageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, STORAGE_DIRECTORY_PATH);

        // Retrieve the relative storage file entities and validate the results.
        assertEquals(storageUnitEntity, herdDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME, STORAGE_DIRECTORY_PATH));

        // Test case insensitivity for the storage name.
        assertEquals(storageUnitEntity, herdDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME.toUpperCase(), STORAGE_DIRECTORY_PATH));
        assertEquals(storageUnitEntity, herdDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME.toLowerCase(), STORAGE_DIRECTORY_PATH));

        // Test case sensitivity of the storage directory path.
        assertNull(herdDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME, STORAGE_DIRECTORY_PATH.toUpperCase()));
        assertNull(herdDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME, STORAGE_DIRECTORY_PATH.toLowerCase()));

        // Confirm negative results when using wrong input parameters.
        assertNull(herdDao.getStorageUnitByStorageNameAndDirectoryPath("I_DO_NOT_EXIST", TEST_S3_KEY_PREFIX));
        assertNull(herdDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME, "I_DO_NOT_EXIST"));
    }

    /**
     * Tests {@link org.finra.herd.dao.HerdDao#getStorageUnitByBusinessObjectDataAndStorageName(BusinessObjectDataEntity, String)}.
     */
    @Test
    public void testGetStorageUnitByBusinessObjectDataAndStorageName()
    {
        StorageUnitEntity storageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, STORAGE_DIRECTORY_PATH);
        BusinessObjectDataEntity businessObjectDataEntity = storageUnitEntity.getBusinessObjectData();

        // test retrieval by name
        assertEquals(storageUnitEntity, herdDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME));

        // test retrieval by name, case insensitive
        assertEquals(storageUnitEntity, herdDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME.toUpperCase()));
        assertEquals(storageUnitEntity, herdDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME.toLowerCase()));

        // test retrieval failure
        assertNull(herdDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, "I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetStorageUnitsByStorageAndBusinessObjectData()
    {
        // Create database entities required for testing.
        StorageUnitEntity storageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, TEST_S3_KEY_PREFIX);

        // Retrieve storage unit entities by storage and business object data.
        List<StorageUnitEntity> resultStorageUnitEntities =
            herdDao.getStorageUnitsByStorageAndBusinessObjectData(storageUnitEntity.getStorage(), Arrays.asList(storageUnitEntity.getBusinessObjectData()));

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
            createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null, new ArrayList<>(), new ArrayList<>(),
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, SUBPARTITION_VALUES, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA, STORAGE_NAMES);

        // Build a list of partition values, large enough to cause executing the select queries in chunks.
        List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < HerdDaoImpl.MAX_PARTITION_FILTERS_PER_REQUEST; i++)
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

        // Retrieve "available" storage units per specified parameters.
        List<StorageUnitEntity> resultStorageUnitEntities1 = herdDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION, null,
            STORAGE_NAMES, null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertNotNull(resultStorageUnitEntities1);
        assertEquals(expectedMultiStorageAvailableStorageUnits, resultStorageUnitEntities1);

        // Retrieve "available" storage units without specifying
        // a business object format version, which is an optional parameter.
        List<StorageUnitEntity> resultStorageUnitEntities2 = herdDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, DATA_VERSION, null, STORAGE_NAMES, null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(resultStorageUnitEntities1, resultStorageUnitEntities2);

        // Retrieve "available" storage units without specifying
        // both business object format version and business object data version.
        List<StorageUnitEntity> resultStorageUnitEntities3 = herdDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, null, STORAGE_NAMES, null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(resultStorageUnitEntities1, resultStorageUnitEntities3);

        // Retrieve the "available" storage units and with VALID business object data
        // status without specifying both business object format version and business object data version.
        List<StorageUnitEntity> resultStorageUnitEntities4 = herdDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, BusinessObjectDataStatusEntity.VALID, STORAGE_NAMES, null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(resultStorageUnitEntities1, resultStorageUnitEntities4);

        // Try to retrieve "available" storage units, with wrong business object data
        // status and without specifying both business object format version and business object data version.
        List<StorageUnitEntity> resultStorageUnitEntities5 = herdDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, BDATA_STATUS, STORAGE_NAMES, null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertTrue(resultStorageUnitEntities5.isEmpty());

        // Retrieve "available" storage units and with VALID business
        // object data status without specifying any of the storages or storage platform type.
        List<StorageUnitEntity> resultStorageUnitEntities6 = herdDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION,
            BusinessObjectDataStatusEntity.VALID, NO_STORAGE_NAMES, null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(resultStorageUnitEntities1, resultStorageUnitEntities6);

        // Try to retrieve "available" storage units without
        // specifying any of the storages and providing a non-existing storage platform type.
        List<StorageUnitEntity> resultStorageUnitEntities7 = herdDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION,
            BusinessObjectDataStatusEntity.VALID, NO_STORAGE_NAMES, "I_DO_NOT_EXIST", null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertTrue(resultStorageUnitEntities7.isEmpty());

        // Try to retrieve "available" storage units when excluding the storage platform type that are test storage belongs to.
        List<StorageUnitEntity> resultStorageUnitEntities8 = herdDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION,
            BusinessObjectDataStatusEntity.VALID, NO_STORAGE_NAMES, null, StoragePlatformEntity.S3, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertTrue(resultStorageUnitEntities8.isEmpty());
    }

    @Test
    public void testGetStorageUnitsByPartitionFiltersAndStoragesNotEnabledStorageUnitStatus()
    {
        // Create enabled and disabled storage units for different partition values.
        StorageUnitEntity enabledStorageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        StorageUnitEntity disabledStorageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, StorageUnitStatusEntity.DISABLED, NO_STORAGE_DIRECTORY_PATH);

        // Build a list of partition filters to select business object data.
        List<List<String>> partitionFilters = new ArrayList<>();
        for (String partitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            partitionFilters.add(Arrays.asList(partitionValue, null, null, null, null));
        }

        // Retrieve "available" storage units per specified parameters.
        List<StorageUnitEntity> resultStorageUnitEntities = herdDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION,
            null, Arrays.asList(STORAGE_NAME), null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(Arrays.asList(enabledStorageUnitEntity), resultStorageUnitEntities);

        // Retrieve "available" storage units without specifying
        // a business object format version, which is an optional parameter.
        resultStorageUnitEntities = herdDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, DATA_VERSION, null, Arrays.asList(STORAGE_NAME), null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(Arrays.asList(enabledStorageUnitEntity), resultStorageUnitEntities);

        // Retrieve storage units regardless of storage unit status per specified parameters.
        resultStorageUnitEntities = herdDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION,
            null, Arrays.asList(STORAGE_NAME), null, null, NO_SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(Arrays.asList(enabledStorageUnitEntity, disabledStorageUnitEntity), resultStorageUnitEntities);

        // Retrieve storage units regardless of storage unit status without specifying
        // a business object format version, which is an optional parameter.
        resultStorageUnitEntities = herdDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, DATA_VERSION, null, Arrays.asList(STORAGE_NAME), null, null, NO_SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(Arrays.asList(enabledStorageUnitEntity, disabledStorageUnitEntity), resultStorageUnitEntities);
    }

    // StorageFile

    @Test
    public void testGetStorageFileByStorageNameAndFilePath()
    {
        // Create relative database entities.
        StorageUnitEntity storageUnitEntity =
            createStorageUnitEntity(StorageEntity.MANAGED_STORAGE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        for (String file : LOCAL_FILES)
        {
            createStorageFileEntity(storageUnitEntity, file, FILE_SIZE_1_KB, ROW_COUNT_1000);
        }

        // Retrieve the relative storage file entities and validate the results.
        for (String file : LOCAL_FILES)
        {
            StorageFileEntity storageFileEntity = herdDao.getStorageFileByStorageNameAndFilePath(StorageEntity.MANAGED_STORAGE, file);
            assertTrue(storageFileEntity.getPath().compareTo(file) == 0);
            assertTrue(storageFileEntity.getFileSizeBytes().compareTo(FILE_SIZE_1_KB) == 0);
            assertTrue(storageFileEntity.getRowCount().compareTo(ROW_COUNT_1000) == 0);
        }

        // Confirm negative results when using wrong input parameters.
        assertNull(herdDao.getStorageFileByStorageNameAndFilePath("I_DO_NOT_EXIST", LOCAL_FILES.get(0)));
        assertNull(herdDao.getStorageFileByStorageNameAndFilePath(StorageEntity.MANAGED_STORAGE, "I_DO_NOT_EXIST"));
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

        StorageEntity storageEntity = herdDao.getStorageByName(StorageEntity.MANAGED_STORAGE);

        StorageUnitEntity storageUnitEntity1 =
            createStorageUnitEntity(storageEntity, businessObjectDataEntity1, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        StorageUnitEntity storageUnitEntity2 =
            createStorageUnitEntity(storageEntity, businessObjectDataEntity2, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        createStorageFileEntity(storageUnitEntity1, LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);
        createStorageFileEntity(storageUnitEntity2, LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);

        try
        {
            // Try to retrieve storage file.
            herdDao.getStorageFileByStorageNameAndFilePath(StorageEntity.MANAGED_STORAGE, LOCAL_FILE);
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
            assertEquals(Long.valueOf(1L), herdDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE, file));
        }

        // Validate that we can get correct file count using upper and lower storage name.
        assertEquals(Long.valueOf(1L), herdDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE.toUpperCase(), LOCAL_FILES.get(0)));
        assertEquals(Long.valueOf(1L), herdDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE.toLowerCase(), LOCAL_FILES.get(0)));

        // Get 0 file count by specifying non-existing storage.
        assertEquals(Long.valueOf(0L), herdDao.getStorageFileCount("I_DO_NOT_EXIST", LOCAL_FILES.get(0)));

        // Get 0 file count by specifying non-existing file path prefix.
        assertEquals(Long.valueOf(0L), herdDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE, "I_DO_NOT_EXIST"));

        // Validate that we can get correct count of files from the LOCAL_FILES list that match "folder" file path prefix.
        assertEquals(Long.valueOf(3L), herdDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE, "folder"));
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
            storageFileEntities = herdDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE, file);
            assertEquals(1, storageFileEntities.size());
            assertEquals(file, storageFileEntities.get(0).getPath());
        }

        // Validate that we can retrieve a file using upper and lower storage name.
        assertEquals(LOCAL_FILES.get(0),
            herdDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE.toUpperCase(), LOCAL_FILES.get(0)).get(0).getPath());
        assertEquals(LOCAL_FILES.get(0),
            herdDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE.toLowerCase(), LOCAL_FILES.get(0)).get(0).getPath());

        // Try to get file entities by specifying non-existing storage.
        assertEquals(0, herdDao.getStorageFilesByStorageAndFilePathPrefix("I_DO_NOT_EXIST", LOCAL_FILES.get(0)).size());

        // Try to get file entities by specifying non-existing file path prefix.
        assertEquals(0, herdDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE, "I_DO_NOT_EXIST").size());

        // Validate that we can retrieve the last 3 files in the expected order from the LOCAL_FILES list that match "folder" file path prefix.
        storageFileEntities = herdDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE, "folder");
        List<String> expectedFiles = Arrays.asList(LOCAL_FILES.get(5), LOCAL_FILES.get(4), LOCAL_FILES.get(3));
        assertEquals(expectedFiles.size(), storageFileEntities.size());
        for (int i = 0; i < expectedFiles.size(); i++)
        {
            assertEquals(expectedFiles.get(i), storageFileEntities.get(i).getPath());
        }
    }

    @Test
    public void testGetStoragePathsByStorageUnits() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.STORAGE_FILE_PATHS_QUERY_PAGINATION_SIZE.getKey(), LOCAL_FILES.size() / 2);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Create database entities required for testing.
            StorageUnitEntity storageUnitEntity =
                createStorageUnitEntity(STORAGE_NAME, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);
            for (String file : LOCAL_FILES)
            {
                createStorageFileEntity(storageUnitEntity, file, FILE_SIZE_1_KB, ROW_COUNT_1000);
            }

            // Retrieve storage file paths by storage units.
            MultiValuedMap<Integer, String> result = herdDao.getStorageFilePathsByStorageUnits(Arrays.asList(storageUnitEntity));

            // Validate the results.
            assertEquals(LOCAL_FILES.size(), result.get(storageUnitEntity.getId()).size());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    private void createDatabaseEntitiesForStorageFilesTesting()
    {
        // Create relative database entities.
        StorageUnitEntity storageUnitEntity =
            createStorageUnitEntity(StorageEntity.MANAGED_STORAGE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);
        for (String file : LOCAL_FILES)
        {
            createStorageFileEntity(storageUnitEntity, file, FILE_SIZE_1_KB, ROW_COUNT_1000);
        }
    }

    // StoragePolicyRuleType

    @Test
    public void testGetStoragePolicyRuleTypeByCode()
    {
        // Create and persist a storage policy rule type entity.
        StoragePolicyRuleTypeEntity storagePolicyRuleTypeEntity = createStoragePolicyRuleTypeEntity(STORAGE_POLICY_RULE_TYPE, DESCRIPTION);

        // Retrieve this storage policy rule type entity by code.
        StoragePolicyRuleTypeEntity resultStoragePolicyRuleTypeEntity = herdDao.getStoragePolicyRuleTypeByCode(STORAGE_POLICY_RULE_TYPE);

        // Validate the returned object.
        assertNotNull(resultStoragePolicyRuleTypeEntity);
        assertEquals(storagePolicyRuleTypeEntity.getCode(), resultStoragePolicyRuleTypeEntity.getCode());

        // Retrieve this storage policy rule type entity by code in upper case.
        resultStoragePolicyRuleTypeEntity = herdDao.getStoragePolicyRuleTypeByCode(STORAGE_POLICY_RULE_TYPE.toUpperCase());

        // Validate the returned object.
        assertNotNull(resultStoragePolicyRuleTypeEntity);
        assertEquals(storagePolicyRuleTypeEntity.getCode(), resultStoragePolicyRuleTypeEntity.getCode());

        // Retrieve this storage policy rule type entity by code in lower case.
        resultStoragePolicyRuleTypeEntity = herdDao.getStoragePolicyRuleTypeByCode(STORAGE_POLICY_RULE_TYPE.toLowerCase());

        // Validate the returned object.
        assertNotNull(resultStoragePolicyRuleTypeEntity);
        assertEquals(storagePolicyRuleTypeEntity.getCode(), resultStoragePolicyRuleTypeEntity.getCode());
    }

    // StoragePolicy

    @Test
    public void testGetStoragePolicyByAltKey()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity =
            createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Retrieve this storage policy by alternate key.
        StoragePolicyEntity resultStoragePolicyEntity = herdDao.getStoragePolicyByAltKey(storagePolicyKey);

        // Validate the returned object.
        assertNotNull(resultStoragePolicyEntity);
        assertEquals(storagePolicyEntity.getId(), resultStoragePolicyEntity.getId());

        // Retrieve this storage policy by alternate key in upper case.
        resultStoragePolicyEntity =
            herdDao.getStoragePolicyByAltKey(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD.toUpperCase(), STORAGE_POLICY_NAME.toUpperCase()));

        // Validate the returned object.
        assertNotNull(resultStoragePolicyEntity);
        assertEquals(storagePolicyEntity.getId(), resultStoragePolicyEntity.getId());

        // Retrieve this storage policy by alternate key in lower case.
        resultStoragePolicyEntity =
            herdDao.getStoragePolicyByAltKey(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD.toLowerCase(), STORAGE_POLICY_NAME.toLowerCase()));

        // Validate the returned object.
        assertNotNull(resultStoragePolicyEntity);
        assertEquals(storagePolicyEntity.getId(), resultStoragePolicyEntity.getId());
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
        JobDefinitionEntity jobDefinitionEntityResult = herdDao.getJobDefinitionByAltKey(NAMESPACE_CD, JOB_NAME);

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
        JobDefinitionEntity jobDefinitionEntityResult = herdDao.getJobDefinitionByAltKey(NAMESPACE_CD, JOB_NAME_2);

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
        JobDefinitionEntity jobDefinitionEntityResult = herdDao.getJobDefinitionByAltKey(NAMESPACE_CD_2, JOB_NAME);

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
        herdDao.getJobDefinitionByAltKey(NAMESPACE_CD, JOB_NAME);
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
            herdDao.getEmrClusterDefinitionByAltKey(new EmrClusterDefinitionKey(NAMESPACE_CD, EMR_CLUSTER_DEFINITION_NAME));

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
        EmrClusterDefinitionEntity emrClusterDefinitionEntityResult = herdDao.getEmrClusterDefinitionByAltKey(NAMESPACE_CD, EMR_CLUSTER_DEFINITION_NAME_2);

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
        EmrClusterDefinitionEntity emrClusterDefinitionEntityResult = herdDao.getEmrClusterDefinitionByAltKey(NAMESPACE_CD_2, EMR_CLUSTER_DEFINITION_NAME_2);

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
        herdDao.getEmrClusterDefinitionByAltKey(NAMESPACE_CD, EMR_CLUSTER_DEFINITION_NAME);
    }

    // SecurityFunction

    @Test
    public void testGetSecurityFunctionsByRole() throws Exception
    {
        // Create role and function.
        SecurityRoleEntity securityRoleEntity = new SecurityRoleEntity();
        securityRoleEntity.setCode("TEST_ROLE");
        herdDao.saveAndRefresh(securityRoleEntity);

        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode("FN_UT_SECURITY_FUNCTION");
        herdDao.saveAndRefresh(securityFunctionEntity);

        List<String> functions = herdDao.getSecurityFunctionsForRole("TEST_ROLE");

        // Add new role to functions mapping.
        SecurityRoleFunctionEntity securityRoleFunctionEntity = new SecurityRoleFunctionEntity();
        securityRoleFunctionEntity.setSecurityRole(securityRoleEntity);
        securityRoleFunctionEntity.setSecurityFunction(securityFunctionEntity);
        herdDao.saveAndRefresh(securityRoleFunctionEntity);

        List<String> functions2 = herdDao.getSecurityFunctionsForRole("TEST_ROLE");

        // Since the functions method is cached, the test function will not be retrieved.
        assertEquals(functions, functions2);

        // Clear the cache and retrieve the functions again.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        functions2 = herdDao.getSecurityFunctionsForRole("TEST_ROLE");

        assertNotEquals(functions, functions2);
    }

    @Test
    public void testGetSecurityFunctions() throws Exception
    {
        List<String> functions = herdDao.getSecurityFunctions();

        // Add a function in functions.
        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode("FN_UT_SECURITY_FUNCTION");

        herdDao.saveAndRefresh(securityFunctionEntity);

        List<String> functions2 = herdDao.getSecurityFunctions();

        // Since the functions method is cached, the test function will not be retrieved.
        assertEquals(functions, functions2);

        // Clear the cache and retrieve the functions again.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        functions2 = herdDao.getSecurityFunctions();

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
        JmsMessageEntity oldestJmsMessageEntity = herdDao.getOldestJmsMessage();

        // Validate the results.
        assertNotNull(oldestJmsMessageEntity);
        assertEquals(jmsMessageEntities.get(0).getId(), oldestJmsMessageEntity.getId());
    }

    @Test
    public void testGetOldestJmsMessageQueueIsEmpty() throws Exception
    {
        // Try to retrieve the oldest JMS message from an empty queue table.
        JmsMessageEntity oldestJmsMessageEntity = herdDao.getOldestJmsMessage();

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
        assertEquals(onDemandPriceEntity, herdDao.getOnDemandPrice(AWS_REGION, EC2_INSTANCE_TYPE));

        // Test case sensitivity for the region and EC2 instance type.
        assertNull(herdDao.getOnDemandPrice(AWS_REGION.toUpperCase(), EC2_INSTANCE_TYPE));
        assertNull(herdDao.getOnDemandPrice(AWS_REGION.toLowerCase(), EC2_INSTANCE_TYPE));
        assertNull(herdDao.getOnDemandPrice(AWS_REGION, EC2_INSTANCE_TYPE.toUpperCase()));
        assertNull(herdDao.getOnDemandPrice(AWS_REGION, EC2_INSTANCE_TYPE.toLowerCase()));

        // Confirm negative results when using wrong input parameters.
        assertNull(herdDao.getOnDemandPrice("I_DO_NOT_EXIST", EC2_INSTANCE_TYPE));
        assertNull(herdDao.getOnDemandPrice(AWS_REGION, "I_DO_NOT_EXIST"));
    }

    // Helper methods.

    /**
     * Gets a date as a string.
     *
     * @param year the year of the date.
     * @param month the month of the date. Note that month is 0-based as per GregorianCalendar.
     * @param day the day of the date.
     *
     * @return the date as a string in the format using HerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.
     */
    private String getDateAsString(int year, int month, int day)
    {
        return new SimpleDateFormat(HerdDao.DEFAULT_SINGLE_DAY_DATE_MASK).format(new GregorianCalendar(year, month, day).getTime());
    }
}

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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
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
        assertTrue(businessObjectFormatEntity.getDescription().equals(String.format("Test format 1")));
        assertTrue(businessObjectFormatEntity.getDocumentSchema().equals(String.format("Document Schema 1")));

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
    public void testGetBusinessObjectFormatCount()
    {
        // Create a partition key group.
        PartitionKeyGroupEntity testPartitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create a business object format that uses this partition key group.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY, PARTITION_KEY_GROUP);

        // Get the number of business object formats that use this partition key group.
        Long result = businessObjectFormatDao.getBusinessObjectFormatCount(testPartitionKeyGroupEntity);

        // Validate the results.
        assertEquals(Long.valueOf(1L), result);
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

        BusinessObjectFormatEntity businessObjectFormatEntityV0 = businessObjectFormatDaoTestHelper
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

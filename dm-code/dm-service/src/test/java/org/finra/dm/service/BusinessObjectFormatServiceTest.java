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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.FileTypeEntity;
import org.finra.dm.model.api.xml.AttributeDefinition;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.dm.model.api.xml.BusinessObjectFormat;
import org.finra.dm.model.api.xml.BusinessObjectFormatDdl;
import org.finra.dm.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.dm.model.api.xml.BusinessObjectFormatKey;
import org.finra.dm.model.api.xml.BusinessObjectFormatKeys;
import org.finra.dm.model.api.xml.BusinessObjectFormatUpdateRequest;
import org.finra.dm.service.helper.Hive13DdlGenerator;

public class BusinessObjectFormatServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "businessObjectFormatServiceImpl")
    private BusinessObjectFormatService businessObjectFormatServiceImpl;

    @Test
    public void testCreateBusinessObjectFormat()
    {
        // Create an initial version of a business object format.
        BusinessObjectFormat businessObjectFormat = createTestBusinessObjectFormat();

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true, PARTITION_KEY, FORMAT_DESCRIPTION,
            getTestAttributeDefinitions(), getTestSchema(), businessObjectFormat);
    }

    // Tests for Update Business Object Format

    @Test
    public void testUpdateBusinessObjectFormat()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat();

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and schema.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION),
                request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION_2, getTestAttributeDefinitions(), getTestSchema2(), updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatLegacy()
    {
        // Create and persist a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormatEntity originalBusinessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY, PARTITION_KEY_GROUP, SCHEMA_DELIMITER_COMMA, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N,
                getTestSchemaColumns(), getTestPartitionColumns());

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and schema and calling a legacy endpoint.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatService
            .updateBusinessObjectFormat(new BusinessObjectFormatKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION), request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION_2, new ArrayList<AttributeDefinition>(), getTestSchema2(),
            updatedBusinessObjectFormat);
    }

    // Tests for Get Business Object Format

    @Test
    public void testGetBusinessObjectFormat()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat();

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .getBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, getTestAttributeDefinitions(), getTestSchema(), resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormatLegacy()
    {
        // Create and persist a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Perform a get by calling a legacy endpoint.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService
            .getBusinessObjectFormat(new BusinessObjectFormatKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, null, null, resultBusinessObjectFormat);
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

    @Test
    public void testGetBusinessObjectFormatsLegacy()
    {
        // Create and persist the relative legacy and regular business object definitions.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME_2, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, false);
        createBusinessObjectDefinitionEntity(NAMESPACE_CD_2, BOD_NAME_2, DATA_PROVIDER_NAME, BOD_DESCRIPTION, false);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : getTestBusinessObjectFormatKeys())
        {
            createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, false, PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition by calling a legacy endpoint.
        BusinessObjectFormatKeys resultKeys = businessObjectFormatService.getBusinessObjectFormats(new BusinessObjectDefinitionKey(null, BOD_NAME), false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    // Tests for Delete Business Object Format

    @Test
    public void testDeleteBusinessObjectFormat() throws Exception
    {
        // Create an initial version of a business object format.
        createTestBusinessObjectFormat();

        // Retrieve the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = dmDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService
            .deleteBusinessObjectFormat(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, getTestAttributeDefinitions(), getTestSchema(), deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(dmDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectFormatLegacy() throws Exception
    {
        // Create and persist a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist a valid business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);

        // Delete the business object format by calling a legacy endpoint.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatService
            .deleteBusinessObjectFormat(new BusinessObjectFormatKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, null, null, deletedBusinessObjectFormat);
        // Ensure that this business object format is no longer there.
        assertNull(dmDao.getBusinessObjectFormatByAltKey(
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
}

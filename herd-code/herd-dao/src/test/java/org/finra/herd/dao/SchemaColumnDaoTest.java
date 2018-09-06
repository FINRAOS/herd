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

import java.util.Arrays;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;

public class SchemaColumnDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetSchemaColumns()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(businessObjectDefinitionKey, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create a file type entity.
        FileTypeEntity fileTypeEntity = fileTypeDaoTestHelper.createFileTypeEntity(FORMAT_FILE_TYPE_CODE);

        // Create and persist database entities required for testing.
        for (Integer formatVersion : Arrays.asList(INITIAL_FORMAT_VERSION, SECOND_FORMAT_VERSION))
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity, formatVersion, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY, null, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE,
                    SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), NO_PARTITION_COLUMNS);
        }

        // Get a list of schema columns from the business object definition matching to the first column name.
        assertEquals(2, schemaColumnDao.getSchemaColumns(businessObjectDefinitionEntity, FIRST_COLUMN_NAME).size());

        // Get a list of schema columns by passing all case-insensitive parameters in uppercase.
        assertEquals(2, schemaColumnDao.getSchemaColumns(businessObjectDefinitionEntity, FIRST_COLUMN_NAME.toUpperCase()).size());

        // Get business object definition column by passing all case-insensitive parameters in lowercase.
        assertEquals(2, schemaColumnDao.getSchemaColumns(businessObjectDefinitionEntity, FIRST_COLUMN_NAME.toLowerCase()).size());

        // Try invalid values for all input parameters.
        assertEquals(0, schemaColumnDao.getSchemaColumns(
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, DESCRIPTION),
            FIRST_COLUMN_NAME).size());
        assertEquals(0, schemaColumnDao.getSchemaColumns(businessObjectDefinitionEntity, "I_DO_NOT_EXIST").size());
    }

    @Test
    public void testGetSchemaColumnsWithDescriptiveFormat()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(businessObjectDefinitionKey, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create a file type entity.
        FileTypeEntity fileTypeEntity = fileTypeDaoTestHelper.createFileTypeEntity(FORMAT_FILE_TYPE_CODE);

        // Create and persist database entities required for testing.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY, null, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), NO_PARTITION_COLUMNS);

        // Update business object definition entity
        businessObjectDefinitionEntity.setDescriptiveBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);

        // Get a list of schema columns from the business object definition matching to the first column name.
        assertEquals(1, schemaColumnDao.getSchemaColumns(businessObjectDefinitionEntity, FIRST_COLUMN_NAME).size());

        // Get a list of schema columns by passing all case-insensitive parameters in uppercase.
        assertEquals(1, schemaColumnDao.getSchemaColumns(businessObjectDefinitionEntity, FIRST_COLUMN_NAME.toUpperCase()).size());

        // Get business object definition column by passing all case-insensitive parameters in lowercase.
        assertEquals(1, schemaColumnDao.getSchemaColumns(businessObjectDefinitionEntity, FIRST_COLUMN_NAME.toLowerCase()).size());

        // Try invalid values for all input parameters.
        assertEquals(0, schemaColumnDao.getSchemaColumns(
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, DESCRIPTION),
            FIRST_COLUMN_NAME).size());
        assertEquals(0, schemaColumnDao.getSchemaColumns(businessObjectDefinitionEntity, "I_DO_NOT_EXIST").size());
    }
}

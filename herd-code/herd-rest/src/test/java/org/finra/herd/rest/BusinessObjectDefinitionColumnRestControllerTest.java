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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumn;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnUpdateRequest;
import org.finra.herd.model.jpa.BusinessObjectDefinitionColumnEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;

/**
 * This class tests various functionality within the business object definition column REST controller.
 */
public class BusinessObjectDefinitionColumnRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateBusinessObjectDefinitionColumn()
    {
        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format.
        SchemaColumnEntity schemaColumnEntity = schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME);

        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create a business object definition column.
        BusinessObjectDefinitionColumn resultBusinessObjectDefinitionColumn = businessObjectDefinitionColumnRestController.createBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnCreateRequest(businessObjectDefinitionColumnKey, COLUMN_NAME, BDEF_COLUMN_DESCRIPTION));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(resultBusinessObjectDefinitionColumn.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION), resultBusinessObjectDefinitionColumn);

        // Validate that the schema column is now linked with the business object definition column.
        assertEquals(Long.valueOf(resultBusinessObjectDefinitionColumn.getId()), Long.valueOf(schemaColumnEntity.getBusinessObjectDefinitionColumn().getId()));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionColumn()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Validate that this business object definition column exists.
        assertNotNull(businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey));

        // Delete this business object definition column.
        BusinessObjectDefinitionColumn deletedBusinessObjectDefinitionColumn =
            businessObjectDefinitionColumnRestController.deleteBusinessObjectDefinitionColumn(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION), deletedBusinessObjectDefinitionColumn);

        // Ensure that this business object definition column is no longer there.
        assertNull(businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey));
    }

    @Test
    public void testGetBusinessObjectDefinitionColumn()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Get the business object definition column.
        BusinessObjectDefinitionColumn resultBusinessObjectDefinitionColumn =
            businessObjectDefinitionColumnRestController.getBusinessObjectDefinitionColumn(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION), resultBusinessObjectDefinitionColumn);
    }

    @Test
    public void testGetBusinessObjectDefinitionColumns()
    {
        // Create and persist business object definition column entities.
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2), DESCRIPTION_2);
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME), DESCRIPTION);

        // Get a list of business object definition column keys.
        BusinessObjectDefinitionColumnKeys resultBusinessObjectDefinitionColumnKeys =
            businessObjectDefinitionColumnRestController.getBusinessObjectDefinitionColumns(BDEF_NAMESPACE, BDEF_NAME);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumnKeys(Arrays.asList(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME),
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2))), resultBusinessObjectDefinitionColumnKeys);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionColumn()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Update the business object definition column.
        BusinessObjectDefinitionColumn updatedBusinessObjectDefinitionColumn = businessObjectDefinitionColumnRestController
            .updateBusinessObjectDefinitionColumn(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME,
                new BusinessObjectDefinitionColumnUpdateRequest(BDEF_COLUMN_DESCRIPTION_2));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION_2), updatedBusinessObjectDefinitionColumn);
    }
}

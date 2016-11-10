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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionColumnEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;

public class BusinessObjectDefinitionColumnDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetBusinessObjectDefinitionColumnByBusinessObjectDefinitionColumnName()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create a business object definition column entity.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Get the business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionColumnEntity.getBusinessObjectDefinition();

        // Get business object definition column.
        assertEquals(businessObjectDefinitionColumnEntity, businessObjectDefinitionColumnDao
            .getBusinessObjectDefinitionColumnByBusinessObjectDefinitionColumnName(businessObjectDefinitionEntity, BDEF_COLUMN_NAME));

        // Get business object definition column by passing all case-insensitive parameters in uppercase.
        assertEquals(businessObjectDefinitionColumnEntity, businessObjectDefinitionColumnDao
            .getBusinessObjectDefinitionColumnByBusinessObjectDefinitionColumnName(businessObjectDefinitionEntity, BDEF_COLUMN_NAME.toUpperCase()));

        // Get business object definition column by passing all case-insensitive parameters in lowercase.
        assertEquals(businessObjectDefinitionColumnEntity, businessObjectDefinitionColumnDao
            .getBusinessObjectDefinitionColumnByBusinessObjectDefinitionColumnName(businessObjectDefinitionEntity, BDEF_COLUMN_NAME.toLowerCase()));

        // Try invalid values for all input parameters.
        assertNull(businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByBusinessObjectDefinitionColumnName(
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, DESCRIPTION),
            BDEF_COLUMN_NAME));
        assertNull(businessObjectDefinitionColumnDao
            .getBusinessObjectDefinitionColumnByBusinessObjectDefinitionColumnName(businessObjectDefinitionEntity, "I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnByBusinessObjectDefinitionColumnNameDuplicateColumns() throws Exception
    {
        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create duplicate business object definition columns.
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionEntity, BDEF_COLUMN_NAME.toUpperCase(), DESCRIPTION);
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionEntity, BDEF_COLUMN_NAME.toLowerCase(), DESCRIPTION_2);

        // Try to get business object definition column when business object definition has duplicate columns.
        try
        {
            businessObjectDefinitionColumnDao
                .getBusinessObjectDefinitionColumnByBusinessObjectDefinitionColumnName(businessObjectDefinitionEntity, BDEF_COLUMN_NAME);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one business object definition column instance with parameters {namespace=\"%s\", " +
                    "businessObjectDefinitionName=\"%s\", businessObjectDefinitionColumnName=\"%s\"}.", BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME),
                e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnByKey()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create a business object definition column entity.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Get business object definition column.
        assertEquals(businessObjectDefinitionColumnEntity,
            businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey));

        // Get business object definition column by passing all case-insensitive parameters in uppercase.
        assertEquals(businessObjectDefinitionColumnEntity, businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), BDEF_COLUMN_NAME.toUpperCase())));

        // Get business object definition column by passing all case-insensitive parameters in lowercase.
        assertEquals(businessObjectDefinitionColumnEntity, businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), BDEF_COLUMN_NAME.toLowerCase())));

        // Try invalid values for all input parameters.
        assertNull(businessObjectDefinitionColumnDao
            .getBusinessObjectDefinitionColumnByKey(new BusinessObjectDefinitionColumnKey("I_DO_NOT_EXIST", BDEF_NAME, BDEF_COLUMN_NAME)));
        assertNull(businessObjectDefinitionColumnDao
            .getBusinessObjectDefinitionColumnByKey(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, "I_DO_NOT_EXIST", BDEF_COLUMN_NAME)));
        assertNull(businessObjectDefinitionColumnDao
            .getBusinessObjectDefinitionColumnByKey(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST")));
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnByKeyDuplicateColumns() throws Exception
    {
        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create duplicate business object definition columns.
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionEntity, BDEF_COLUMN_NAME.toUpperCase(), DESCRIPTION);
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionEntity, BDEF_COLUMN_NAME.toLowerCase(), DESCRIPTION_2);

        // Try to get business object definition column when business object definition has duplicate columns.
        try
        {
            businessObjectDefinitionColumnDao
                .getBusinessObjectDefinitionColumnByKey(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME));
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one business object definition column instance with parameters {namespace=\"%s\", " +
                    "businessObjectDefinitionName=\"%s\", businessObjectDefinitionColumnName=\"%s\"}.", BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME),
                e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnsByBusinessObjectDefinition()
    {
        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create business object definition columns.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity = businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionEntity, BDEF_COLUMN_NAME, DESCRIPTION);
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity2 = businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionEntity, BDEF_COLUMN_NAME_2, DESCRIPTION_2);

        // Get a list of business object definition column entities.
        List<BusinessObjectDefinitionColumnEntity> businessObjectDefinitionColumnEntities =
            businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnsByBusinessObjectDefinition(businessObjectDefinitionEntity);

        // Validate the business object definition column entities.
        assertEquals(businessObjectDefinitionColumnEntities.get(0).getBusinessObjectDefinition(), businessObjectDefinitionEntity);
        assertEquals(businessObjectDefinitionColumnEntities.get(0).getName(), BDEF_COLUMN_NAME);
        assertEquals(businessObjectDefinitionColumnEntities.get(0).getDescription(), DESCRIPTION);

        assertEquals(businessObjectDefinitionColumnEntities.get(1).getBusinessObjectDefinition(), businessObjectDefinitionEntity);
        assertEquals(businessObjectDefinitionColumnEntities.get(1).getName(), BDEF_COLUMN_NAME_2);
        assertEquals(businessObjectDefinitionColumnEntities.get(1).getDescription(), DESCRIPTION_2);
    }
}

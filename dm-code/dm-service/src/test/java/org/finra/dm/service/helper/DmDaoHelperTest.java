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
package org.finra.dm.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.jpa.StoragePlatformEntity;
import org.finra.dm.model.jpa.StorageUnitEntity;
import org.finra.dm.service.AbstractServiceTest;

/**
 * This class tests functionality within the DmDaoHelper class.
 */
public class DmDaoHelperTest extends AbstractServiceTest
{
    @Autowired
    DmDaoHelper dmDaoHelper;

    @Test
    public void testIsBusinessObjectDataAttributeRequired()
    {
        // Create and persist test database entities required for testing.
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY);
        createBusinessObjectDataAttributeDefinitionEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            ATTRIBUTE_NAME_1_MIXED_CASE);
        createBusinessObjectDataAttributeDefinitionEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            ATTRIBUTE_NAME_2_MIXED_CASE);

        // Validate the functionality.
        assertTrue(dmDaoHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_1_MIXED_CASE, businessObjectFormatEntity));
        assertTrue(dmDaoHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_2_MIXED_CASE, businessObjectFormatEntity));
        assertFalse(dmDaoHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_3_MIXED_CASE, businessObjectFormatEntity));
        assertTrue(dmDaoHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), businessObjectFormatEntity));
        assertTrue(dmDaoHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), businessObjectFormatEntity));
    }

    @Test
    public void testGetStorageAttributeValueByName()
    {
        // Prepare test data.
        StorageEntity storageEntity = createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3);
        createStorageAttributeEntity(storageEntity, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);
        createStorageAttributeEntity(storageEntity, ATTRIBUTE_NAME_2_MIXED_CASE, BLANK_TEXT);
        createStorageAttributeEntity(storageEntity, ATTRIBUTE_NAME_3_MIXED_CASE, null);
        dmDao.saveAndRefresh(storageEntity);

        // Retrieve optional attribute values.
        Assert.assertEquals(ATTRIBUTE_VALUE_1, dmDaoHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE, storageEntity, false));
        Assert.assertEquals(BLANK_TEXT, dmDaoHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_2_MIXED_CASE, storageEntity, false));
        Assert.assertNull(dmDaoHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_3_MIXED_CASE, storageEntity, false));

        // Validate case insensitivity.
        Assert.assertEquals(ATTRIBUTE_VALUE_1, dmDaoHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), storageEntity, false));
        Assert.assertEquals(ATTRIBUTE_VALUE_1, dmDaoHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), storageEntity, false));

        // Retrieve a required attribute value.
        Assert.assertEquals(ATTRIBUTE_VALUE_1, dmDaoHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE, storageEntity, true));

        // Try to retrieve a missing required attribute values when
        // - attribute does not exist
        // - attribute exists with a blank text value
        // - attribute exists with a null value
        String attributeNoExist = "I_DO_NOT_EXIST";
        for (String attributeName : Arrays.asList(attributeNoExist, ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_NAME_3_MIXED_CASE))
        {
            try
            {
                dmDaoHelper.getStorageAttributeValueByName(attributeName, storageEntity, true);
            }
            catch (IllegalStateException e)
            {
                if (attributeName.equals(attributeNoExist))
                {
                    Assert.assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.", attributeName, STORAGE_NAME), e.getMessage());
                }
                else
                {
                    Assert.assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must have a value that is not blank.", attributeName, STORAGE_NAME),
                        e.getMessage());
                }
            }
        }
    }

    @Test
    public void testGetStorageUnitEntity()
    {
        // Create and persist test database entities.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
        StorageEntity storageEntity = createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3);
        StorageUnitEntity storageUnitEntity = createStorageUnitEntity(storageEntity, businessObjectDataEntity);

        // Try to retrieve a non existing storage unit.
        try
        {
            dmDaoHelper.getStorageUnitEntity(businessObjectDataEntity, "I_DO_NOT_EXIST");
            fail("Should throw an ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Could not find storage unit in \"I_DO_NOT_EXIST\" storage for the business object data {%s}.", dmHelper
                .businessObjectDataKeyToString(
                    new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        NO_SUBPARTITION_VALUES, DATA_VERSION))), e.getMessage());
        }

        // Retrieve an existing storage unit entity.
        StorageUnitEntity resultStorageUnitEntity = dmDaoHelper.getStorageUnitEntity(businessObjectDataEntity, STORAGE_NAME);
        assertNotNull(resultStorageUnitEntity);
        assertEquals(storageUnitEntity.getId(), resultStorageUnitEntity.getId());
    }

    @Test
    public void testGetPartitionValue()
    {
        // Create and persist test database entities.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Retrieve primary and sub-partition values along with trying the "out of bounds" cases.
        assertEquals(null, dmDaoHelper.getPartitionValue(businessObjectDataEntity, 0));
        assertEquals(PARTITION_VALUE, dmDaoHelper.getPartitionValue(businessObjectDataEntity, 1));
        for (int partitionColumnPosition = 2; partitionColumnPosition <= BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1; partitionColumnPosition++)
        {
            assertEquals(SUBPARTITION_VALUES.get(partitionColumnPosition - 2),
                dmDaoHelper.getPartitionValue(businessObjectDataEntity, partitionColumnPosition));
        }
        assertEquals(null, dmDaoHelper.getPartitionValue(businessObjectDataEntity, BusinessObjectDataEntity.MAX_SUBPARTITIONS + 2));
    }
}

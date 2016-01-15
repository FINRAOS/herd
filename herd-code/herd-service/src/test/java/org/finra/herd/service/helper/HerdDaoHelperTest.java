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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the HerdDaoHelper class.
 */
public class HerdDaoHelperTest extends AbstractServiceTest
{
    @Autowired
    HerdDaoHelper herdDaoHelper;

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
        assertTrue(herdDaoHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_1_MIXED_CASE, businessObjectFormatEntity));
        assertTrue(herdDaoHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_2_MIXED_CASE, businessObjectFormatEntity));
        assertFalse(herdDaoHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_3_MIXED_CASE, businessObjectFormatEntity));
        assertTrue(herdDaoHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), businessObjectFormatEntity));
        assertTrue(herdDaoHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), businessObjectFormatEntity));
    }

    @Test
    public void testGetStorageAttributeValueByName()
    {
        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(ATTRIBUTE_NAME_2_MIXED_CASE, BLANK_TEXT));
        attributes.add(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE, null));
        StorageEntity storageEntity = createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, attributes);

        // Retrieve optional attribute values.
        Assert.assertEquals(ATTRIBUTE_VALUE_1, storageDaoHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE, storageEntity, false));
        Assert.assertEquals(BLANK_TEXT, storageDaoHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_2_MIXED_CASE, storageEntity, false));
        Assert.assertNull(storageDaoHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_3_MIXED_CASE, storageEntity, false));

        // Validate case insensitivity.
        Assert
            .assertEquals(ATTRIBUTE_VALUE_1, storageDaoHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), storageEntity, false));
        Assert
            .assertEquals(ATTRIBUTE_VALUE_1, storageDaoHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), storageEntity, false));

        // Retrieve a required attribute value.
        Assert.assertEquals(ATTRIBUTE_VALUE_1, storageDaoHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE, storageEntity, true));

        // Try to retrieve a missing required attribute values when
        // - attribute does not exist
        // - attribute exists with a blank text value
        // - attribute exists with a null value
        String attributeNoExist = "I_DO_NOT_EXIST";
        for (String attributeName : Arrays.asList(attributeNoExist, ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_NAME_3_MIXED_CASE))
        {
            try
            {
                storageDaoHelper.getStorageAttributeValueByName(attributeName, storageEntity, true);
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
        StorageUnitEntity storageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);
        BusinessObjectDataEntity businessObjectDataEntity = storageUnitEntity.getBusinessObjectData();

        // Try to retrieve a non existing storage unit.
        try
        {
            storageDaoHelper.getStorageUnitEntity(businessObjectDataEntity, "I_DO_NOT_EXIST");
            fail("Should throw an ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Could not find storage unit in \"I_DO_NOT_EXIST\" storage for the business object data {%s}.", herdHelper
                .businessObjectDataKeyToString(
                    new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        NO_SUBPARTITION_VALUES, DATA_VERSION))), e.getMessage());
        }

        // Retrieve an existing storage unit entity.
        StorageUnitEntity resultStorageUnitEntity = storageDaoHelper.getStorageUnitEntity(businessObjectDataEntity, STORAGE_NAME);
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
        assertEquals(null, herdDaoHelper.getPartitionValue(businessObjectDataEntity, 0));
        assertEquals(PARTITION_VALUE, herdDaoHelper.getPartitionValue(businessObjectDataEntity, 1));
        for (int partitionColumnPosition = 2; partitionColumnPosition <= BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1; partitionColumnPosition++)
        {
            assertEquals(SUBPARTITION_VALUES.get(partitionColumnPosition - 2),
                herdDaoHelper.getPartitionValue(businessObjectDataEntity, partitionColumnPosition));
        }
        assertEquals(null, herdDaoHelper.getPartitionValue(businessObjectDataEntity, BusinessObjectDataEntity.MAX_SUBPARTITIONS + 2));
    }
}

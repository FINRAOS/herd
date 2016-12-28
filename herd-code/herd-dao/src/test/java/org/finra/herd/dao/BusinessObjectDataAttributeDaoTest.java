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

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;

public class BusinessObjectDataAttributeDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetBusinessObjectDataAttributeByKey()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(businessObjectDataKey, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Get business object data attribute.
        assertEquals(businessObjectDataAttributeEntity, businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE)));

        // Get business object data attribute by passing all case-insensitive parameters in uppercase.
        assertEquals(businessObjectDataAttributeEntity, businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase())));

        // Get business object data attribute by passing all case-insensitive parameters in lowercase.
        assertEquals(businessObjectDataAttributeEntity, businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase())));

        // Try invalid values for all input parameters.
        assertNull(businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE)));
        assertNull(businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE)));
        assertNull(businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE)));
        assertNull(businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE)));
        assertNull(businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE)));
        assertNull(businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "I_DO_NOT_EXIST",
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE)));
        assertNull(businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList("I_DO_NOT_EXIST", SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE)));
        assertNull(businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), "I_DO_NOT_EXIST", SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE)));
        assertNull(businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), "I_DO_NOT_EXIST", SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE)));
        assertNull(businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), "I_DO_NOT_EXIST"), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE)));
        assertNull(businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INVALID_DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE)));
        assertNull(businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, "I_DO_NOT_EXIST")));
    }

    @Test
    public void testGetBusinessObjectDataAttributeByKeyDuplicateAttributes()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create duplicate business object data attributes.
        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(businessObjectDataKey, ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1);
        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(businessObjectDataKey, ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_2);

        // Try to get business object data attribute when business object data has duplicate attributes.
        try
        {
            businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
                new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one business object data attribute instance with parameters {namespace=\"%s\", " +
                "businessObjectDefinitionName=\"%s\", businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", " +
                "businessObjectFormatVersion=\"%d\", businessObjectDataPartitionValue=\"%s\", businessObjectDataSubPartitionValues=\"%s\", " +
                "businessObjectDataVersion=\"%d\", businessObjectDataAttributeName=\"%s\"}.", BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, StringUtils.join(SUBPARTITION_VALUES, ","), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE),
                e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataAttributeByKeyDuplicateAttributesNoSubPartitionValues()
    {
        // Create a business object data key without subpartition values.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create duplicate business object data attributes.
        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(businessObjectDataKey, ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1);
        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(businessObjectDataKey, ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_2);

        // Try to get business object data attribute when business object data has duplicate attributes.
        try
        {
            businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(
                new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one business object data attribute instance with parameters {namespace=\"%s\", " +
                "businessObjectDefinitionName=\"%s\", businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", " +
                "businessObjectFormatVersion=\"%d\", businessObjectDataPartitionValue=\"%s\", businessObjectDataSubPartitionValues=\"\", " +
                "businessObjectDataVersion=\"%d\", businessObjectDataAttributeName=\"%s\"}.", BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), e.getMessage());
        }
    }
}

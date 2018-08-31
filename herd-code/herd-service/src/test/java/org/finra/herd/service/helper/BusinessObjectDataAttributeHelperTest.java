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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the BusinessObjectDataAttributeHelper class.
 */
public class BusinessObjectDataAttributeHelperTest extends AbstractServiceTest
{
    @Test
    public void testIsBusinessObjectDataAttributeRequired()
    {
        // Create and persist test database entities required for testing.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        businessObjectFormatDaoTestHelper
            .createBusinessObjectDataAttributeDefinitionEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE);
        businessObjectFormatDaoTestHelper
            .createBusinessObjectDataAttributeDefinitionEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                ATTRIBUTE_NAME_2_MIXED_CASE);

        // Validate the functionality.
        assertTrue(businessObjectDataAttributeHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_1_MIXED_CASE, businessObjectFormatEntity));
        assertTrue(businessObjectDataAttributeHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_2_MIXED_CASE, businessObjectFormatEntity));
        assertFalse(businessObjectDataAttributeHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_3_MIXED_CASE, businessObjectFormatEntity));
        assertTrue(
            businessObjectDataAttributeHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), businessObjectFormatEntity));
        assertTrue(
            businessObjectDataAttributeHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), businessObjectFormatEntity));
    }
}

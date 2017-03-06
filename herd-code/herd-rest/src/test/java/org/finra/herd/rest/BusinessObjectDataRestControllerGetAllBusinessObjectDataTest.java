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

import java.util.Arrays;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKeys;

/**
 * This class tests get all business object data functionality within the business object data REST controller.
 */
public class BusinessObjectDataRestControllerGetAllBusinessObjectDataTest extends AbstractRestTest
{
    @Test
    public void testGetAllBusinessObjectDataByBusinessObjectDefinition()
    {
        // Create two business object data entities that belong to the same business object definition, but to two different business object formats.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Retrieve the keys for business object data.
        assertEquals(new BusinessObjectDataKeys(Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION))),
            businessObjectDataRestController.getAllBusinessObjectDataByBusinessObjectDefinition(BDEF_NAMESPACE, BDEF_NAME));
    }

    @Test
    public void testGetAllBusinessObjectDataByBusinessObjectFormat()
    {
        // Create two business object data entities that belong to the same business object format version.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Retrieve the keys for business object data.
        assertEquals(new BusinessObjectDataKeys(Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION),
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION))), businessObjectDataRestController
            .getAllBusinessObjectDataByBusinessObjectFormat(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION));
    }
}

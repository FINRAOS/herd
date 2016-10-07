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

import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataVersions;

/**
 * This class tests get business object data versions functionality within the business object data REST controller.
 */
public class BusinessObjectDataRestControllerGetBusinessObjectDataVersionsTest extends AbstractRestTest
{
    private static final int NUMBER_OF_FORMAT_VERSIONS = 2;

    private static final int NUMBER_OF_DATA_VERSIONS_PER_FORMAT_VERSION = 3;

    @Test
    public void testGetBusinessObjectDataVersions()
    {
        // Create and persist test database entities.
        createTestDatabaseEntities(SUBPARTITION_VALUES);

        // Retrieve the relative business object data version by specifying values for all input parameters.
        for (int businessObjectFormatVersion = INITIAL_FORMAT_VERSION; businessObjectFormatVersion < NUMBER_OF_FORMAT_VERSIONS; businessObjectFormatVersion++)
        {
            for (int businessObjectDataVersion = INITIAL_DATA_VERSION; businessObjectDataVersion < NUMBER_OF_DATA_VERSIONS_PER_FORMAT_VERSION;
                businessObjectDataVersion++)
            {
                BusinessObjectDataVersions businessObjectDataVersions = businessObjectDataRestController
                    .getBusinessObjectDataVersions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_VALUE,
                        getDelimitedFieldValues(SUBPARTITION_VALUES), businessObjectFormatVersion, businessObjectDataVersion);

                // Validate the returned object.
                assertNotNull(businessObjectDataVersions);
                assertEquals(1, businessObjectDataVersions.getBusinessObjectDataVersions().size());
                businessObjectDataServiceTestHelper
                    .validateBusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion, PARTITION_VALUE,
                        SUBPARTITION_VALUES, businessObjectDataVersion,
                        businessObjectDataVersions.getBusinessObjectDataVersions().get(0).getBusinessObjectDataKey());
                assertEquals(BDATA_STATUS, businessObjectDataVersions.getBusinessObjectDataVersions().get(0).getStatus());
            }
        }
    }

    @Test
    public void testGetBusinessObjectDataMissingOptionalParameters()
    {
        // Create and persist a business object data entities without subpartition values.
        createTestDatabaseEntities(NO_SUBPARTITION_VALUES);

        // Retrieve business object data versions without specifying any of the optional parameters including the subpartition values.
        BusinessObjectDataVersions resultBusinessObjectDataVersions = businessObjectDataRestController
            .getBusinessObjectDataVersions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_VALUE, null, null, null);

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataVersions);
        assertEquals(NUMBER_OF_FORMAT_VERSIONS * NUMBER_OF_DATA_VERSIONS_PER_FORMAT_VERSION,
            resultBusinessObjectDataVersions.getBusinessObjectDataVersions().size());
    }

    /**
     * Create and persist database entities required for testing.
     */
    private void createTestDatabaseEntities(List<String> subPartitionValues)
    {
        for (int businessObjectFormatVersion = INITIAL_FORMAT_VERSION; businessObjectFormatVersion < NUMBER_OF_FORMAT_VERSIONS; businessObjectFormatVersion++)
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion,
                    FORMAT_DESCRIPTION, businessObjectFormatVersion == SECOND_FORMAT_VERSION, PARTITION_KEY);

            for (int businessObjectDataVersion = INITIAL_DATA_VERSION; businessObjectDataVersion < NUMBER_OF_DATA_VERSIONS_PER_FORMAT_VERSION;
                businessObjectDataVersion++)
            {
                businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion,
                        PARTITION_VALUE, subPartitionValues, businessObjectDataVersion, businessObjectDataVersion == SECOND_DATA_VERSION, BDATA_STATUS);
            }
        }
    }
}

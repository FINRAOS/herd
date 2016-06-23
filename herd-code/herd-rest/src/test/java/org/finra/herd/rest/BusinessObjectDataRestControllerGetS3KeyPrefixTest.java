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

import javax.servlet.ServletRequest;

import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.api.xml.SchemaColumn;

/**
 * This class tests the getS3KeyPrefix functionality within the business object data REST controller.
 */
public class BusinessObjectDataRestControllerGetS3KeyPrefixTest extends AbstractRestTest
{
    @Test
    public void testGetS3KeyPrefix()
    {
        // Create database entities required for testing. Please note that we are not passing the flag to create a business object data entity.
        createDatabaseEntitiesForGetS3KeyPrefixTesting(false);

        // Get the test partition columns.
        List<SchemaColumn> testPartitionColumns = getTestPartitionColumns();
        String testPartitionKey = testPartitionColumns.get(0).getName();
        List<SchemaColumn> testSubPartitionColumns = testPartitionColumns.subList(1, SUBPARTITION_VALUES.size() + 1);

        // Get an S3 key prefix by passing all parameters including partition key, business object data version,
        // and "create new version" flag (has no effect when data version is specified).
        S3KeyPrefixInformation resultS3KeyPrefixInformation = businessObjectDataRestController
            .getS3KeyPrefix(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey, PARTITION_VALUE,
                getDelimitedFieldValues(SUBPARTITION_VALUES), DATA_VERSION, STORAGE_NAME, false, getServletRequestWithPartitionInfo(testPartitionKey));

        // Get the expected S3 key prefix value using the business object data version.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testPartitionKey,
                PARTITION_VALUE, testSubPartitionColumns.toArray(new SchemaColumn[testSubPartitionColumns.size()]),
                SUBPARTITION_VALUES.toArray(new String[SUBPARTITION_VALUES.size()]), DATA_VERSION);

        // Validate the results.
        assertNotNull(resultS3KeyPrefixInformation);
        assertEquals(expectedS3KeyPrefix, resultS3KeyPrefixInformation.getS3KeyPrefix());
    }

    /**
     * Gets a servlet request with a partition key and partition value.
     *
     * @param partitionKey the partition key.
     *
     * @return the servlet request.
     */
    private ServletRequest getServletRequestWithPartitionInfo(String partitionKey)
    {
        // Create a servlet request that contains the partition key and value.
        MockHttpServletRequest servletRequest = new MockHttpServletRequest();
        servletRequest.setParameter("partitionKey", partitionKey);
        servletRequest.setParameter("partitionValue", PARTITION_VALUE);
        return servletRequest;
    }
}

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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.finra.herd.model.api.xml.BusinessObjectDataPartitionsRequest;

/**
 * This class tests generateBusinessObjectDataDdlCollection functionality within the business object data service.
 */
public class BusinessObjectDataServiceGenerateBusinessObjectDataPartitionsTest extends AbstractServiceTest
{
    @Test
    public void testGenerateBusinessObjectDataPartitionslMissingRequiredParameters()
    {
        BusinessObjectDataPartitionsRequest request;

        // Try to retrieve business object data partitions when business object definition name parameter is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectDefinitionName(BLANK_TEXT);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when business object definition name parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to retrieve business object data partitions when business object format usage parameter is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectFormatUsage(BLANK_TEXT);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when business object format usage parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to retrieve business object data partitions when business object format file type parameter is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectFormatFileType(BLANK_TEXT);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when business object format file type parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to retrieve business object data partitions when partition value list is null.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        request.setPartitionValueFilters(Collections.EMPTY_LIST);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when partition value list is null.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one partition value filter must be specified.", e.getMessage());
        }

        // Try to retrieve business object data partitions when standalone storage name parameter is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        request.setStorageNames(null);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when standalone storage name parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A list of storage names must be specified.", e.getMessage());
        }

        // Try to check business object data availability when standalone storage name parameter value is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        request.setStorageNames(Collections.singletonList(BLANK_TEXT));
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when storage name parameter in the list of storage names is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsTrimParametersStorageNames()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        // Try to retrieve business object data partitions when business object definition name parameter is not specified.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        List<String> storageNames = Arrays.asList(addWhitespace(AbstractServiceTest.STORAGE_NAME));
        request.setStorageNames(storageNames);

        // Verify storageNames in request are trimmed
        businessObjectDataService.generateBusinessObjectDataPartitions(request);
        assertEquals(AbstractServiceTest.STORAGE_NAME, request.getStorageNames().get(0));
    }
}

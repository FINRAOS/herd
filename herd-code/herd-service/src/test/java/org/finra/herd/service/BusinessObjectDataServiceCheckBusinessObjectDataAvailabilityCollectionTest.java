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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityRequest;

/**
 * This class tests checkBusinessObjectDataAvailabilityCollection functionality within the business object data service.
 */
public class BusinessObjectDataServiceCheckBusinessObjectDataAvailabilityCollectionTest extends AbstractServiceTest
{
    @Test
    public void testCheckBusinessObjectDataAvailabilityCollection()
    {
        // Prepare database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityCollectionTesting();

        // Check an availability for a collection of business object data.
        BusinessObjectDataAvailabilityCollectionResponse resultBusinessObjectDataAvailabilityCollectionResponse =
            businessObjectDataService.checkBusinessObjectDataAvailabilityCollection(getTestBusinessObjectDataAvailabilityCollectionRequest());

        // Validate the response object.
        assertEquals(getExpectedBusinessObjectDataAvailabilityCollectionResponse(), resultBusinessObjectDataAvailabilityCollectionResponse);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityCollectionMissingRequiredParameters()
    {
        BusinessObjectDataAvailabilityCollectionRequest request;

        // Try to check business object data availability collection when business object data availability collection request is null.
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailabilityCollection(null);
            fail("Should throw an IllegalArgumentException when business object data availability collection request is null.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data availability collection request must be specified.", e.getMessage());
        }

        // Try to check business object data availability collection when business object data availability request is not specified.
        request = getTestBusinessObjectDataAvailabilityCollectionRequest();
        for (List<BusinessObjectDataAvailabilityRequest> businessObjectDataAvailabilityRequests : Arrays
            .asList(null, new ArrayList<BusinessObjectDataAvailabilityRequest>()))
        {
            request.setBusinessObjectDataAvailabilityRequests(businessObjectDataAvailabilityRequests);
            try
            {
                businessObjectDataService.checkBusinessObjectDataAvailabilityCollection(request);
                fail("Should throw an IllegalArgumentException when business object data availability request is not specified.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("At least one business object data availability request must be specified.", e.getMessage());
            }
        }

        // Try to check business object data availability collection when business object data availability request is null.
        request = getTestBusinessObjectDataAvailabilityCollectionRequest();
        request.getBusinessObjectDataAvailabilityRequests().set(0, null);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailabilityCollection(request);
            fail("Should throw an IllegalArgumentException when business object data availability request is null.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data availability request must be specified.", e.getMessage());
        }
    }
}

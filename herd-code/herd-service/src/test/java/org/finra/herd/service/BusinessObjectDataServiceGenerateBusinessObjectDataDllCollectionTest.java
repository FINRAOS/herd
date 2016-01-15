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

import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;

/**
 * This class tests generateBusinessObjectDataDdlCollection functionality within the business object data service.
 */
public class BusinessObjectDataServiceGenerateBusinessObjectDataDllCollectionTest extends AbstractServiceTest
{
    @Test
    public void testGenerateBusinessObjectDataDdlCollection()
    {
        // Prepare database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataDdlCollectionTesting();

        // Generate DDL for a collection of business object data.
        BusinessObjectDataDdlCollectionResponse resultBusinessObjectDataDdlCollectionResponse =
            businessObjectDataService.generateBusinessObjectDataDdlCollection(getTestBusinessObjectDataDdlCollectionRequest());

        // Validate the response object.
        assertEquals(getExpectedBusinessObjectDataDdlCollectionResponse(), resultBusinessObjectDataDdlCollectionResponse);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlCollectionMissingRequiredParameters()
    {
        BusinessObjectDataDdlCollectionRequest request;

        // Try to generate business object data ddl collection collection when business object data ddl collection request is null.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdlCollection(null);
            fail("Should throw an IllegalArgumentException when business object data DDL collection request is null.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data DDL collection request must be specified.", e.getMessage());
        }

        // Try to generate business object data ddl collection collection when business object data ddl request is not specified.
        request = getTestBusinessObjectDataDdlCollectionRequest();
        for (List<BusinessObjectDataDdlRequest> businessObjectDataDdlRequests : Arrays.asList(null, new ArrayList<BusinessObjectDataDdlRequest>()))
        {
            request.setBusinessObjectDataDdlRequests(businessObjectDataDdlRequests);
            try
            {
                businessObjectDataService.generateBusinessObjectDataDdlCollection(request);
                fail("Should throw an IllegalArgumentException when business object data DDL request is not specified.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("At least one business object data DDL request must be specified.", e.getMessage());
            }
        }

        // Try to generate business object data ddl collection collection when business object data ddl request is null.
        request = getTestBusinessObjectDataDdlCollectionRequest();
        request.getBusinessObjectDataDdlRequests().set(0, null);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdlCollection(request);
            fail("Should throw an IllegalArgumentException when business object data DDL request is null.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data DDL request must be specified.", e.getMessage());
        }
    }
}

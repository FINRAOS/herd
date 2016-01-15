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

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionResponse;

/**
 * This class tests generateBusinessObjectDataDdlCollection functionality within the business object data REST controller.
 */
public class BusinessObjectDataRestControllerGenerateBusinessObjectDataDdlCollectionTest extends AbstractRestTest
{
    @Test
    public void testGenerateBusinessObjectDataDdlCollection()
    {
        // Prepare database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataDdlCollectionTesting();

        // Generate DDL for a collection of business object data.
        BusinessObjectDataDdlCollectionResponse resultBusinessObjectDataDdlCollectionResponse =
            businessObjectDataRestController.generateBusinessObjectDataDdlCollection(getTestBusinessObjectDataDdlCollectionRequest());

        // Validate the response object.
        assertEquals(getExpectedBusinessObjectDataDdlCollectionResponse(), resultBusinessObjectDataDdlCollectionResponse);
    }
}

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

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataPartitionsRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataRetryStoragePolicyTransitionRequest;

public class BusinessObjectDataServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "businessObjectDataServiceImpl")
    private BusinessObjectDataService businessObjectDataServiceImpl;

    /**
     * This method is to get the coverage for the business object data service method that starts the new transaction.
     */
    @Test
    public void testBusinessObjectDataServiceMethodsNewTx() throws Exception
    {
        BusinessObjectDataAvailabilityRequest businessObjectDataAvailabilityRequest = new BusinessObjectDataAvailabilityRequest();
        try
        {
            businessObjectDataServiceImpl.checkBusinessObjectDataAvailability(businessObjectDataAvailabilityRequest);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        BusinessObjectDataAvailabilityCollectionRequest businessObjectDataAvailabilityCollectionRequest = new BusinessObjectDataAvailabilityCollectionRequest();
        try
        {
            businessObjectDataServiceImpl.checkBusinessObjectDataAvailabilityCollection(businessObjectDataAvailabilityCollectionRequest);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one business object data availability request must be specified.", e.getMessage());
        }

        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = new BusinessObjectDataCreateRequest();
        try
        {
            businessObjectDataServiceImpl.createBusinessObjectData(businessObjectDataCreateRequest);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataServiceImpl
                .getBusinessObjectData(new BusinessObjectDataKey(), null, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY,
                    NO_INCLUDE_STORAGE_UNIT_STATUS_HISTORY, NO_EXCLUDE_BUSINESS_OBJECT_DATA_STORAGE_FILES);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataServiceImpl.generateBusinessObjectDataDdl(new BusinessObjectDataDdlRequest());
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataServiceImpl.generateBusinessObjectDataPartitions(new BusinessObjectDataPartitionsRequest());
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataServiceImpl.generateBusinessObjectDataDdlCollection(new BusinessObjectDataDdlCollectionRequest());
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one business object data DDL request must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataServiceImpl.invalidateUnregisteredBusinessObjectData(new BusinessObjectDataInvalidateUnregisteredRequest());
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The namespace is required", e.getMessage());
        }

        try
        {
            businessObjectDataServiceImpl
                .retryStoragePolicyTransition(new BusinessObjectDataKey(), new BusinessObjectDataRetryStoragePolicyTransitionRequest());
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataServiceImpl.restoreBusinessObjectData(new BusinessObjectDataKey(), EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION, BATCH_RESTORE_MODE);
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }
}

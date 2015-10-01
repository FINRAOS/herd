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
package org.finra.dm.rest;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataAvailability;
import org.finra.dm.model.api.xml.BusinessObjectDataAvailabilityRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataStatus;
import org.finra.dm.service.impl.BusinessObjectDataServiceImpl;

/**
 * This class tests checkBusinessObjectDataAvailability functionality within the business object data REST controller.
 */
public class BusinessObjectDataRestControllerCheckBusinessObjectDataAvailabilityTest extends AbstractRestTest
{
    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueList()
    {
        // Prepare test data and execute the check business object data availability request.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataAvailability resultAvailability = businessObjectDataRestController.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_NOT_AVAILABLE, null,
                DATA_VERSION, BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueListStandalonePartitionValueFilter()
    {
        // Prepare test data and execute the check business object data availability request with a standalone partition value filter.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setPartitionValueFilter(request.getPartitionValueFilters().get(0));
        request.setPartitionValueFilters(null);
        BusinessObjectDataAvailability resultAvailability = businessObjectDataRestController.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_NOT_AVAILABLE, null,
                DATA_VERSION, BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, true);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueRange()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(PARTITION_KEY_GROUP);
        createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        BusinessObjectDataAvailabilityRequest request;
        BusinessObjectDataAvailability resultAvailability;
        List<BusinessObjectDataStatus> expectedAvailableStatuses;
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses;

        // Execute the check business object data availability request when start partition value is less than the end partition value.
        request = getTestBusinessObjectDataAvailabilityRequest(START_PARTITION_VALUE, END_PARTITION_VALUE);
        resultAvailability = businessObjectDataRestController.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PROCESS_DATE_PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        expectedNotAvailableStatuses = getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            PROCESS_DATE_PARTITION_VALUES_NOT_AVAILABLE, null, DATA_VERSION, BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);

        // Execute the check business object data availability request when start partition value is equal to the end partition value.
        request = getTestBusinessObjectDataAvailabilityRequest(START_PARTITION_VALUE, START_PARTITION_VALUE);
        resultAvailability = businessObjectDataRestController.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, Arrays.asList(START_PARTITION_VALUE),
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, DATA_VERSION,
                BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }
}

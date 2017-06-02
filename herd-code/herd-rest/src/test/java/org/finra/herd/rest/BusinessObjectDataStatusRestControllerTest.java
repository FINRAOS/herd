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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusInformation;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateResponse;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.service.BusinessObjectDataStatusService;
import org.finra.herd.service.NotificationEventService;

/**
 * This class tests various functionality within the business object data status REST controller.
 */
public class BusinessObjectDataStatusRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private BusinessObjectDataStatusRestController businessObjectDataStatusRestController;

    @Mock
    private BusinessObjectDataStatusService businessObjectDataStatusService;

    @Mock
    private NotificationEventService notificationEventService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetBusinessObjectDataStatus()
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        BusinessObjectDataStatusInformation businessObjectDataStatusInformation = new BusinessObjectDataStatusInformation();
        when(businessObjectDataStatusService.getBusinessObjectDataStatus(businessObjectDataKey, PARTITION_KEY)).thenReturn(businessObjectDataStatusInformation);
        // Get the business object data status information.
        BusinessObjectDataStatusInformation resultBusinessObjectDataStatusInformation = businessObjectDataStatusRestController
            .getBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, PARTITION_VALUE,
                getDelimitedFieldValues(SUBPARTITION_VALUES), FORMAT_VERSION, DATA_VERSION);

        // Verify the external calls.
        verify(businessObjectDataStatusService).getBusinessObjectDataStatus(businessObjectDataKey, PARTITION_KEY);
        verifyNoMoreInteractions(businessObjectDataStatusService);
        // Validate the returned object.
        assertEquals(businessObjectDataStatusInformation, resultBusinessObjectDataStatusInformation);
    }

    @Test
    public void testGetBusinessObjectDataStatusMissingOptionalParameters()
    {
        // Test if we can get status for the business object data without specifying optional parameters
        // and with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);
            BusinessObjectDataStatusInformation businessObjectDataStatusInformation = new BusinessObjectDataStatusInformation();

            BusinessObjectDataKey businessObjectDataKey =
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues, null);
            when(businessObjectDataStatusService.getBusinessObjectDataStatus(businessObjectDataKey, BLANK_TEXT))
                .thenReturn(businessObjectDataStatusInformation);

            // Get the business object data status information without specifying optional parameters.
            BusinessObjectDataStatusInformation resultBusinessObjectDataStatusInformation = businessObjectDataStatusRestController
                .getBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, BLANK_TEXT, PARTITION_VALUE,
                    getDelimitedFieldValues(subPartitionValues), null, null);

            // Verify the external calls.
            verify(businessObjectDataStatusService).getBusinessObjectDataStatus(businessObjectDataKey, BLANK_TEXT);
            assertEquals(businessObjectDataStatusInformation, resultBusinessObjectDataStatusInformation);
        }
        verifyNoMoreInteractions(businessObjectDataStatusService);
    }

    @Test
    public void testGetBusinessObjectDataStatusMissingOptionalParametersPassedAsNulls()
    {
        BusinessObjectDataStatusInformation businessObjectDataStatusInformation = new BusinessObjectDataStatusInformation();

        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, new ArrayList<>(), null);
        when(businessObjectDataStatusService.getBusinessObjectDataStatus(businessObjectDataKey, null)).thenReturn(businessObjectDataStatusInformation);


        // Get the business object data status by passing null values for the partition key and the list of sub-partition values.
        BusinessObjectDataStatusInformation resultBusinessObjectDataStatusInformation = businessObjectDataStatusRestController
            .getBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, null, null, null);

        // Verify the external calls.
        verify(businessObjectDataStatusService).getBusinessObjectDataStatus(businessObjectDataKey, null);
        assertEquals(businessObjectDataStatusInformation, resultBusinessObjectDataStatusInformation);
    }

    @Test
    public void testUpdateBusinessObjectDataStatus()
    {
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);
        BusinessObjectDataStatusUpdateRequest request = new BusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2);

        BusinessObjectDataStatusUpdateResponse businessObjectDataStatusUpdateResponse = new BusinessObjectDataStatusUpdateResponse();
        when(businessObjectDataStatusService.updateBusinessObjectDataStatus(businessObjectDataKey, request)).thenReturn(businessObjectDataStatusUpdateResponse);

        // Update the business object data status.
        BusinessObjectDataStatusUpdateResponse updatedResponse = businessObjectDataStatusRestController
            .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION, request);


        // Verify the external calls.
        verify(businessObjectDataStatusService).updateBusinessObjectDataStatus(businessObjectDataKey, request);
        verifyNoMoreInteractions(businessObjectDataStatusService);
        // Validate the returned object.
        assertEquals(businessObjectDataStatusUpdateResponse, updatedResponse);
    }

    @Test
    public void testUpdateBusinessObjectDataStatusMissingOptionalParameters()
    {
        // Test if we can retrieve an attribute for the business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            BusinessObjectDataKey businessObjectDataKey =
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION);
            BusinessObjectDataStatusUpdateRequest request = new BusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2);
            BusinessObjectDataStatusUpdateResponse businessObjectDataStatusUpdateResponse = new BusinessObjectDataStatusUpdateResponse();

            when(businessObjectDataStatusService.updateBusinessObjectDataStatus(businessObjectDataKey, request))
                .thenReturn(businessObjectDataStatusUpdateResponse);

            // Update the business object data status using the relative endpoint.
            BusinessObjectDataStatusUpdateResponse response = null;

            switch (i)
            {
                case 0:

                    response = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            DATA_VERSION, new BusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 1:
                    response = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), DATA_VERSION, new BusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 2:
                    response = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), DATA_VERSION, new BusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 3:
                    response = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), DATA_VERSION,
                            new BusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 4:
                    response = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), subPartitionValues.get(3), DATA_VERSION,
                            new BusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
            }
            verify(businessObjectDataStatusService).updateBusinessObjectDataStatus(businessObjectDataKey, request);
            assertEquals(businessObjectDataStatusUpdateResponse, response);
        }
        verifyNoMoreInteractions(businessObjectDataStatusService);
    }
}

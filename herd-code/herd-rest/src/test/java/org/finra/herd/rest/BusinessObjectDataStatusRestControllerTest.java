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

import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusInformation;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateResponse;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
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
    private HerdStringHelper herdStringHelper;

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
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a delimited list of sub-partition values.
        String delimitedSubPartitionValues = String.join("|", SUBPARTITION_VALUES);

        // Create a business object data status information.
        BusinessObjectDataStatusInformation businessObjectDataStatusInformation = new BusinessObjectDataStatusInformation(businessObjectDataKey, BDATA_STATUS);

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues)).thenReturn(SUBPARTITION_VALUES);
        when(businessObjectDataStatusService.getBusinessObjectDataStatus(businessObjectDataKey, PARTITION_KEY)).thenReturn(businessObjectDataStatusInformation);

        // Call the method under test.
        BusinessObjectDataStatusInformation result = businessObjectDataStatusRestController
            .getBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, PARTITION_VALUE,
                delimitedSubPartitionValues, FORMAT_VERSION, DATA_VERSION);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues);
        verify(businessObjectDataStatusService).getBusinessObjectDataStatus(businessObjectDataKey, PARTITION_KEY);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataStatusInformation, result);
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

            // Create a business object data key.
            BusinessObjectDataKey businessObjectDataKey =
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues, null);

            // Create a delimited list of sub-partition values.
            String delimitedSubPartitionValues = String.join("|", subPartitionValues);

            // Create a business object data status information.
            BusinessObjectDataStatusInformation businessObjectDataStatusInformation =
                new BusinessObjectDataStatusInformation(businessObjectDataKey, BDATA_STATUS);

            // Mock the external calls.
            when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues)).thenReturn(subPartitionValues);
            when(businessObjectDataStatusService.getBusinessObjectDataStatus(businessObjectDataKey, BLANK_TEXT))
                .thenReturn(businessObjectDataStatusInformation);

            // Call the method under test.
            BusinessObjectDataStatusInformation result = businessObjectDataStatusRestController
                .getBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, BLANK_TEXT, PARTITION_VALUE,
                    delimitedSubPartitionValues, null, null);

            // Verify the external calls.
            verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues);
            verify(businessObjectDataStatusService).getBusinessObjectDataStatus(businessObjectDataKey, BLANK_TEXT);
            verifyNoMoreInteractionsHelper();

            // Validate the results.
            assertEquals(businessObjectDataStatusInformation, result);
        }
    }

    @Test
    public void testGetBusinessObjectDataStatusMissingOptionalParametersPassedAsNulls()
    {
        // Create a business object data status information.
        BusinessObjectDataStatusInformation businessObjectDataStatusInformation = new BusinessObjectDataStatusInformation();

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, new ArrayList<>(), null);

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(null)).thenReturn(new ArrayList<>());
        when(businessObjectDataStatusService.getBusinessObjectDataStatus(businessObjectDataKey, null)).thenReturn(businessObjectDataStatusInformation);

        // Call the method under test.
        BusinessObjectDataStatusInformation result = businessObjectDataStatusRestController
            .getBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, null, null, null);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(null);
        verify(businessObjectDataStatusService).getBusinessObjectDataStatus(businessObjectDataKey, null);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataStatusInformation, result);
    }

    @Test
    public void testUpdateBusinessObjectDataStatus()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data status update request.
        BusinessObjectDataStatusUpdateRequest businessObjectDataStatusUpdateRequest = new BusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2);

        // Create a business object data status update response.
        BusinessObjectDataStatusUpdateResponse businessObjectDataStatusUpdateResponse =
            new BusinessObjectDataStatusUpdateResponse(businessObjectDataKey, BDATA_STATUS_2, BDATA_STATUS);

        // Mock the external calls.
        when(businessObjectDataStatusService.updateBusinessObjectDataStatus(businessObjectDataKey, businessObjectDataStatusUpdateRequest))
            .thenReturn(businessObjectDataStatusUpdateResponse);

        // Call the method under test.
        BusinessObjectDataStatusUpdateResponse result = businessObjectDataStatusRestController
            .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION,
                businessObjectDataStatusUpdateRequest);

        // Verify the external calls.
        verify(businessObjectDataStatusService).updateBusinessObjectDataStatus(businessObjectDataKey, businessObjectDataStatusUpdateRequest);
        verify(notificationEventService)
            .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG, businessObjectDataKey,
                BDATA_STATUS_2, BDATA_STATUS);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataStatusUpdateResponse, result);
    }

    @Test
    public void testUpdateBusinessObjectDataStatusMissingOptionalParameters()
    {
        // Test if we can retrieve an attribute for the business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create a business object data key.
            BusinessObjectDataKey businessObjectDataKey =
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION);

            // Create a business object data status update request.
            BusinessObjectDataStatusUpdateRequest businessObjectDataStatusUpdateRequest = new BusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2);

            // Create a business object data status update response.
            BusinessObjectDataStatusUpdateResponse businessObjectDataStatusUpdateResponse =
                new BusinessObjectDataStatusUpdateResponse(businessObjectDataKey, BDATA_STATUS_2, BDATA_STATUS);

            // Mock the external calls.
            when(businessObjectDataStatusService.updateBusinessObjectDataStatus(businessObjectDataKey, businessObjectDataStatusUpdateRequest))
                .thenReturn(businessObjectDataStatusUpdateResponse);

            // Call the relative method under test.
            BusinessObjectDataStatusUpdateResponse result = null;
            switch (i)
            {
                case 0:

                    result = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            DATA_VERSION, businessObjectDataStatusUpdateRequest);
                    break;
                case 1:
                    result = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), DATA_VERSION, businessObjectDataStatusUpdateRequest);
                    break;
                case 2:
                    result = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), DATA_VERSION, businessObjectDataStatusUpdateRequest);
                    break;
                case 3:
                    result = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), DATA_VERSION,
                            businessObjectDataStatusUpdateRequest);
                    break;
                case 4:
                    result = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), subPartitionValues.get(3), DATA_VERSION,
                            businessObjectDataStatusUpdateRequest);
                    break;
            }

            // Verify the external calls.
            verify(businessObjectDataStatusService).updateBusinessObjectDataStatus(businessObjectDataKey, businessObjectDataStatusUpdateRequest);
            verify(notificationEventService)
                .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG, businessObjectDataKey,
                    BDATA_STATUS_2, BDATA_STATUS);
            verifyNoMoreInteractionsHelper();

            // Validate the results.
            assertEquals(businessObjectDataStatusUpdateResponse, result);
        }
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataStatusService, herdStringHelper, notificationEventService);
    }
}

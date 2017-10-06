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

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateResponse;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.service.BusinessObjectDataStorageUnitStatusService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.helper.StorageUnitHelper;

/**
 * This class tests various functionality within the business object data storage unit status REST controller.
 */
public class BusinessObjectDataStorageUnitStatusRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private BusinessObjectDataStorageUnitStatusRestController businessObjectDataStorageUnitStatusRestController;

    @Mock
    private BusinessObjectDataStorageUnitStatusService businessObjectDataStorageUnitStatusService;

    @Mock
    private NotificationEventService notificationEventService;

    @Mock
    private StorageUnitHelper storageUnitHelper;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testUpdateBusinessObjectDataStorageUnitStatus()
    {
        // Test for the business object data having all possible number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create a business object data key.
            BusinessObjectDataKey businessObjectDataKey =
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION);

            // Create a business object data storage unit key.
            BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
                new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, STORAGE_NAME);

            // Create a business object data storage unit status update request.
            BusinessObjectDataStorageUnitStatusUpdateRequest request = new BusinessObjectDataStorageUnitStatusUpdateRequest(STORAGE_UNIT_STATUS_2);

            // Create a business object data storage unit status update response.
            BusinessObjectDataStorageUnitStatusUpdateResponse expectedResponse =
                new BusinessObjectDataStorageUnitStatusUpdateResponse(businessObjectDataStorageUnitKey, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS);

            // Mock the external calls.
            when(businessObjectDataStorageUnitStatusService.updateBusinessObjectDataStorageUnitStatus(businessObjectDataStorageUnitKey, request))
                .thenReturn(expectedResponse);
            when(storageUnitHelper.getBusinessObjectDataKey(businessObjectDataStorageUnitKey)).thenReturn(businessObjectDataKey);

            // Call the method under test.
            BusinessObjectDataStorageUnitStatusUpdateResponse result = null;

            switch (i)
            {
                case 0:
                    result = businessObjectDataStorageUnitStatusRestController
                        .updateBusinessObjectDataStorageUnitStatus(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                            PARTITION_VALUE, DATA_VERSION, STORAGE_NAME, request);
                    break;
                case 1:
                    result = businessObjectDataStorageUnitStatusRestController
                        .updateBusinessObjectDataStorageUnitStatus(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                            PARTITION_VALUE, subPartitionValues.get(0), DATA_VERSION, STORAGE_NAME, request);
                    break;
                case 2:
                    result = businessObjectDataStorageUnitStatusRestController
                        .updateBusinessObjectDataStorageUnitStatus(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                            PARTITION_VALUE, subPartitionValues.get(0), subPartitionValues.get(1), DATA_VERSION, STORAGE_NAME, request);
                    break;
                case 3:
                    result = businessObjectDataStorageUnitStatusRestController
                        .updateBusinessObjectDataStorageUnitStatus(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                            PARTITION_VALUE, subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), DATA_VERSION, STORAGE_NAME,
                            request);
                    break;
                case 4:
                    result = businessObjectDataStorageUnitStatusRestController
                        .updateBusinessObjectDataStorageUnitStatus(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                            PARTITION_VALUE, subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), subPartitionValues.get(3),
                            DATA_VERSION, STORAGE_NAME, request);
                    break;
            }

            // Verify the external calls.
            verify(businessObjectDataStorageUnitStatusService).updateBusinessObjectDataStorageUnitStatus(businessObjectDataStorageUnitKey, request);
            verify(storageUnitHelper).getBusinessObjectDataKey(businessObjectDataStorageUnitKey);
            verify(notificationEventService)
                .processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG, businessObjectDataKey,
                    STORAGE_NAME, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS);
            verifyNoMoreInteractions(businessObjectDataStorageUnitStatusService, notificationEventService, storageUnitHelper);

            // Validate the results.
            assertEquals(expectedResponse, result);
        }
    }
}

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
package org.finra.herd.service.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateResponse;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;
import org.finra.herd.service.helper.StorageUnitHelper;
import org.finra.herd.service.helper.StorageUnitStatusDaoHelper;

/**
 * This class tests functionality within the business object data storage unit status service implementation.
 */
public class BusinessObjectDataStorageUnitStatusServiceImplTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @InjectMocks
    private BusinessObjectDataStorageUnitStatusServiceImpl businessObjectDataStorageUnitStatusServiceImpl;

    @Mock
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Mock
    private StorageUnitHelper storageUnitHelper;

    @Mock
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testUpdateBusinessObjectDataStorageUnitStatus()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data storage unit key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper.createStorageUnitEntity(businessObjectDataStorageUnitKey, STORAGE_UNIT_STATUS);

        // Create a storage unit status entity.
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2);

        // Create a business object data storage unit status update request.
        BusinessObjectDataStorageUnitStatusUpdateRequest request = new BusinessObjectDataStorageUnitStatusUpdateRequest(STORAGE_UNIT_STATUS_2);

        // Create a business object data storage unit status update response.
        BusinessObjectDataStorageUnitStatusUpdateResponse expectedResponse =
            new BusinessObjectDataStorageUnitStatusUpdateResponse(businessObjectDataStorageUnitKey, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS);

        // Mock the external calls.
        when(storageUnitDaoHelper.getStorageUnitEntityByKey(businessObjectDataStorageUnitKey)).thenReturn(storageUnitEntity);
        when(storageUnitStatusDaoHelper.getStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2)).thenReturn(storageUnitStatusEntity);
        doAnswer(new Answer<Void>()
        {
            public Void answer(InvocationOnMock invocation)
            {
                // Get the storage unit entity parameter.
                StorageUnitEntity storageUnitEntity = (StorageUnitEntity) invocation.getArguments()[0];
                StorageUnitStatusEntity storageUnitStatusEntity = (StorageUnitStatusEntity) invocation.getArguments()[1];

                // Update storage unit status.
                storageUnitEntity.setStatus(storageUnitStatusEntity);
                return null;
            }
        }).when(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, storageUnitStatusEntity, STORAGE_UNIT_STATUS_2);
        when(businessObjectDataHelper.createBusinessObjectDataKeyFromEntity(storageUnitEntity.getBusinessObjectData())).thenReturn(businessObjectDataKey);
        when(storageUnitHelper.createBusinessObjectDataStorageUnitKey(businessObjectDataKey, STORAGE_NAME)).thenReturn(businessObjectDataStorageUnitKey);

        // Call the method under test.
        BusinessObjectDataStorageUnitStatusUpdateResponse result =
            businessObjectDataStorageUnitStatusServiceImpl.updateBusinessObjectDataStorageUnitStatus(businessObjectDataStorageUnitKey, request);

        // Verify the external calls.
        verify(storageUnitHelper).validateBusinessObjectDataStorageUnitKey(businessObjectDataStorageUnitKey);
        verify(storageUnitDaoHelper).getStorageUnitEntityByKey(businessObjectDataStorageUnitKey);
        verify(storageUnitStatusDaoHelper).getStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2);
        verify(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, storageUnitStatusEntity, STORAGE_UNIT_STATUS_2);
        verify(businessObjectDataHelper).createBusinessObjectDataKeyFromEntity(storageUnitEntity.getBusinessObjectData());
        verify(storageUnitHelper).createBusinessObjectDataStorageUnitKey(businessObjectDataKey, STORAGE_NAME);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(expectedResponse, result);
    }

    @Test
    public void testUpdateBusinessObjectDataStorageUnitStatusImplMissingStorageUnitStatus()
    {
        // Create a business object data storage unit key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Try to call the method under test when update request has a blank business object data storage unit status.
        try
        {
            businessObjectDataStorageUnitStatusServiceImpl.updateBusinessObjectDataStorageUnitStatusImpl(businessObjectDataStorageUnitKey,
                new BusinessObjectDataStorageUnitStatusUpdateRequest(BLANK_TEXT));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data storage unit status must be specified.", e.getMessage());
        }

        // Verify the external calls.
        verify(storageUnitHelper).validateBusinessObjectDataStorageUnitKey(businessObjectDataStorageUnitKey);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataHelper, storageUnitDaoHelper, storageUnitHelper, storageUnitStatusDaoHelper);
    }
}

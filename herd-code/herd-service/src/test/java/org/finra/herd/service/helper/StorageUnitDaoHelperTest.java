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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.MessageNotificationEventService;

/**
 * This class tests functionality within the StorageUnitDaoHelper class.
 */
public class StorageUnitDaoHelperTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private MessageNotificationEventService messageNotificationEventService;

    @Mock
    private StorageUnitDao storageUnitDao;

    @InjectMocks
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Mock
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetStorageUnitEntityByBusinessObjectDataAndStorageName()
    {
        // Create and persist test database entities.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Get the business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = storageUnitEntity.getBusinessObjectData();

        // Create a storage unit key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Mock the external calls.
        when(storageUnitDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME)).thenReturn(storageUnitEntity);

        // Call the method under test.
        StorageUnitEntity result = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);

        // Verify the external calls.
        verify(storageUnitDao).getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(storageUnitEntity, result);
    }

    @Test
    public void testGetStorageUnitEntityByBusinessObjectDataAndStorageNameStorageUnitNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist test database entities.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Get the business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = storageUnitEntity.getBusinessObjectData();

        // Mock the external calls.
        when(storageUnitDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME)).thenReturn(null);
        when(businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY_AS_STRING);

        // Try to call the method under test.
        try
        {
            storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String
                    .format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", STORAGE_NAME, BUSINESS_OBJECT_DATA_KEY_AS_STRING),
                e.getMessage());
        }

        // Verify the external calls.
        verify(storageUnitDao).getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME);
        verify(businessObjectDataHelper).businessObjectDataEntityAltKeyToString(businessObjectDataEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetStorageUnitEntityByKey()
    {
        // Create a storage unit key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Create and persist a storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Mock the external calls.
        when(storageUnitDao.getStorageUnitByKey(businessObjectDataStorageUnitKey)).thenReturn(storageUnitEntity);

        // Call the method under test.
        StorageUnitEntity result = storageUnitDaoHelper.getStorageUnitEntityByKey(businessObjectDataStorageUnitKey);

        // Verify the external calls.
        verify(storageUnitDao).getStorageUnitByKey(businessObjectDataStorageUnitKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(storageUnitEntity, result);
    }

    @Test
    public void testGetStorageUnitEntityByKeyStorageUnitNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage unit key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Mock the external calls.
        when(storageUnitDao.getStorageUnitByKey(businessObjectDataStorageUnitKey)).thenReturn(null);

        // Try to call the method under test.
        try
        {
            storageUnitDaoHelper.getStorageUnitEntityByKey(businessObjectDataStorageUnitKey);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data storage unit {%s, storageName: \"%s\"} doesn't exist.",
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey), STORAGE_NAME), e.getMessage());
        }

        // Verify the external calls.
        verify(storageUnitDao).getStorageUnitByKey(businessObjectDataStorageUnitKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testSetStorageUnitStatus()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a storage status entity.
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2);

        // Mock the external calls.
        when(businessObjectDataHelper.getBusinessObjectDataKey(storageUnitEntity.getBusinessObjectData())).thenReturn(businessObjectDataKey);

        // Call the method under test.
        storageUnitDaoHelper.setStorageUnitStatus(storageUnitEntity, storageUnitStatusEntity);

        // Verify the external calls.
        verify(businessObjectDataHelper).getBusinessObjectDataKey(storageUnitEntity.getBusinessObjectData());
        verify(messageNotificationEventService)
            .processStorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS_2, NO_STORAGE_UNIT_STATUS);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(STORAGE_UNIT_STATUS_2, storageUnitEntity.getStatus().getCode());
    }

    @Test
    public void testUpdateStorageUnitStatusNewStatusPassedAsEntity()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a storage status entity.
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2);

        // Mock the external calls.
        when(businessObjectDataHelper.getBusinessObjectDataKey(storageUnitEntity.getBusinessObjectData())).thenReturn(businessObjectDataKey);

        // Call the method under test.
        storageUnitDaoHelper.updateStorageUnitStatus(storageUnitEntity, storageUnitStatusEntity, STORAGE_UNIT_STATUS_2);

        // Verify the external calls.
        verify(storageUnitDao).saveAndRefresh(storageUnitEntity);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(storageUnitEntity.getBusinessObjectData());
        verify(messageNotificationEventService)
            .processStorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(STORAGE_UNIT_STATUS_2, storageUnitEntity.getStatus().getCode());
    }

    @Test
    public void testUpdateStorageUnitStatusNewStatusPassedAsString()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a storage status entity.
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2);

        // Mock the external calls.
        when(storageUnitStatusDaoHelper.getStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2)).thenReturn(storageUnitStatusEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(storageUnitEntity.getBusinessObjectData())).thenReturn(businessObjectDataKey);

        // Call the method under test.
        storageUnitDaoHelper.updateStorageUnitStatus(storageUnitEntity, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS_2);

        // Verify the external calls.
        verify(storageUnitStatusDaoHelper).getStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2);
        verify(storageUnitDao).saveAndRefresh(storageUnitEntity);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(storageUnitEntity.getBusinessObjectData());
        verify(messageNotificationEventService)
            .processStorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(STORAGE_UNIT_STATUS_2, storageUnitEntity.getStatus().getCode());
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataHelper, messageNotificationEventService, storageUnitDao, storageUnitStatusDaoHelper);
    }
}

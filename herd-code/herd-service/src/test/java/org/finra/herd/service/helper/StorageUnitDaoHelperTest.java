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

import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAME;
import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAMESPACE;
import static org.finra.herd.dao.AbstractDaoTest.DATA_VERSION;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_VERSION;
import static org.finra.herd.dao.AbstractDaoTest.NO_STORAGE_UNIT_STATUS;
import static org.finra.herd.dao.AbstractDaoTest.PARTITION_VALUE;
import static org.finra.herd.dao.AbstractDaoTest.STORAGE_NAME;
import static org.finra.herd.dao.AbstractDaoTest.STORAGE_UNIT_STATUS;
import static org.finra.herd.dao.AbstractDaoTest.STORAGE_UNIT_STATUS_2;
import static org.finra.herd.dao.AbstractDaoTest.SUBPARTITION_VALUES;
import static org.finra.herd.service.AbstractServiceTest.BUSINESS_OBJECT_DATA_KEY_AS_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

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
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.MessageNotificationEventService;

public class StorageUnitDaoHelperTest
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
    public void testGetStorageUnitEntityByBusinessObjectDataAndStorage()
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

        // Create a storage entity.
        StorageEntity storageEntity = new StorageEntity();

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();

        // Mock the external calls.
        when(storageUnitDao.getStorageUnitByBusinessObjectDataAndStorage(businessObjectDataEntity, storageEntity)).thenReturn(storageUnitEntity);

        // Call the method under test.
        StorageUnitEntity result = storageUnitDaoHelper.getStorageUnitEntityByBusinessObjectDataAndStorage(businessObjectDataEntity, storageEntity);

        // Validate the results.
        assertEquals(storageUnitEntity, result);

        // Verify the external calls.
        verify(storageUnitDao).getStorageUnitByBusinessObjectDataAndStorage(businessObjectDataEntity, storageEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetStorageUnitEntityByBusinessObjectDataAndStorageName()
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();

        // Mock the external calls.
        when(storageUnitDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME)).thenReturn(storageUnitEntity);

        // Call the method under test.
        StorageUnitEntity result = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);

        // Validate the results.
        assertEquals(storageUnitEntity, result);

        // Verify the external calls.
        verify(storageUnitDao).getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetStorageUnitEntityByBusinessObjectDataAndStorageNameStorageUnitNoExists()
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

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
    public void testGetStorageUnitEntityByBusinessObjectDataAndStorageStorageUnitNoExists()
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

        // Create a storage entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        // Mock the external calls.
        when(storageUnitDao.getStorageUnitByBusinessObjectDataAndStorage(businessObjectDataEntity, storageEntity)).thenReturn(null);
        when(businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY_AS_STRING);

        // Try to call the method under test.
        try
        {
            storageUnitDaoHelper.getStorageUnitEntityByBusinessObjectDataAndStorage(businessObjectDataEntity, storageEntity);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String
                    .format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", STORAGE_NAME, BUSINESS_OBJECT_DATA_KEY_AS_STRING),
                e.getMessage());
        }

        // Verify the external calls.
        verify(storageUnitDao).getStorageUnitByBusinessObjectDataAndStorage(businessObjectDataEntity, storageEntity);
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

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();

        // Mock the external calls.
        when(storageUnitDao.getStorageUnitByKey(businessObjectDataStorageUnitKey)).thenReturn(storageUnitEntity);

        // Call the method under test.
        StorageUnitEntity result = storageUnitDaoHelper.getStorageUnitEntityByKey(businessObjectDataStorageUnitKey);

        // Validate the results.
        assertEquals(storageUnitEntity, result);

        // Verify the external calls.
        verify(storageUnitDao).getStorageUnitByKey(businessObjectDataStorageUnitKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetStorageUnitEntityByKeyStorageUnitNoExists()
    {
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
            assertEquals(String.format(
                "Business object data storage unit {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", " +
                    "businessObjectDataSubPartitionValues: \"%s,%s,%s,%s\", businessObjectDataVersion: %d, storageName: \"%s\"} doesn't exist.", BDEF_NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1),
                SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION, STORAGE_NAME), e.getMessage());
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

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

        // Create a storage entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setStorage(storageEntity);

        // Create a storage status entity.
        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(STORAGE_UNIT_STATUS);

        // Mock the external calls.
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(businessObjectDataKey);

        // Call the method under test.
        storageUnitDaoHelper.setStorageUnitStatus(storageUnitEntity, storageUnitStatusEntity);

        // Validate the results.
        assertEquals(STORAGE_UNIT_STATUS, storageUnitEntity.getStatus().getCode());

        // Verify the external calls.
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(messageNotificationEventService)
            .processStorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateStorageUnitStatusNewStatusPassedAsEntity()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

        // Create a storage entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        // Create a storage status entity.
        StorageUnitStatusEntity oldStorageUnitStatusEntity = new StorageUnitStatusEntity();
        oldStorageUnitStatusEntity.setCode(STORAGE_UNIT_STATUS);

        // Create a storage status entity.
        StorageUnitStatusEntity newStorageUnitStatusEntity = new StorageUnitStatusEntity();
        newStorageUnitStatusEntity.setCode(STORAGE_UNIT_STATUS_2);

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setStatus(oldStorageUnitStatusEntity);
        storageUnitEntity.setHistoricalStatuses(new ArrayList<>());

        // Mock the external calls.
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(businessObjectDataKey);

        // Call the method under test.
        storageUnitDaoHelper.updateStorageUnitStatus(storageUnitEntity, newStorageUnitStatusEntity, STORAGE_UNIT_STATUS_2);

        // Validate the results.
        assertEquals(STORAGE_UNIT_STATUS_2, storageUnitEntity.getStatus().getCode());

        // Verify the external calls.
        verify(storageUnitDao).saveAndRefresh(storageUnitEntity);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(messageNotificationEventService)
            .processStorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateStorageUnitStatusNewStatusPassedAsString()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

        // Create a storage entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        // Create a storage status entity.
        StorageUnitStatusEntity oldStorageUnitStatusEntity = new StorageUnitStatusEntity();
        oldStorageUnitStatusEntity.setCode(STORAGE_UNIT_STATUS);

        // Create a storage status entity.
        StorageUnitStatusEntity newStorageUnitStatusEntity = new StorageUnitStatusEntity();
        newStorageUnitStatusEntity.setCode(STORAGE_UNIT_STATUS_2);

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setStatus(oldStorageUnitStatusEntity);
        storageUnitEntity.setHistoricalStatuses(new ArrayList<>());

        // Mock the external calls.
        when(storageUnitStatusDaoHelper.getStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2)).thenReturn(newStorageUnitStatusEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(businessObjectDataKey);

        // Call the method under test.
        storageUnitDaoHelper.updateStorageUnitStatus(storageUnitEntity, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS_2);

        // Validate the results.
        assertEquals(STORAGE_UNIT_STATUS_2, storageUnitEntity.getStatus().getCode());

        // Verify the external calls.
        verify(storageUnitStatusDaoHelper).getStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2);
        verify(storageUnitDao).saveAndRefresh(storageUnitEntity);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(messageNotificationEventService)
            .processStorageUnitStatusChangeNotificationEvent(businessObjectDataKey, STORAGE_NAME, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataHelper, messageNotificationEventService, storageUnitDao, storageUnitStatusDaoHelper);
    }
}
